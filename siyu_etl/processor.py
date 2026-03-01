"""
数据处理流程模块

该模块负责：
1. 解析 Excel 文件并插入任务到数据库
2. 从数据库获取待推送任务并批量推送到 webhook
3. 提供解析、推送等不同模式的流程控制
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from siyu_etl.archive import archive_file
from siyu_etl.circuit_breaker import CircuitBreaker
from siyu_etl.config import AppConfig
from siyu_etl.db import backfill_pending_store_ids, init_db, insert_task, requeue_skipped_member_trade_with_store_id
from siyu_etl.excel_detect import detect_sheet
from siyu_etl.excel_read import read_rows
from siyu_etl.fingerprint import identify_row
from siyu_etl.scheduler import fetch_pending_tasks, iter_batches
from siyu_etl.uploader import CircuitOpenError, NoResponseStopError, send_batch, webhook_for_file_type


@dataclass(frozen=True)
class ParseStats:
    """
    解析统计信息
    
    Attributes:
        parsed_rows: 解析的行数
        inserted_rows: 插入的行数
        duplicate_rows: 重复的行数
        skipped_rows: 跳过的行数
    """
    parsed_rows: int
    inserted_rows: int
    duplicate_rows: int
    skipped_rows: int


@dataclass(frozen=True)
class RunStats:
    """
    运行统计信息
    
    Attributes:
        parsed_rows: 解析的行数
        inserted_rows: 插入的行数
        duplicate_rows: 重复的行数
        skipped_rows: 跳过的行数
        sent_batches: 发送的批次数量
    """
    parsed_rows: int
    inserted_rows: int
    duplicate_rows: int
    skipped_rows: int
    sent_batches: int


@dataclass(frozen=True)
class PushStats:
    """
    推送统计信息
    
    Attributes:
        mode: 推送模式（"preview" 或 "real"）
        pending_rows: 待推送行数
        total_batches: 总批次数
        attempted_batches: 尝试推送的批次数
        success_batches: 成功推送的批次数
        skipped_batches: 跳过的批次数
        stopped_reason: 停止原因
        last_errors: 最近的错误列表（最多20条）
    """
    mode: str  # "preview" | "real"
    pending_rows: int
    total_batches: int
    attempted_batches: int
    success_batches: int
    skipped_batches: int
    stopped_reason: str
    last_errors: list[str]


def _parse_files(
    *,
    cfg: AppConfig,
    db_path: Path,
    file_paths: list[Path],
    log: Callable[[str], None],
    progress: Callable[[int, int, str], None],
    stop_flag: Callable[[], bool],
) -> ParseStats:
    """
    解析文件并插入到数据库的公共函数
    
    Args:
        cfg: 应用配置
        db_path: 数据库路径
        file_paths: 要处理的文件路径列表
        log: 日志回调函数
        progress: 进度回调函数
        stop_flag: 停止标志检查函数
        
    Returns:
        ParseStats 对象，包含解析统计信息
    """
    init_db(db_path)
    parsed = inserted = dup = skipped = 0
    # 全量上传：每行使用唯一 fingerprint，不判重，全部插入
    insert_seq = 0

    total_files = len(file_paths)
    for idx, fp in enumerate(file_paths, start=1):
        if stop_flag():
            log("已停止：退出处理")
            break

        log(f"识别文件: {fp.name}")
        det = detect_sheet(fp)
        log(f"识别结果: {det.file_type} header_row={det.header_row_0based}")
        url = webhook_for_file_type(cfg, det.file_type)

        last_ui = 0.0
        file_row_count = 0
        file_inserted = 0
        file_dup = 0
        file_skipped = 0
        for rr in read_rows(
            fp, header_row_0based=det.header_row_0based, headers=det.headers, file_type=det.file_type
        ):
            if stop_flag():
                log("已停止：退出读取")
                break
            file_row_count += 1
            parsed += 1

            ident = identify_row(
                file_type=det.file_type, row=rr.data, timestamp_column=det.timestamp_column
            )
            if not ident.store_name:
                skipped += 1
                file_skipped += 1
                continue
            if not ident.fingerprint:
                skipped += 1
                file_skipped += 1
                continue

            insert_seq += 1
            fingerprint_unique = f"{ident.fingerprint}#{insert_seq}"
            res = insert_task(
                db_path,
                fingerprint=fingerprint_unique,
                file_type=det.file_type,
                store_id=ident.store_id,
                store_name=ident.store_name,
                timestamp=ident.timestamp,
                raw_data=rr.data,
                webhook_url=url,
            )
            if res.inserted:
                inserted += 1
                file_inserted += 1
            else:
                dup += 1
                file_dup += 1

            now = time.time()
            if now - last_ui > 0.3:
                progress(idx, max(total_files, 1), f"解析 {fp.name} 行={file_row_count}")
                last_ui = now

        log(
            f"文件解析完成: {fp.name} 解析行数={file_row_count} "
            f"插入={file_inserted} 重复={file_dup} 跳过={file_skipped}"
        )

        # archive after parse (skip if already in processed/)
        try:
            if "processed" in fp.parts:
                log("已在 processed 目录，跳过归档")
            elif cfg.archive_to_processed_dir:
                dst = archive_file(
                    fp,
                    to_processed_dir=cfg.archive_to_processed_dir,
                    suffix=cfg.archive_suffix,
                )
                log(f"已归档: {dst}")
        except Exception as e:
            log(f"归档失败（不影响流程）: {e}")

    return ParseStats(
        parsed_rows=parsed,
        inserted_rows=inserted,
        duplicate_rows=dup,
        skipped_rows=skipped,
    )


def run_pipeline(
    *,
    cfg: AppConfig,
    db_path: Path,
    file_paths: list[Path],
    breaker: CircuitBreaker,
    log: Callable[[str], None],
    progress: Callable[[int, int, str], None],
    stop_flag: Callable[[], bool],
) -> RunStats:
    """
    端到端处理流程
    
    流程：
    1. 解析选定的文件 -> 插入任务到 SQLite
    2. 获取待推送任务 -> 分组/批处理 -> 推送到 webhook
    
    Args:
        cfg: 应用配置
        db_path: 数据库路径
        file_paths: 要处理的文件路径列表
        breaker: 熔断器实例
        log: 日志回调函数
        progress: 进度回调函数
        stop_flag: 停止标志检查函数
        
    Returns:
        RunStats 对象，包含运行统计信息
    """
    # Parse files
    parse_stats = _parse_files(
        cfg=cfg,
        db_path=db_path,
        file_paths=file_paths,
        log=log,
        progress=progress,
        stop_flag=stop_flag,
    )

    # push pending
    tasks = fetch_pending_tasks(db_path)
    log(f"待推送任务数(PENDING): {len(tasks)}")
    batches = list(iter_batches(tasks, batch_size=cfg.batch_size))
    total_batches = len(batches)

    # Initialize sent_batches to avoid UnboundLocalError if batches is empty
    sent_batches = 0

    # Preview fastest path: do not iterate thousands of batches; only preview.
    if cfg.dry_run:
        log(f"[预演] 预计 batches={total_batches}（每包{cfg.batch_size}条）")
        for i, b in enumerate(batches[:3], start=1):
            try:
                send_batch(cfg=cfg, db_path=db_path, breaker=breaker, batch=b, logger=log)
            except Exception as e:
                log(f"[预演] 预览失败: {e}")
                break
        sent_batches = 0
    else:
        for i, b in enumerate(batches, start=1):
            if stop_flag():
                log("已停止：退出推送")
                break
            try:
                send_batch(cfg=cfg, db_path=db_path, breaker=breaker, batch=b, logger=log)
                sent_batches += 1
            except CircuitOpenError as e:
                log(str(e))
                # stop pushing further batches for now; user can reset
                break
            finally:
                progress(i, max(total_batches, 1), f"推送 batch {i}/{total_batches}")

    log("流程完成")
    progress(1, 1, "完成")
    return RunStats(
        parsed_rows=parse_stats.parsed_rows,
        inserted_rows=parse_stats.inserted_rows,
        duplicate_rows=parse_stats.duplicate_rows,
        skipped_rows=parse_stats.skipped_rows,
        sent_batches=sent_batches,
    )


def parse_only(
    *,
    cfg: AppConfig,
    db_path: Path,
    file_paths: list[Path],
    log: Callable[[str], None],
    progress: Callable[[int, int, str], None],
    stop_flag: Callable[[], bool],
) -> RunStats:
    """
    仅解析模式：解析/清洗/去重/入库，不推送
    
    该函数只负责将 Excel 文件解析并插入到数据库，不会执行推送操作。
    推送操作需要单独调用 push_only 函数。
    
    Args:
        cfg: 应用配置
        db_path: 数据库路径
        file_paths: 要处理的文件路径列表
        log: 日志回调函数
        progress: 进度回调函数
        stop_flag: 停止标志检查函数
        
    Returns:
        RunStats 对象，包含运行统计信息
    """
    parse_stats = _parse_files(
        cfg=cfg,
        db_path=db_path,
        file_paths=file_paths,
        log=log,
        progress=progress,
        stop_flag=stop_flag,
    )

    progress(1, 1, "解析完成")
    return RunStats(
        parsed_rows=parse_stats.parsed_rows,
        inserted_rows=parse_stats.inserted_rows,
        duplicate_rows=parse_stats.duplicate_rows,
        skipped_rows=parse_stats.skipped_rows,
        sent_batches=0,
    )


def push_only(
    *,
    cfg: AppConfig,
    db_path: Path,
    breaker: CircuitBreaker,
    log: Callable[[str], None],
    progress: Callable[[int, int, str], None],
    stop_flag: Callable[[], bool],
    file_type_filter: Optional[str] = None,
) -> PushStats:
    """
    仅推送模式：从 SQLite 发送 PENDING 状态的任务
    
    严格规则：如果某个批次没有获得响应，停止并不再发送下一个批次。
    
    该函数会：
    1. 回填待推送任务的 store_id（升级兼容性）
    2. 重新排队之前跳过的会员交易明细行（如果它们有 store_id）
    3. 获取待推送任务并批量推送
    
    Args:
        cfg: 应用配置
        db_path: 数据库路径
        breaker: 熔断器实例
        log: 日志回调函数
        progress: 进度回调函数
        stop_flag: 停止标志检查函数
        file_type_filter: 可选的文件类型过滤，只推送指定类型的任务
        
    Returns:
        PushStats 对象，包含推送统计信息
    """
    init_db(db_path)

    # Backfill store_id for old pending rows (upgrade safety)
    backfill_pending_store_ids(db_path)
    # Requeue previously skipped member_trade rows that actually have store_id in raw_data
    requeue_skipped_member_trade_with_store_id(db_path)

    tasks = fetch_pending_tasks(db_path, file_type_filter=file_type_filter)
    pending_rows = len(tasks)
    batches = list(iter_batches(tasks, batch_size=cfg.batch_size))
    total_batches = len(batches)

    mode = "preview" if cfg.dry_run else "real"
    last_errors: list[str] = []

    attempted = 0
    success_batches = 0
    skipped_batches = 0
    stopped_reason = ""

    if file_type_filter:
        log(f"[过滤] 仅推送文件类型: {file_type_filter}")
    
    if cfg.dry_run:
        log(f"[预演] 待推送行数={pending_rows}，预计 batches={total_batches}（每包{cfg.batch_size}条）")
        for i, b in enumerate(batches[:3], start=1):
            attempted += 1
            try:
                send_batch(cfg=cfg, db_path=db_path, breaker=breaker, batch=b, logger=log)
            except Exception as e:
                stopped_reason = f"预演失败: {e}"
                last_errors.append(str(e))
                break
            finally:
                progress(i, max(min(total_batches, 3), 1), f"预览 batch {i}/{min(total_batches, 3)}")
        if not stopped_reason:
            stopped_reason = "预演完成（仅展示前3包）"
        progress(1, 1, "预览完成")
        return PushStats(
            mode=mode,
            pending_rows=pending_rows,
            total_batches=total_batches,
            attempted_batches=attempted,
            success_batches=0,
            skipped_batches=0,
            stopped_reason=stopped_reason,
            last_errors=last_errors,
        )

    log(f"真实推送开始：待推送行数={pending_rows}，batches={total_batches}" + (f"（仅推送: {file_type_filter}）" if file_type_filter else ""))
    for i, b in enumerate(batches, start=1):
        if stop_flag():
            stopped_reason = "用户停止"
            break

        attempted += 1
        try:
            res = send_batch(cfg=cfg, db_path=db_path, breaker=breaker, batch=b, logger=log)
            if res.success:
                success_batches += 1
            else:
                skipped_batches += 1
                if res.error:
                    last_errors.append(res.error)
        except NoResponseStopError as e:
            stopped_reason = str(e)
            last_errors.append(stopped_reason)
            break
        except CircuitOpenError as e:
            stopped_reason = str(e)
            last_errors.append(stopped_reason)
            break
        except Exception as e:
            # Unknown exception: stop to be safe
            stopped_reason = f"未知异常，已停止后续推送: {e}"
            last_errors.append(stopped_reason)
            break
        finally:
            progress(i, max(total_batches, 1), f"推送 batch {i}/{total_batches}")

    if not stopped_reason:
        stopped_reason = "推送完成"
    progress(1, 1, "推送结束")
    return PushStats(
        mode=mode,
        pending_rows=pending_rows,
        total_batches=total_batches,
        attempted_batches=attempted,
        success_batches=success_batches,
        skipped_batches=skipped_batches,
        stopped_reason=stopped_reason,
        last_errors=last_errors[-20:],
    )


