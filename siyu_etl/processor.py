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
from siyu_etl.batch_service import BatchService
from siyu_etl.circuit_breaker import CircuitBreaker
from siyu_etl.config import AppConfig
from siyu_etl.db import (
    FILE_STATUS_PARSE_FAILED,
    FILE_STATUS_PARSING,
    FILE_STATUS_READY_TO_UPLOAD,
    FILE_STATUS_STOPPED,
    FILE_STATUS_UPLOAD_FAILED,
    FILE_STATUS_UPLOAD_SUCCESS,
    FILE_STATUS_UPLOADING,
    SESSION_STATUS_COMPLETED,
    SESSION_STATUS_FAILED,
    SESSION_STATUS_PARSED,
    SESSION_STATUS_PARSING,
    SESSION_STATUS_PARTIAL_FAILED,
    SESSION_STATUS_STOPPED,
    SESSION_STATUS_UPLOADING,
    backfill_pending_store_ids,
    backfill_pending_store_ids_for_session,
    init_db,
    insert_task,
    requeue_skipped_member_trade_with_store_id,
    requeue_skipped_member_trade_with_store_id_for_session,
)
from siyu_etl.excel_detect import detect_sheet
from siyu_etl.excel_read import read_rows
from siyu_etl.fingerprint import identify_row
from siyu_etl.scheduler import fetch_pending_tasks, iter_batches
from siyu_etl.uploader import CircuitOpenError, NoResponseStopError, send_batch, webhook_for_file_type


@dataclass(frozen=True)
class ParseStats:
    parsed_rows: int
    inserted_rows: int
    duplicate_rows: int
    skipped_rows: int
    session_id: str = ""


@dataclass(frozen=True)
class RunStats:
    parsed_rows: int
    inserted_rows: int
    duplicate_rows: int
    skipped_rows: int
    sent_batches: int
    session_id: str = ""


@dataclass(frozen=True)
class PushStats:
    mode: str
    pending_rows: int
    total_batches: int
    attempted_batches: int
    success_batches: int
    skipped_batches: int
    stopped_reason: str
    last_errors: list[str]
    session_id: str = ""


def _ensure_session(batch_service: BatchService, file_paths: list[Path], session_id: str | None) -> str:
    if session_id:
        return session_id
    return batch_service.create_session_with_files(file_paths)


def _parse_files(
    *,
    cfg: AppConfig,
    db_path: Path,
    file_paths: list[Path],
    log: Callable[[str], None],
    progress: Callable[[int, int, str], None],
    stop_flag: Callable[[], bool],
    session_id: str | None = None,
) -> ParseStats:
    init_db(db_path)
    batch_service = BatchService(db_path)
    active_session_id = _ensure_session(batch_service, file_paths, session_id)
    batch_service.update_session_status(
        session_id=active_session_id,
        status=SESSION_STATUS_PARSING,
        started=True,
    )

    files_by_path = {Path(f.file_path): f for f in batch_service.list_files(active_session_id)}
    parsed = inserted = dup = skipped = 0
    insert_seq = 0
    total_files = len(file_paths)
    session_had_failure = False

    for idx, fp in enumerate(file_paths, start=1):
        if stop_flag():
            log("已停止：退出处理")
            break

        file_record = files_by_path.get(Path(fp))
        if file_record is None:
            file_id = batch_service.add_file(
                session_id=active_session_id,
                file_path=str(fp),
                file_name=fp.name,
                file_size=int(fp.stat().st_size) if fp.exists() else 0,
                file_mtime=str(int(fp.stat().st_mtime)) if fp.exists() else "",
            )
            file_record = batch_service.get_file(file_id)
            files_by_path[Path(fp)] = file_record
        assert file_record is not None

        batch_service.update_file_status(
            file_id=file_record.file_id,
            status=FILE_STATUS_PARSING,
            current_stage="parsing",
            parse_error="",
            upload_error="",
            parse_rows=0,
            uploaded_rows=0,
        )

        try:
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
                fp,
                header_row_0based=det.header_row_0based,
                headers=det.headers,
                file_type=det.file_type,
            ):
                if stop_flag():
                    log("已停止：退出读取")
                    break
                file_row_count += 1
                parsed += 1

                ident = identify_row(
                    file_type=det.file_type,
                    row=rr.data,
                    timestamp_column=det.timestamp_column,
                )
                if not ident.store_name or not ident.fingerprint:
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
                    session_id=active_session_id,
                    file_id=file_record.file_id,
                    source_file_name=fp.name,
                    source_file_path=str(fp),
                )
                if res.inserted:
                    inserted += 1
                    file_inserted += 1
                else:
                    dup += 1
                    file_dup += 1

                now = time.time()
                if now - last_ui > 0.3:
                    batch_service.update_file_status(
                        file_id=file_record.file_id,
                        status=FILE_STATUS_PARSING,
                        file_type=det.file_type,
                        parse_rows=file_inserted,
                        current_stage="parsing",
                    )
                    progress(idx, max(total_files, 1), f"解析 {fp.name} 行={file_row_count}")
                    last_ui = now

            was_stopped = stop_flag()
            final_status = FILE_STATUS_STOPPED if was_stopped else FILE_STATUS_READY_TO_UPLOAD
            batch_service.update_file_status(
                file_id=file_record.file_id,
                status=final_status,
                file_type=det.file_type,
                parse_rows=file_inserted,
                current_stage="stopped" if was_stopped else "parsed",
            )

            log(
                f"文件解析完成: {fp.name} 解析行数={file_row_count} "
                f"插入={file_inserted} 重复={file_dup} 跳过={file_skipped}"
            )

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

        except Exception as e:
            session_had_failure = True
            batch_service.update_file_status(
                file_id=file_record.file_id,
                status=FILE_STATUS_PARSE_FAILED,
                parse_error=str(e),
                current_stage="parse_failed",
            )
            log(f"文件解析失败: {fp.name} error={e}")
            continue

    summary = batch_service.summary(active_session_id)
    if stop_flag():
        batch_service.update_session_status(
            session_id=active_session_id,
            status=SESSION_STATUS_STOPPED if not session_had_failure else SESSION_STATUS_PARTIAL_FAILED,
            last_error="用户停止解析",
        )
    elif summary and summary.failed_files > 0:
        batch_service.update_session_status(
            session_id=active_session_id,
            status=SESSION_STATUS_PARTIAL_FAILED,
            last_error="部分文件解析失败",
        )
    elif summary and summary.total_files > 0:
        batch_service.update_session_status(
            session_id=active_session_id,
            status=SESSION_STATUS_PARSED,
        )
    else:
        batch_service.update_session_status(
            session_id=active_session_id,
            status=SESSION_STATUS_FAILED,
            last_error="没有可用文件",
        )

    return ParseStats(
        parsed_rows=parsed,
        inserted_rows=inserted,
        duplicate_rows=dup,
        skipped_rows=skipped,
        session_id=active_session_id,
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
    parse_stats = _parse_files(
        cfg=cfg,
        db_path=db_path,
        file_paths=file_paths,
        log=log,
        progress=progress,
        stop_flag=stop_flag,
    )

    push_stats = push_only(
        cfg=cfg,
        db_path=db_path,
        breaker=breaker,
        log=log,
        progress=progress,
        stop_flag=stop_flag,
        session_id=parse_stats.session_id,
    )

    log("流程完成")
    progress(1, 1, "完成")
    return RunStats(
        parsed_rows=parse_stats.parsed_rows,
        inserted_rows=parse_stats.inserted_rows,
        duplicate_rows=parse_stats.duplicate_rows,
        skipped_rows=parse_stats.skipped_rows,
        sent_batches=push_stats.success_batches,
        session_id=parse_stats.session_id,
    )


def parse_only(
    *,
    cfg: AppConfig,
    db_path: Path,
    file_paths: list[Path],
    log: Callable[[str], None],
    progress: Callable[[int, int, str], None],
    stop_flag: Callable[[], bool],
    session_id: str | None = None,
) -> RunStats:
    parse_stats = _parse_files(
        cfg=cfg,
        db_path=db_path,
        file_paths=file_paths,
        log=log,
        progress=progress,
        stop_flag=stop_flag,
        session_id=session_id,
    )

    progress(1, 1, "解析完成")
    return RunStats(
        parsed_rows=parse_stats.parsed_rows,
        inserted_rows=parse_stats.inserted_rows,
        duplicate_rows=parse_stats.duplicate_rows,
        skipped_rows=parse_stats.skipped_rows,
        sent_batches=0,
        session_id=parse_stats.session_id,
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
    session_id: str | None = None,
) -> PushStats:
    init_db(db_path)
    batch_service = BatchService(db_path)

    if session_id:
        backfill_pending_store_ids_for_session(db_path, session_id=session_id)
        requeue_skipped_member_trade_with_store_id_for_session(db_path, session_id=session_id)
    else:
        backfill_pending_store_ids(db_path)
        requeue_skipped_member_trade_with_store_id(db_path)

    if session_id:
        batch_service.update_session_status(
            session_id=session_id,
            status=SESSION_STATUS_UPLOADING,
            started=True,
        )

    tasks = fetch_pending_tasks(db_path, file_type_filter=file_type_filter, session_id=session_id)
    pending_rows = len(tasks)
    batches = list(iter_batches(tasks, batch_size=cfg.batch_size))
    total_batches = len(batches)

    mode = "real"
    last_errors: list[str] = []
    attempted = 0
    success_batches = 0
    skipped_batches = 0
    stopped_reason = ""

    if file_type_filter:
        log(f"[过滤] 仅推送文件类型: {file_type_filter}")
    if session_id:
        log(f"[本次上传] session_id={session_id}")

    log(
        f"推送开始：待推送行数={pending_rows}，batches={total_batches}"
        + (f"（仅推送: {file_type_filter}）" if file_type_filter else "")
    )
    for i, b in enumerate(batches, start=1):
        if stop_flag():
            stopped_reason = "用户停止"
            break

        attempted += 1
        if b.file_id:
            batch_service.update_file_status(
                file_id=b.file_id,
                status=FILE_STATUS_UPLOADING,
                current_stage="uploading",
                upload_error="",
            )
        try:
            res = send_batch(cfg=cfg, db_path=db_path, breaker=breaker, batch=b, logger=log)
            if res.success:
                success_batches += 1
                if b.file_id:
                    uploaded_rows = batch_service.count_file_tasks(file_id=b.file_id, status="SUCCESS")
                    batch_service.update_file_status(
                        file_id=b.file_id,
                        status=FILE_STATUS_UPLOAD_SUCCESS,
                        uploaded_rows=uploaded_rows,
                        current_stage="uploaded",
                    )
            else:
                skipped_batches += 1
                if res.error:
                    last_errors.append(res.error)
                if b.file_id:
                    batch_service.update_file_status(
                        file_id=b.file_id,
                        status=FILE_STATUS_UPLOAD_FAILED,
                        upload_error=res.error,
                        current_stage="upload_failed",
                    )
        except NoResponseStopError as e:
            stopped_reason = str(e)
            last_errors.append(stopped_reason)
            if b.file_id:
                batch_service.update_file_status(
                    file_id=b.file_id,
                    status=FILE_STATUS_UPLOAD_FAILED,
                    upload_error=stopped_reason,
                    current_stage="upload_failed",
                )
            break
        except CircuitOpenError as e:
            stopped_reason = str(e)
            last_errors.append(stopped_reason)
            if b.file_id:
                batch_service.update_file_status(
                    file_id=b.file_id,
                    status=FILE_STATUS_UPLOAD_FAILED,
                    upload_error=stopped_reason,
                    current_stage="upload_failed",
                )
            break
        except Exception as e:
            stopped_reason = f"未知异常，已停止后续推送: {e}"
            last_errors.append(stopped_reason)
            if b.file_id:
                batch_service.update_file_status(
                    file_id=b.file_id,
                    status=FILE_STATUS_UPLOAD_FAILED,
                    upload_error=stopped_reason,
                    current_stage="upload_failed",
                )
            break
        finally:
            progress(i, max(total_batches, 1), f"推送 batch {i}/{total_batches}")

    if not stopped_reason:
        stopped_reason = "推送完成"

    if session_id:
        summary = batch_service.summary(session_id)
        if stopped_reason == "推送完成" and summary and summary.failed_files == 0:
            batch_service.update_session_status(
                session_id=session_id,
                status=SESSION_STATUS_COMPLETED,
                finished=True,
            )
        elif summary and summary.uploaded_files > 0:
            batch_service.update_session_status(
                session_id=session_id,
                status=SESSION_STATUS_PARTIAL_FAILED,
                last_error=stopped_reason,
                finished=True,
            )
        else:
            batch_service.update_session_status(
                session_id=session_id,
                status=SESSION_STATUS_FAILED,
                last_error=stopped_reason,
                finished=True,
            )

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
        session_id=session_id or "",
    )
