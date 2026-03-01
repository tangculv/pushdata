"""
数据上传模块

该模块负责：
1. 将数据批次推送到 webhook
2. 处理重试逻辑
3. 更新任务状态（成功/跳过）
4. 与熔断器交互
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import requests
from requests.exceptions import RequestException, Timeout, ConnectionError as RequestsConnectionError

from siyu_etl.circuit_breaker import CircuitBreaker
from siyu_etl.config import AppConfig
from siyu_etl.constants import RETRY_BACKOFFS
from siyu_etl.db import STATUS_SKIPPED, STATUS_SUCCESS, update_tasks_error, update_tasks_status
from siyu_etl.excel_detect import (
    FILETYPE_COUPON_STAT,
    FILETYPE_INCOME_DISCOUNT,
    FILETYPE_INSTORE_ORDER,
    FILETYPE_MEMBER_CARD_EXPORT,
    FILETYPE_MEMBER_STORAGE,
    FILETYPE_MEMBER_TRADE,
)
from siyu_etl.scheduler import Batch


class UploadError(RuntimeError):
    """上传错误异常"""
    pass


class CircuitOpenError(RuntimeError):
    """熔断器打开异常"""
    pass


class NoResponseStopError(RuntimeError):
    """
    无响应停止异常
    
    严格发送规则：
    如果某个批次无法获得任何响应（超时/网络问题），必须停止发送下一个批次。
    """


@dataclass(frozen=True)
class UploadResult:
    """
    上传结果数据类
    
    Attributes:
        success: 是否成功
        status_code: HTTP 状态码，如果请求失败则为 None
        error: 错误信息（如果失败）
    """
    success: bool
    status_code: int | None
    error: str = ""


def webhook_for_file_type(cfg: AppConfig, file_type: str) -> str:
    """
    根据文件类型获取对应的 webhook URL
    
    Args:
        cfg: 应用配置
        file_type: 文件类型
        
    Returns:
        webhook URL
        
    Raises:
        ValueError: 未知的文件类型
    """
    wh = cfg.webhooks
    if file_type == FILETYPE_MEMBER_TRADE:
        return wh.member_trade_detail
    if file_type == FILETYPE_INSTORE_ORDER:
        return wh.in_store_order_detail
    if file_type == FILETYPE_INCOME_DISCOUNT:
        return wh.income_discount_stat
    if file_type == FILETYPE_COUPON_STAT:
        return wh.coupon_stat
    if file_type == FILETYPE_MEMBER_STORAGE:
        return wh.member_storage_analysis
    if file_type == FILETYPE_MEMBER_CARD_EXPORT:
        return wh.member_card_export
    raise ValueError(
        f"无法识别文件类型: {file_type}。\n"
        f"支持的文件类型：会员交易明细、店内订单明细(已结账)、"
        f"收入优惠统计、优惠券统计表、会员储值消费分析表、会员卡导出。\n"
        f"请确认文件名包含上述关键词。"
    )


def _post_json(cfg: AppConfig, url: str, payload: dict) -> UploadResult:
    """
    发送 JSON POST 请求到 webhook
    
    Args:
        cfg: 应用配置
        url: webhook URL
        payload: 请求负载（字典）
        
    Returns:
        UploadResult 对象，包含请求结果
    """
    try:
        resp = requests.post(
            url,
            json=payload,
            timeout=cfg.request_timeout_seconds,
            headers={"Content-Type": "application/json"},
        )
    except Timeout as e:
        return UploadResult(success=False, status_code=None, error=f"REQUEST_TIMEOUT: {e}")
    except RequestsConnectionError as e:
        return UploadResult(success=False, status_code=None, error=f"CONNECTION_ERROR: {e}")
    except RequestException as e:
        return UploadResult(success=False, status_code=None, error=f"REQUEST_ERROR: {e}")

    if resp.status_code != 200:
        return UploadResult(
            success=False,
            status_code=resp.status_code,
            error=f"HTTP_{resp.status_code}: {resp.text[:500]}",
        )

    # Business response: expect {code:0}
    try:
        j = resp.json()
        code = j.get("code")
        if code == 0:
            return UploadResult(success=True, status_code=200)
        return UploadResult(
            success=False, status_code=200, error=f"BUSINESS_ERROR: {j!r}"
        )
    except (ValueError, KeyError) as e:
        # If response isn't JSON, still treat as success only when 200? safer: fail.
        return UploadResult(success=False, status_code=200, error=f"INVALID_JSON: {resp.text[:500]} (parse error: {e})")


def send_batch(
    *,
    cfg: AppConfig,
    db_path: Path,
    breaker: CircuitBreaker,
    batch: Batch,
    logger: Optional[Callable[[str], None]] = None,
) -> UploadResult:
    """
    发送一个批次，包含重试/跳过/熔断器逻辑
    
    处理逻辑：
    - 成功时：将所有项目标记为 SUCCESS，重置失败计数器
    - 最终失败时：将所有项目标记为 SKIPPED，增加失败计数器，可能打开熔断器
    - dry_run=True：不发送 HTTP 请求，不更新数据库状态（仅记录日志）
    
    Args:
        cfg: 应用配置
        db_path: 数据库路径
        breaker: 熔断器实例
        batch: 要发送的批次
        logger: 可选的日志回调函数
        
    Returns:
        UploadResult 对象，包含发送结果
        
    Raises:
        CircuitOpenError: 熔断器已打开时抛出
        NoResponseStopError: 批次无法获得响应时抛出
    """
    if breaker.is_open(batch.file_type, batch.store_name):
        raise CircuitOpenError(f"已熔断: {batch.file_type} / {batch.store_name}")

    url = webhook_for_file_type(cfg, batch.file_type)
    
    # 根据文件类型构建不同的 payload 结构
    if batch.file_type == FILETYPE_MEMBER_CARD_EXPORT:
        # 会员卡导出：只包含 level 字段，不包含 storeId 和 storeName
        level = ""
        if batch.items:
            level = (batch.items[0].data.get("卡等级") or "").strip()
            if not level:
                level = "空等级"
        payload = {
            "platformKey": cfg.platform_key,
            "level": level,
            "data": [{**t.data, "fingerprint": t.fingerprint} for t in batch.items],
        }
    else:
        # 其他数据源：包含 storeId 和 storeName
        payload = {
            "platformKey": cfg.platform_key,
            "storeName": batch.store_name,
            "data": [{**t.data, "fingerprint": t.fingerprint} for t in batch.items],
        }
        # 优先使用 batch.store_id（从数据库中的 store_id 字段）
        sid = (getattr(batch, "store_id", "") or "").strip()
        # 如果 batch.store_id 为空，尝试从数据中提取（fallback for old rows）
        if not sid and batch.items:
            d0 = batch.items[0].data
            # 对于会员储值消费分析表，优先使用"机构编码"
            if batch.file_type == FILETYPE_MEMBER_STORAGE:
                sid = (d0.get("机构编码") or "").strip()
            else:
                sid = (d0.get("操作门店机构编码") or d0.get("开卡门店机构编码") or d0.get("机构编码") or "").strip()
        # normalize placeholder '-'
        if sid == "-":
            sid = ""

        # Always include storeId for consistency; empty-string means "空店"
        payload["storeId"] = sid
        # 使用实际的 storeName，即使 storeId 为空
        # 只有当 storeName 也为空时，才使用"空"作为占位符
        if not payload.get("storeName") or payload["storeName"].strip() == "":
            payload["storeName"] = "空"

    fps = [t.fingerprint for t in batch.items]

    # 打印推送地址和参数详情（用于排查）
    if logger:
        import json
        mode_prefix = "[预演] " if cfg.dry_run else ""
        logger(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger(f"{mode_prefix}推送地址: {url}")
        logger(f"{mode_prefix}文件类型: {batch.file_type}")
        logger(f"{mode_prefix}门店名称: {batch.store_name}")
        logger(f"{mode_prefix}数据条数: {len(batch.items)}")
        logger(f"{mode_prefix}参数详情:")
        logger(f"  - platformKey: {cfg.platform_key}")
        if batch.file_type == FILETYPE_MEMBER_CARD_EXPORT:
            logger(f"  - level: {payload.get('level', '')}")
        else:
            logger(f"  - storeId: {payload.get('storeId', '')}")
            logger(f"  - storeName: {payload.get('storeName', '')}")
        logger(f"  - data 条数: {len(payload.get('data', []))}")
        # 打印前3条数据的 fingerprint（用于调试）
        if payload.get('data'):
            logger(f"  - 前3条 fingerprint: {[item.get('fingerprint', '')[:16] + '...' for item in payload['data'][:3]]}")
        # 打印完整的 JSON payload（用于调试）
        logger(f"{mode_prefix}完整 JSON payload:")
        try:
            payload_json = json.dumps(payload, ensure_ascii=False, indent=2)
            # 如果 JSON 太长，只显示前 2000 个字符
            if len(payload_json) > 2000:
                logger(f"{payload_json[:2000]}...")
                logger(f"  (JSON 已截断，总长度: {len(payload_json)} 字符)")
            else:
                logger(payload_json)
        except Exception as e:
            logger(f"  JSON 序列化失败: {e}")
        logger(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    if cfg.dry_run:
        return UploadResult(success=True, status_code=None, error="")

    backoffs = RETRY_BACKOFFS
    last: UploadResult = UploadResult(success=False, status_code=None, error="UNKNOWN")
    for attempt in range(0, len(backoffs) + 1):
        if attempt > 0:
            wait_s = backoffs[attempt - 1]
            if logger:
                logger(f"重试 {attempt}/{len(backoffs)}，等待 {wait_s}s ...")
            time.sleep(wait_s)

        if logger:
            logger(f"推送 {batch.file_type} / {batch.store_name} 条数={len(batch.items)} attempt={attempt + 1}")
        last = _post_json(cfg, url, payload)
        if last.success:
            update_tasks_status(db_path, fingerprints=fps, status=STATUS_SUCCESS, error="")
            breaker.record_success(batch.file_type, batch.store_name)
            if logger:
                logger("推送成功")
            return last

        if logger:
            logger(f"推送失败: {last.error}")

        # If we didn't get any response (status_code is None), keep retrying this batch,
        # but never move on to the next batch. After final retry, stop the whole push loop.
        if last.status_code is None and attempt == len(backoffs):
            update_tasks_error(db_path, fingerprints=fps, error=last.error)
            raise NoResponseStopError(
                f"上一包未获得返回（网络/超时），已停止后续推送：{batch.file_type} / {batch.store_name}；error={last.error}"
            )

    # final failure -> SKIPPED
    update_tasks_status(db_path, fingerprints=fps, status=STATUS_SKIPPED, error=last.error)
    opened = breaker.record_failure(batch.file_type, batch.store_name)
    if opened:
        raise CircuitOpenError(f"触发熔断: {batch.file_type} / {batch.store_name}")
    return last


