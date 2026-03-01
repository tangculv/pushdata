"""
任务调度和批处理模块

该模块负责：
1. 从数据库获取待推送任务
2. 将任务按文件类型和门店分组
3. 将分组后的任务分批处理
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

from siyu_etl.db import STATUS_PENDING
from siyu_etl.excel_detect import FILETYPE_MEMBER_CARD_EXPORT, FILETYPE_MEMBER_STORAGE


@dataclass(frozen=True)
class TaskRow:
    """
    任务行数据类
    
    Attributes:
        fingerprint: 任务指纹
        file_type: 文件类型
        store_id: 门店 ID
        store_name: 门店名称
        timestamp: 时间戳
        data: 任务数据字典
        webhook_url: webhook URL（可选）
    """
    fingerprint: str
    file_type: str
    store_id: str
    store_name: str
    timestamp: str
    data: dict[str, str]
    webhook_url: str | None


@dataclass(frozen=True)
class Batch:
    """
    批次数据类
    
    一个批次包含同一文件类型和同一门店的多个任务。
    
    Attributes:
        file_type: 文件类型
        store_id: 门店 ID
        store_name: 门店名称（如果 store_id 为空则为"空"）
        items: 任务列表
    """
    file_type: str
    store_id: str
    store_name: str
    items: list[TaskRow]


def fetch_pending_tasks(
    db_path: Path, 
    limit: Optional[int] = None,
    file_type_filter: Optional[str] = None,
) -> list[TaskRow]:
    """
    从数据库获取待推送任务
    
    任务按文件类型、分组字段、排序字段和 ID 排序。
    - 普通数据源：按 file_type, store_id, store_name, timestamp 排序
    - 会员卡导出：按 file_type, level(卡等级), store_name(开卡门店), id 排序
    
    Args:
        db_path: 数据库文件路径
        limit: 可选的最大返回数量
        file_type_filter: 可选的文件类型过滤，只返回指定类型的任务
        
    Returns:
        任务列表
    """
    from siyu_etl.db import db_connection
    
    with db_connection(Path(db_path)) as conn:
        # 先获取所有待推送任务（包含 id 用于最终排序）
        sql = """
SELECT id, fingerprint, file_type, COALESCE(store_id, ''), store_name, COALESCE(timestamp, ''), raw_data, webhook_url
FROM upload_tasks
WHERE status = ?
""".strip()
        params: tuple = (STATUS_PENDING,)
        if file_type_filter is not None:
            sql += " AND file_type = ?"
            params = (STATUS_PENDING, file_type_filter)
        if limit is not None:
            sql += " LIMIT ?"
            if file_type_filter is not None:
                params = (STATUS_PENDING, file_type_filter, int(limit))
            else:
                params = (STATUS_PENDING, int(limit))

        cur = conn.execute(sql, params)
        rows = cur.fetchall()
        
        # 解析数据并提取分组/排序字段
        tasks_with_keys: list[tuple[TaskRow, tuple, int]] = []
        for task_id, fp, ft, sid, sn, ts, raw, webhook in rows:
            try:
                data = json.loads(raw)
            except Exception:
                data = {}
            # normalize store_id placeholder
            sid_norm = str(sid) if sid else ""
            if sid_norm.strip() == "-":
                sid_norm = ""
            
            file_type = str(ft)
            task = TaskRow(
                fingerprint=str(fp),
                file_type=file_type,
                store_id=sid_norm,
                store_name=str(sn),
                timestamp=str(ts),
                data=data,
                webhook_url=str(webhook) if webhook else None,
            )
            
            # 根据文件类型确定排序键
            if file_type == FILETYPE_MEMBER_CARD_EXPORT:
                # 会员卡导出：按卡等级分组，按开卡门店排序
                level = (data.get("卡等级") or "").strip()
                if not level:
                    level = "空等级"
                store_name_for_sort = data.get("开卡门店", "")
                sort_key = (file_type, level, store_name_for_sort)
            else:
                # 其他数据源：按 store_id/store_name 分组，按 timestamp 排序
                sort_key = (file_type, sid_norm, str(sn), str(ts))
            
            tasks_with_keys.append((task, sort_key, task_id))
        
        # 排序：先按排序键，再按 id（保证稳定性）
        tasks_with_keys.sort(key=lambda x: (x[1], x[2]))
        
        # 返回排序后的任务列表
        return [task for task, _, _ in tasks_with_keys]


def iter_batches(tasks: list[TaskRow], batch_size: int) -> Iterator[Batch]:
    """
    将任务分组并分批处理
    
    分组规则：
    - 普通数据源：按 (file_type, store_id_or_store_name) 分组
    - 会员卡导出：按 (file_type, 卡等级) 分组
    
    保持任务已按排序字段排序，然后分批产出。
    
    Args:
        tasks: 任务列表（应已排序）
        batch_size: 每批的大小
        
    Yields:
        Batch 对象，每个批次包含同一文件类型和同一分组键的任务
        
    Raises:
        ValueError: batch_size 必须大于 0
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    cur_key: tuple[str, str] | None = None
    buf: list[TaskRow] = []

    def flush() -> Iterator[Batch]:
        nonlocal buf, cur_key
        if not buf or cur_key is None:
            return iter(())
        ft, group_key = cur_key
        
        # 根据文件类型确定 Batch 的 store_id 和 store_name
        if ft == FILETYPE_MEMBER_CARD_EXPORT:
            # 会员卡导出：使用空字符串作为 store_id 和 store_name（payload 中不使用）
            sid = ""
            sn = ""
        else:
            # 其他数据源：使用 store_id 和 store_name
            sid = (buf[0].store_id or "").strip()
            # 使用实际的 store_name，即使 store_id 为空
            # 只有当 store_id 为空且 store_name 也为空时，才使用"空"作为占位符
            sn = buf[0].store_name if buf[0].store_name else "空"
        
        batches: list[Batch] = []
        for i in range(0, len(buf), batch_size):
            batches.append(
                Batch(
                    file_type=ft,
                    store_id=sid,
                    store_name=sn,
                    items=buf[i : i + batch_size],
                )
            )
        buf = []
        cur_key = None
        return iter(batches)

    for t in tasks:
        # 根据文件类型确定分组键
        if t.file_type == FILETYPE_MEMBER_CARD_EXPORT:
            # 会员卡导出：按卡等级分组
            level = (t.data.get("卡等级") or "").strip()
            if not level:
                level = "空等级"
            group_key = level
        else:
            # 其他数据源：按 store_id 或 store_name 分组
            # 对于会员储值消费分析表，即使 store_id 为空，也应该使用它作为分组键（归为"空店"组）
            # 而不是回退到 store_name，这样可以确保始终使用机构编码作为分组标识
            if t.file_type == FILETYPE_MEMBER_STORAGE:
                # 会员储值消费分析表：始终使用 store_id 作为分组键，即使为空
                group_key = t.store_id if t.store_id else ""
            else:
                # 其他数据源：如果 store_id 为空，则使用 store_name
                group_key = t.store_id or t.store_name
        
        key = (t.file_type, group_key)
        if cur_key is None:
            cur_key = key
        if key != cur_key:
            yield from flush()
            cur_key = key
        buf.append(t)

    yield from flush()


