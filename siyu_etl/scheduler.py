"""
任务调度和批处理模块

该模块负责：
1. 从数据库获取待推送任务
2. 将任务按文件类型和门店分组
3. 将分组后的任务分批处理
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

from siyu_etl.db import STATUS_PENDING
from siyu_etl.excel_detect import FILETYPE_MEMBER_CARD_EXPORT, FILETYPE_MEMBER_STORAGE


@dataclass(frozen=True)
class TaskRow:
    fingerprint: str
    file_type: str
    store_id: str
    store_name: str
    timestamp: str
    data: dict[str, str]
    webhook_url: str | None
    session_id: str = ""
    file_id: str = ""
    source_file_name: str = ""
    source_file_path: str = ""


@dataclass(frozen=True)
class Batch:
    file_type: str
    store_id: str
    store_name: str
    items: list[TaskRow]
    session_id: str = ""
    file_id: str = ""
    source_file_name: str = ""


def fetch_pending_tasks(
    db_path: Path,
    limit: Optional[int] = None,
    file_type_filter: Optional[str] = None,
    session_id: Optional[str] = None,
) -> list[TaskRow]:
    from siyu_etl.db import db_connection

    with db_connection(Path(db_path)) as conn:
        sql = """
SELECT
    id,
    fingerprint,
    file_type,
    COALESCE(store_id, ''),
    store_name,
    COALESCE(timestamp, ''),
    raw_data,
    webhook_url,
    COALESCE(session_id, ''),
    COALESCE(file_id, ''),
    COALESCE(source_file_name, ''),
    COALESCE(source_file_path, '')
FROM upload_tasks
WHERE status = ?
""".strip()
        params: list[object] = [STATUS_PENDING]
        if file_type_filter is not None:
            sql += " AND file_type = ?"
            params.append(file_type_filter)
        if session_id is not None:
            sql += " AND session_id = ?"
            params.append(session_id)
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))

        rows = conn.execute(sql, tuple(params)).fetchall()

        tasks_with_keys: list[tuple[TaskRow, tuple, int]] = []
        for (
            task_id,
            fp,
            ft,
            sid,
            sn,
            ts,
            raw,
            webhook,
            sess_id,
            file_id,
            source_file_name,
            source_file_path,
        ) in rows:
            try:
                data = json.loads(raw)
            except Exception:
                data = {}

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
                session_id=str(sess_id or ""),
                file_id=str(file_id or ""),
                source_file_name=str(source_file_name or ""),
                source_file_path=str(source_file_path or ""),
            )

            if file_type == FILETYPE_MEMBER_CARD_EXPORT:
                level = (data.get("卡等级") or "").strip() or "空等级"
                store_name_for_sort = data.get("开卡门店", "")
                sort_key = (str(sess_id or ""), str(file_id or ""), file_type, level, store_name_for_sort)
            else:
                sort_key = (str(sess_id or ""), str(file_id or ""), file_type, sid_norm, str(sn), str(ts))

            tasks_with_keys.append((task, sort_key, int(task_id)))

        tasks_with_keys.sort(key=lambda x: (x[1], x[2]))
        return [task for task, _, _ in tasks_with_keys]


def iter_batches(tasks: list[TaskRow], batch_size: int) -> Iterator[Batch]:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    cur_key: tuple[str, str, str, str] | None = None
    buf: list[TaskRow] = []

    def flush() -> Iterator[Batch]:
        nonlocal buf, cur_key
        if not buf or cur_key is None:
            return iter(())
        ft, group_key, batch_session_id, batch_file_id = cur_key

        if ft == FILETYPE_MEMBER_CARD_EXPORT:
            sid = ""
            sn = ""
        else:
            sid = (buf[0].store_id or "").strip()
            sn = buf[0].store_name if buf[0].store_name else "空"

        batches: list[Batch] = []
        for i in range(0, len(buf), batch_size):
            chunk = buf[i : i + batch_size]
            batches.append(
                Batch(
                    file_type=ft,
                    store_id=sid,
                    store_name=sn,
                    items=chunk,
                    session_id=batch_session_id,
                    file_id=batch_file_id,
                    source_file_name=chunk[0].source_file_name if chunk else "",
                )
            )
        buf = []
        cur_key = None
        return iter(batches)

    for t in tasks:
        if t.file_type == FILETYPE_MEMBER_CARD_EXPORT:
            level = (t.data.get("卡等级") or "").strip() or "空等级"
            group_key = level
        else:
            if t.file_type == FILETYPE_MEMBER_STORAGE:
                group_key = t.store_id if t.store_id else ""
            else:
                group_key = t.store_id or t.store_name

        key = (t.file_type, group_key, t.session_id or "", t.file_id or "")
        if cur_key is None:
            cur_key = key
        if key != cur_key:
            yield from flush()
            cur_key = key
        buf.append(t)

    yield from flush()
