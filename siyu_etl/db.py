"""
数据库操作模块

该模块负责：
1. SQLite 数据库的初始化和连接管理
2. 任务（upload_tasks）的增删改查
3. 任务状态管理（PENDING、SUCCESS、SKIPPED、FAILED）
4. 数据迁移和兼容性处理（如 store_id 回填）
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional

from siyu_etl.fingerprint import extract_store_id


# 任务状态常量
STATUS_PENDING = "PENDING"  # 待处理
STATUS_SUCCESS = "SUCCESS"   # 成功
STATUS_SKIPPED = "SKIPPED"   # 跳过
STATUS_FAILED = "FAILED"     # 失败

# 数据库版本管理
DB_VERSION = 1  # 当前数据库版本


@dataclass(frozen=True)
class InsertResult:
    """
    插入结果数据类
    
    Attributes:
        inserted: 是否成功插入
        reason: 插入失败的原因（如果失败）
    """
    inserted: bool
    reason: str = ""


def connect(db_path: Path) -> sqlite3.Connection:
    """
    连接到 SQLite 数据库
    
    配置了 WAL（Write-Ahead Logging）模式和 NORMAL 同步模式以提高性能。
    
    Args:
        db_path: 数据库文件路径
        
    Returns:
        SQLite 连接对象
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


@contextmanager
def db_connection(db_path: Path) -> Iterator[sqlite3.Connection]:
    """
    数据库连接上下文管理器
    
    自动管理数据库连接的打开和关闭，确保连接在使用后正确关闭。
    
    Args:
        db_path: 数据库文件路径
        
    Yields:
        SQLite 连接对象
        
    Example:
        ```python
        with db_connection(db_path) as conn:
            conn.execute("SELECT * FROM upload_tasks")
            conn.commit()
        ```
    """
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


def _get_db_version(conn: sqlite3.Connection) -> int:
    """
    获取当前数据库版本
    
    Args:
        conn: 数据库连接
        
    Returns:
        数据库版本号，如果版本表不存在则返回 0
    """
    try:
        result = conn.execute("SELECT version FROM db_version ORDER BY version DESC LIMIT 1;").fetchone()
        return int(result[0]) if result else 0
    except sqlite3.OperationalError:
        # 版本表不存在，返回 0
        return 0


def _migrate_db(conn: sqlite3.Connection, from_version: int, to_version: int) -> None:
    """
    执行数据库迁移
    
    Args:
        conn: 数据库连接
        from_version: 当前版本
        to_version: 目标版本
    """
    # 迁移 0 -> 1: 添加 store_id 列（如果不存在）
    if from_version < 1 <= to_version:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(upload_tasks);").fetchall()]
        if "store_id" not in cols:
            conn.execute("ALTER TABLE upload_tasks ADD COLUMN store_id TEXT DEFAULT '';")
    
    # 更新版本号
    conn.execute("UPDATE db_version SET version = ?;", (to_version,))
    conn.commit()


def init_db(db_path: Path) -> None:
    """
    初始化数据库，创建表结构和索引
    
    如果表已存在，则只创建缺失的索引。
    如果数据库是旧版本，会自动执行迁移。
    
    Args:
        db_path: 数据库文件路径
    """
    db_path = Path(db_path)
    with db_connection(db_path) as conn:
        # 创建版本管理表
        conn.execute(
            """
CREATE TABLE IF NOT EXISTS db_version (
    version INTEGER PRIMARY KEY
);
""".strip()
        )
        
        # 如果版本表为空，插入初始版本
        if conn.execute("SELECT COUNT(*) FROM db_version;").fetchone()[0] == 0:
            conn.execute("INSERT INTO db_version (version) VALUES (?);", (0,))
        
        # 创建主表
        conn.execute(
            """
CREATE TABLE IF NOT EXISTS upload_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL UNIQUE,
    file_type TEXT NOT NULL,
    store_id TEXT DEFAULT '',
    store_name TEXT NOT NULL,
    timestamp TEXT,
    raw_data TEXT NOT NULL,
    status TEXT DEFAULT 'PENDING',
    webhook_url TEXT,
    error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""".strip()
        )
        
        # 检查并执行迁移
        current_version = _get_db_version(conn)
        if current_version < DB_VERSION:
            _migrate_db(conn, current_version, DB_VERSION)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_upload_tasks_status ON upload_tasks(status);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_upload_tasks_store ON upload_tasks(store_name);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_upload_tasks_store_id ON upload_tasks(store_id);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_upload_tasks_type ON upload_tasks(file_type);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_upload_tasks_ts ON upload_tasks(timestamp);"
        )
        conn.commit()


def clear_all_tasks(db_path: Path) -> None:
    """
    清空所有任务（用于重置功能）
    
    Args:
        db_path: 数据库文件路径
    """
    with db_connection(Path(db_path)) as conn:
        conn.execute("DELETE FROM upload_tasks;")
        conn.commit()


def insert_task(
    db_path: Path,
    *,
    fingerprint: str,
    file_type: str,
    store_id: str,
    store_name: str,
    timestamp: str,
    raw_data: dict[str, Any],
    webhook_url: Optional[str] = None,
) -> InsertResult:
    """
    插入一个新任务到数据库
    
    如果 fingerprint 已存在（重复），则返回插入失败的结果。
    
    Args:
        db_path: 数据库文件路径
        fingerprint: 行指纹（唯一标识）
        file_type: 文件类型
        store_id: 门店 ID
        store_name: 门店名称
        timestamp: 时间戳
        raw_data: 原始数据字典（会被序列化为 JSON）
        webhook_url: webhook URL（可选）
        
    Returns:
        InsertResult 对象，包含插入结果
    """
    with db_connection(Path(db_path)) as conn:
        try:
            conn.execute(
                """
INSERT INTO upload_tasks (fingerprint, file_type, store_id, store_name, timestamp, raw_data, status, webhook_url)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);
""".strip(),
                (
                    fingerprint,
                    file_type,
                    store_id,
                    store_name,
                    timestamp,
                    json.dumps(raw_data, ensure_ascii=False, sort_keys=True),
                    STATUS_PENDING,
                    webhook_url,
                ),
            )
            conn.commit()
            return InsertResult(inserted=True)
        except sqlite3.IntegrityError:
            return InsertResult(inserted=False, reason="DUPLICATE_FINGERPRINT")


def update_task_status(
    db_path: Path,
    *,
    fingerprint: str,
    status: str,
    error: str = "",
) -> None:
    """
    更新单个任务的状态
    
    Args:
        db_path: 数据库文件路径
        fingerprint: 任务指纹
        status: 新状态
        error: 错误信息（可选）
    """
    with db_connection(Path(db_path)) as conn:
        conn.execute(
            """
UPDATE upload_tasks
SET status = ?, error = ?, updated_at = CURRENT_TIMESTAMP
WHERE fingerprint = ?;
""".strip(),
            (status, error, fingerprint),
        )
        conn.commit()


def update_tasks_status(
    db_path: Path,
    *,
    fingerprints: list[str],
    status: str,
    error: str = "",
) -> None:
    """
    批量更新多个任务的状态
    
    Args:
        db_path: 数据库文件路径
        fingerprints: 任务指纹列表
        status: 新状态
        error: 错误信息（可选）
    """
    if not fingerprints:
        return
    with db_connection(Path(db_path)) as conn:
        qmarks = ",".join(["?"] * len(fingerprints))
        conn.execute(
            f"""
UPDATE upload_tasks
SET status = ?, error = ?, updated_at = CURRENT_TIMESTAMP
WHERE fingerprint IN ({qmarks});
""".strip(),
            (status, error, *fingerprints),
        )
        conn.commit()


def update_tasks_error(
    db_path: Path,
    *,
    fingerprints: list[str],
    error: str,
) -> None:
    """
    仅更新任务的错误信息，不改变状态
    
    最佳努力：记录错误但不改变状态（用于临时"无响应"情况，
    我们希望停止发送下一个批次，但保持任务为 PENDING 状态以便稍后重试）。
    
    Args:
        db_path: 数据库文件路径
        fingerprints: 任务指纹列表
        error: 错误信息
    """
    if not fingerprints:
        return
    with db_connection(Path(db_path)) as conn:
        qmarks = ",".join(["?"] * len(fingerprints))
        conn.execute(
            f"""
UPDATE upload_tasks
SET error = ?, updated_at = CURRENT_TIMESTAMP
WHERE fingerprint IN ({qmarks});
""".strip(),
            (error, *fingerprints),
        )
        conn.commit()


def backfill_pending_store_ids(db_path: Path) -> int:
    """
    回填待推送任务的 store_id
    
    为在支持 store_id 之前插入的现有待推送行回填 store_id。
    这修复了实际升级场景，其中旧的 PENDING 数据原本只会使用 storeName 推送。
    
    Args:
        db_path: 数据库文件路径
        
    Returns:
        更新的行数
    """
    db_path = Path(db_path)
    with db_connection(db_path) as conn:
        cur = conn.execute(
            """
SELECT fingerprint, file_type, raw_data
FROM upload_tasks
WHERE status = 'PENDING' AND (store_id IS NULL OR store_id = '');
""".strip()
        )
        rows = cur.fetchall()
        updates: list[tuple[str, str]] = []  # (store_id, fingerprint)
        for fp, ft, raw in rows:
            try:
                data = json.loads(raw)
            except Exception:
                continue
            file_type = str(ft)
            store_id = extract_store_id(file_type, data).strip()
            if store_id:
                updates.append((store_id, str(fp)))

        if not updates:
            return 0

        conn.executemany(
            "UPDATE upload_tasks SET store_id = ?, updated_at = CURRENT_TIMESTAMP WHERE fingerprint = ?;",
            updates,
        )
        conn.commit()

        return len(updates)


def requeue_skipped_member_trade_with_store_id(db_path: Path) -> int:
    """
    重新排队之前跳过的会员交易明细行（如果它们有 store_id）
    
    根本原因修复：解决"Excel 中有 1530 行但只上传了 1030 行"的问题。
    一些行之前仅使用 storeName 推送，失败后被标记为 SKIPPED。
    在我们添加 storeId 支持后，这些 SKIPPED 行不会被重试（推送只读取 PENDING 状态）。
    
    该函数：
    - 查找 store_id 为空的 SKIPPED 状态的会员交易明细行
    - 从 raw_data 中提取 store_id（操作门店机构编码/开卡门店机构编码）
    - 更新 store_id 并将状态重置为 PENDING 以便重试
    
    Args:
        db_path: 数据库文件路径
        
    Returns:
        重新排队的行数
    """
    db_path = Path(db_path)
    with db_connection(db_path) as conn:
        cur = conn.execute(
            """
SELECT fingerprint, raw_data
FROM upload_tasks
WHERE file_type = '会员交易明细'
  AND status = 'SKIPPED'
  AND (store_id IS NULL OR store_id = '');
""".strip()
        )
        rows = cur.fetchall()
        updates: list[tuple[str, str]] = []  # (store_id, fingerprint)
        for fp, raw in rows:
            try:
                data = json.loads(raw)
            except Exception:
                continue
            store_id = extract_store_id("会员交易明细", data).strip()
            if store_id:
                updates.append((store_id, str(fp)))

        if not updates:
            return 0

        # Reset to PENDING for retry, clear error
        conn.executemany(
            "UPDATE upload_tasks SET store_id = ?, status = 'PENDING', error = '', updated_at = CURRENT_TIMESTAMP WHERE fingerprint = ?;",
            updates,
        )
        conn.commit()

        return len(updates)


