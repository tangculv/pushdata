"""
数据库操作模块

该模块负责：
1. SQLite 数据库的初始化和连接管理
2. 任务（upload_tasks）的增删改查
3. 任务状态管理（PENDING、SUCCESS、SKIPPED、FAILED）
4. 数据迁移和兼容性处理（如 store_id 回填）
5. 批量上传 session / file 级持久化
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional

from siyu_etl.fingerprint import extract_store_id
from siyu_etl.excel_detect import FILETYPE_MEMBER_TRADE


# 任务状态常量
STATUS_PENDING = "PENDING"  # 待处理
STATUS_SUCCESS = "SUCCESS"   # 成功
STATUS_SKIPPED = "SKIPPED"   # 跳过
STATUS_FAILED = "FAILED"     # 失败

# 批量任务状态
SESSION_STATUS_CREATED = "CREATED"
SESSION_STATUS_PARSING = "PARSING"
SESSION_STATUS_PARSED = "PARSED"
SESSION_STATUS_UPLOADING = "UPLOADING"
SESSION_STATUS_COMPLETED = "COMPLETED"
SESSION_STATUS_PARTIAL_FAILED = "PARTIAL_FAILED"
SESSION_STATUS_STOPPED = "STOPPED"
SESSION_STATUS_FAILED = "FAILED"

# 文件状态
FILE_STATUS_PENDING_PARSE = "PENDING_PARSE"
FILE_STATUS_PARSING = "PARSING"
FILE_STATUS_PARSE_SUCCESS = "PARSE_SUCCESS"
FILE_STATUS_PARSE_FAILED = "PARSE_FAILED"
FILE_STATUS_READY_TO_UPLOAD = "READY_TO_UPLOAD"
FILE_STATUS_UPLOADING = "UPLOADING"
FILE_STATUS_UPLOAD_SUCCESS = "UPLOAD_SUCCESS"
FILE_STATUS_UPLOAD_FAILED = "UPLOAD_FAILED"
FILE_STATUS_STOPPED = "STOPPED"

# 数据库版本管理
DB_VERSION = 3  # 当前数据库版本


@dataclass(frozen=True)
class InsertResult:
    inserted: bool
    reason: str = ""


@dataclass(frozen=True)
class BatchSessionRecord:
    session_id: str
    mode: str
    status: str
    total_files: int
    parsed_files: int
    uploaded_files: int
    failed_files: int
    total_rows: int
    uploaded_rows: int
    last_error: str
    created_at: str
    started_at: str
    finished_at: str


@dataclass(frozen=True)
class BatchFileRecord:
    file_id: str
    session_id: str
    file_path: str
    file_name: str
    file_size: int
    file_mtime: str
    file_hash: str
    file_type: str
    status: str
    parse_rows: int
    uploaded_rows: int
    parse_error: str
    upload_error: str
    current_stage: str
    created_at: str
    updated_at: str


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


@contextmanager
def db_connection(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


def _get_db_version(conn: sqlite3.Connection) -> int:
    try:
        result = conn.execute("SELECT version FROM db_version ORDER BY version DESC LIMIT 1;").fetchone()
        return int(result[0]) if result else 0
    except sqlite3.OperationalError:
        return 0


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()]


def _ensure_column(conn: sqlite3.Connection, table: str, name: str, ddl: str) -> None:
    if name not in _table_columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl};")


def _migrate_db(conn: sqlite3.Connection, from_version: int, to_version: int) -> None:
    if from_version < 1 <= to_version:
        _ensure_column(conn, "upload_tasks", "store_id", "store_id TEXT DEFAULT ''")

    if from_version < 2 <= to_version:
        _ensure_column(conn, "upload_tasks", "session_id", "session_id TEXT DEFAULT ''")
        _ensure_column(conn, "upload_tasks", "file_id", "file_id TEXT DEFAULT ''")
        _ensure_column(conn, "upload_tasks", "source_file_name", "source_file_name TEXT DEFAULT ''")
        _ensure_column(conn, "upload_tasks", "source_file_path", "source_file_path TEXT DEFAULT ''")

        conn.execute(
            """
CREATE TABLE IF NOT EXISTS batch_sessions (
    session_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL DEFAULT 'parse_then_upload',
    status TEXT NOT NULL DEFAULT 'CREATED',
    total_files INTEGER NOT NULL DEFAULT 0,
    parsed_files INTEGER NOT NULL DEFAULT 0,
    uploaded_files INTEGER NOT NULL DEFAULT 0,
    failed_files INTEGER NOT NULL DEFAULT 0,
    total_rows INTEGER NOT NULL DEFAULT 0,
    uploaded_rows INTEGER NOT NULL DEFAULT 0,
    last_error TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP DEFAULT '',
    finished_at TIMESTAMP DEFAULT ''
);
""".strip()
        )
        conn.execute(
            """
CREATE TABLE IF NOT EXISTS batch_files (
    file_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    file_size INTEGER NOT NULL DEFAULT 0,
    file_mtime TEXT DEFAULT '',
    file_hash TEXT DEFAULT '',
    file_type TEXT DEFAULT '',
    status TEXT NOT NULL DEFAULT 'PENDING_PARSE',
    parse_rows INTEGER NOT NULL DEFAULT 0,
    uploaded_rows INTEGER NOT NULL DEFAULT 0,
    parse_error TEXT DEFAULT '',
    upload_error TEXT DEFAULT '',
    current_stage TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""".strip()
        )

    if from_version < 3 <= to_version:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_upload_tasks_session_id ON upload_tasks(session_id);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_upload_tasks_file_id ON upload_tasks(file_id);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_batch_files_session_id ON batch_files(session_id);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_batch_files_status ON batch_files(status);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_batch_sessions_status ON batch_sessions(status);"
        )

    conn.execute("UPDATE db_version SET version = ?;", (to_version,))
    conn.commit()


def init_db(db_path: Path) -> None:
    db_path = Path(db_path)
    with db_connection(db_path) as conn:
        conn.execute(
            """
CREATE TABLE IF NOT EXISTS db_version (
    version INTEGER PRIMARY KEY
);
""".strip()
        )
        if conn.execute("SELECT COUNT(*) FROM db_version;").fetchone()[0] == 0:
            conn.execute("INSERT INTO db_version (version) VALUES (?);", (0,))

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
    session_id TEXT DEFAULT '',
    file_id TEXT DEFAULT '',
    source_file_name TEXT DEFAULT '',
    source_file_path TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""".strip()
        )

        current_version = _get_db_version(conn)
        if current_version < DB_VERSION:
            _migrate_db(conn, current_version, DB_VERSION)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_upload_tasks_status ON upload_tasks(status);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_upload_tasks_store ON upload_tasks(store_name);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_upload_tasks_store_id ON upload_tasks(store_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_upload_tasks_type ON upload_tasks(file_type);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_upload_tasks_ts ON upload_tasks(timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_upload_tasks_session_id ON upload_tasks(session_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_upload_tasks_file_id ON upload_tasks(file_id);")
        conn.commit()


def clear_all_tasks(db_path: Path) -> None:
    with db_connection(Path(db_path)) as conn:
        conn.execute("DELETE FROM upload_tasks;")
        conn.commit()




def clear_batch_runtime_data(db_path: Path) -> None:
    with db_connection(Path(db_path)) as conn:
        conn.execute("DELETE FROM batch_files;")
        conn.execute("DELETE FROM batch_sessions;")
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
    session_id: str = "",
    file_id: str = "",
    source_file_name: str = "",
    source_file_path: str = "",
) -> InsertResult:
    with db_connection(Path(db_path)) as conn:
        try:
            conn.execute(
                """
INSERT INTO upload_tasks (
    fingerprint, file_type, store_id, store_name, timestamp, raw_data, status, webhook_url,
    session_id, file_id, source_file_name, source_file_path
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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
                    session_id,
                    file_id,
                    source_file_name,
                    source_file_path,
                ),
            )
            conn.commit()
            return InsertResult(inserted=True)
        except sqlite3.IntegrityError:
            return InsertResult(inserted=False, reason="DUPLICATE_FINGERPRINT")


def create_batch_session(db_path: Path, *, mode: str = "parse_then_upload", session_id: str | None = None) -> str:
    init_db(db_path)
    sid = session_id or uuid.uuid4().hex
    with db_connection(Path(db_path)) as conn:
        conn.execute(
            "INSERT INTO batch_sessions (session_id, mode, status) VALUES (?, ?, ?);",
            (sid, mode, SESSION_STATUS_CREATED),
        )
        conn.commit()
    return sid


def update_batch_session_status(
    db_path: Path,
    *,
    session_id: str,
    status: str,
    last_error: str = "",
    started: bool = False,
    finished: bool = False,
) -> None:
    fields = ["status = ?", "last_error = ?"]
    params: list[Any] = [status, last_error]
    if started:
        fields.append("started_at = CURRENT_TIMESTAMP")
    if finished:
        fields.append("finished_at = CURRENT_TIMESTAMP")
    params.append(session_id)
    with db_connection(Path(db_path)) as conn:
        conn.execute(
            f"UPDATE batch_sessions SET {', '.join(fields)} WHERE session_id = ?;",
            tuple(params),
        )
        conn.commit()


def refresh_batch_session_counters(db_path: Path, *, session_id: str) -> None:
    with db_connection(Path(db_path)) as conn:
        row = conn.execute(
            """
SELECT
    COUNT(*) AS total_files,
    SUM(CASE WHEN status IN ('PARSE_SUCCESS', 'READY_TO_UPLOAD', 'UPLOADING', 'UPLOAD_SUCCESS') THEN 1 ELSE 0 END) AS parsed_files,
    SUM(CASE WHEN status = 'UPLOAD_SUCCESS' THEN 1 ELSE 0 END) AS uploaded_files,
    SUM(CASE WHEN status IN ('PARSE_FAILED', 'UPLOAD_FAILED') THEN 1 ELSE 0 END) AS failed_files,
    COALESCE(SUM(parse_rows), 0) AS total_rows,
    COALESCE(SUM(uploaded_rows), 0) AS uploaded_rows
FROM batch_files
WHERE session_id = ?;
""".strip(),
            (session_id,),
        ).fetchone()
        conn.execute(
            """
UPDATE batch_sessions
SET total_files = ?, parsed_files = ?, uploaded_files = ?, failed_files = ?, total_rows = ?, uploaded_rows = ?
WHERE session_id = ?;
""".strip(),
            (
                int(row["total_files"] or 0),
                int(row["parsed_files"] or 0),
                int(row["uploaded_files"] or 0),
                int(row["failed_files"] or 0),
                int(row["total_rows"] or 0),
                int(row["uploaded_rows"] or 0),
                session_id,
            ),
        )
        conn.commit()


def create_batch_file(
    db_path: Path,
    *,
    session_id: str,
    file_path: str,
    file_name: str,
    file_size: int = 0,
    file_mtime: str = "",
    file_hash: str = "",
    file_type: str = "",
    status: str = FILE_STATUS_PENDING_PARSE,
    file_id: str | None = None,
) -> str:
    init_db(db_path)
    fid = file_id or uuid.uuid4().hex
    with db_connection(Path(db_path)) as conn:
        conn.execute(
            """
INSERT INTO batch_files (
    file_id, session_id, file_path, file_name, file_size, file_mtime, file_hash, file_type, status
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
""".strip(),
            (fid, session_id, file_path, file_name, int(file_size), file_mtime, file_hash, file_type, status),
        )
        conn.commit()
    refresh_batch_session_counters(db_path, session_id=session_id)
    return fid


def update_batch_file_status(
    db_path: Path,
    *,
    file_id: str,
    status: str,
    file_type: str | None = None,
    parse_rows: int | None = None,
    uploaded_rows: int | None = None,
    parse_error: str | None = None,
    upload_error: str | None = None,
    current_stage: str | None = None,
) -> None:
    sets = ["status = ?", "updated_at = CURRENT_TIMESTAMP"]
    params: list[Any] = [status]
    if file_type is not None:
        sets.append("file_type = ?")
        params.append(file_type)
    if parse_rows is not None:
        sets.append("parse_rows = ?")
        params.append(int(parse_rows))
    if uploaded_rows is not None:
        sets.append("uploaded_rows = ?")
        params.append(int(uploaded_rows))
    if parse_error is not None:
        sets.append("parse_error = ?")
        params.append(parse_error)
    if upload_error is not None:
        sets.append("upload_error = ?")
        params.append(upload_error)
    if current_stage is not None:
        sets.append("current_stage = ?")
        params.append(current_stage)
    params.append(file_id)

    with db_connection(Path(db_path)) as conn:
        conn.execute(
            f"UPDATE batch_files SET {', '.join(sets)} WHERE file_id = ?;",
            tuple(params),
        )
        row = conn.execute("SELECT session_id FROM batch_files WHERE file_id = ?;", (file_id,)).fetchone()
        conn.commit()
    if row:
        refresh_batch_session_counters(db_path, session_id=str(row[0]))


def list_batch_files(db_path: Path, *, session_id: str) -> list[BatchFileRecord]:
    with db_connection(Path(db_path)) as conn:
        rows = conn.execute(
            "SELECT * FROM batch_files WHERE session_id = ? ORDER BY created_at, file_name;",
            (session_id,),
        ).fetchall()
    return [
        BatchFileRecord(
            file_id=str(r["file_id"]),
            session_id=str(r["session_id"]),
            file_path=str(r["file_path"]),
            file_name=str(r["file_name"]),
            file_size=int(r["file_size"] or 0),
            file_mtime=str(r["file_mtime"] or ""),
            file_hash=str(r["file_hash"] or ""),
            file_type=str(r["file_type"] or ""),
            status=str(r["status"]),
            parse_rows=int(r["parse_rows"] or 0),
            uploaded_rows=int(r["uploaded_rows"] or 0),
            parse_error=str(r["parse_error"] or ""),
            upload_error=str(r["upload_error"] or ""),
            current_stage=str(r["current_stage"] or ""),
            created_at=str(r["created_at"] or ""),
            updated_at=str(r["updated_at"] or ""),
        )
        for r in rows
    ]


def get_batch_session(db_path: Path, *, session_id: str) -> BatchSessionRecord | None:
    with db_connection(Path(db_path)) as conn:
        row = conn.execute("SELECT * FROM batch_sessions WHERE session_id = ?;", (session_id,)).fetchone()
    if not row:
        return None
    return BatchSessionRecord(
        session_id=str(row["session_id"]),
        mode=str(row["mode"]),
        status=str(row["status"]),
        total_files=int(row["total_files"] or 0),
        parsed_files=int(row["parsed_files"] or 0),
        uploaded_files=int(row["uploaded_files"] or 0),
        failed_files=int(row["failed_files"] or 0),
        total_rows=int(row["total_rows"] or 0),
        uploaded_rows=int(row["uploaded_rows"] or 0),
        last_error=str(row["last_error"] or ""),
        created_at=str(row["created_at"] or ""),
        started_at=str(row["started_at"] or ""),
        finished_at=str(row["finished_at"] or ""),
    )


# ===== legacy / existing helpers below =====
def _load_raw_data(raw_data: str) -> dict[str, Any]:
    try:
        return json.loads(raw_data)
    except Exception:
        return {}


def update_tasks_status(db_path: Path, *, fingerprints: list[str], status: str, error: str = "") -> None:
    if not fingerprints:
        return
    placeholders = ",".join(["?"] * len(fingerprints))
    with db_connection(Path(db_path)) as conn:
        conn.execute(
            f"UPDATE upload_tasks SET status = ?, error = ?, updated_at = CURRENT_TIMESTAMP WHERE fingerprint IN ({placeholders});",
            (status, error, *fingerprints),
        )
        conn.commit()


def update_tasks_error(db_path: Path, *, fingerprints: list[str], error: str) -> None:
    if not fingerprints:
        return
    placeholders = ",".join(["?"] * len(fingerprints))
    with db_connection(Path(db_path)) as conn:
        conn.execute(
            f"UPDATE upload_tasks SET error = ?, updated_at = CURRENT_TIMESTAMP WHERE fingerprint IN ({placeholders});",
            (error, *fingerprints),
        )
        conn.commit()


def _backfill_pending_store_ids(conn: sqlite3.Connection, *, session_id: str | None = None) -> int:
    sql = (
        "SELECT id, raw_data FROM upload_tasks "
        "WHERE status = ? AND (store_id IS NULL OR TRIM(store_id) = '' OR store_id = '-')"
    )
    params: list[Any] = [STATUS_PENDING]
    if session_id is not None:
        sql += " AND session_id = ?"
        params.append(session_id)
    rows = conn.execute(sql + ";", tuple(params)).fetchall()

    updated = 0
    for row in rows:
        raw = _load_raw_data(str(row["raw_data"]))
        file_type = str(raw.get("_file_type") or raw.get("文件类型") or "")
        if not file_type:
            file_type = FILETYPE_MEMBER_TRADE if "交易流水号" in raw else ""
        if not file_type:
            continue
        store_id = extract_store_id(file_type, raw)
        if store_id:
            conn.execute(
                "UPDATE upload_tasks SET store_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?;",
                (store_id, row["id"]),
            )
            updated += 1
    conn.commit()
    return updated


def backfill_pending_store_ids(db_path: Path) -> int:
    with db_connection(Path(db_path)) as conn:
        return _backfill_pending_store_ids(conn)


def backfill_pending_store_ids_for_session(db_path: Path, *, session_id: str) -> int:
    with db_connection(Path(db_path)) as conn:
        return _backfill_pending_store_ids(conn, session_id=session_id)


def _requeue_skipped_member_trade_with_store_id(conn: sqlite3.Connection, *, session_id: str | None = None) -> int:
    sql = "SELECT id, raw_data FROM upload_tasks WHERE status = ? AND file_type = ?"
    params: list[Any] = [STATUS_SKIPPED, FILETYPE_MEMBER_TRADE]
    if session_id is not None:
        sql += " AND session_id = ?"
        params.append(session_id)
    rows = conn.execute(sql + ";", tuple(params)).fetchall()

    updated = 0
    for row in rows:
        raw = _load_raw_data(str(row["raw_data"]))
        store_id = extract_store_id(FILETYPE_MEMBER_TRADE, raw)
        if store_id:
            conn.execute(
                "UPDATE upload_tasks SET store_id = ?, status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?;",
                (store_id, STATUS_PENDING, row["id"]),
            )
            updated += 1
    conn.commit()
    return updated


def requeue_skipped_member_trade_with_store_id(db_path: Path) -> int:
    with db_connection(Path(db_path)) as conn:
        return _requeue_skipped_member_trade_with_store_id(conn)


def requeue_skipped_member_trade_with_store_id_for_session(db_path: Path, *, session_id: str) -> int:
    with db_connection(Path(db_path)) as conn:
        return _requeue_skipped_member_trade_with_store_id(conn, session_id=session_id)



def get_batch_file(db_path: Path, *, file_id: str) -> BatchFileRecord | None:
    with db_connection(Path(db_path)) as conn:
        row = conn.execute("SELECT * FROM batch_files WHERE file_id = ?;", (file_id,)).fetchone()
    if not row:
        return None
    return BatchFileRecord(
        file_id=str(row["file_id"]),
        session_id=str(row["session_id"]),
        file_path=str(row["file_path"]),
        file_name=str(row["file_name"]),
        file_size=int(row["file_size"] or 0),
        file_mtime=str(row["file_mtime"] or ""),
        file_hash=str(row["file_hash"] or ""),
        file_type=str(row["file_type"] or ""),
        status=str(row["status"]),
        parse_rows=int(row["parse_rows"] or 0),
        uploaded_rows=int(row["uploaded_rows"] or 0),
        parse_error=str(row["parse_error"] or ""),
        upload_error=str(row["upload_error"] or ""),
        current_stage=str(row["current_stage"] or ""),
        created_at=str(row["created_at"] or ""),
        updated_at=str(row["updated_at"] or ""),
    )


def get_tasks_count_by_file(db_path: Path, *, file_id: str, status: str | None = None) -> int:
    sql = "SELECT COUNT(*) FROM upload_tasks WHERE file_id = ?"
    params: list[Any] = [file_id]
    if status is not None:
        sql += " AND status = ?"
        params.append(status)
    with db_connection(Path(db_path)) as conn:
        row = conn.execute(sql + ";", tuple(params)).fetchone()
    return int(row[0] or 0)


def count_session_tasks(
    db_path: Path,
    *,
    session_id: str,
    status: str | None = None,
    file_id: str | None = None,
) -> int:
    sql = "SELECT COUNT(*) FROM upload_tasks WHERE session_id = ?"
    params: list[Any] = [session_id]
    if file_id is not None:
        sql += " AND file_id = ?"
        params.append(file_id)
    if status is not None:
        sql += " AND status = ?"
        params.append(status)
    with db_connection(Path(db_path)) as conn:
        row = conn.execute(sql + ";", tuple(params)).fetchone()
    return int(row[0] or 0)
