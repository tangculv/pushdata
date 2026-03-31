from pathlib import Path

from siyu_etl.batch_service import BatchService
from siyu_etl.db import (
    FILE_STATUS_PARSE_SUCCESS,
    FILE_STATUS_PENDING_PARSE,
    FILE_STATUS_UPLOAD_SUCCESS,
    connect,
    init_db,
    insert_task,
)


def test_init_db_creates_batch_tables(tmp_path: Path):
    db_path = tmp_path / "test.sqlite3"
    init_db(db_path)
    conn = connect(db_path)
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()}
        assert "upload_tasks" in tables
        assert "batch_sessions" in tables
        assert "batch_files" in tables

        cols = {r[1] for r in conn.execute("PRAGMA table_info(upload_tasks);").fetchall()}
        assert "session_id" in cols
        assert "file_id" in cols
        assert "source_file_name" in cols
        assert "source_file_path" in cols
    finally:
        conn.close()


def test_batch_service_basic_flow(tmp_path: Path):
    db_path = tmp_path / "test.sqlite3"
    service = BatchService(db_path)
    session_id = service.create_session()
    file_id = service.add_file(
        session_id=session_id,
        file_path="/tmp/a.xlsx",
        file_name="a.xlsx",
        file_size=123,
        file_mtime="2026-03-17T00:00:00",
    )

    files = service.list_files(session_id)
    assert len(files) == 1
    assert files[0].file_id == file_id
    assert files[0].status == FILE_STATUS_PENDING_PARSE

    service.update_file_status(file_id=file_id, status=FILE_STATUS_PARSE_SUCCESS, parse_rows=10, file_type="会员交易明细")
    summary = service.summary(session_id)
    assert summary is not None
    assert summary.total_files == 1
    assert summary.parsed_files == 1
    assert summary.total_rows == 10

    service.update_file_status(file_id=file_id, status=FILE_STATUS_UPLOAD_SUCCESS, uploaded_rows=10)
    summary2 = service.summary(session_id)
    assert summary2 is not None
    assert summary2.uploaded_files == 1
    assert summary2.uploaded_rows == 10


def test_insert_task_with_session_fields(tmp_path: Path):
    db_path = tmp_path / "test.sqlite3"
    init_db(db_path)
    res = insert_task(
        db_path,
        fingerprint="fp-1",
        file_type="会员交易明细",
        store_id="s1",
        store_name="门店A",
        timestamp="2026-03-17 10:00:00",
        raw_data={"a": 1},
        session_id="sess-1",
        file_id="file-1",
        source_file_name="a.xlsx",
        source_file_path="/tmp/a.xlsx",
    )
    assert res.inserted is True

    conn = connect(db_path)
    try:
        row = conn.execute(
            "SELECT session_id, file_id, source_file_name, source_file_path FROM upload_tasks WHERE fingerprint = 'fp-1';"
        ).fetchone()
        assert row[0] == "sess-1"
        assert row[1] == "file-1"
        assert row[2] == "a.xlsx"
        assert row[3] == "/tmp/a.xlsx"
    finally:
        conn.close()
