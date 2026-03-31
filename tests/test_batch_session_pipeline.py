from __future__ import annotations

from pathlib import Path

import openpyxl

from siyu_etl.circuit_breaker import CircuitBreaker
from siyu_etl.config import DEFAULT_CONFIG
from siyu_etl.db import STATUS_PENDING, STATUS_SUCCESS, connect, count_session_tasks, init_db
from siyu_etl.processor import parse_only, push_only
from siyu_etl.scheduler import fetch_pending_tasks, iter_batches


def _make_member_trade_excel(path: Path, tx_prefix: str, store_name: str = "测试门店") -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["说明行"])
    ws.append(["交易流水号", "交易时间", "操作门店"])
    ws.append([f"{tx_prefix}-1", "2026-03-17 10:00:00", store_name])
    ws.append([f"{tx_prefix}-2", "2026-03-17 11:00:00", store_name])
    wb.save(path)
    wb.close()


def test_parse_only_creates_session_scoped_tasks(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.sqlite3"
    init_db(db_path)
    file_path = tmp_path / "会员交易明细_A.xlsx"
    _make_member_trade_excel(file_path, "A")

    stats = parse_only(
        cfg=DEFAULT_CONFIG,
        db_path=db_path,
        file_paths=[file_path],
        log=lambda *_: None,
        progress=lambda *_: None,
        stop_flag=lambda: False,
    )

    assert stats.session_id
    assert stats.inserted_rows == 2
    assert count_session_tasks(db_path, session_id=stats.session_id) == 2

    conn = connect(db_path)
    try:
        rows = conn.execute(
            "SELECT session_id, file_id, source_file_name, source_file_path, status FROM upload_tasks"
        ).fetchall()
        assert len(rows) == 2
        assert all(row[0] == stats.session_id for row in rows)
        assert all(row[1] for row in rows)
        assert all(row[2] == file_path.name for row in rows)
        assert all(row[3] == str(file_path) for row in rows)
        assert all(row[4] == STATUS_PENDING for row in rows)
    finally:
        conn.close()


def test_push_only_only_processes_current_session(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "pipeline.sqlite3"
    init_db(db_path)

    file_a = tmp_path / "会员交易明细_A.xlsx"
    file_b = tmp_path / "会员交易明细_B.xlsx"
    _make_member_trade_excel(file_a, "A", store_name="门店A")
    _make_member_trade_excel(file_b, "B", store_name="门店B")

    stats_a = parse_only(
        cfg=DEFAULT_CONFIG,
        db_path=db_path,
        file_paths=[file_a],
        log=lambda *_: None,
        progress=lambda *_: None,
        stop_flag=lambda: False,
    )
    stats_b = parse_only(
        cfg=DEFAULT_CONFIG,
        db_path=db_path,
        file_paths=[file_b],
        log=lambda *_: None,
        progress=lambda *_: None,
        stop_flag=lambda: False,
    )

    sent_sessions: list[str] = []

    def _fake_send_batch(*, cfg, db_path, breaker, batch, logger=None):
        from siyu_etl.db import STATUS_SUCCESS, update_tasks_status
        fps = [item.fingerprint for item in batch.items]
        update_tasks_status(db_path, fingerprints=fps, status=STATUS_SUCCESS, error="")
        sent_sessions.append(batch.session_id)

        class _Res:
            success = True
            error = ""

        return _Res()

    monkeypatch.setattr("siyu_etl.processor.send_batch", _fake_send_batch)

    push_stats = push_only(
        cfg=DEFAULT_CONFIG,
        db_path=db_path,
        breaker=CircuitBreaker(threshold=3),
        log=lambda *_: None,
        progress=lambda *_: None,
        stop_flag=lambda: False,
        session_id=stats_a.session_id,
    )

    assert push_stats.session_id == stats_a.session_id
    assert sent_sessions
    assert all(session_id == stats_a.session_id for session_id in sent_sessions)
    assert count_session_tasks(db_path, session_id=stats_a.session_id, status=STATUS_SUCCESS) == 2
    assert count_session_tasks(db_path, session_id=stats_b.session_id, status=STATUS_PENDING) == 2


def test_scheduler_keeps_batches_within_file_boundaries(tmp_path: Path) -> None:
    db_path = tmp_path / "scheduler.sqlite3"
    init_db(db_path)

    for idx in range(2):
        for row_no in range(2):
            file_id = f"file-{idx}"
            session_id = "session-1"
            fingerprint = f"fp-{idx}-{row_no}"
            from siyu_etl.db import insert_task
            insert_task(
                db_path,
                fingerprint=fingerprint,
                file_type="会员交易明细",
                store_id="",
                store_name="门店A",
                timestamp=f"2026-03-17 10:0{row_no}:00",
                raw_data={"交易流水号": fingerprint, "交易时间": f"2026-03-17 10:0{row_no}:00", "操作门店": "门店A"},
                webhook_url="https://example.com/hook",
                session_id=session_id,
                file_id=file_id,
                source_file_name=f"{file_id}.xlsx",
                source_file_path=f"/tmp/{file_id}.xlsx",
            )

    tasks = fetch_pending_tasks(db_path, session_id="session-1")
    batches = list(iter_batches(tasks, batch_size=10))

    assert len(batches) == 2
    assert {batch.file_id for batch in batches} == {"file-0", "file-1"}
    assert all(len(batch.items) == 2 for batch in batches)


def test_push_only_session_scope_does_not_touch_other_skipped_rows(tmp_path: Path) -> None:
    from siyu_etl.db import STATUS_SKIPPED, connect, insert_task

    db_path = tmp_path / "session_scope.sqlite3"
    init_db(db_path)

    insert_task(
        db_path,
        fingerprint="skip-a",
        file_type="会员交易明细",
        store_id="",
        store_name="门店A",
        timestamp="2026-03-17 10:00:00",
        raw_data={"交易流水号": "skip-a", "交易时间": "2026-03-17 10:00:00", "操作门店": "门店A", "操作门店机构编码": "A001"},
        webhook_url="https://example.com/a",
        session_id="session-a",
        file_id="file-a",
        source_file_name="a.xlsx",
        source_file_path="/tmp/a.xlsx",
    )
    insert_task(
        db_path,
        fingerprint="skip-b",
        file_type="会员交易明细",
        store_id="",
        store_name="门店B",
        timestamp="2026-03-17 11:00:00",
        raw_data={"交易流水号": "skip-b", "交易时间": "2026-03-17 11:00:00", "操作门店": "门店B", "操作门店机构编码": "B001"},
        webhook_url="https://example.com/b",
        session_id="session-b",
        file_id="file-b",
        source_file_name="b.xlsx",
        source_file_path="/tmp/b.xlsx",
    )

    conn = connect(db_path)
    try:
        conn.execute("UPDATE upload_tasks SET status = ? WHERE session_id IN (?, ?);", (STATUS_SKIPPED, "session-a", "session-b"))
        conn.commit()
    finally:
        conn.close()

    from siyu_etl.db import requeue_skipped_member_trade_with_store_id_for_session
    updated = requeue_skipped_member_trade_with_store_id_for_session(db_path, session_id="session-a")
    assert updated == 1

    conn = connect(db_path)
    try:
        rows = conn.execute("SELECT session_id, status, store_id FROM upload_tasks ORDER BY session_id;").fetchall()
        assert rows[0][0] == "session-a" and rows[0][1] == STATUS_PENDING and rows[0][2] == "A001"
        assert rows[1][0] == "session-b" and rows[1][1] == STATUS_SKIPPED and rows[1][2] == ""
    finally:
        conn.close()
