from __future__ import annotations

from pathlib import Path

from siyu_etl.config import DEFAULT_CONFIG
from siyu_etl.db import init_db, insert_task
from siyu_etl.fingerprint import generate_fingerprint, identify_row
from siyu_etl.scheduler import fetch_pending_tasks, iter_batches


def test_generate_fingerprint_member_trade() -> None:
    row = {"交易流水号": "2001", "交易时间": "2025-12-31 00:00:00"}
    assert generate_fingerprint("会员交易明细", row) == "2001"


def test_db_insert_duplicate_and_batching(tmp_path: Path) -> None:
    db_path = tmp_path / "t.sqlite3"
    init_db(db_path)

    data = {"交易流水号": "X1", "交易时间": "2025-12-31 00:00:00", "操作门店": "A"}
    ident = identify_row(file_type="会员交易明细", row=data, timestamp_column="交易时间")

    r1 = insert_task(
        db_path,
        fingerprint=ident.fingerprint,
        file_type="会员交易明细",
        store_id=ident.store_id,
        store_name=ident.store_name,
        timestamp=ident.timestamp,
        raw_data=data,
        webhook_url=DEFAULT_CONFIG.webhooks.member_trade_detail,
    )
    r2 = insert_task(
        db_path,
        fingerprint=ident.fingerprint,
        file_type="会员交易明细",
        store_id=ident.store_id,
        store_name=ident.store_name,
        timestamp=ident.timestamp,
        raw_data=data,
        webhook_url=DEFAULT_CONFIG.webhooks.member_trade_detail,
    )
    assert r1.inserted is True
    assert r2.inserted is False

    tasks = fetch_pending_tasks(db_path)
    batches = list(iter_batches(tasks, batch_size=100))
    assert len(batches) == 1
    # If store_id is empty, we group as the special '空店'
    assert batches[0].store_name in ("A", "空")


