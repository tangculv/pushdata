from pathlib import Path

from siyu_etl.batch_service import BatchService
from siyu_etl.db import (
    FILE_STATUS_READY_TO_UPLOAD,
    SESSION_STATUS_PARSED,
    init_db,
    insert_task,
)


def test_find_latest_resumable_session(tmp_path):
    db_path = tmp_path / 'resume.sqlite3'
    init_db(db_path)
    svc = BatchService(db_path)

    old_session = svc.create_session()
    old_file = svc.add_file(session_id=old_session, file_path='a.xlsx', file_name='a.xlsx')
    svc.update_file_status(file_id=old_file, status=FILE_STATUS_READY_TO_UPLOAD, parse_rows=1)
    svc.update_session_status(session_id=old_session, status=SESSION_STATUS_PARSED)
    insert_task(
        db_path,
        fingerprint='old-1',
        file_type='收入优惠统计',
        store_id='001',
        store_name='门店A',
        timestamp='2026-04-01 10:00:00',
        raw_data={'a': 1},
        session_id=old_session,
        file_id=old_file,
        source_file_name='a.xlsx',
        source_file_path='a.xlsx',
    )

    new_session = svc.create_session()
    new_file = svc.add_file(session_id=new_session, file_path='b.xlsx', file_name='b.xlsx')
    svc.update_file_status(file_id=new_file, status=FILE_STATUS_READY_TO_UPLOAD, parse_rows=2)
    svc.update_session_status(session_id=new_session, status=SESSION_STATUS_PARSED)
    insert_task(
        db_path,
        fingerprint='new-1',
        file_type='收入优惠统计',
        store_id='002',
        store_name='门店B',
        timestamp='2026-04-01 11:00:00',
        raw_data={'b': 2},
        session_id=new_session,
        file_id=new_file,
        source_file_name='b.xlsx',
        source_file_path='b.xlsx',
    )

    sessions = svc.list_sessions(limit=20)
    picked = ''
    pending_rows = 0
    for session in sessions:
        pending = svc.count_session_tasks(session_id=session.session_id, status='PENDING')
        if pending <= 0:
            continue
        if session.status in {'COMPLETED'}:
            continue
        picked = session.session_id
        pending_rows = pending
        break

    assert picked == new_session
    assert pending_rows == 1
