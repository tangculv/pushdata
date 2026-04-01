from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from siyu_etl.db import (
    BatchFileRecord,
    BatchSessionRecord,
    FILE_STATUS_PENDING_PARSE,
    SESSION_STATUS_CREATED,
    count_session_tasks,
    create_batch_file,
    create_batch_session,
    get_batch_file,
    get_batch_session,
    list_batch_sessions,
    get_tasks_count_by_file,
    list_batch_files,
    refresh_batch_session_counters,
    update_batch_file_status,
    update_batch_session_status,
)


@dataclass(frozen=True)
class SessionSummary:
    session_id: str
    status: str
    total_files: int
    parsed_files: int
    uploaded_files: int
    failed_files: int
    total_rows: int
    uploaded_rows: int
    last_error: str


class BatchService:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)

    def create_session(self, mode: str = "parse_then_upload") -> str:
        return create_batch_session(self.db_path, mode=mode)

    def add_file(
        self,
        *,
        session_id: str,
        file_path: str,
        file_name: str,
        file_size: int = 0,
        file_mtime: str = "",
        file_hash: str = "",
    ) -> str:
        return create_batch_file(
            self.db_path,
            session_id=session_id,
            file_path=file_path,
            file_name=file_name,
            file_size=file_size,
            file_mtime=file_mtime,
            file_hash=file_hash,
            status=FILE_STATUS_PENDING_PARSE,
        )

    def list_files(self, session_id: str) -> list[BatchFileRecord]:
        return list_batch_files(self.db_path, session_id=session_id)

    def get_file(self, file_id: str) -> BatchFileRecord | None:
        return get_batch_file(self.db_path, file_id=file_id)

    def get_session(self, session_id: str) -> BatchSessionRecord | None:
        return get_batch_session(self.db_path, session_id=session_id)

    def list_sessions(self, limit: int = 20) -> list[BatchSessionRecord]:
        return list_batch_sessions(self.db_path, limit=limit)

    def update_file_status(self, *, file_id: str, status: str, **kwargs) -> None:
        update_batch_file_status(self.db_path, file_id=file_id, status=status, **kwargs)

    def update_session_status(
        self,
        *,
        session_id: str,
        status: str,
        last_error: str = "",
        started: bool = False,
        finished: bool = False,
    ) -> None:
        update_batch_session_status(
            self.db_path,
            session_id=session_id,
            status=status,
            last_error=last_error,
            started=started,
            finished=finished,
        )

    def create_session_with_files(self, files: list[Path], mode: str = "parse_then_upload") -> str:
        session_id = self.create_session(mode=mode)
        if not files:
            self.update_session_status(session_id=session_id, status=SESSION_STATUS_CREATED)
            return session_id
        for fp in files:
            stat = fp.stat() if fp.exists() else None
            self.add_file(
                session_id=session_id,
                file_path=str(fp),
                file_name=fp.name,
                file_size=int(stat.st_size) if stat else 0,
                file_mtime=str(int(stat.st_mtime)) if stat else "",
            )
        refresh_batch_session_counters(self.db_path, session_id=session_id)
        return session_id

    def summary(self, session_id: str) -> SessionSummary | None:
        refresh_batch_session_counters(self.db_path, session_id=session_id)
        s = self.get_session(session_id)
        if not s:
            return None
        return SessionSummary(
            session_id=s.session_id,
            status=s.status,
            total_files=s.total_files,
            parsed_files=s.parsed_files,
            uploaded_files=s.uploaded_files,
            failed_files=s.failed_files,
            total_rows=s.total_rows,
            uploaded_rows=s.uploaded_rows,
            last_error=s.last_error,
        )

    def count_file_tasks(self, *, file_id: str, status: str | None = None) -> int:
        return get_tasks_count_by_file(self.db_path, file_id=file_id, status=status)

    def count_session_tasks(self, *, session_id: str, status: str | None = None, file_id: str | None = None) -> int:
        return count_session_tasks(self.db_path, session_id=session_id, status=status, file_id=file_id)
