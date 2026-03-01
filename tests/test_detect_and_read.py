from __future__ import annotations

from pathlib import Path

import openpyxl

from siyu_etl.excel_detect import (
    FILETYPE_MEMBER_TRADE,
    detect_sheet,
)
from siyu_etl.excel_read import read_rows


def _make_member_trade_xlsx(path: Path) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["会员交易明细（测试）"])
    ws.append(["交易流水号", "交易时间", "操作门店", "交易金额"])
    ws.append(["T1", "2025-12-31 23:59:54", "山禾田 ·日料小屋（龙华壹方天地店）", 0.0])
    ws.append(["T2", "2025-12-31 23:00:00", "山禾田 ·日料小屋（龙华壹方天地店）", 12.3])
    wb.save(path)


def test_detect_and_read_member_trade(tmp_path: Path) -> None:
    p = tmp_path / "山禾田-会员交易明细-测试.xlsx"
    _make_member_trade_xlsx(p)

    det = detect_sheet(p)
    assert det.file_type == FILETYPE_MEMBER_TRADE
    assert det.header_row_0based == 1

    rows = list(read_rows(p, header_row_0based=det.header_row_0based, headers=det.headers))
    assert len(rows) == 2
    assert rows[0].data["交易流水号"] == "T1"
    assert rows[0].data["交易时间"].startswith("2025-12-31")


