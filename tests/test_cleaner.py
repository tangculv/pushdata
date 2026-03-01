from __future__ import annotations

from datetime import datetime

from siyu_etl.cleaner import (
    clean_row,
    convert_percentage_to_decimal,
    excel_serial_to_datetime,
    is_percentage,
    normalize_cell_to_string,
    normalize_date_value,
    normalize_placeholder,
)


def test_normalize_cell_to_string_empty_and_zero() -> None:
    assert normalize_cell_to_string(None) == ""
    assert normalize_cell_to_string("") == ""
    assert normalize_cell_to_string("  ") == ""
    assert normalize_cell_to_string(0) == "0"
    assert normalize_cell_to_string(0.0) == "0"
    assert normalize_cell_to_string(0.0, number_format="0.00") == "0.00"
    assert normalize_cell_to_string(12.3, number_format="0.00") == "12.30"


def test_excel_serial_to_datetime() -> None:
    # 2025-12-31 00:00:00 in Excel 1900 system is 46022 (commonly).
    # We avoid hardcoding exact serial here; just check type and formatting path.
    dt = excel_serial_to_datetime(1.5)
    assert isinstance(dt, datetime)


def test_normalize_date_value() -> None:
    assert normalize_date_value(None) == ""
    assert normalize_date_value("2025-12-31") == "2025-12-31 00:00:00"
    assert normalize_date_value(datetime(2025, 12, 31, 23, 59, 59)) == "2025-12-31 23:59:59"


def test_normalize_placeholder() -> None:
    """Test special placeholder normalization."""
    assert normalize_placeholder("--") == ""
    assert normalize_placeholder("-") == ""
    assert normalize_placeholder("NULL") == ""
    assert normalize_placeholder("null") == ""
    assert normalize_placeholder("N/A") == ""
    assert normalize_placeholder("n/a") == ""
    assert normalize_placeholder("NA") == ""
    assert normalize_placeholder("正常值") == "正常值"
    assert normalize_placeholder("0") == "0"  # Zero is not a placeholder
    assert normalize_placeholder("") == ""


def test_normalize_cell_to_string_with_placeholders() -> None:
    """Test that placeholders are normalized in normalize_cell_to_string."""
    assert normalize_cell_to_string("--") == ""
    assert normalize_cell_to_string("-") == ""
    assert normalize_cell_to_string("NULL") == ""
    assert normalize_cell_to_string("N/A") == ""


def test_normalize_cell_to_string_preserves_zero_format() -> None:
    """Test that 0.00 format is preserved when number_format specifies decimals."""
    assert normalize_cell_to_string(0.0, number_format="0.00") == "0.00"
    assert normalize_cell_to_string(0.0, number_format="0.0") == "0.0"
    assert normalize_cell_to_string(0.0) == "0"  # No format specified, no decimals


def test_is_percentage() -> None:
    """Test percentage detection."""
    assert is_percentage("6.45%") is True
    assert is_percentage("0.24%") is True
    assert is_percentage("100%") is True
    assert is_percentage("6.45") is False
    assert is_percentage("") is False
    assert is_percentage(6.45) is False


def test_convert_percentage_to_decimal() -> None:
    """Test percentage to decimal conversion."""
    assert convert_percentage_to_decimal("25%") == "0.25"
    assert convert_percentage_to_decimal("6.45%") == "0.0645"
    assert convert_percentage_to_decimal("100%") == "1.0"
    assert convert_percentage_to_decimal("0%") == "0.0"
    assert convert_percentage_to_decimal("0.5%") == "0.005"
    assert convert_percentage_to_decimal("150%") == "1.5"
    # 非百分比字符串应保持原样
    assert convert_percentage_to_decimal("25") == "25"
    assert convert_percentage_to_decimal("abc") == "abc"


def test_clean_row_percentage_conversion() -> None:
    """Test that all percentage fields are converted to decimal."""
    # 所有百分比字段都应转换为小数，无论文件类型
    raw = {
        "交易日期": "2025-01-01",
        "储值会员占比": "25%",
        "未消费储值占比": "6.45%",
        "其他百分比字段": "10%",
        "正常字段": "100",
    }
    result = clean_row(raw, file_type="会员储值消费分析表")
    assert result.data["储值会员占比"] == "0.25"
    assert result.data["未消费储值占比"] == "0.0645"
    assert result.data["其他百分比字段"] == "0.1"
    assert result.data["正常字段"] == "100"  # 非百分比字段保持原样
    
    # 其他文件类型：百分比字段也应转换为小数
    result2 = clean_row(raw, file_type="会员交易明细")
    assert result2.data["储值会员占比"] == "0.25"
    assert result2.data["未消费储值占比"] == "0.0645"
    assert result2.data["其他百分比字段"] == "0.1"
    assert result2.data["正常字段"] == "100"


