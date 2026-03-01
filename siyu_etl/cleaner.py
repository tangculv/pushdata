"""
数据清洗模块

该模块负责：
1. 将 Excel 单元格值标准化为字符串
2. 处理日期时间格式转换
3. 处理占位符值（如 "--", "NULL" 等）
4. 处理百分比值
5. 保持空值和零值的严格区分
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from dateutil import parser as date_parser


# 日期列关键词，用于识别日期列
DATE_COLUMN_KEYWORDS = ("时间", "日期", "date", "time")

# 特殊占位符值，应转换为空字符串
# 根据 PRD：这些值应被视为空值
PLACEHOLDER_VALUES = {
    "--",
    "-",
    "NULL",
    "null",
    "Null",
    "N/A",
    "n/a",
    "NA",
}


def is_date_column(col_name: str) -> bool:
    """
    判断列名是否为日期列
    
    Args:
        col_name: 列名
        
    Returns:
        如果是日期列则返回 True，否则返回 False
    """
    s = (col_name or "").strip().lower()
    return any(k in s for k in DATE_COLUMN_KEYWORDS)


def normalize_placeholder(value: str) -> str:
    """
    Normalize special placeholder values to empty string.
    According to PRD: "--", "-", "NULL", "N/A" etc. should be converted to "".
    """
    if not isinstance(value, str):
        return value
    s = value.strip()
    if s in PLACEHOLDER_VALUES:
        return ""
    return s


# 用于从 Excel 数字格式中提取小数位数的正则表达式
_DECIMALS_RE = re.compile(r"\.(0+)")


def _decimals_from_excel_number_format(fmt: str) -> Optional[int]:
    """
    从 Excel 数字格式字符串中提取小数位数
    
    支持的格式模式示例：
      - '0.00' -> 2
      - '#,##0.000' -> 3
    
    Args:
        fmt: Excel 数字格式字符串
        
    Returns:
        小数位数，如果无法提取则返回 None
    """
    if not fmt:
        return None
    m = _DECIMALS_RE.search(fmt)
    if not m:
        return None
    return len(m.group(1))


def _format_number(v: float, decimals: Optional[int]) -> str:
    """
    格式化数字为字符串
    
    Args:
        v: 浮点数
        decimals: 小数位数，如果指定则使用该位数，否则自动去除尾随零
        
    Returns:
        格式化后的字符串
    """
    if decimals is not None and decimals >= 0:
        return format(v, f".{decimals}f")
    # 避免科学计数法；保持最小的小数字符串
    s = format(v, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def normalize_cell_to_string(
    value: Any,
    *,
    number_format: str | None = None,
) -> str:
    """
    Convert a raw cell value to string with strict empty vs zero distinction.
    - None / empty -> ""
    - numeric -> string (try to respect number_format decimals for cases like 0.00)
    - special placeholders -> "" (after normalization)
    """
    if value is None:
        return ""

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, (int, float)):
        decimals = _decimals_from_excel_number_format(number_format or "")
        result = _format_number(float(value), decimals)
        # Handle special case: if number_format specifies decimals and value is 0.00, preserve format
        if decimals is not None and decimals > 0 and float(value) == 0.0:
            result = format(0.0, f".{decimals}f")
        return result

    s = str(value).strip()
    if s == "":
        return ""
    
    # Normalize placeholders
    s = normalize_placeholder(s)
    
    return s


def excel_serial_to_datetime(excel_days: float) -> datetime:
    """
    将 Excel 序列日期转换为 Python datetime 对象
    
    Excel 序列日期系统：第 0 天是 1899-12-30（常见转换方式）
    
    Args:
        excel_days: Excel 序列日期（浮点数，可能包含时间部分）
        
    Returns:
        对应的 datetime 对象
    """
    base = datetime(1899, 12, 30)
    return base + timedelta(days=float(excel_days))


def normalize_date_value(value: Any) -> str:
    """
    - None/empty -> ""
    - datetime -> YYYY-MM-DD HH:mm:ss
    - number -> Excel serial date -> YYYY-MM-DD HH:mm:ss
    - string -> try parse; if fail keep original string
    - placeholders -> "" (after normalization)
    """
    if value is None:
        return ""

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, (int, float)):
        try:
            dt = excel_serial_to_datetime(float(value))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return normalize_cell_to_string(value)

    s = str(value).strip()
    if s == "":
        return ""
    
    # Normalize placeholders first
    s = normalize_placeholder(s)
    if s == "":
        return ""

    try:
        dt = date_parser.parse(s)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return s


@dataclass(frozen=True)
class CleanResult:
    """
    清洗结果数据类
    
    Attributes:
        data: 清洗后的数据字典（所有值都是字符串）
        warnings: 警告信息列表
    """
    data: dict[str, str]
    warnings: list[str]


def is_percentage(value: str) -> bool:
    """
    Check if a string value is a percentage (ends with %).
    All percentage values will be converted to decimal format.
    """
    if not isinstance(value, str):
        return False
    return value.strip().endswith("%")


def convert_percentage_to_decimal(value: str) -> str:
    """
    将百分比字符串转换为小数字符串
    
    例如：
    - "25%" -> "0.25"
    - "6.45%" -> "0.0645"
    - "100%" -> "1.0"
    - "0%" -> "0.0"
    
    Args:
        value: 百分比字符串（如 "25%"）
        
    Returns:
        小数字符串（如 "0.25"）
    """
    if not isinstance(value, str):
        return value
    
    s = value.strip()
    if not s.endswith("%"):
        return value
    
    try:
        # 移除 % 符号并转换为浮点数
        num_value = float(s[:-1])
        # 除以 100 得到小数
        decimal_value = num_value / 100.0
        # 转换为字符串，保留必要的小数位
        # 如果结果是整数，保留一位小数（如 1.0），否则保留原精度
        if decimal_value == int(decimal_value):
            return f"{decimal_value:.1f}"
        # 去除尾随零，但至少保留一位小数
        result = f"{decimal_value:.10f}".rstrip("0").rstrip(".")
        if "." not in result:
            result += ".0"
        return result
    except (ValueError, TypeError):
        # 如果转换失败，返回原值
        return value


def clean_row(
    raw: dict[str, Any],
    *,
    number_formats: dict[str, str] | None = None,
    file_type: str | None = None,
) -> CleanResult:
    """
    清洗一行数据，转换为纯字符串字典
    
    处理规则：
    1. 日期列：标准化为 YYYY-MM-DD HH:mm:ss 格式
    2. 百分比值：所有百分比字段都转换为小数字符串（如 "25%" -> "0.25"，"6.45%" -> "0.0645"）
    3. 数值：转换为字符串，尊重 number_format
    4. 占位符：标准化为空字符串
    5. 空值 vs 零值：严格区分（空值 -> ""，零值 -> "0" 或 "0.00"）
    
    Args:
        raw: 原始数据字典
        number_formats: 可选的表头到 Excel 数字格式的映射
        file_type: 文件类型（当前未使用，保留用于未来扩展）
        
    Returns:
        CleanResult 对象，包含清洗后的数据和警告信息
    """
    warnings: list[str] = []
    out: dict[str, str] = {}
    
    for k, v in raw.items():
        if is_date_column(k):
            out[k] = normalize_date_value(v)
            if out[k] != "" and v is not None and isinstance(v, str) and out[k] == v.strip():
                # string date parse failed; keep as-is, but warn.
                # (If parse succeeded, formatted value would differ)
                pass
        else:
            # 检查是否是百分比字符串，如果是则转换为小数
            if isinstance(v, str) and is_percentage(v):
                out[k] = convert_percentage_to_decimal(v)
            else:
                out[k] = normalize_cell_to_string(v, number_format=(number_formats or {}).get(k))
        if out[k] == "" and v not in (None, "", " "):
            # This can happen for values that stringify to empty unexpectedly.
            pass
    return CleanResult(data=out, warnings=warnings)


