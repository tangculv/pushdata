"""
数据指纹生成和行识别模块

该模块负责：
1. 为数据行生成唯一指纹（用于去重）
2. 从数据行中提取门店 ID、门店名称、时间戳等信息
3. 根据不同的文件类型使用不同的指纹生成策略
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from siyu_etl.excel_detect import (
    FILETYPE_COUPON_STAT,
    FILETYPE_INCOME_DISCOUNT,
    FILETYPE_INSTORE_ORDER,
    FILETYPE_MEMBER_CARD_EXPORT,
    FILETYPE_MEMBER_STORAGE,
    FILETYPE_MEMBER_TRADE,
)


def md5_text(s: str) -> str:
    """
    计算字符串的 MD5 哈希值
    
    Args:
        s: 输入字符串
        
    Returns:
        MD5 哈希值的十六进制字符串
    """
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def md5_row(row: dict[str, str]) -> str:
    """
    计算数据行的 MD5 哈希值（作为兜底指纹）
    
    Args:
        row: 数据行字典
        
    Returns:
        MD5 哈希值的十六进制字符串
    """
    return md5_text(json.dumps(row, ensure_ascii=False, sort_keys=True))


def _pick_first(row: dict[str, str], keys: Iterable[str]) -> str:
    """
    从数据行中按顺序查找第一个非空值
    
    Args:
        row: 数据行字典
        keys: 要查找的键的迭代器
        
    Returns:
        第一个非空值，如果都为空则返回空字符串
    """
    for k in keys:
        # 检查键是否存在（即使值为None或空字符串，只要键存在就尝试处理）
        if k in row:
            v = row[k]
            # 如果值不是None，转换为字符串并去除空白
            if v is not None:
                v_str = str(v).strip()
                if v_str:
                    return v_str
            # 如果值为None，继续查找下一个键
    return ""


@dataclass(frozen=True)
class RowIdentity:
    """
    行身份信息数据类
    
    Attributes:
        file_type: 文件类型
        fingerprint: 行指纹（唯一标识）
        store_id: 门店 ID
        store_name: 门店名称
        timestamp: 时间戳
    """
    file_type: str
    fingerprint: str
    store_id: str
    store_name: str
    timestamp: str


def extract_store_id(file_type: str, row: dict[str, str]) -> str:
    """
    Prefer storeId when present in Excel.
    - 会员交易明细: 操作门店机构编码 (fallback: 开卡门店机构编码)
    - 店内订单明细: 机构编码
    - 优惠券统计表: 机构编码
    - 收入优惠统计: 通常无机构编码，返回空字符串
    - 会员储值消费分析表: 机构编码
    - 会员卡导出: 通常无机构编码，返回空字符串
    """
    if file_type == FILETYPE_MEMBER_TRADE:
        return _pick_first(row, ("操作门店机构编码", "开卡门店机构编码"))
    if file_type == FILETYPE_INSTORE_ORDER:
        return _pick_first(row, ("机构编码",))
    if file_type == FILETYPE_COUPON_STAT:
        return _pick_first(row, ("机构编码",))
    if file_type == FILETYPE_INCOME_DISCOUNT:
        return _pick_first(row, ("机构编码",))
    if file_type == FILETYPE_MEMBER_STORAGE:
        # 优先使用"机构编码"
        # 重要：必须直接查找"机构编码"字段，不依赖字段顺序
        # 即使字段值为空字符串，也要返回它，而不是使用store_name
        # 这样可以确保分组时使用store_id（即使为空）而不是store_name
        
        # 首先检查"机构编码"字段是否存在
        if "机构编码" in row:
            jg_code = row["机构编码"]
            # 如果值不是None，转换为字符串并去除空白
            if jg_code is not None:
                jg_code_str = str(jg_code).strip()
                # 如果有值，直接返回
                if jg_code_str:
                    return jg_code_str
                # 如果值为空字符串，也返回空字符串（不使用store_name作为兜底）
                return ""
        
        # 如果"机构编码"字段不存在，尝试查找其他可能的字段名（处理表头解析可能的差异）
        # 尝试查找包含"机构"和"编码"的字段（处理可能的字段名变体）
        for key in row.keys():
            if "机构" in key and "编码" in key and key != "机构编码":
                val = row.get(key)
                if val is not None:
                    val_str = str(val).strip()
                    if val_str:
                        return val_str
        
        # 如果找不到任何机构编码字段，返回空字符串
        return ""
    if file_type == FILETYPE_MEMBER_CARD_EXPORT:
        return _pick_first(row, ("机构编码",))  # 通常无，返回空字符串
    return _pick_first(row, ("机构编码", "操作门店机构编码", "开卡门店机构编码"))


def extract_store_name(file_type: str, row: dict[str, str]) -> str:
    """
    从数据行中提取门店名称
    
    根据不同的文件类型，使用不同的字段名提取门店名称。
    
    Args:
        file_type: 文件类型
        row: 数据行字典
        
    Returns:
        门店名称，如果找不到则返回空字符串
    """
    if file_type == FILETYPE_MEMBER_TRADE:
        return _pick_first(row, ("操作门店", "开卡门店"))
    if file_type == FILETYPE_INSTORE_ORDER:
        return _pick_first(row, ("门店", "门店名称"))
    if file_type == FILETYPE_INCOME_DISCOUNT:
        return _pick_first(row, ("门店",))
    if file_type == FILETYPE_COUPON_STAT:
        return _pick_first(row, ("门店",))
    if file_type == FILETYPE_MEMBER_STORAGE:
        return _pick_first(row, ("开卡门店",))
    return _pick_first(row, ("门店", "门店名称", "操作门店", "开卡门店"))


def extract_timestamp(file_type: str, row: dict[str, str], timestamp_column: str) -> str:
    """
    从数据行中提取时间戳
    
    首先尝试使用指定的时间戳列，如果为空则根据文件类型使用备用字段。
    
    Args:
        file_type: 文件类型
        row: 数据行字典
        timestamp_column: 时间戳列名
        
    Returns:
        时间戳字符串，如果找不到则返回空字符串
    """
    v = (row.get(timestamp_column) or "").strip()
    if v:
        return v
    # fallback per known tables
    if file_type == FILETYPE_MEMBER_TRADE:
        return _pick_first(row, ("交易时间", "结账时间"))
    if file_type == FILETYPE_INSTORE_ORDER:
        return _pick_first(row, ("下单时间", "结账时间"))
    if file_type == FILETYPE_INCOME_DISCOUNT:
        return _pick_first(row, ("营业日期",))
    if file_type == FILETYPE_COUPON_STAT:
        return _pick_first(row, ("交易日期",))
    if file_type == FILETYPE_MEMBER_STORAGE:
        return _pick_first(row, ("交易日期",))
    return ""


def generate_fingerprint(file_type: str, row: dict[str, str]) -> str:
    """
    PRD 主键规则 + 兜底：
    - 会员交易明细: 交易流水号
    - 店内订单明细: 优先 订单明细表(第一列 ID)；若不存在，退化为(订单号+下单时间+门店)；再退化为整行 MD5
    - 收入优惠统计: MD5(门店+营业日期+编码+结账方式类型+结账方式+类型)
    - 优惠券统计表: MD5(交易日期+门店+券名称)
    - 会员储值消费分析表: MD5(交易日期+机构编码+卡类型名称)
    - 会员卡导出: 会员卡号（若为空，退化为整行 MD5）
    """
    if file_type == FILETYPE_MEMBER_TRADE:
        fp = (row.get("交易流水号") or "").strip()
        return fp or md5_row(row)

    if file_type == FILETYPE_INSTORE_ORDER:
        fp = _pick_first(row, ("订单明细表", "订单明细ID", "明细ID"))
        if fp:
            return fp
        combo = "|".join(
            [
                _pick_first(row, ("门店", "门店名称")),
                _pick_first(row, ("订单号", "订单编号")),
                _pick_first(row, ("下单时间", "结账时间")),
            ]
        ).strip("|")
        return md5_text(combo) if combo else md5_row(row)

    if file_type == FILETYPE_INCOME_DISCOUNT:
        parts = [
            _pick_first(row, ("门店",)),
            _pick_first(row, ("营业日期",)),
            _pick_first(row, ("编码",)),
            _pick_first(row, ("结账方式类型",)),
            _pick_first(row, ("结账方式",)),
            _pick_first(row, ("类型",)),
        ]
        combo = "|".join(parts)
        if combo.strip("|"):
            return md5_text(combo)
        return md5_row(row)

    if file_type == FILETYPE_COUPON_STAT:
        parts = [
            _pick_first(row, ("交易日期",)),
            _pick_first(row, ("门店",)),
            _pick_first(row, ("券名称",)),
        ]
        combo = "|".join(parts)
        if combo.strip("|"):
            return md5_text(combo)
        return md5_row(row)

    if file_type == FILETYPE_MEMBER_STORAGE:
        parts = [
            _pick_first(row, ("交易日期",)),
            _pick_first(row, ("机构编码",)),
            _pick_first(row, ("卡类型名称",)),
        ]
        combo = "|".join(parts)
        if combo.strip("|"):
            return md5_text(combo)
        return md5_row(row)

    if file_type == FILETYPE_MEMBER_CARD_EXPORT:
        fp = (row.get("会员卡号") or "").strip()
        return fp or md5_row(row)

    return md5_row(row)


def identify_row(
    *,
    file_type: str,
    row: dict[str, str],
    timestamp_column: str,
) -> RowIdentity:
    """
    识别数据行的身份信息
    
    从数据行中提取所有身份信息（指纹、门店 ID、门店名称、时间戳）。
    
    Args:
        file_type: 文件类型
        row: 数据行字典
        timestamp_column: 时间戳列名
        
    Returns:
        RowIdentity 对象，包含所有身份信息
    """
    store_id = extract_store_id(file_type, row).strip()
    store_name = extract_store_name(file_type, row).strip()
    ts = extract_timestamp(file_type, row, timestamp_column=timestamp_column).strip()
    fp = generate_fingerprint(file_type, row).strip()
    return RowIdentity(file_type=file_type, fingerprint=fp, store_id=store_id, store_name=store_name, timestamp=ts)


