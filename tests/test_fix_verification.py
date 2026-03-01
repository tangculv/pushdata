#!/usr/bin/env python3
"""
验证修复：测试机构编码提取逻辑
"""

import sys
from pathlib import Path

from siyu_etl.fingerprint import extract_store_id, extract_store_name
from siyu_etl.excel_detect import FILETYPE_MEMBER_STORAGE

def test_extract_store_id():
    """测试机构编码提取逻辑"""
    print("=" * 80)
    print("测试机构编码提取逻辑")
    print("=" * 80)
    
    # 测试1: 机构编码在开卡门店后面，且有值
    row1 = {
        "交易日期": "2025-12-01",
        "开卡门店": "山禾田・日料小屋(宝安大悦城店)",
        "机构编码": "MD00004",
        "卡类型名称": "山禾田会员卡"
    }
    print("\n测试1: 机构编码在开卡门店后面，且有值")
    print(f"  数据: {row1}")
    store_id = extract_store_id(FILETYPE_MEMBER_STORAGE, row1)
    store_name = extract_store_name(FILETYPE_MEMBER_STORAGE, row1)
    print(f"  提取结果:")
    print(f"    store_id (机构编码): '{store_id}'")
    print(f"    store_name (开卡门店): '{store_name}'")
    assert store_id == "MD00004", f"期望 'MD00004'，实际得到 '{store_id}'"
    assert store_name == "山禾田・日料小屋(宝安大悦城店)", f"期望门店名称，实际得到 '{store_name}'"
    print("  ✓ 测试通过")
    
    # 测试2: 机构编码在开卡门店前面，且有值
    row2 = {
        "交易日期": "2025-12-01",
        "机构编码": "MD00005",
        "开卡门店": "山禾田・日料小屋(龙华壹方天地店)",
        "卡类型名称": "山禾田会员卡"
    }
    print("\n测试2: 机构编码在开卡门店前面，且有值")
    print(f"  数据: {row2}")
    store_id = extract_store_id(FILETYPE_MEMBER_STORAGE, row2)
    store_name = extract_store_name(FILETYPE_MEMBER_STORAGE, row2)
    print(f"  提取结果:")
    print(f"    store_id (机构编码): '{store_id}'")
    print(f"    store_name (开卡门店): '{store_name}'")
    assert store_id == "MD00005", f"期望 'MD00005'，实际得到 '{store_id}'"
    print("  ✓ 测试通过")
    
    # 测试3: 机构编码为空字符串
    row3 = {
        "交易日期": "2025-12-01",
        "开卡门店": "山禾田・日料小屋(宝安大悦城店)",
        "机构编码": "",
        "卡类型名称": "山禾田会员卡"
    }
    print("\n测试3: 机构编码为空字符串")
    print(f"  数据: {row3}")
    store_id = extract_store_id(FILETYPE_MEMBER_STORAGE, row3)
    store_name = extract_store_name(FILETYPE_MEMBER_STORAGE, row3)
    print(f"  提取结果:")
    print(f"    store_id (机构编码): '{store_id}'")
    print(f"    store_name (开卡门店): '{store_name}'")
    assert store_id == "", f"期望空字符串，实际得到 '{store_id}'"
    print("  ✓ 测试通过（返回空字符串，不使用store_name作为兜底）")
    
    # 测试4: 机构编码为None
    row4 = {
        "交易日期": "2025-12-01",
        "开卡门店": "山禾田・日料小屋(宝安大悦城店)",
        "机构编码": None,
        "卡类型名称": "山禾田会员卡"
    }
    print("\n测试4: 机构编码为None")
    print(f"  数据: {row4}")
    store_id = extract_store_id(FILETYPE_MEMBER_STORAGE, row4)
    store_name = extract_store_name(FILETYPE_MEMBER_STORAGE, row4)
    print(f"  提取结果:")
    print(f"    store_id (机构编码): '{store_id}'")
    print(f"    store_name (开卡门店): '{store_name}'")
    assert store_id == "", f"期望空字符串，实际得到 '{store_id}'"
    print("  ✓ 测试通过（返回空字符串，不使用store_name作为兜底）")
    
    # 测试5: 机构编码字段不存在
    row5 = {
        "交易日期": "2025-12-01",
        "开卡门店": "山禾田・日料小屋(宝安大悦城店)",
        "卡类型名称": "山禾田会员卡"
    }
    print("\n测试5: 机构编码字段不存在")
    print(f"  数据: {row5}")
    store_id = extract_store_id(FILETYPE_MEMBER_STORAGE, row5)
    store_name = extract_store_name(FILETYPE_MEMBER_STORAGE, row5)
    print(f"  提取结果:")
    print(f"    store_id (机构编码): '{store_id}'")
    print(f"    store_name (开卡门店): '{store_name}'")
    assert store_id == "", f"期望空字符串，实际得到 '{store_id}'"
    print("  ✓ 测试通过（返回空字符串，不使用store_name作为兜底）")
    
    print("\n" + "=" * 80)
    print("所有测试通过！✓")
    print("=" * 80)
    print("\n修复验证：")
    print("1. ✓ 机构编码字段无论在哪里都能正确提取")
    print("2. ✓ 即使机构编码为空，也返回空字符串，不使用开卡门店作为兜底")
    print("3. ✓ 确保分组时始终使用机构编码作为标识")

if __name__ == "__main__":
    test_extract_store_id()
