#!/usr/bin/env python3
"""
测试字段顺序问题：检查机构编码是否因为字段顺序而被忽略
"""

# 模拟数据，模拟"开卡门店"在"机构编码"前面的情况
test_row1 = {
    "交易日期": "2025-12-01",
    "开卡门店": "山禾田・日料小屋(宝安大悦城店)",
    "机构编码": "MD00004",
    "卡类型名称": "山禾田会员卡"
}

# 模拟数据，模拟"机构编码"在"开卡门店"前面的情况
test_row2 = {
    "交易日期": "2025-12-01",
    "机构编码": "MD00004",
    "开卡门店": "山禾田・日料小屋(宝安大悦城店)",
    "卡类型名称": "山禾田会员卡"
}

# 模拟数据，机构编码为空字符串
test_row3 = {
    "交易日期": "2025-12-01",
    "开卡门店": "山禾田・日料小屋(宝安大悦城店)",
    "机构编码": "",
    "卡类型名称": "山禾田会员卡"
}

# 模拟数据，机构编码为None
test_row4 = {
    "交易日期": "2025-12-01",
    "开卡门店": "山禾田・日料小屋(宝安大悦城店)",
    "机构编码": None,
    "卡类型名称": "山禾田会员卡"
}

def _pick_first(row: dict[str, str], keys: tuple[str, ...]) -> str:
    """模拟当前的_pick_first实现"""
    for k in keys:
        # 直接获取值，不先转换为空字符串
        v = row.get(k)
        if v is not None:
            v_str = str(v).strip()
            if v_str:
                return v_str
    return ""

print("=" * 80)
print("测试字段顺序对提取的影响")
print("=" * 80)

print("\n测试1: 开卡门店在机构编码前面，机构编码有值")
print(f"  数据: {test_row1}")
result = _pick_first(test_row1, ("机构编码",))
print(f"  提取结果: '{result}'")
print(f"  是否正确: {'✓' if result == 'MD00004' else '✗'}")

print("\n测试2: 机构编码在开卡门店前面，机构编码有值")
print(f"  数据: {test_row2}")
result = _pick_first(test_row2, ("机构编码",))
print(f"  提取结果: '{result}'")
print(f"  是否正确: {'✓' if result == 'MD00004' else '✗'}")

print("\n测试3: 机构编码为空字符串")
print(f"  数据: {test_row3}")
result = _pick_first(test_row3, ("机构编码",))
print(f"  提取结果: '{result}'")
print(f"  是否正确: {'✓' if result == '' else '✗'}")

print("\n测试4: 机构编码为None")
print(f"  数据: {test_row4}")
result = _pick_first(test_row4, ("机构编码",))
print(f"  提取结果: '{result}'")
print(f"  是否正确: {'✓' if result == '' else '✗'}")

print("\n" + "=" * 80)
print("检查字典键的顺序")
print("=" * 80)
print(f"test_row1的键顺序: {list(test_row1.keys())}")
print(f"test_row2的键顺序: {list(test_row2.keys())}")
