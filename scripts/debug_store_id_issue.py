#!/usr/bin/env python3
"""
调试脚本：检查会员储值消费分析表的机构编码提取问题
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

from siyu_etl.excel_detect import detect_file_type
from siyu_etl.excel_parse import parse_excel_file
from siyu_etl.fingerprint import extract_store_id, extract_store_name, identify_row

def main():
    excel_file = project_root / "data" / "山禾田_会员储值消费分析表_2026-01-24 12_17_45_a049681_1769228269877.xlsx"
    
    if not excel_file.exists():
        print(f"错误：文件不存在: {excel_file}")
        return
    
    print("=" * 100)
    print("调试：会员储值消费分析表的机构编码提取问题")
    print("=" * 100)
    
    # 检测文件类型
    result = detect_file_type(excel_file)
    print(f"\n文件类型: {result.file_type}")
    print(f"表头行索引 (0-based): {result.header_row_0based}")
    print(f"\n表头列表:")
    for i, header in enumerate(result.headers, 1):
        print(f"  {i}. '{header}'")
    
    # 检查是否有"机构编码"字段
    if "机构编码" in result.headers:
        print(f"\n✓ 表头中包含'机构编码'字段")
        idx = result.headers.index("机构编码")
        print(f"  位置: 第 {idx + 1} 列")
    else:
        print(f"\n✗ 表头中不包含'机构编码'字段")
        # 查找相似的字段名
        similar = [h for h in result.headers if "机构" in h or "编码" in h]
        if similar:
            print(f"  相似的字段: {similar}")
    
    # 解析前几行数据
    print(f"\n解析前3行数据:")
    print("-" * 100)
    rows = list(parse_excel_file(excel_file, result))
    
    for i, row_data in enumerate(rows[:3], 1):
        print(f"\n第 {i} 行:")
        print(f"  所有字段: {list(row_data.keys())}")
        
        # 检查机构编码字段
        if "机构编码" in row_data:
            jg_code = row_data["机构编码"]
            print(f"  '机构编码'字段值: '{jg_code}' (类型: {type(jg_code).__name__})")
            print(f"  '机构编码'是否为空: {not jg_code or not str(jg_code).strip()}")
        else:
            print(f"  ✗ 数据行中不包含'机构编码'字段")
            # 查找相似的字段名
            similar = [k for k in row_data.keys() if "机构" in k or "编码" in k]
            if similar:
                print(f"  相似的字段: {similar}")
        
        # 检查开卡门店字段
        if "开卡门店" in row_data:
            store_name = row_data["开卡门店"]
            print(f"  '开卡门店'字段值: '{store_name}'")
        
        # 测试提取逻辑
        store_id = extract_store_id(result.file_type, row_data)
        store_name_extracted = extract_store_name(result.file_type, row_data)
        
        print(f"\n  提取结果:")
        print(f"    store_id (机构编码): '{store_id}'")
        print(f"    store_name (开卡门店): '{store_name_extracted}'")
        
        # 完整的身份识别
        ident = identify_row(
            file_type=result.file_type,
            row=row_data,
            timestamp_column=result.timestamp_column
        )
        print(f"\n  完整身份信息:")
        print(f"    fingerprint: {ident.fingerprint[:32]}...")
        print(f"    store_id: '{ident.store_id}'")
        print(f"    store_name: '{ident.store_name}'")
        print(f"    timestamp: '{ident.timestamp}'")
        
        if not ident.store_id and ident.store_name:
            print(f"\n  ⚠️  问题：store_id为空，但store_name有值！")
            print(f"     这会导致程序使用store_name作为分组键，而不是store_id")

if __name__ == "__main__":
    main()
