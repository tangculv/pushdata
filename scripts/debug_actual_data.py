#!/usr/bin/env python3
"""
调试实际数据：检查机构编码字段的提取问题
"""

import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

try:
    from siyu_etl.excel_detect import detect_file_type
    from siyu_etl.excel_read import read_rows
    from siyu_etl.fingerprint import extract_store_id, extract_store_name, identify_row
except ImportError as e:
    print(f"导入错误: {e}")
    print("请确保已安装所有依赖")
    sys.exit(1)

def main():
    excel_file = project_root / "data" / "山禾田_会员储值消费分析表_2026-01-24 12_17_45_a049681_1769228269877.xlsx"
    
    if not excel_file.exists():
        print(f"错误：文件不存在: {excel_file}")
        return
    
    print("=" * 100)
    print("调试：检查实际数据中的机构编码提取")
    print("=" * 100)
    
    # 检测文件类型
    result = detect_file_type(excel_file)
    print(f"\n文件类型: {result.file_type}")
    print(f"表头行索引 (0-based): {result.header_row_0based}")
    print(f"\n表头列表 (按顺序):")
    for i, header in enumerate(result.headers, 1):
        marker = " ← 机构编码" if header == "机构编码" else ""
        marker2 = " ← 开卡门店" if header == "开卡门店" else ""
        print(f"  {i:2d}. '{header}'{marker}{marker2}")
    
    # 检查字段位置
    if "机构编码" in result.headers:
        jg_idx = result.headers.index("机构编码")
        print(f"\n✓ '机构编码'在表头中的位置: 第 {jg_idx + 1} 列")
    else:
        print(f"\n✗ 表头中不包含'机构编码'字段")
    
    if "开卡门店" in result.headers:
        kkmd_idx = result.headers.index("开卡门店")
        print(f"✓ '开卡门店'在表头中的位置: 第 {kkmd_idx + 1} 列")
        if "机构编码" in result.headers:
            if jg_idx < kkmd_idx:
                print(f"  → '机构编码'在'开卡门店'前面")
            else:
                print(f"  → '机构编码'在'开卡门店'后面")
    
    # 读取前3行数据
    print(f"\n读取前3行数据:")
    print("-" * 100)
    rows = list(read_rows(
        excel_file,
        header_row_0based=result.header_row_0based,
        headers=result.headers,
        file_type=result.file_type
    ))
    
    for i, read_row in enumerate(rows[:3], 1):
        row_data = read_row.data
        print(f"\n第 {i} 行 (清洗后):")
        print(f"  字段数量: {len(row_data)}")
        print(f"  所有字段名: {list(row_data.keys())}")
        
        # 检查机构编码字段
        if "机构编码" in row_data:
            jg_code = row_data["机构编码"]
            print(f"\n  '机构编码'字段:")
            print(f"    原始值: {repr(jg_code)}")
            print(f"    类型: {type(jg_code).__name__}")
            print(f"    长度: {len(jg_code) if isinstance(jg_code, str) else 'N/A'}")
            print(f"    是否为空: {not jg_code or not str(jg_code).strip()}")
            print(f"    去除空白后: {repr(str(jg_code).strip())}")
        else:
            print(f"\n  ✗ 数据行中不包含'机构编码'字段")
            # 查找相似的字段名
            similar = [k for k in row_data.keys() if "机构" in k or "编码" in k]
            if similar:
                print(f"    相似的字段: {similar}")
        
        # 检查开卡门店字段
        if "开卡门店" in row_data:
            kkmd = row_data["开卡门店"]
            print(f"\n  '开卡门店'字段:")
            print(f"    值: {repr(kkmd)}")
        
        # 测试提取逻辑
        print(f"\n  提取测试:")
        store_id = extract_store_id(result.file_type, row_data)
        store_name_extracted = extract_store_name(result.file_type, row_data)
        
        print(f"    extract_store_id('机构编码'): '{store_id}'")
        print(f"    extract_store_name('开卡门店'): '{store_name_extracted}'")
        
        # 完整的身份识别
        ident = identify_row(
            file_type=result.file_type,
            row=row_data,
            timestamp_column=result.timestamp_column
        )
        print(f"\n  完整身份信息:")
        print(f"    store_id: '{ident.store_id}'")
        print(f"    store_name: '{ident.store_name}'")
        
        if not ident.store_id and ident.store_name:
            print(f"\n  ⚠️  问题：store_id为空，但store_name有值！")
            print(f"     这会导致程序使用store_name作为分组键，而不是store_id")
            print(f"     需要检查为什么'机构编码'字段没有被正确提取")

if __name__ == "__main__":
    main()
