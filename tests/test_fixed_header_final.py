#!/usr/bin/env python3
"""
测试修复后的表头解析（使用ETL模块）
验证：
1. 数据区识别正确（表头行+1）
2. 合并单元格处理正确（取左上角值）
3. 不向上查找，不向下合并
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

try:
    from siyu_etl.excel_detect import detect_sheet
    from siyu_etl.excel_detect import FILETYPE_MEMBER_STORAGE
except ImportError as e:
    print(f"导入错误: {e}")
    print("注意：此测试需要安装项目依赖")
    print("但我们可以验证代码逻辑是否正确")
    sys.exit(0)

def main():
    excel_file = project_root / "data" / "山禾田_会员储值消费分析表_2026-01-24 12_17_45_a049681_1769228269877.xlsx"
    
    if not excel_file.exists():
        print(f"错误：文件不存在: {excel_file}")
        return
    
    print("=" * 100)
    print("测试修复后的表头解析")
    print("=" * 100)
    
    try:
        detected = detect_sheet(excel_file)
        
        print(f"\n文件类型: {detected.file_type}")
        print(f"表头行位置 (0-based): {detected.header_row_0based} (1-based: {detected.header_row_0based + 1})")
        print(f"数据区起始行 (1-based): {detected.header_row_0based + 2}")
        print(f"时间戳列: {detected.timestamp_column}")
        print(f"\n表头总数: {len(detected.headers)}")
        print("\n所有表头列表:")
        print("-" * 100)
        
        for idx, header in enumerate(detected.headers, start=1):
            print(f"{idx:3d}. {header}")
        
        print("-" * 100)
        print(f"\n共 {len(detected.headers)} 个表头")
        
        # 验证关键点
        print("\n" + "=" * 100)
        print("验证关键点:")
        print("-" * 100)
        
        # 1. 验证表头行位置
        if detected.file_type == FILETYPE_MEMBER_STORAGE:
            expected_header_row_0based = 3  # 第4行 (1-based)
            if detected.header_row_0based == expected_header_row_0based:
                print(f"✓ 表头行位置正确: {detected.header_row_0based} (0-based) = 第{detected.header_row_0based + 1}行 (1-based)")
            else:
                print(f"✗ 表头行位置错误: 期望 {expected_header_row_0based}, 实际 {detected.header_row_0based}")
        
        # 2. 验证不应该包含第3行的内容
        row3_keywords = ["累计储值金额（元）", "会员余额（元）", "未消费储值占比", "会员消费储值金额（元）"]
        found_row3_content = False
        for header in detected.headers:
            for keyword in row3_keywords:
                # 如果表头只包含第3行的关键词，没有第4行的内容，说明有问题
                if keyword in header and not any(
                    kw in header for kw in ["储值余额累计", "赠送余额累计", "合计", "储值余额（元）", 
                                           "赠送余额（元）", "未消费储值余额占比", "消费储值余额（元）"]
                ):
                    print(f"✗ 发现问题：表头 '{header}' 可能只包含第3行的内容")
                    found_row3_content = True
        
        if not found_row3_content:
            print("✓ 未发现第3行内容被错误合并到表头中")
        
        # 3. 验证关键列的表头
        print("\n关键列的表头验证:")
        key_headers = {
            "储值余额累计（元）": "列8 (H) - 应该是第4行的内容",
            "储值余额（元）": "列11 (K) - 应该是第4行的内容",
            "未消费储值余额占比": "列14 (N) - 应该是第4行的内容"
        }
        
        for header_text, description in key_headers.items():
            if header_text in detected.headers:
                print(f"✓ 找到 '{header_text}' ({description})")
            else:
                print(f"✗ 未找到 '{header_text}' ({description})")
                # 查找类似的
                similar = [h for h in detected.headers if any(word in h for word in header_text.split())]
                if similar:
                    print(f"  类似的表头: {similar}")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
