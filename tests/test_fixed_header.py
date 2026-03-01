#!/usr/bin/env python3
"""
测试修复后的表头解析
"""

import sys
from pathlib import Path

# 添加项目路径到 sys.path
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

try:
    from siyu_etl.excel_detect import detect_sheet
except ImportError as e:
    print(f"导入错误: {e}")
    print("可能需要安装依赖或使用虚拟环境")
    sys.exit(1)

def main():
    excel_file = project_root / "data" / "山禾田_会员储值消费分析表_2026-01-24 12_17_45_a049681_1769228269877.xlsx"
    
    if not excel_file.exists():
        print(f"错误：文件不存在: {excel_file}")
        return
    
    print("=" * 100)
    print("测试修复后的表头解析（使用ETL模块）")
    print("=" * 100)
    
    try:
        detected = detect_sheet(excel_file)
        
        print(f"\n文件类型: {detected.file_type}")
        print(f"表头行位置 (0-based): {detected.header_row_0based}")
        print(f"时间戳列: {detected.timestamp_column}")
        print(f"\n表头总数: {len(detected.headers)}")
        print("\n所有表头列表:")
        print("-" * 100)
        
        for idx, header in enumerate(detected.headers, start=1):
            print(f"{idx:3d}. {header}")
        
        print("-" * 100)
        print(f"\n共 {len(detected.headers)} 个表头")
        
        # 验证用户特别关注的列
        print("\n" + "=" * 100)
        print("验证用户特别关注的列:")
        print("-" * 100)
        
        # 找到这些表头在列表中的位置
        target_headers = {
            "累计储值金额（元）储值余额累计（元）": "列8 (H)",
            "会员余额（元）储值余额（元）": "列11 (K)",
            "未消费储值占比未消费储值余额占比": "列14 (N)"
        }
        
        for header_text, expected_col in target_headers.items():
            if header_text in detected.headers:
                idx = detected.headers.index(header_text) + 1
                print(f"✓ 找到 '{header_text}' 在位置 {idx} ({expected_col})")
            else:
                print(f"✗ 未找到 '{header_text}'")
                # 查找类似的
                similar = [h for h in detected.headers if any(word in h for word in header_text.split())]
                if similar:
                    print(f"  类似的表头: {similar}")
        
        # 检查是否还有第3行的内容被错误合并
        print("\n" + "=" * 100)
        print("检查是否还有第3行内容被错误合并:")
        print("-" * 100)
        row3_keywords = ["累计储值金额（元）", "会员余额（元）", "未消费储值占比", "会员消费储值金额（元）"]
        found_issues = []
        for header in detected.headers:
            for keyword in row3_keywords:
                # 如果表头只包含第3行的关键词，没有第4行的内容，说明有问题
                if keyword in header and not any(
                    kw in header for kw in ["储值余额累计", "赠送余额累计", "合计", "储值余额（元）", "赠送余额（元）", "未消费储值余额占比", "消费储值余额（元）"]
                ):
                    found_issues.append(header)
        
        if found_issues:
            print("✗ 发现问题：以下表头可能只包含第3行的内容:")
            for issue in found_issues:
                print(f"  - {issue}")
        else:
            print("✓ 未发现问题：所有表头都是第4行的内容")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
