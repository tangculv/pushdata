from __future__ import annotations

from siyu_etl.fingerprint import generate_fingerprint


def test_income_discount_fingerprint_simplified() -> None:
    """测试收入优惠统计的指纹规则：门店+营业日期+编码+结账方式类型+结账方式+类型"""
    row = {
        "门店": "A店",
        "营业日期": "2025-01-01",
        "编码": "C001",
        "结账方式类型": "类型A",
        "结账方式": "现金",
        "类型": "类型1",
    }
    fp = generate_fingerprint("收入优惠统计", row)
    # 验证指纹是基于门店+营业日期+编码+结账方式类型+结账方式+类型生成的MD5
    assert len(fp) == 32  # MD5 长度
    assert fp != ""
    
    # 相同组合应该生成相同指纹（其他列不同不影响）
    row2 = {
        "门店": "A店",
        "营业日期": "2025-01-01",
        "编码": "C001",
        "结账方式类型": "类型A",
        "结账方式": "现金",
        "类型": "类型1",
        "其他列": "任意",  # 不参与指纹
    }
    fp2 = generate_fingerprint("收入优惠统计", row2)
    assert fp == fp2  # 应该相同


def test_coupon_stat_fingerprint_simplified() -> None:
    """测试优惠券统计表的简化指纹规则：交易日期+门店+券名称"""
    row = {
        "交易日期": "2025-01-01",
        "门店": "A店",
        "券名称": "优惠券1",
        "券类型": "满减",  # 不再参与指纹生成
        "发券数量": "10",  # 不再参与指纹生成
    }
    fp = generate_fingerprint("优惠券统计表", row)
    assert len(fp) == 32  # MD5 长度
    assert fp != ""
    
    # 相同组合应该生成相同指纹
    row2 = {
        "交易日期": "2025-01-01",
        "门店": "A店",
        "券名称": "优惠券1",
        "券类型": "折扣",  # 不同值，但不影响指纹
        "发券数量": "20",  # 不同值，但不影响指纹
    }
    fp2 = generate_fingerprint("优惠券统计表", row2)
    assert fp == fp2  # 应该相同


def test_member_storage_fingerprint_simplified() -> None:
    """测试会员储值消费分析表的简化指纹规则：交易日期+机构编码+卡类型名称"""
    row = {
        "交易日期": "2025-01-01",
        "机构编码": "MD001",
        "卡类型名称": "VIP卡",
        "开卡门店": "A店",  # 不再参与指纹生成
    }
    fp = generate_fingerprint("会员储值消费分析表", row)
    assert len(fp) == 32  # MD5 长度
    assert fp != ""
    
    # 相同组合应该生成相同指纹
    row2 = {
        "交易日期": "2025-01-01",
        "机构编码": "MD001",
        "卡类型名称": "VIP卡",
        "开卡门店": "B店",  # 不同值，但不影响指纹
    }
    fp2 = generate_fingerprint("会员储值消费分析表", row2)
    assert fp == fp2  # 应该相同


def test_fingerprint_different_combinations() -> None:
    """测试不同组合生成不同指纹"""
    # 收入优惠统计：不同编码应该生成不同指纹
    row1 = {"门店": "A店", "营业日期": "2025-01-01", "编码": "C001", "结账方式类型": "A", "结账方式": "现金", "类型": "类型1"}
    row2 = {"门店": "A店", "营业日期": "2025-01-01", "编码": "C002", "结账方式类型": "A", "结账方式": "现金", "类型": "类型1"}
    fp1 = generate_fingerprint("收入优惠统计", row1)
    fp2 = generate_fingerprint("收入优惠统计", row2)
    assert fp1 != fp2  # 应该不同
    
    # 优惠券统计表：不同券名称应该生成不同指纹
    row3 = {"交易日期": "2025-01-01", "门店": "A店", "券名称": "优惠券1"}
    row4 = {"交易日期": "2025-01-01", "门店": "A店", "券名称": "优惠券2"}
    fp3 = generate_fingerprint("优惠券统计表", row3)
    fp4 = generate_fingerprint("优惠券统计表", row4)
    assert fp3 != fp4  # 应该不同
