"""
配置管理模块

该模块负责：
1. 从 JSON 文件加载配置
2. 将配置保存到 JSON 文件
3. 提供默认配置路径
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from siyu_etl.config import AppConfig, Webhooks


def default_config_path() -> Path:
    """
    获取默认配置文件路径
    
    项目本地配置文件，便于交付。
    
    Returns:
        配置文件路径
    """
    return Path("siyu_etl_config.json")


def load_config(path: Path | None = None) -> AppConfig:
    """
    从 JSON 文件加载配置
    
    如果文件不存在或解析失败，返回默认配置。
    
    Args:
        path: 配置文件路径，如果为 None 则使用默认路径
        
    Returns:
        AppConfig 对象
    """
    p = path or default_config_path()
    if not p.exists():
        return AppConfig()
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return AppConfig()

    cfg = AppConfig()
    cfg.platform_key = str(raw.get("platform_key", cfg.platform_key))
    cfg.batch_size = int(raw.get("batch_size", cfg.batch_size))
    cfg.request_timeout_seconds = int(raw.get("request_timeout_seconds", cfg.request_timeout_seconds))
    cfg.dry_run = bool(raw.get("dry_run", cfg.dry_run))
    cfg.archive_to_processed_dir = bool(raw.get("archive_to_processed_dir", cfg.archive_to_processed_dir))
    cfg.archive_suffix = str(raw.get("archive_suffix", cfg.archive_suffix))
    cfg.last_open_dir = str(raw.get("last_open_dir", cfg.last_open_dir))

    wh_raw: dict[str, Any] = raw.get("webhooks", {}) if isinstance(raw.get("webhooks"), dict) else {}
    cfg.webhooks = Webhooks(
        member_trade_detail=str(wh_raw.get("member_trade_detail", cfg.webhooks.member_trade_detail)),
        income_discount_stat=str(wh_raw.get("income_discount_stat", cfg.webhooks.income_discount_stat)),
        coupon_stat=str(wh_raw.get("coupon_stat", cfg.webhooks.coupon_stat)),
        in_store_order_detail=str(wh_raw.get("in_store_order_detail", cfg.webhooks.in_store_order_detail)),
        member_storage_analysis=str(wh_raw.get("member_storage_analysis", cfg.webhooks.member_storage_analysis)),
        member_card_export=str(wh_raw.get("member_card_export", cfg.webhooks.member_card_export)),
    )
    return cfg


def save_config(cfg: AppConfig, path: Path | None = None) -> None:
    """
    将配置保存到 JSON 文件
    
    Args:
        cfg: 应用配置对象
        path: 配置文件路径，如果为 None 则使用默认路径
    """
    p = path or default_config_path()
    data = asdict(cfg)
    # Convert Path to str for json
    for k in ("data_dir", "db_path"):
        if k in data:
            data[k] = str(data[k])
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


