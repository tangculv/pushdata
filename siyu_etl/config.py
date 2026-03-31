"""
应用配置模块

该模块定义了应用程序的配置结构，包括：
1. Webhook URL 配置
2. 应用运行参数（批次大小、超时时间等）
3. 文件归档配置
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


# 项目名称
PROJECT_NAME = "siyu_etl"


@dataclass(frozen=True)
class Webhooks:
    """
    Webhook URL 配置
    
    包含各种文件类型对应的 webhook URL。
    每个字段对应一个数据源的传输接口地址。
    """
    # 会员交易明细数据源的 webhook URL
    member_trade_detail: str = (
        "https://cs.mlkee.com/api/workflow/hooks/Njk3MjQzYjM0ZTIwN2EwZGE1NDk2ZWUw"
    )
    # 收入优惠统计数据源的 webhook URL
    income_discount_stat: str = (
        "https://cs.mlkee.com/api/workflow/hooks2/Njk3MzI3YWJkMDlkNGFkYTFjOTYzZTIw"
    )
    # 优惠券统计表数据源的 webhook URL
    coupon_stat: str = (
        "https://cs.mlkee.com/api/workflow/hooks2/Njk3MzI3YWZkMDlkNGFkYTFjOTY0M2My"
    )
    # 店内订单明细数据源的 webhook URL
    in_store_order_detail: str = (
        "https://cs.mlkee.com/api/workflow/hooks2/Njk3MzIyY2ZkMDlkNGFkYTFjOTU2NTc3"
    )
    # 会员储值消费分析表数据源的 webhook URL
    member_storage_analysis: str = (
        "https://cs.mlkee.com/api/workflow/hooks2/Njk3OWNhYjZkMDlkNGFkYTFjNTNiMDk5"
    )
    # 会员卡导出数据源的 webhook URL
    member_card_export: str = (
        "https://cs.mlkee.com/api/workflow/hooks2/Njk3MzJkY2NkMDlkNGFkYTFjOTczMzg1"
    )


@dataclass
class AppConfig:
    """
    应用配置数据类
    
    包含应用程序的所有配置参数。
    
    注意：platform_key 优先从环境变量 SIYU_PLATFORM_KEY 读取，
    如果未设置则使用默认值（仅用于开发/测试）。
    """
    platform_key: str = ""
    batch_size: int = 100
    request_timeout_seconds: int = 30
    dry_run: bool = True

    archive_to_processed_dir: bool = True
    archive_suffix: str = "_processed"

    data_dir: Path = Path("data")
    db_path: Path = Path("siyu_etl.sqlite3")
    last_open_dir: str = ""

    webhooks: Webhooks = Webhooks()

    def __post_init__(self) -> None:
        """
        初始化后处理：从环境变量读取敏感信息
        """
        # 从环境变量读取 platform_key，如果未设置则使用默认值
        if not self.platform_key:
            env_key = os.getenv("SIYU_PLATFORM_KEY")
            if env_key:
                object.__setattr__(self, "platform_key", env_key)
            else:
                # 开发/测试默认值（生产环境应通过环境变量设置）
                object.__setattr__(
                    self,
                    "platform_key",
                    "f5edd587da7166bdcc6967dc2532e5aa6bcac92a09b1c3144ee05ad3e514bbf7",
                )


DEFAULT_CONFIG = AppConfig()


