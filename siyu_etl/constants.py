"""
项目常量定义模块

集中管理所有配置常量，便于统一调整和维护。
"""

from __future__ import annotations

# Excel 文件处理限制
MAX_SCAN_ROWS = 300_000  # Excel 文件最大扫描行数（20万行）
MAX_SCAN_COLS = 250  # Excel 文件最大扫描列数

# 熔断器配置
DEFAULT_CIRCUIT_BREAKER_THRESHOLD = 5  # 默认熔断器失败阈值（连续失败5次后熔断）

# 网络请求配置
RETRY_BACKOFFS = [2, 5, 10]  # 重试间隔（秒），指数退避策略
