"""
熔断器模块

该模块实现了熔断器模式，用于防止在服务不可用时继续发送请求。
当某个 (file_type, store_name) 组合连续失败达到阈值时，熔断器会打开，
阻止后续请求发送，直到手动重置。
"""

from __future__ import annotations

from dataclasses import dataclass

from siyu_etl.constants import DEFAULT_CIRCUIT_BREAKER_THRESHOLD


@dataclass(frozen=True)
class CircuitState:
    """
    熔断器状态数据类
    
    Attributes:
        is_open: 熔断器是否打开
        failure_count: 失败计数
        threshold: 失败阈值（达到此值则打开熔断器）
    """
    is_open: bool
    failure_count: int
    threshold: int


class CircuitBreaker:
    """
    熔断器类
    
    为每个 (file_type, store_name) 组合维护独立的熔断器状态。
    规则：连续 5 次最终失败 -> 打开熔断器；UI 可以重置。
    """

    def __init__(self, threshold: int = DEFAULT_CIRCUIT_BREAKER_THRESHOLD) -> None:
        """
        初始化熔断器
        
        Args:
            threshold: 失败阈值（默认：5）
        """
        self._threshold = int(threshold)
        self._failure_count: dict[tuple[str, str], int] = {}
        self._open: set[tuple[str, str]] = set()

    def state(self, file_type: str, store_name: str) -> CircuitState:
        """
        获取指定组合的熔断器状态
        
        Args:
            file_type: 文件类型
            store_name: 门店名称
            
        Returns:
            CircuitState 对象
        """
        key = (file_type, store_name)
        return CircuitState(
            is_open=key in self._open,
            failure_count=self._failure_count.get(key, 0),
            threshold=self._threshold,
        )

    def is_open(self, file_type: str, store_name: str) -> bool:
        """
        检查指定组合的熔断器是否打开
        
        Args:
            file_type: 文件类型
            store_name: 门店名称
            
        Returns:
            如果熔断器打开则返回 True
        """
        return (file_type, store_name) in self._open

    def record_success(self, file_type: str, store_name: str) -> None:
        """
        记录成功，重置失败计数并关闭熔断器
        
        Args:
            file_type: 文件类型
            store_name: 门店名称
        """
        key = (file_type, store_name)
        self._failure_count[key] = 0
        self._open.discard(key)

    def record_failure(self, file_type: str, store_name: str) -> bool:
        """
        Returns True if circuit becomes open after this failure.
        """
        key = (file_type, store_name)
        cnt = self._failure_count.get(key, 0) + 1
        self._failure_count[key] = cnt
        if cnt >= self._threshold:
            self._open.add(key)
            return True
        return False

    def reset(self) -> None:
        """
        重置所有熔断器状态
        
        清空所有失败计数和打开状态，用于手动重置功能。
        """
        self._failure_count.clear()
        self._open.clear()


