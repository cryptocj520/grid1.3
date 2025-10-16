"""
持仓跟踪器接口

定义持仓跟踪的抽象接口
"""

from abc import ABC, abstractmethod
from typing import Dict, List
from decimal import Decimal
from datetime import datetime

from ..models import GridOrder, GridStatistics, GridMetrics


class IPositionTracker(ABC):
    """
    持仓跟踪器接口

    跟踪网格系统的持仓和盈亏
    """

    @abstractmethod
    def record_filled_order(self, order: GridOrder):
        """
        记录成交订单

        Args:
            order: 成交订单
        """
        pass

    @abstractmethod
    def get_current_position(self) -> Decimal:
        """
        获取当前持仓

        Returns:
            持仓数量（正数=多头，负数=空头）
        """
        pass

    @abstractmethod
    def get_average_cost(self) -> Decimal:
        """
        获取平均持仓成本

        Returns:
            平均成本
        """
        pass

    @abstractmethod
    def calculate_unrealized_pnl(self, current_price: Decimal) -> Decimal:
        """
        计算未实现盈亏

        Args:
            current_price: 当前价格

        Returns:
            未实现盈亏
        """
        pass

    @abstractmethod
    def get_realized_pnl(self) -> Decimal:
        """
        获取已实现盈亏

        Returns:
            已实现盈亏
        """
        pass

    @abstractmethod
    def get_total_pnl(self, current_price: Decimal) -> Decimal:
        """
        获取总盈亏（已实现+未实现）

        Args:
            current_price: 当前价格

        Returns:
            总盈亏
        """
        pass

    @abstractmethod
    def get_statistics(self) -> GridStatistics:
        """
        获取统计数据

        Returns:
            网格统计数据
        """
        pass

    @abstractmethod
    def get_metrics(self) -> GridMetrics:
        """
        获取性能指标

        Returns:
            网格性能指标
        """
        pass

    @abstractmethod
    def get_trade_history(self, limit: int = 10) -> List[Dict]:
        """
        获取交易历史

        Args:
            limit: 返回记录数

        Returns:
            交易记录列表
        """
        pass

    @abstractmethod
    def reset(self):
        """重置跟踪器"""
        pass

    @abstractmethod
    def sync_initial_position(self, position: Decimal, entry_price: Decimal):
        """
        同步初始持仓（系统启动时从交易所获取）

        Args:
            position: 初始持仓数量（正数=多仓，负数=空仓）
            entry_price: 平均入场价格
        """
        pass
