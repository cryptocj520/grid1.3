"""
网格策略接口

定义网格策略的抽象接口
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Tuple
from decimal import Decimal

from ..models import GridConfig, GridOrder, GridOrderSide


class IGridStrategy(ABC):
    """
    网格策略接口
    
    定义网格策略的核心功能
    """
    
    @abstractmethod
    def initialize(self, config: GridConfig) -> List[GridOrder]:
        """
        初始化网格
        
        Args:
            config: 网格配置
        
        Returns:
            初始订单列表
        """
        pass
    
    @abstractmethod
    def calculate_reverse_order(
        self,
        filled_order: GridOrder,
        grid_interval: Decimal
    ) -> Tuple[GridOrderSide, Decimal, int]:
        """
        计算反向订单参数
        
        Args:
            filled_order: 已成交订单
            grid_interval: 网格间隔
        
        Returns:
            (订单方向, 价格, 网格ID)
        """
        pass
    
    @abstractmethod
    def get_grid_prices(self) -> List[Decimal]:
        """
        获取所有网格价格
        
        Returns:
            价格列表
        """
        pass
    
    @abstractmethod
    def validate_price_range(self, current_price: Decimal) -> bool:
        """
        验证当前价格是否在网格区间内
        
        Args:
            current_price: 当前价格
        
        Returns:
            是否在区间内
        """
        pass

