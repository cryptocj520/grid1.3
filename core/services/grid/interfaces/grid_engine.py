"""
网格执行引擎接口

定义网格交易执行引擎的抽象接口
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Callable
from decimal import Decimal

from ..models import GridOrder, GridConfig


class IGridEngine(ABC):
    """
    网格执行引擎接口
    
    负责订单的执行和管理
    """
    
    @abstractmethod
    async def initialize(self, config: GridConfig):
        """
        初始化执行引擎
        
        Args:
            config: 网格配置
        """
        pass
    
    @abstractmethod
    async def place_order(self, order: GridOrder) -> GridOrder:
        """
        下单
        
        Args:
            order: 网格订单
        
        Returns:
            更新后的订单（包含交易所订单ID）
        """
        pass
    
    @abstractmethod
    async def place_batch_orders(self, orders: List[GridOrder]) -> List[GridOrder]:
        """
        批量下单
        
        Args:
            orders: 订单列表
        
        Returns:
            更新后的订单列表
        """
        pass
    
    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """
        取消订单
        
        Args:
            order_id: 订单ID
        
        Returns:
            是否成功
        """
        pass
    
    @abstractmethod
    async def cancel_all_orders(self) -> int:
        """
        取消所有订单
        
        Returns:
            取消的订单数量
        """
        pass
    
    @abstractmethod
    async def get_order_status(self, order_id: str) -> Optional[GridOrder]:
        """
        查询订单状态
        
        Args:
            order_id: 订单ID
        
        Returns:
            订单信息
        """
        pass
    
    @abstractmethod
    async def get_current_price(self) -> Decimal:
        """
        获取当前市场价格
        
        Returns:
            当前价格
        """
        pass
    
    @abstractmethod
    def subscribe_order_updates(self, callback: Callable):
        """
        订阅订单更新
        
        Args:
            callback: 回调函数，接收订单更新
        """
        pass
    
    @abstractmethod
    async def start(self):
        """启动执行引擎"""
        pass
    
    @abstractmethod
    async def stop(self):
        """停止执行引擎"""
        pass

