"""
网格订单模型

定义网格交易订单的数据结构
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional
from decimal import Decimal
from datetime import datetime


class GridOrderSide(Enum):
    """订单方向"""
    BUY = "buy"      # 买单
    SELL = "sell"    # 卖单


class GridOrderStatus(Enum):
    """订单状态"""
    PENDING = "pending"        # 挂单中（等待成交）
    FILLED = "filled"          # 已成交
    CANCELLED = "cancelled"    # 已取消
    FAILED = "failed"          # 失败


@dataclass
class GridOrder:
    """
    网格订单
    
    代表网格系统中的一个限价订单
    """
    
    # 订单标识
    order_id: str                          # 交易所订单ID
    grid_id: int                           # 所属网格层级 (1-based)
    
    # 订单参数
    side: GridOrderSide                    # 订单方向（买/卖）
    price: Decimal                         # 限价价格
    amount: Decimal                        # 订单数量
    
    # 订单状态
    status: GridOrderStatus                # 订单状态
    
    # 时间信息
    created_at: datetime                   # 创建时间
    filled_at: Optional[datetime] = None   # 成交时间
    
    # 成交信息
    filled_price: Optional[Decimal] = None # 实际成交价格
    filled_amount: Optional[Decimal] = None # 实际成交数量
    
    # 关联信息
    parent_order_id: Optional[str] = None  # 父订单ID（如果是反向订单）
    reverse_order_id: Optional[str] = None # 反向订单ID（成交后创建的订单）
    
    # 额外数据
    exchange_data: dict = None             # 交易所原始数据
    
    def __post_init__(self):
        """初始化后处理"""
        if self.exchange_data is None:
            self.exchange_data = {}
    
    def mark_filled(self, filled_price: Decimal, filled_amount: Decimal):
        """标记订单为已成交"""
        self.status = GridOrderStatus.FILLED
        self.filled_at = datetime.now()
        self.filled_price = filled_price
        self.filled_amount = filled_amount
    
    def mark_cancelled(self):
        """标记订单为已取消"""
        self.status = GridOrderStatus.CANCELLED
    
    def mark_failed(self):
        """标记订单为失败"""
        self.status = GridOrderStatus.FAILED
    
    def is_buy_order(self) -> bool:
        """是否为买单"""
        return self.side == GridOrderSide.BUY
    
    def is_sell_order(self) -> bool:
        """是否为卖单"""
        return self.side == GridOrderSide.SELL
    
    def is_filled(self) -> bool:
        """是否已成交"""
        return self.status == GridOrderStatus.FILLED
    
    def is_pending(self) -> bool:
        """是否挂单中"""
        return self.status == GridOrderStatus.PENDING
    
    def get_total_value(self) -> Decimal:
        """获取订单总价值"""
        return self.price * self.amount
    
    def get_profit_from_reverse(self, reverse_price: Decimal) -> Decimal:
        """
        计算与反向订单的利润
        
        Args:
            reverse_price: 反向订单的价格
        
        Returns:
            利润金额（不含手续费）
        """
        if self.is_buy_order():
            # 买单：反向卖单价格更高才有利润
            return (reverse_price - self.price) * self.amount
        else:
            # 卖单：反向买单价格更低才有利润
            return (self.price - reverse_price) * self.amount
    
    def __repr__(self) -> str:
        return (
            f"GridOrder(id={self.order_id}, grid={self.grid_id}, "
            f"{self.side.value} {self.amount}@{self.price}, status={self.status.value})"
        )

