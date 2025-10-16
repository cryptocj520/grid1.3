"""
重构后的套利系统数据模型

定义精度信息、交易计划、执行结果等核心数据结构
"""

from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from decimal import Decimal


class ArbitrageDirection(Enum):
    """套利方向"""
    LONG_A_SHORT_B = "long_a_short_b"    # A交易所买入，B交易所卖出
    LONG_B_SHORT_A = "long_b_short_a"    # B交易所买入，A交易所卖出
    NEUTRAL = "neutral"                   # 无套利机会


class ArbitrageStatus(Enum):
    """套利状态"""
    PENDING = "pending"                   # 待执行
    EXECUTING = "executing"               # 执行中
    ACTIVE = "active"                     # 活跃中
    CLOSING = "closing"                   # 平仓中
    CLOSED = "closed"                     # 已平仓
    FAILED = "failed"                     # 执行失败
    CANCELLED = "cancelled"               # 已取消


class OrderType(Enum):
    """订单类型"""
    MARKET = "market"                     # 市价单
    LIMIT = "limit"                       # 限价单
    STOP = "stop"                         # 止损单


class ExecutionStrategy(Enum):
    """执行策略"""
    IMMEDIATE = "immediate"               # 立即执行
    TWAP = "twap"                        # 时间加权平均价格
    AGGRESSIVE = "aggressive"             # 激进执行


@dataclass
class PrecisionInfo:
    """交易精度信息"""
    symbol: str
    exchange: str
    price_precision: int                  # 价格小数位数
    amount_precision: int                 # 数量小数位数
    min_order_size: Decimal              # 最小订单数量
    max_order_size: Decimal              # 最大订单数量
    tick_size: Decimal                   # 最小价格变动
    step_size: Decimal                   # 最小数量变动
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class MarketSnapshot:
    """市场快照数据"""
    symbol: str
    timestamp: datetime
    exchanges_data: Dict[str, Any]       # 每个交易所的数据
    spread_percentage: Decimal           # 价差百分比
    direction: ArbitrageDirection        # 套利方向
    best_bid: Decimal                    # 最佳买价
    best_ask: Decimal                    # 最佳卖价
    volume_info: Dict[str, Decimal]      # 成交量信息


@dataclass
class TradePlan:
    """交易计划"""
    plan_id: str
    symbol: str
    direction: ArbitrageDirection
    
    # 交易参数
    long_exchange: str                   # 买入交易所
    short_exchange: str                  # 卖出交易所
    quantity: Decimal                    # 交易数量
    expected_profit: Decimal             # 预期利润
    
    # 执行参数
    order_type: OrderType = OrderType.MARKET
    execution_strategy: ExecutionStrategy = ExecutionStrategy.IMMEDIATE
    
    # 风险控制
    max_slippage: Decimal = Decimal('0.01')  # 最大滑点
    timeout: int = 30                    # 超时时间（秒）
    
    # 时间信息
    created_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'plan_id': self.plan_id,
            'symbol': self.symbol,
            'direction': self.direction.value,
            'long_exchange': self.long_exchange,
            'short_exchange': self.short_exchange,
            'quantity': str(self.quantity),
            'expected_profit': str(self.expected_profit),
            'order_type': self.order_type.value,
            'execution_strategy': self.execution_strategy.value,
            'max_slippage': str(self.max_slippage),
            'timeout': self.timeout,
            'created_at': self.created_at.isoformat()
        }


@dataclass
class OrderInfo:
    """订单信息"""
    order_id: str
    exchange: str
    symbol: str
    side: str                           # 'buy' or 'sell'
    amount: Decimal
    price: Optional[Decimal] = None
    filled_amount: Decimal = Decimal('0')
    status: str = "pending"
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None


@dataclass
class ExecutionResult:
    """执行结果"""
    plan_id: str
    success: bool
    long_order: Optional[OrderInfo] = None
    short_order: Optional[OrderInfo] = None
    actual_profit: Optional[Decimal] = None
    execution_time: Optional[float] = None  # 执行耗时（秒）
    error_message: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ArbitragePosition:
    """套利持仓"""
    position_id: str
    symbol: str
    direction: ArbitrageDirection
    status: ArbitrageStatus
    
    # 持仓信息
    long_exchange: str
    short_exchange: str
    quantity: Decimal
    entry_price_diff: Decimal           # 入场价差
    current_price_diff: Optional[Decimal] = None  # 当前价差
    
    # 订单信息
    long_order: Optional[OrderInfo] = None
    short_order: Optional[OrderInfo] = None
    
    # 盈亏信息
    unrealized_pnl: Decimal = Decimal('0')
    realized_pnl: Optional[Decimal] = None
    
    # 时间信息
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None


@dataclass
class RiskAssessment:
    """风险评估结果"""
    symbol: str
    risk_score: float                   # 风险得分 (0-1)
    max_position_size: Decimal          # 最大持仓规模
    recommended_size: Decimal           # 建议持仓规模
    warnings: List[str] = field(default_factory=list)
    risk_factors: Dict[str, float] = field(default_factory=dict)
    
    @property
    def is_acceptable(self) -> bool:
        """风险是否可接受"""
        return self.risk_score < 0.8  # 80%以下的风险可接受
    
    @property
    def can_execute(self) -> bool:
        """是否可以执行"""
        return self.risk_score < 0.9 and self.recommended_size > 0


@dataclass
class ArbitrageOpportunity:
    """套利机会"""
    opportunity_id: str
    symbol: str
    direction: ArbitrageDirection
    spread_percentage: Decimal
    expected_profit: Decimal
    confidence: float                   # 置信度 (0-1)
    urgency: float                     # 紧急程度 (0-1)
    market_snapshot: MarketSnapshot
    risk_assessment: RiskAssessment
    expires_at: datetime               # 机会过期时间
    detected_at: datetime = field(default_factory=datetime.now)
    
    @property
    def is_expired(self) -> bool:
        """是否已过期"""
        return datetime.now() > self.expires_at
    
    @property
    def is_valid(self) -> bool:
        """是否有效"""
        return not self.is_expired and self.risk_assessment.can_execute


# 工具函数
def adjust_precision(value: Decimal, precision: int) -> Decimal:
    """调整数值精度"""
    if precision <= 0:
        return value.quantize(Decimal('1'))
    else:
        quantizer = Decimal('0.1') ** precision
        return value.quantize(quantizer)


def calculate_spread_percentage(price_a: Decimal, price_b: Decimal) -> Decimal:
    """计算价差百分比"""
    if price_a == 0 or price_b == 0:
        return Decimal('0')
    
    avg_price = (price_a + price_b) / 2
    spread = abs(price_a - price_b)
    return (spread / avg_price) * 100


def determine_direction(price_a: Decimal, price_b: Decimal) -> ArbitrageDirection:
    """确定套利方向"""
    if price_a > price_b:
        return ArbitrageDirection.LONG_B_SHORT_A  # B交易所买入，A交易所卖出
    elif price_a < price_b:
        return ArbitrageDirection.LONG_A_SHORT_B  # A交易所买入，B交易所卖出
    else:
        return ArbitrageDirection.NEUTRAL 