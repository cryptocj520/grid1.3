"""
交易所数据模型和枚举定义

定义了交易所适配层使用的所有数据结构和枚举类型，
确保不同交易所之间数据格式的统一性和一致性。
"""

from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from decimal import Decimal


class ExchangeType(Enum):
    """交易所类型枚举"""
    SPOT = "spot"                    # 现货交易
    FUTURES = "futures"              # 期货交易
    PERPETUAL = "perpetual"          # 永续合约
    OPTIONS = "options"              # 期权交易
    MARGIN = "margin"                # 杠杆交易


class OrderSide(Enum):
    """订单方向枚举"""
    BUY = "buy"                      # 买入
    SELL = "sell"                    # 卖出


class OrderType(Enum):
    """订单类型枚举"""
    MARKET = "market"                # 市价单
    LIMIT = "limit"                  # 限价单
    STOP = "stop"                    # 止损单
    STOP_LIMIT = "stop_limit"        # 止损限价单
    TAKE_PROFIT = "take_profit"      # 止盈单
    TAKE_PROFIT_LIMIT = "take_profit_limit"  # 止盈限价单
    IOC = "ioc"                      # 立即成交或撤销
    FOK = "fok"                      # 全部成交或撤销
    POST_ONLY = "post_only"          # 只做Maker


class OrderStatus(Enum):
    """订单状态枚举"""
    PENDING = "pending"              # 待处理
    OPEN = "open"                    # 开放/部分成交
    FILLED = "filled"                # 完全成交
    CANCELED = "canceled"            # 已撤销
    REJECTED = "rejected"            # 已拒绝
    EXPIRED = "expired"              # 已过期
    UNKNOWN = "unknown"              # 未知状态


class PositionSide(Enum):
    """持仓方向枚举"""
    LONG = "long"                    # 多头
    SHORT = "short"                  # 空头
    BOTH = "both"                    # 双向持仓


class MarginMode(Enum):
    """保证金模式枚举"""
    CROSS = "cross"                  # 全仓模式
    ISOLATED = "isolated"            # 逐仓模式


@dataclass
class OrderData:
    """订单数据模型"""
    id: str                          # 订单ID
    client_id: Optional[str]         # 客户端订单ID
    symbol: str                      # 交易对
    side: OrderSide                  # 订单方向
    type: OrderType                  # 订单类型
    amount: Decimal                  # 订单数量
    price: Optional[Decimal]         # 订单价格
    filled: Decimal                  # 已成交数量
    remaining: Decimal               # 剩余数量
    cost: Decimal                    # 成交金额
    average: Optional[Decimal]       # 平均成交价
    status: OrderStatus              # 订单状态
    timestamp: datetime              # 创建时间
    updated: Optional[datetime]      # 更新时间
    fee: Optional[Dict[str, Any]]    # 手续费信息
    trades: List[Dict[str, Any]]     # 成交记录
    params: Dict[str, Any]           # 额外参数
    raw_data: Dict[str, Any]         # 原始数据

    def __post_init__(self):
        """数据验证和转换"""
        if isinstance(self.amount, (int, float, str)):
            self.amount = Decimal(str(self.amount))
        if isinstance(self.filled, (int, float, str)):
            self.filled = Decimal(str(self.filled))
        if isinstance(self.remaining, (int, float, str)):
            self.remaining = Decimal(str(self.remaining))
        if isinstance(self.cost, (int, float, str)):
            self.cost = Decimal(str(self.cost))
        if self.price is not None and isinstance(self.price, (int, float, str)):
            self.price = Decimal(str(self.price))
        if self.average is not None and isinstance(self.average, (int, float, str)):
            self.average = Decimal(str(self.average))


@dataclass
class PositionData:
    """持仓数据模型"""
    symbol: str                      # 交易对
    side: PositionSide               # 持仓方向
    size: Decimal                    # 持仓数量
    entry_price: Decimal             # 开仓均价
    mark_price: Optional[Decimal]    # 标记价格
    current_price: Optional[Decimal]  # 当前价格
    unrealized_pnl: Decimal         # 未实现盈亏
    realized_pnl: Decimal           # 已实现盈亏
    percentage: Optional[Decimal]    # 收益率百分比
    leverage: int                    # 杠杆倍数
    margin_mode: MarginMode          # 保证金模式
    margin: Decimal                  # 保证金
    liquidation_price: Optional[Decimal]  # 强平价格
    timestamp: datetime              # 更新时间
    raw_data: Dict[str, Any]         # 原始数据

    def __post_init__(self):
        """数据验证和转换"""
        # 处理持仓数量，None转为0
        if self.size is None:
            self.size = Decimal('0')
        elif isinstance(self.size, (int, float, str)):
            self.size = Decimal(str(self.size))

        # 处理开仓均价，None转为0
        if self.entry_price is None:
            self.entry_price = Decimal('0')
        elif isinstance(self.entry_price, (int, float, str)):
            self.entry_price = Decimal(str(self.entry_price))

        # 处理未实现盈亏，None转为0
        if self.unrealized_pnl is None:
            self.unrealized_pnl = Decimal('0')
        elif isinstance(self.unrealized_pnl, (int, float, str)):
            self.unrealized_pnl = Decimal(str(self.unrealized_pnl))

        # 处理已实现盈亏，None转为0
        if self.realized_pnl is None:
            self.realized_pnl = Decimal('0')
        elif isinstance(self.realized_pnl, (int, float, str)):
            self.realized_pnl = Decimal(str(self.realized_pnl))

        # 处理保证金，None转为0
        if self.margin is None:
            self.margin = Decimal('0')
        elif isinstance(self.margin, (int, float, str)):
            self.margin = Decimal(str(self.margin))

        # 处理可选的Decimal字段
        for field_name in ['mark_price', 'current_price', 'percentage', 'liquidation_price']:
            value = getattr(self, field_name)
            if value is not None and isinstance(value, (int, float, str)):
                setattr(self, field_name, Decimal(str(value)))


@dataclass
class BalanceData:
    """余额数据模型"""
    currency: str                    # 币种
    free: Decimal                    # 可用余额
    used: Decimal                    # 冻结余额
    total: Decimal                   # 总余额
    usd_value: Optional[Decimal]     # USD价值
    timestamp: datetime              # 更新时间
    raw_data: Dict[str, Any]         # 原始数据

    def __post_init__(self):
        """数据验证和转换"""
        if isinstance(self.free, (int, float, str)):
            self.free = Decimal(str(self.free))
        if isinstance(self.used, (int, float, str)):
            self.used = Decimal(str(self.used))
        if isinstance(self.total, (int, float, str)):
            self.total = Decimal(str(self.total))
        if self.usd_value is not None and isinstance(self.usd_value, (int, float, str)):
            self.usd_value = Decimal(str(self.usd_value))


@dataclass
class TickerData:
    """行情数据模型

    包含了统一的价格、成交量、资金费率等市场数据。
    设计为涵盖现货和合约市场的所有常见字段。
    """
    symbol: str                      # 交易对符号
    timestamp: datetime              # 主时间戳（原始字段，保持兼容性）

    # === 基础价格信息 ===
    bid: Optional[Decimal] = None           # 最佳买一价
    ask: Optional[Decimal] = None           # 最佳卖一价
    bid_size: Optional[Decimal] = None      # 最佳买一数量
    ask_size: Optional[Decimal] = None      # 最佳卖一数量
    last: Optional[Decimal] = None          # 最新成交价格
    open: Optional[Decimal] = None          # 开盘价
    high: Optional[Decimal] = None          # 24小时最高价
    low: Optional[Decimal] = None           # 24小时最低价
    close: Optional[Decimal] = None         # 收盘价（通常等同于last）

    # === 成交量信息 ===
    volume: Optional[Decimal] = None        # 24小时成交量（基础资产）
    quote_volume: Optional[Decimal] = None  # 24小时成交额（计价资产）
    trades_count: Optional[int] = None      # 24小时成交笔数

    # === 价格变化信息 ===
    change: Optional[Decimal] = None        # 24小时价格变化（绝对值）
    percentage: Optional[Decimal] = None    # 24小时涨跌幅（百分比）

    # === 合约特有信息（期货/永续合约） ===
    funding_rate: Optional[Decimal] = None  # 当前资金费率（永续合约）
    predicted_funding_rate: Optional[Decimal] = None  # 预测资金费率
    funding_time: Optional[datetime] = None  # 当前资金费率生效时间
    next_funding_time: Optional[datetime] = None  # 下次资金费率收取时间
    funding_interval: Optional[int] = None  # 资金费率收取间隔（秒）

    # === 价格参考信息 ===
    index_price: Optional[Decimal] = None   # 指数价格（合约标的指数）
    mark_price: Optional[Decimal] = None    # 标记价格（用于强平计算）
    oracle_price: Optional[Decimal] = None  # 预言机价格（去中心化交易所）

    # === 持仓和合约信息 ===
    open_interest: Optional[Decimal] = None  # 未平仓合约数量
    open_interest_value: Optional[Decimal] = None  # 未平仓合约价值
    delivery_date: Optional[datetime] = None  # 交割日期（期货合约）

    # === 时间相关信息 ===
    high_time: Optional[datetime] = None    # 最高价发生时间
    low_time: Optional[datetime] = None     # 最低价发生时间
    start_time: Optional[datetime] = None   # 统计周期开始时间
    end_time: Optional[datetime] = None     # 统计周期结束时间

    # === 合约标识信息 ===
    contract_id: Optional[str] = None       # 合约ID（交易所内部标识）
    contract_name: Optional[str] = None     # 合约名称
    base_currency: Optional[str] = None     # 基础货币
    quote_currency: Optional[str] = None    # 计价货币
    contract_size: Optional[Decimal] = None  # 合约规模/乘数
    tick_size: Optional[Decimal] = None     # 最小价格变动单位
    lot_size: Optional[Decimal] = None      # 最小下单数量单位

    # === 时间戳链条 ===
    exchange_timestamp: Optional[datetime] = None    # 交易所原始时间戳
    received_timestamp: Optional[datetime] = None    # 数据接收时间
    processed_timestamp: Optional[datetime] = None   # 数据处理时间
    sent_timestamp: Optional[datetime] = None        # 数据发送时间

    # === 原始数据保留 ===
    raw_data: Dict[str, Any] = field(default_factory=dict)  # 原始数据

    def __post_init__(self):
        """数据验证和转换

        将所有数值字段自动转换为Decimal类型，确保精度和一致性。
        处理时间戳字段的格式转换。
        """
        # 需要转换为Decimal的价格和数量字段
        decimal_fields = [
            'bid', 'ask', 'bid_size', 'ask_size', 'last', 'open', 'high', 'low', 'close',
            'volume', 'quote_volume', 'change', 'percentage',
            'funding_rate', 'predicted_funding_rate', 'index_price', 'mark_price', 'oracle_price',
            'open_interest', 'open_interest_value', 'contract_size', 'tick_size', 'lot_size'
        ]

        for field_name in decimal_fields:
            value = getattr(self, field_name)
            if value is not None and isinstance(value, (int, float, str)):
                try:
                    setattr(self, field_name, Decimal(str(value)))
                except (ValueError, TypeError):
                    # 如果转换失败，记录警告但不中断
                    setattr(self, field_name, None)

        # 处理时间戳字段的转换（从毫秒时间戳转换为datetime）
        timestamp_fields = [
            'funding_time', 'next_funding_time', 'high_time', 'low_time',
            'start_time', 'end_time', 'delivery_date'
        ]

        for field_name in timestamp_fields:
            value = getattr(self, field_name)
            if value is not None and isinstance(value, (int, float, str)):
                try:
                    # 假设是毫秒时间戳
                    timestamp_ms = int(float(value))
                    if timestamp_ms > 1e12:  # 毫秒时间戳
                        timestamp_s = timestamp_ms / 1000
                    else:  # 秒时间戳
                        timestamp_s = timestamp_ms
                    setattr(self, field_name,
                            datetime.fromtimestamp(timestamp_s))
                except (ValueError, TypeError, OSError):
                    # 转换失败时保持None
                    setattr(self, field_name, None)

    @property
    def spread(self) -> Optional[Decimal]:
        """计算买卖价差"""
        if self.bid is not None and self.ask is not None:
            return self.ask - self.bid
        return None

    @property
    def spread_percentage(self) -> Optional[Decimal]:
        """计算买卖价差百分比"""
        if self.spread is not None and self.bid is not None and self.bid > 0:
            return (self.spread / self.bid) * Decimal('100')
        return None

    @property
    def mid_price(self) -> Optional[Decimal]:
        """计算中间价格"""
        if self.bid is not None and self.ask is not None:
            return (self.bid + self.ask) / Decimal('2')
        return None

    @property
    def is_futures_contract(self) -> bool:
        """判断是否为期货/永续合约"""
        return any([
            self.funding_rate is not None,
            self.index_price is not None,
            self.mark_price is not None,
            self.open_interest is not None
        ])

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，方便序列化"""
        result = {}
        for field_name, field_value in self.__dict__.items():
            if isinstance(field_value, Decimal):
                result[field_name] = float(field_value)
            elif isinstance(field_value, datetime):
                result[field_name] = field_value.isoformat()
            else:
                result[field_name] = field_value
        return result


@dataclass
class OHLCVData:
    """OHLCV K线数据模型"""
    symbol: str                      # 交易对
    timeframe: str                   # 时间周期
    timestamp: datetime              # 时间戳
    open: Decimal                    # 开盘价
    high: Decimal                    # 最高价
    low: Decimal                     # 最低价
    close: Decimal                   # 收盘价
    volume: Decimal                  # 成交量
    quote_volume: Optional[Decimal]  # 成交额
    trades_count: Optional[int]      # 成交笔数
    raw_data: Dict[str, Any]         # 原始数据

    def __post_init__(self):
        """数据验证和转换"""
        decimal_fields = ['open', 'high', 'low',
                          'close', 'volume', 'quote_volume']
        for field_name in decimal_fields:
            value = getattr(self, field_name)
            if value is not None and isinstance(value, (int, float, str)):
                setattr(self, field_name, Decimal(str(value)))


@dataclass
class OrderBookLevel:
    """订单簿层级数据"""
    price: Decimal                   # 价格
    size: Decimal                    # 数量
    count: Optional[int] = None      # 订单数量

    def __post_init__(self):
        """数据验证和转换"""
        if isinstance(self.price, (int, float, str)):
            self.price = Decimal(str(self.price))
        if isinstance(self.size, (int, float, str)):
            self.size = Decimal(str(self.size))


@dataclass
class OrderBookData:
    """订单簿数据模型"""
    symbol: str                      # 交易对
    bids: List[OrderBookLevel]       # 买单
    asks: List[OrderBookLevel]       # 卖单
    timestamp: datetime              # 时间戳（原始字段，保持兼容性）
    nonce: Optional[int]             # 序列号

    # 新增的详细时间戳链条
    exchange_timestamp: Optional[datetime] = None    # 交易所原始时间戳
    received_timestamp: Optional[datetime] = None    # 数据接收时间
    processed_timestamp: Optional[datetime] = None   # 数据处理时间
    sent_timestamp: Optional[datetime] = None        # 数据发送时间

    raw_data: Dict[str, Any] = field(default_factory=dict)  # 原始数据

    @property
    def best_bid(self) -> Optional[OrderBookLevel]:
        """最优买价"""
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Optional[OrderBookLevel]:
        """最优卖价"""
        return self.asks[0] if self.asks else None

    @property
    def spread(self) -> Optional[Decimal]:
        """买卖价差"""
        if self.best_bid and self.best_ask:
            return self.best_ask.price - self.best_bid.price
        return None


@dataclass
class TradeData:
    """成交数据模型"""
    id: str                          # 成交ID
    symbol: str                      # 交易对
    side: OrderSide                  # 成交方向
    amount: Decimal                  # 成交数量
    price: Decimal                   # 成交价格
    cost: Decimal                    # 成交金额
    fee: Optional[Dict[str, Any]]    # 手续费
    timestamp: datetime              # 成交时间
    order_id: Optional[str]          # 关联订单ID
    raw_data: Dict[str, Any]         # 原始数据

    def __post_init__(self):
        """数据验证和转换"""
        if isinstance(self.amount, (int, float, str)):
            self.amount = Decimal(str(self.amount))
        if isinstance(self.price, (int, float, str)):
            self.price = Decimal(str(self.price))
        if isinstance(self.cost, (int, float, str)):
            self.cost = Decimal(str(self.cost))


@dataclass
class ExchangeInfo:
    """交易所信息数据模型"""
    name: str                        # 交易所名称
    id: str                          # 交易所ID
    type: ExchangeType               # 交易所类型
    supported_features: List[str]    # 支持的功能
    rate_limits: Dict[str, Any]      # 频率限制
    precision: Dict[str, Any]        # 精度配置
    fees: Dict[str, Any]            # 手续费配置
    markets: Dict[str, Any]         # 市场信息
    status: str                     # 状态
    timestamp: datetime             # 更新时间


# 工具函数
def decimal_to_float(value: Optional[Decimal]) -> Optional[float]:
    """将Decimal转换为float，用于JSON序列化"""
    return float(value) if value is not None else None


def ensure_decimal(value: Any) -> Decimal:
    """确保值为Decimal类型"""
    if value is None:
        return Decimal('0')
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def format_decimal(value: Decimal, precision: int) -> str:
    """格式化Decimal为指定精度的字符串"""
    if precision == 0:
        return str(int(value))
    format_str = f"{{:.{precision}f}}"
    return format_str.format(float(value))
