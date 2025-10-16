"""
交易所统一接口定义

定义了所有交易所适配器必须实现的标准接口，
确保不同交易所之间的API一致性和可替换性。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
from typing import Dict, List, Optional, Any, AsyncGenerator, Callable
from decimal import Decimal

from ...services.events import Event
from .models import (
    ExchangeType,
    OrderSide,
    OrderType,
    OrderData,
    PositionData,
    BalanceData,
    TickerData,
    OHLCVData,
    OrderBookData,
    TradeData,
    ExchangeInfo
)


class ExchangeStatus(Enum):
    """交易所状态枚举"""
    DISCONNECTED = "disconnected"    # 未连接
    CONNECTING = "connecting"        # 连接中
    CONNECTED = "connected"          # 已连接
    AUTHENTICATED = "authenticated"  # 已认证
    ERROR = "error"                  # 错误状态
    MAINTENANCE = "maintenance"      # 维护中


@dataclass
class ExchangeConfig:
    """交易所配置数据模型"""
    # 基础配置
    exchange_id: str                         # 交易所ID
    name: str                               # 交易所名称
    exchange_type: ExchangeType             # 交易所类型

    # 认证配置
    api_key: str                            # API密钥
    api_secret: str                         # API密钥
    api_passphrase: Optional[str] = None    # API密码短语（部分交易所需要）
    wallet_address: Optional[str] = None    # 钱包地址（如Hyperliquid）

    # 网络配置
    testnet: bool = False                   # 是否使用测试网
    base_url: Optional[str] = None          # 自定义API基础URL
    ws_url: Optional[str] = None            # WebSocket URL

    # 交易配置
    default_leverage: int = 1               # 默认杠杆倍数
    default_margin_mode: str = "cross"      # 默认保证金模式
    symbol_mapping: Dict[str, str] = field(default_factory=dict)  # 交易对映射 (已弃用，建议使用统一符号转换服务)

    # 限制配置
    rate_limits: Dict[str, Any] = field(default_factory=dict)     # 频率限制
    precision: Dict[str, Any] = field(default_factory=dict)       # 精度配置

    # 功能开关
    enable_websocket: bool = True           # 启用WebSocket
    enable_auto_reconnect: bool = True      # 启用自动重连
    enable_heartbeat: bool = True           # 启用心跳检测

    # 超时配置
    connect_timeout: int = 30               # 连接超时（秒）
    request_timeout: int = 10               # 请求超时（秒）
    heartbeat_interval: int = 30            # 心跳间隔（秒）

    # 重试配置
    max_retry_attempts: int = 3             # 最大重试次数
    retry_delay: float = 1.0                # 重试延迟（秒）

    # 额外参数
    extra_params: Dict[str, Any] = field(default_factory=dict)


class ExchangeInterface(ABC):
    """
    交易所统一接口

    所有交易所适配器都必须实现此接口，确保API的一致性。
    采用异步设计，支持事件驱动的数据流。
    """

    def __init__(self, config: ExchangeConfig):
        """
        初始化交易所接口

        Args:
            config: 交易所配置
        """
        self.config = config
        self.status = ExchangeStatus.DISCONNECTED
        self._event_callbacks: Dict[str, List[Callable]] = {}
        self._last_heartbeat = datetime.now()

    # === 生命周期管理 ===

    @abstractmethod
    async def connect(self) -> bool:
        """
        连接到交易所

        Returns:
            bool: 连接是否成功
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """断开与交易所的连接"""
        pass

    @abstractmethod
    async def authenticate(self) -> bool:
        """
        进行身份认证

        Returns:
            bool: 认证是否成功
        """
        pass

    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """
        健康检查

        Returns:
            Dict: 健康状态信息
        """
        pass

    # === 市场数据接口 ===

    @abstractmethod
    async def get_exchange_info(self) -> ExchangeInfo:
        """
        获取交易所信息

        Returns:
            ExchangeInfo: 交易所信息
        """
        pass

    @abstractmethod
    async def get_ticker(self, symbol: str) -> TickerData:
        """
        获取单个交易对行情

        Args:
            symbol: 交易对符号

        Returns:
            TickerData: 行情数据
        """
        pass

    @abstractmethod
    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """
        获取多个交易对行情

        Args:
            symbols: 交易对符号列表，None表示获取所有

        Returns:
            List[TickerData]: 行情数据列表
        """
        pass

    @abstractmethod
    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """
        获取订单簿

        Args:
            symbol: 交易对符号
            limit: 深度限制

        Returns:
            OrderBookData: 订单簿数据
        """
        pass

    @abstractmethod
    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OHLCVData]:
        """
        获取K线数据

        Args:
            symbol: 交易对符号
            timeframe: 时间框架（如'1m', '5m', '1h', '1d'）
            since: 开始时间
            limit: 数据条数限制

        Returns:
            List[OHLCVData]: K线数据列表
        """
        pass

    @abstractmethod
    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """
        获取最近成交记录

        Args:
            symbol: 交易对符号
            since: 开始时间
            limit: 数据条数限制

        Returns:
            List[TradeData]: 成交数据列表
        """
        pass

    # === 账户和交易接口 ===

    @abstractmethod
    async def get_balances(self) -> List[BalanceData]:
        """
        获取账户余额

        Returns:
            List[BalanceData]: 余额数据列表
        """
        pass

    @abstractmethod
    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """
        获取持仓信息

        Args:
            symbols: 交易对符号列表，None表示获取所有

        Returns:
            List[PositionData]: 持仓数据列表
        """
        pass

    @abstractmethod
    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> OrderData:
        """
        创建订单

        Args:
            symbol: 交易对符号
            side: 订单方向
            order_type: 订单类型
            amount: 数量
            price: 价格（限价单必需）
            params: 额外参数

        Returns:
            OrderData: 订单数据
        """
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """
        取消订单

        Args:
            order_id: 订单ID
            symbol: 交易对符号

        Returns:
            OrderData: 订单数据
        """
        pass

    @abstractmethod
    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """
        取消所有订单

        Args:
            symbol: 交易对符号，None表示取消所有交易对的订单

        Returns:
            List[OrderData]: 被取消的订单列表
        """
        pass

    @abstractmethod
    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """
        获取订单信息

        Args:
            order_id: 订单ID
            symbol: 交易对符号

        Returns:
            OrderData: 订单数据
        """
        pass

    @abstractmethod
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """
        获取开放订单

        Args:
            symbol: 交易对符号，None表示获取所有交易对的开放订单

        Returns:
            List[OrderData]: 开放订单列表
        """
        pass

    @abstractmethod
    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OrderData]:
        """
        获取历史订单

        Args:
            symbol: 交易对符号
            since: 开始时间
            limit: 数据条数限制

        Returns:
            List[OrderData]: 历史订单列表
        """
        pass

    # === 交易设置接口 ===

    @abstractmethod
    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """
        设置杠杆倍数

        Args:
            symbol: 交易对符号
            leverage: 杠杆倍数

        Returns:
            Dict: 设置结果
        """
        pass

    @abstractmethod
    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """
        设置保证金模式

        Args:
            symbol: 交易对符号
            margin_mode: 保证金模式（'cross' 或 'isolated'）

        Returns:
            Dict: 设置结果
        """
        pass

    # === 实时数据流接口 ===

    @abstractmethod
    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """
        订阅行情数据流

        Args:
            symbol: 交易对符号
            callback: 数据回调函数
        """
        pass

    @abstractmethod
    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """
        订阅订单簿数据流

        Args:
            symbol: 交易对符号
            callback: 数据回调函数
        """
        pass

    @abstractmethod
    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """
        订阅成交数据流

        Args:
            symbol: 交易对符号
            callback: 数据回调函数
        """
        pass

    @abstractmethod
    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        订阅用户数据流（订单更新、持仓变化等）

        Args:
            callback: 数据回调函数
        """
        pass

    @abstractmethod
    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """
        取消订阅

        Args:
            symbol: 交易对符号，None表示取消所有订阅
        """
        pass

    # === 工具方法 ===

    def get_status(self) -> ExchangeStatus:
        """获取当前状态"""
        return self.status

    def is_connected(self) -> bool:
        """检查是否已连接"""
        return self.status in [ExchangeStatus.CONNECTED, ExchangeStatus.AUTHENTICATED]

    def is_authenticated(self) -> bool:
        """检查是否已认证"""
        return self.status == ExchangeStatus.AUTHENTICATED

    def get_config(self) -> ExchangeConfig:
        """获取配置信息"""
        return self.config

    async def emit_event(self, event: Event) -> None:
        """
        发出事件

        Args:
            event: 要发出的事件
        """
        # 子类应该实现具体的事件发出逻辑
        # 通常是通过MESA事件总线发出
        pass

    def add_event_callback(self, event_type: str, callback: Callable) -> None:
        """
        添加事件回调

        Args:
            event_type: 事件类型
            callback: 回调函数
        """
        if event_type not in self._event_callbacks:
            self._event_callbacks[event_type] = []
        self._event_callbacks[event_type].append(callback)

    def remove_event_callback(self, event_type: str, callback: Callable) -> None:
        """
        移除事件回调

        Args:
            event_type: 事件类型
            callback: 回调函数
        """
        if event_type in self._event_callbacks:
            self._event_callbacks[event_type].remove(callback)

    # === 符号映射工具 ===
    # 注意：符号映射功能已移至统一的符号转换服务
    # 这些方法保留用于向后兼容，但建议使用符号转换服务

    def map_symbol(self, symbol: str) -> str:
        """
        映射交易对符号到交易所特定格式
        
        @deprecated: 建议使用统一的符号转换服务
        Args:
            symbol: 通用交易对符号

        Returns:
            str: 交易所特定的交易对符号
        """
        return self.config.symbol_mapping.get(symbol, symbol)

    def reverse_map_symbol(self, exchange_symbol: str) -> str:
        """
        反向映射交易所符号到通用格式
        
        @deprecated: 建议使用统一的符号转换服务
        Args:
            exchange_symbol: 交易所特定的交易对符号

        Returns:
            str: 通用交易对符号
        """
        # 创建反向映射字典
        reverse_mapping = {v: k for k, v in self.config.symbol_mapping.items()}
        return reverse_mapping.get(exchange_symbol, exchange_symbol)

    # === 私有辅助方法 ===

    def _update_heartbeat(self) -> None:
        """更新心跳时间"""
        self._last_heartbeat = datetime.now()

    def _is_heartbeat_alive(self) -> bool:
        """检查心跳是否正常"""
        if not self.config.enable_heartbeat:
            return True

        elapsed = (datetime.now() - self._last_heartbeat).total_seconds()
        return elapsed < self.config.heartbeat_interval * 2  # 允许2倍心跳间隔的延迟
