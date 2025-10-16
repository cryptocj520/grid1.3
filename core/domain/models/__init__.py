"""
领域模型

定义核心业务实体和数据结构
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set
from datetime import datetime
from decimal import Decimal
from enum import Enum


class DataType(Enum):
    """支持的数据类型枚举"""
    TICKER = "ticker"
    ORDERBOOK = "orderbook"
    TRADES = "trades"
    USER_DATA = "user_data"
    
    @classmethod
    def get_all_types(cls) -> List[str]:
        """获取所有支持的数据类型"""
        return [dt.value for dt in cls]
    
    @classmethod
    def from_string(cls, value: str) -> 'DataType':
        """从字符串创建数据类型"""
        for dt in cls:
            if dt.value == value.lower():
                return dt
        raise ValueError(f"不支持的数据类型: {value}")
    
    @classmethod
    def from_list(cls, values: List[str]) -> List['DataType']:
        """从字符串列表创建数据类型列表"""
        return [cls.from_string(v) for v in values]


@dataclass
class DataTypeConfig:
    """数据类型配置"""
    enabled_types: Set[DataType] = field(default_factory=set)
    disabled_types: Set[DataType] = field(default_factory=set)
    
    def __post_init__(self):
        """初始化后处理"""
        # 如果没有指定启用类型，默认启用ticker和orderbook
        if not self.enabled_types and not self.disabled_types:
            self.enabled_types = {DataType.TICKER, DataType.ORDERBOOK}
    
    def is_enabled(self, data_type: DataType) -> bool:
        """检查数据类型是否启用"""
        if self.disabled_types and data_type in self.disabled_types:
            return False
        if self.enabled_types:
            return data_type in self.enabled_types
        return True
    
    def get_enabled_types(self) -> List[DataType]:
        """获取启用的数据类型列表"""
        if self.enabled_types:
            return [dt for dt in self.enabled_types if dt not in self.disabled_types]
        else:
            # 如果没有明确启用的类型，返回所有类型除了禁用的
            return [dt for dt in DataType if dt not in self.disabled_types]
    
    def get_enabled_type_names(self) -> List[str]:
        """获取启用的数据类型名称列表"""
        return [dt.value for dt in self.get_enabled_types()]


@dataclass
class ExchangeDataTypeConfig:
    """交易所数据类型配置"""
    exchange_id: str
    data_types: DataTypeConfig = field(default_factory=DataTypeConfig)
    max_symbols_per_type: Dict[DataType, int] = field(default_factory=dict)
    priority_symbols: List[str] = field(default_factory=list)
    
    def get_max_symbols(self, data_type: DataType) -> Optional[int]:
        """获取指定数据类型的最大符号数"""
        return self.max_symbols_per_type.get(data_type)
    
    def set_max_symbols(self, data_type: DataType, max_symbols: int):
        """设置指定数据类型的最大符号数"""
        self.max_symbols_per_type[data_type] = max_symbols


@dataclass
class MonitoringDataTypeConfig:
    """监控数据类型配置"""
    global_enabled_types: Set[DataType] = field(default_factory=lambda: {DataType.TICKER, DataType.ORDERBOOK})
    exchange_configs: Dict[str, ExchangeDataTypeConfig] = field(default_factory=dict)
    
    def get_exchange_config(self, exchange_id: str) -> Optional[ExchangeDataTypeConfig]:
        """获取交易所配置"""
        return self.exchange_configs.get(exchange_id)
    
    def set_exchange_config(self, exchange_id: str, config: ExchangeDataTypeConfig):
        """设置交易所配置"""
        self.exchange_configs[exchange_id] = config
    
    def get_enabled_types_for_exchange(self, exchange_id: str) -> List[DataType]:
        """获取指定交易所的启用数据类型"""
        exchange_config = self.get_exchange_config(exchange_id)
        if exchange_config:
            return exchange_config.data_types.get_enabled_types()
        else:
            # 如果没有交易所特定配置，使用全局配置
            return list(self.global_enabled_types)


@dataclass
class SubscriptionStatus:
    """订阅状态"""
    exchange_id: str
    symbol: str
    data_type: DataType
    status: str = "pending"  # pending, active, error, cancelled
    error_message: Optional[str] = None
    last_update: datetime = field(default_factory=datetime.now)
    
    def is_active(self) -> bool:
        """检查订阅是否活跃"""
        return self.status == "active"
    
    def is_error(self) -> bool:
        """检查订阅是否有错误"""
        return self.status == "error"


@dataclass
class SubscriptionSummary:
    """订阅摘要"""
    total_subscriptions: int = 0
    active_subscriptions: int = 0
    error_subscriptions: int = 0
    pending_subscriptions: int = 0
    
    by_exchange: Dict[str, Dict[str, int]] = field(default_factory=dict)
    by_data_type: Dict[DataType, Dict[str, int]] = field(default_factory=dict)
    
    def update_from_status(self, status: SubscriptionStatus):
        """从订阅状态更新摘要"""
        self.total_subscriptions += 1
        
        if status.is_active():
            self.active_subscriptions += 1
        elif status.is_error():
            self.error_subscriptions += 1
        else:
            self.pending_subscriptions += 1
        
        # 更新按交易所统计
        if status.exchange_id not in self.by_exchange:
            self.by_exchange[status.exchange_id] = {"total": 0, "active": 0, "error": 0, "pending": 0}
        
        self.by_exchange[status.exchange_id]["total"] += 1
        if status.is_active():
            self.by_exchange[status.exchange_id]["active"] += 1
        elif status.is_error():
            self.by_exchange[status.exchange_id]["error"] += 1
        else:
            self.by_exchange[status.exchange_id]["pending"] += 1
        
        # 更新按数据类型统计
        if status.data_type not in self.by_data_type:
            self.by_data_type[status.data_type] = {"total": 0, "active": 0, "error": 0, "pending": 0}
        
        self.by_data_type[status.data_type]["total"] += 1
        if status.is_active():
            self.by_data_type[status.data_type]["active"] += 1
        elif status.is_error():
            self.by_data_type[status.data_type]["error"] += 1
        else:
            self.by_data_type[status.data_type]["pending"] += 1


@dataclass
class ExchangeData:
    """交易所数据"""
    exchange_id: str
    name: str
    base_url: str
    ws_url: str
    testnet: bool = True
    connected: bool = False
    last_update: datetime = None
    
    def __post_init__(self):
        if self.last_update is None:
            self.last_update = datetime.now()


@dataclass
class PriceData:
    """价格数据"""
    symbol: str
    exchange: str
    price: float
    volume: float
    timestamp: datetime
    last_update: datetime
    
    def __post_init__(self):
        if self.last_update is None:
            self.last_update = datetime.now()


@dataclass
class SpreadData:
    """价差数据"""
    symbol: str
    exchange1: str
    exchange2: str
    price1: float
    price2: float
    spread: float
    spread_pct: float
    volume1: float
    volume2: float
    timestamp: datetime
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class SymbolInfo:
    """交易对信息"""
    symbol: str
    base_currency: str
    quote_currency: str
    contract_type: str
    price_precision: int
    quantity_precision: int
    min_quantity: Decimal
    max_quantity: Decimal
    min_price: Decimal
    max_price: Decimal
    active: bool = True


@dataclass
class MarketData:
    """市场数据"""
    symbol: str
    exchange: str
    ticker: Optional[Dict[str, Any]] = None
    orderbook: Optional[Dict[str, Any]] = None
    trades: Optional[List[Dict[str, Any]]] = None
    last_update: datetime = None
    
    def __post_init__(self):
        if self.last_update is None:
            self.last_update = datetime.now()


@dataclass
class ExchangeStatus:
    """交易所状态"""
    exchange_id: str
    connected: bool
    authenticated: bool
    websocket_connected: bool
    last_heartbeat: datetime
    message_count: int
    error_count: int
    uptime: float
    
    def __post_init__(self):
        if self.last_heartbeat is None:
            self.last_heartbeat = datetime.now()


# 导出所有类
__all__ = [
    'DataType',
    'DataTypeConfig',
    'ExchangeDataTypeConfig',
    'MonitoringDataTypeConfig',
    'SubscriptionStatus',
    'SubscriptionSummary',
    'ExchangeData',
    'PriceData', 
    'SpreadData',
    'SymbolInfo',
    'MarketData',
    'ExchangeStatus'
]
