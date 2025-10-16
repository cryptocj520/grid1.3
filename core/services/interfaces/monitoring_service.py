"""
监控服务接口

定义交易所数据监控、WebSocket连接管理、数据聚合等功能的标准接口
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

from ...domain.models import ExchangeData, PriceData, SpreadData


class SubscriptionStrategy(Enum):
    """订阅策略"""
    TICKER_ONLY = "ticker_only"           # 只订阅ticker数据
    ORDERBOOK_ONLY = "orderbook_only"     # 只订阅orderbook数据
    BOTH = "both"                         # 同时订阅ticker和orderbook
    CUSTOM = "custom"                     # 自定义订阅策略


@dataclass
class ExchangeSubscriptionConfig:
    """交易所订阅配置"""
    exchange_id: str
    strategy: SubscriptionStrategy = SubscriptionStrategy.TICKER_ONLY
    ticker_symbols: List[str] = None          # ticker订阅的交易对
    orderbook_symbols: List[str] = None       # orderbook订阅的交易对
    enabled: bool = True
    
    def __post_init__(self):
        if self.ticker_symbols is None:
            self.ticker_symbols = []
        if self.orderbook_symbols is None:
            self.orderbook_symbols = []


@dataclass
class MonitoringStats:
    """监控统计信息"""
    total_messages: int = 0
    exchange_messages: Dict[str, int] = None
    connected_exchanges: int = 0
    errors: int = 0
    uptime: float = 0.0
    
    def __post_init__(self):
        if self.exchange_messages is None:
            self.exchange_messages = {}


@dataclass
class MonitoringConfig:
    """监控配置 - 统一版本"""
    # WebSocket和Socket.IO配置
    socketio_port: int = 8765
    refresh_interval: float = 1.0
    max_symbols: int = 200
    
    # 订阅策略配置
    default_strategy: SubscriptionStrategy = SubscriptionStrategy.TICKER_ONLY
    exchange_configs: Dict[str, ExchangeSubscriptionConfig] = None
    
    # 交易所基础配置
    exchanges: Dict[str, Dict[str, Any]] = None
    
    # Web界面配置
    enable_web_interface: bool = True
    web_port: int = 8080
    
    # 指标收集配置
    enable_metrics: bool = True
    metrics_interval: int = 60
    
    # 告警配置
    enable_alerts: bool = True
    alert_channels: List[str] = None
    
    # 日志配置
    log_level: str = "INFO"
    log_file: Optional[str] = None
    
    def __post_init__(self):
        if self.exchanges is None:
            self.exchanges = {
                'edgex': {
                    'name': 'EdgeX Exchange',
                    'enabled': True,
                    'base_url': 'https://pro.edgex.exchange',
                    'ws_url': 'wss://quote.edgex.exchange/api/v1/public/ws',
                    'testnet': True
                },
                'backpack': {
                    'name': 'Backpack Exchange',
                    'enabled': True,
                    'base_url': 'https://api.backpack.exchange',
                    'ws_url': 'wss://ws.backpack.exchange',
                    'testnet': True
                },
                'hyperliquid': {
                    'name': 'Hyperliquid',
                    'enabled': True,
                    'base_url': 'https://api.hyperliquid.xyz',
                    'ws_url': 'wss://api.hyperliquid.xyz/ws',
                    'testnet': False
                }
            }
        
        if self.exchange_configs is None:
            self.exchange_configs = {}
        
        if self.alert_channels is None:
            self.alert_channels = ["email", "webhook"]


class MonitoringService(ABC):
    """监控服务接口"""
    
    @abstractmethod
    async def start(self) -> bool:
        """启动监控服务"""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """停止监控服务"""
        pass
    
    @abstractmethod
    async def get_stats(self) -> MonitoringStats:
        """获取监控统计信息"""
        pass
    
    @abstractmethod
    async def get_price_data(self) -> Dict[str, PriceData]:
        """获取价格数据"""
        pass
    
    @abstractmethod
    async def get_spread_data(self) -> Dict[str, SpreadData]:
        """获取价差数据"""
        pass
    
    @abstractmethod
    async def subscribe_updates(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """订阅数据更新"""
        pass
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        pass
    
    # === 新增灵活订阅方法 ===
    
    @abstractmethod
    async def subscribe_ticker(self, exchange_id: str, symbols: List[str]) -> bool:
        """订阅ticker数据"""
        pass
    
    @abstractmethod
    async def subscribe_orderbook(self, exchange_id: str, symbols: List[str]) -> bool:
        """订阅orderbook数据"""
        pass
    
    @abstractmethod
    async def unsubscribe_ticker(self, exchange_id: str, symbols: List[str]) -> bool:
        """取消订阅ticker数据"""
        pass
    
    @abstractmethod
    async def unsubscribe_orderbook(self, exchange_id: str, symbols: List[str]) -> bool:
        """取消订阅orderbook数据"""
        pass
    
    @abstractmethod
    async def configure_exchange_subscription(self, config: ExchangeSubscriptionConfig) -> bool:
        """配置交易所订阅策略"""
        pass
    
    @abstractmethod
    async def get_subscription_status(self) -> Dict[str, Dict[str, Any]]:
        """获取订阅状态"""
        pass 