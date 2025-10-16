"""
配置服务接口

定义配置管理功能的标准接口
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass


@dataclass
class ExchangeConfig:
    """交易所配置"""
    exchange_id: str
    name: str
    enabled: bool
    base_url: str
    ws_url: str
    testnet: bool
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    max_symbols: int = 50
    rate_limit: int = 1000
    timeout: int = 30


@dataclass
class SymbolConfig:
    """交易对配置"""
    symbol: str
    exchange_id: str
    enabled: bool
    priority: int = 1
    min_volume: float = 0.0
    max_spread: float = 0.05
    category: str = "crypto"


@dataclass
class SubscriptionConfig:
    """订阅配置"""
    exchange_id: str
    data_types: List[str]  # ['ticker', 'orderbook', 'trades']
    symbols: List[str]
    batch_size: int = 10
    retry_count: int = 3
    retry_delay: int = 5


@dataclass
class MonitoringConfiguration:
    """监控配置"""
    exchanges: Dict[str, ExchangeConfig]
    subscriptions: Dict[str, SubscriptionConfig]
    symbols: Dict[str, SymbolConfig]
    global_settings: Dict[str, Any]


class IConfigurationService(ABC):
    """配置服务接口"""
    
    @abstractmethod
    async def initialize(self) -> bool:
        """初始化配置服务"""
        pass
    
    @abstractmethod
    async def load_config(self, config_path: str = None) -> MonitoringConfiguration:
        """加载配置"""
        pass
    
    @abstractmethod
    async def save_config(self, config: MonitoringConfiguration, config_path: str = None) -> bool:
        """保存配置"""
        pass
    
    @abstractmethod
    async def get_exchange_config(self, exchange_id: str) -> Optional[ExchangeConfig]:
        """获取交易所配置"""
        pass
    
    @abstractmethod
    async def get_subscription_config(self, exchange_id: str) -> Optional[SubscriptionConfig]:
        """获取订阅配置"""
        pass
    
    @abstractmethod
    async def get_symbol_config(self, symbol: str, exchange_id: str) -> Optional[SymbolConfig]:
        """获取交易对配置"""
        pass
    
    @abstractmethod
    async def get_enabled_exchanges(self) -> List[str]:
        """获取启用的交易所"""
        pass
    
    @abstractmethod
    async def get_symbols_for_exchange(self, exchange_id: str) -> List[str]:
        """获取交易所的交易对"""
        pass
    
    @abstractmethod
    async def update_exchange_config(self, exchange_id: str, config: ExchangeConfig) -> bool:
        """更新交易所配置"""
        pass
    
    @abstractmethod
    async def update_subscription_config(self, exchange_id: str, config: SubscriptionConfig) -> bool:
        """更新订阅配置"""
        pass
    
    @abstractmethod
    async def reload_config(self) -> bool:
        """重新加载配置"""
        pass
    
    @abstractmethod
    def get_config_snapshot(self) -> Dict[str, Any]:
        """获取配置快照"""
        pass 