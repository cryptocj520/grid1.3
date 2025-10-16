"""
交易所订阅管理器

支持两种订阅模式：
1. 硬编码模式（predefined）：使用配置文件中预定义的交易对
2. 动态模式（dynamic）：使用符号缓存服务动态获取交易对

整合新的符号缓存服务，同时保持与现有适配器代码的兼容性
"""

import asyncio
import time
from enum import Enum
from typing import Dict, List, Optional, Callable, Any, Set
from dataclasses import dataclass, field
from datetime import datetime
import re

from ...logging import get_system_logger
from ...services.symbol_manager.interfaces.symbol_cache import ISymbolCacheService
# 移除不必要的导入以解决循环依赖
# from ...services.symbol_manager.implementations.symbol_cache_service import SymbolCacheServiceImpl


class SubscriptionMode(Enum):
    """订阅模式枚举"""
    PREDEFINED = "predefined"  # 硬编码模式
    DYNAMIC = "dynamic"        # 动态模式


class DataType(Enum):
    """数据类型枚举"""
    TICKER = "ticker"
    ORDERBOOK = "orderbook"
    TRADES = "trades"
    USER_DATA = "user_data"


@dataclass
class SubscriptionInfo:
    """订阅信息"""
    symbol: str
    data_type: DataType
    callback: Optional[Callable] = None
    subscribed_at: float = field(default_factory=time.time)
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DiscoveryConfig:
    """市场发现配置"""
    enabled: bool = True
    filter_criteria: Dict[str, Any] = field(default_factory=dict)
    auto_discovery_interval: int = 600  # 自动发现间隔（秒）
    max_retry_attempts: int = 3
    retry_delay: int = 5


@dataclass
class BatchSubscriptionConfig:
    """批量订阅配置"""
    enabled: bool = True
    batch_size: int = 10
    delay_between_batches: float = 1.0


class SubscriptionManager:
    """订阅管理器 - 支持硬编码和动态两种模式"""
    
    def __init__(self, 
                 exchange_config: Dict[str, Any],
                 symbol_cache_service: Optional[ISymbolCacheService] = None,
                 logger=None):
        """初始化订阅管理器
        
        Args:
            exchange_config: 交易所配置字典
            symbol_cache_service: 符号缓存服务（动态模式需要）
            logger: 日志器
        """
        self.logger = logger or get_system_logger()
        self.exchange_config = exchange_config
        self.symbol_cache_service = symbol_cache_service
        
        # 解析配置
        self._parse_config()
        
        # 订阅状态
        self.subscriptions: Dict[str, SubscriptionInfo] = {}
        self.active_symbols: Set[str] = set()
        self.subscription_stats = {
            'total_subscriptions': 0,
            'active_subscriptions': 0,
            'failed_subscriptions': 0,
            'last_update': time.time()
        }
        
        # 动态模式相关
        self._cached_symbols: List[str] = []
        self._last_discovery_time: float = 0
        
        self.logger.info(f"✅ 订阅管理器初始化完成，模式: {self.mode.value}")
    
    def _parse_config(self):
        """解析配置文件"""
        subscription_config = self.exchange_config.get('subscription_mode', {})
        
        # 订阅模式
        mode_str = subscription_config.get('mode', 'predefined')
        self.mode = SubscriptionMode(mode_str)
        
        # 硬编码模式配置
        predefined_config = subscription_config.get('predefined', {})
        self.predefined_symbols = predefined_config.get('symbols', [])
        self.predefined_data_types = predefined_config.get('data_types', {})
        
        # 批量订阅配置
        batch_config = predefined_config.get('batch_subscription', {})
        self.batch_config = BatchSubscriptionConfig(
            enabled=batch_config.get('enabled', True),
            batch_size=batch_config.get('batch_size', 10),
            delay_between_batches=batch_config.get('delay_between_batches', 1.0)
        )
        
        # 动态模式配置
        dynamic_config = subscription_config.get('dynamic', {})
        self.dynamic_data_types = dynamic_config.get('data_types', {})
        
        # 市场发现配置
        discovery_config = dynamic_config.get('discovery', {})
        self.discovery_config = DiscoveryConfig(
            enabled=discovery_config.get('enabled', True),
            filter_criteria=discovery_config.get('filter_criteria', {}),
            auto_discovery_interval=discovery_config.get('auto_discovery_interval', 600),
            max_retry_attempts=discovery_config.get('max_retry_attempts', 3),
            retry_delay=discovery_config.get('retry_delay', 5)
        )
        
        # 动态订阅配置
        dynamic_sub_config = dynamic_config.get('dynamic_subscription', {})
        if dynamic_sub_config:
            self.discovery_config.auto_discovery_interval = dynamic_sub_config.get('auto_discovery_interval', 600)
            self.discovery_config.max_retry_attempts = dynamic_sub_config.get('max_retry_attempts', 3)
            self.discovery_config.retry_delay = dynamic_sub_config.get('retry_delay', 5)
    
    def get_subscription_symbols(self) -> List[str]:
        """获取要订阅的交易对列表"""
        if self.mode == SubscriptionMode.PREDEFINED:
            return self.predefined_symbols.copy()
        else:
            # 动态模式返回缓存的符号
            return self._cached_symbols.copy()
    
    def should_subscribe_data_type(self, data_type: DataType) -> bool:
        """检查是否应该订阅指定的数据类型"""
        if self.mode == SubscriptionMode.PREDEFINED:
            return self.predefined_data_types.get(data_type.value, False)
        else:
            return self.dynamic_data_types.get(data_type.value, False)
    
    async def discover_symbols(self, get_supported_symbols_func: Callable[[], List[str]]) -> List[str]:
        """发现交易对符号（动态模式）"""
        if self.mode == SubscriptionMode.PREDEFINED:
            self.logger.warning("硬编码模式不支持符号发现")
            return self.predefined_symbols.copy()
        
        # 检查是否需要重新发现
        current_time = time.time()
        if (self._cached_symbols and 
            current_time - self._last_discovery_time < self.discovery_config.auto_discovery_interval):
            self.logger.info("使用缓存的符号列表")
            return self._cached_symbols.copy()
        
        try:
            # 优先使用符号缓存服务
            if self.symbol_cache_service and self.symbol_cache_service.is_cache_valid():
                # 从符号缓存服务获取符号
                # 注意：这里需要知道交易所ID，我们可以从配置中获取
                exchange_id = self.exchange_config.get('exchange_id')
                if exchange_id:
                    symbols = self.symbol_cache_service.get_symbols_for_exchange(exchange_id)
                    if symbols:
                        self._cached_symbols = symbols
                        self._last_discovery_time = current_time
                        self.logger.info(f"✅ 从符号缓存服务获取 {len(symbols)} 个符号")
                        return symbols
            
            # 回退到直接调用适配器的方法
            self.logger.info("🔄 回退到直接符号发现...")
            symbols = await get_supported_symbols_func()
            
            if symbols:
                # 应用过滤条件
                filtered_symbols = self._apply_filter_criteria(symbols)
                self._cached_symbols = filtered_symbols
                self._last_discovery_time = current_time
                self.logger.info(f"✅ 动态发现 {len(filtered_symbols)} 个符号")
                return filtered_symbols
            else:
                self.logger.warning("⚠️ 符号发现失败，返回空列表")
                return []
                
        except Exception as e:
            self.logger.error(f"❌ 符号发现失败: {e}")
            return []
    
    def _apply_filter_criteria(self, symbols: List[str]) -> List[str]:
        """应用过滤条件"""
        if not self.discovery_config.filter_criteria:
            return symbols
        
        filtered_symbols = symbols.copy()
        criteria = self.discovery_config.filter_criteria
        
        # 应用包含模式
        include_patterns = criteria.get('include_patterns', [])
        if include_patterns:
            filtered_symbols = [
                s for s in filtered_symbols 
                if any(self._match_pattern(s, pattern) for pattern in include_patterns)
            ]
        
        # 应用排除模式
        exclude_patterns = criteria.get('exclude_patterns', [])
        if exclude_patterns:
            filtered_symbols = [
                s for s in filtered_symbols 
                if not any(self._match_pattern(s, pattern) for pattern in exclude_patterns)
            ]
        
        # 应用数量限制
        max_symbols = criteria.get('max_symbols', 0)
        if max_symbols > 0 and len(filtered_symbols) > max_symbols:
            filtered_symbols = filtered_symbols[:max_symbols]
        
        return filtered_symbols
    
    def _match_pattern(self, symbol: str, pattern: str) -> bool:
        """匹配符号模式"""
        # 简单的通配符匹配
        regex_pattern = pattern.replace('*', '.*')
        return bool(re.match(regex_pattern, symbol))
    
    def add_subscription(self, symbol: str, data_type: DataType, callback: Optional[Callable] = None):
        """添加订阅"""
        key = f"{symbol}_{data_type.value}"
        
        if key in self.subscriptions:
            self.logger.debug(f"订阅已存在: {key}")
            return
        
        self.subscriptions[key] = SubscriptionInfo(
            symbol=symbol,
            data_type=data_type,
            callback=callback
        )
        
        self.active_symbols.add(symbol)
        self.subscription_stats['total_subscriptions'] += 1
        self.subscription_stats['active_subscriptions'] += 1
        self.subscription_stats['last_update'] = time.time()
        
        self.logger.debug(f"✅ 添加订阅: {key}")
    
    def remove_subscription(self, symbol: str, data_type: DataType):
        """移除订阅"""
        key = f"{symbol}_{data_type.value}"
        
        if key in self.subscriptions:
            del self.subscriptions[key]
            self.subscription_stats['active_subscriptions'] -= 1
            self.subscription_stats['last_update'] = time.time()
            self.logger.debug(f"🗑️ 移除订阅: {key}")
            
            # 检查是否还有该符号的其他订阅
            if not any(sub.symbol == symbol for sub in self.subscriptions.values()):
                self.active_symbols.discard(symbol)
    
    def get_active_symbols(self) -> List[str]:
        """获取活跃的符号列表"""
        return list(self.active_symbols)
    
    def get_subscription_stats(self) -> Dict[str, Any]:
        """获取订阅统计信息"""
        return {
            'mode': self.mode.value,
            'total_symbols': len(self.active_symbols),
            'total_subscriptions': self.subscription_stats['total_subscriptions'],
            'active_subscriptions': self.subscription_stats['active_subscriptions'],
            'failed_subscriptions': self.subscription_stats['failed_subscriptions'],
            'last_update': self.subscription_stats['last_update'],
            'cached_symbols_count': len(self._cached_symbols),
            'last_discovery_time': self._last_discovery_time
        }
    
    def clear_subscriptions(self):
        """清除所有订阅"""
        self.subscriptions.clear()
        self.active_symbols.clear()
        self.subscription_stats['active_subscriptions'] = 0
        self.subscription_stats['last_update'] = time.time()
        self.logger.info("🗑️ 清除所有订阅")
    
    def get_subscription_info(self, symbol: str, data_type: DataType) -> Optional[SubscriptionInfo]:
        """获取订阅信息"""
        key = f"{symbol}_{data_type.value}"
        return self.subscriptions.get(key)


def create_subscription_manager(exchange_config: Dict[str, Any], 
                              symbol_cache_service: Optional[ISymbolCacheService] = None,
                              logger=None) -> SubscriptionManager:
    """创建订阅管理器工厂函数"""
    return SubscriptionManager(
        exchange_config=exchange_config,
        symbol_cache_service=symbol_cache_service,
        logger=logger
    ) 