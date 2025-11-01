"""
交易所工厂模式实现

提供统一的交易所适配器创建和管理机制，
支持配置驱动的适配器创建和注册机制。
"""

from typing import Dict, Type, Optional, Any, List
from dataclasses import dataclass

# 使用简化的统一日志入口
from ...logging import get_system_logger

# EventBus在新架构中已移除，使用Optional[Any]替代
from .interface import ExchangeInterface, ExchangeConfig
from .models import ExchangeType


@dataclass
class ExchangeRegistryEntry:
    """交易所注册表条目"""
    adapter_class: Type[ExchangeInterface]
    exchange_type: ExchangeType
    name: str
    description: str
    supported_features: List[str]
    default_config: Dict[str, Any]


class ExchangeFactory:
    """
    交易所适配器工厂

    负责创建和管理交易所适配器实例，支持：
    - 交易所适配器类注册
    - 配置驱动的适配器创建
    - 适配器类型发现
    - 默认配置管理
    """

    def __init__(self):
        """初始化工厂"""
        self._registry: Dict[str, ExchangeRegistryEntry] = {}
        self._instances: Dict[str, ExchangeInterface] = {}
        self.logger = get_system_logger()

        # 注册内置适配器
        self._register_builtin_adapters()

    def _register_builtin_adapters(self) -> None:
        """注册内置的交易所适配器"""
        try:
            # 延迟导入避免循环依赖
            from .adapters.hyperliquid import HyperliquidAdapter
            from .adapters.backpack import BackpackAdapter
            from .adapters.binance import BinanceAdapter
            from .adapters.edgex import EdgeXAdapter
            from .adapters.lighter import LighterAdapter

            # 注册Hyperliquid适配器
            self.register_adapter(
                exchange_id="hyperliquid",
                adapter_class=HyperliquidAdapter,
                exchange_type=ExchangeType.PERPETUAL,
                name="Hyperliquid",
                description="Hyperliquid永续合约交易所",
                supported_features=[
                    "spot_trading", "perpetual_trading", "websocket",
                    "orderbook", "ticker", "ohlcv", "user_stream"
                ],
                default_config={
                    "testnet": False,
                    "default_leverage": 1,
                    "enable_websocket": True,
                    "rate_limits": {
                        "ticker": {"max_requests": 100, "time_window": 60},
                        "orderbook": {"max_requests": 100, "time_window": 60},
                        "trading": {"max_requests": 50, "time_window": 60}
                    }
                }
            )

            # 注册Backpack适配器
            self.register_adapter(
                exchange_id="backpack",
                adapter_class=BackpackAdapter,
                exchange_type=ExchangeType.PERPETUAL,
                name="Backpack",
                description="Backpack永续合约交易所",
                supported_features=[
                    "spot_trading", "perpetual_trading", "orderbook",
                    "ticker", "ohlcv", "user_data"
                ],
                default_config={
                    "testnet": False,
                    "default_leverage": 1,
                    "enable_websocket": False,  # Backpack主要使用REST API
                    "rate_limits": {
                        "ticker": {"max_requests": 60, "time_window": 60},
                        "orderbook": {"max_requests": 60, "time_window": 60},
                        "trading": {"max_requests": 30, "time_window": 60}
                    }
                }
            )

            # 注册Binance适配器
            self.register_adapter(
                exchange_id="binance",
                adapter_class=BinanceAdapter,
                exchange_type=ExchangeType.FUTURES,
                name="Binance Futures",
                description="币安期货交易所",
                supported_features=[
                    "spot_trading", "futures_trading", "websocket",
                    "orderbook", "ticker", "ohlcv", "user_stream"
                ],
                default_config={
                    "testnet": False,
                    "default_leverage": 1,
                    "enable_websocket": True,
                    "rate_limits": {
                        "ticker": {"max_requests": 1200, "time_window": 60},
                        "orderbook": {"max_requests": 1200, "time_window": 60},
                        "trading": {"max_requests": 600, "time_window": 60}
                    }
                }
            )

            # 注册EdgeX适配器
            self.register_adapter(
                exchange_id="edgex",
                adapter_class=EdgeXAdapter,
                exchange_type=ExchangeType.PERPETUAL,
                name="EdgeX",
                description="EdgeX永续合约交易所",
                supported_features=[
                    "perpetual_trading", "websocket", "orderbook",
                    "ticker", "ohlcv", "user_data"
                ],
                default_config={
                    "testnet": False,
                    "default_leverage": 10,
                    "enable_websocket": True,
                    "rate_limits": {
                        "ticker": {"max_requests": 100, "time_window": 60},
                        "orderbook": {"max_requests": 100, "time_window": 60},
                        "trading": {"max_requests": 20, "time_window": 60}
                    }
                }
            )

            # 注册Lighter适配器
            self.register_adapter(
                exchange_id="lighter",
                adapter_class=LighterAdapter,
                exchange_type=ExchangeType.PERPETUAL,
                name="Lighter",
                description="Lighter永续合约交易所",
                supported_features=[
                    "perpetual_trading", "websocket", "orderbook",
                    "ticker", "trades", "user_data"
                ],
                default_config={
                    "testnet": False,
                    "default_leverage": 1,
                    "enable_websocket": True,
                    "rate_limits": {
                        "ticker": {"max_requests": 100, "time_window": 60},
                        "orderbook": {"max_requests": 100, "time_window": 60},
                        "trading": {"max_requests": 10, "time_window": 60}
                    }
                }
            )

            self.logger.info("内置交易所适配器注册完成")

        except ImportError as e:
            self.logger.warning(f"部分内置适配器注册失败: {str(e)}")

    def register_adapter(
        self,
        exchange_id: str,
        adapter_class: Type[ExchangeInterface],
        exchange_type: ExchangeType,
        name: str,
        description: str,
        supported_features: List[str],
        default_config: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        注册交易所适配器

        Args:
            exchange_id: 交易所ID
            adapter_class: 适配器类
            exchange_type: 交易所类型
            name: 交易所名称
            description: 交易所描述
            supported_features: 支持的功能列表
            default_config: 默认配置
        """
        if exchange_id in self._registry:
            self.logger.warning(f"交易所适配器已存在，覆盖注册: {exchange_id}")

        entry = ExchangeRegistryEntry(
            adapter_class=adapter_class,
            exchange_type=exchange_type,
            name=name,
            description=description,
            supported_features=supported_features or [],
            default_config=default_config or {}
        )

        self._registry[exchange_id] = entry
        self.logger.info(f"注册交易所适配器: {exchange_id} ({name})")

    def unregister_adapter(self, exchange_id: str) -> None:
        """
        注销交易所适配器

        Args:
            exchange_id: 交易所ID
        """
        if exchange_id in self._registry:
            del self._registry[exchange_id]
            self.logger.info(f"注销交易所适配器: {exchange_id}")

        # 同时移除实例
        if exchange_id in self._instances:
            del self._instances[exchange_id]

    def create_adapter(
        self,
        exchange_id: str,
        config: Optional[ExchangeConfig] = None,
        event_bus: Optional[Any] = None,
        **kwargs
    ) -> ExchangeInterface:
        """
        创建交易所适配器实例

        Args:
            exchange_id: 交易所ID
            config: 交易所配置（可选，如果不提供将使用默认配置）
            event_bus: 事件总线实例
            **kwargs: 额外配置参数

        Returns:
            ExchangeInterface: 交易所适配器实例

        Raises:
            ValueError: 交易所ID未注册
            Exception: 适配器创建失败
        """
        if exchange_id not in self._registry:
            raise ValueError(f"未注册的交易所ID: {exchange_id}")

        entry = self._registry[exchange_id]

        try:
            # 如果没有提供配置，创建默认配置
            if config is None:
                config = self._create_default_config(exchange_id, **kwargs)
            else:
                # 合并默认配置和用户配置
                config = self._merge_config(config, entry.default_config)

            # 创建适配器实例
            if event_bus:
                adapter = entry.adapter_class(config, event_bus)
            else:
                adapter = entry.adapter_class(config)

            self.logger.info(f"创建交易所适配器实例: {exchange_id}")
            return adapter

        except Exception as e:
            self.logger.error(f"创建交易所适配器失败 {exchange_id}: {str(e)}")
            raise

    def get_or_create_adapter(
        self,
        exchange_id: str,
        config: Optional[ExchangeConfig] = None,
        event_bus: Optional[Any] = None,
        **kwargs
    ) -> ExchangeInterface:
        """
        获取或创建交易所适配器实例（单例模式）

        Args:
            exchange_id: 交易所ID
            config: 交易所配置
            event_bus: 事件总线实例
            **kwargs: 额外配置参数

        Returns:
            ExchangeInterface: 交易所适配器实例
        """
        if exchange_id in self._instances:
            return self._instances[exchange_id]

        adapter = self.create_adapter(exchange_id, config, event_bus, **kwargs)
        self._instances[exchange_id] = adapter

        return adapter

    def _create_default_config(self, exchange_id: str, **kwargs) -> ExchangeConfig:
        """
        创建默认配置

        Args:
            exchange_id: 交易所ID
            **kwargs: 额外配置参数

        Returns:
            ExchangeConfig: 默认配置
        """
        entry = self._registry[exchange_id]

        # 从kwargs中提取必需的配置
        api_key = kwargs.get('api_key', '')
        api_secret = kwargs.get('api_secret', '')

        if not api_key or not api_secret:
            raise ValueError(f"创建 {exchange_id} 适配器需要 api_key 和 api_secret")

        # 合并默认配置和用户配置
        config_data = entry.default_config.copy()
        config_data.update(kwargs)

        return ExchangeConfig(
            exchange_id=exchange_id,
            name=entry.name,
            exchange_type=entry.exchange_type,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=kwargs.get('api_passphrase'),
            wallet_address=kwargs.get('wallet_address'),
            testnet=config_data.get('testnet', False),
            base_url=config_data.get('base_url'),
            ws_url=config_data.get('ws_url'),
            default_leverage=config_data.get('default_leverage', 1),
            default_margin_mode=config_data.get(
                'default_margin_mode', 'cross'),
            symbol_mapping=config_data.get('symbol_mapping', {}),
            rate_limits=config_data.get('rate_limits', {}),
            precision=config_data.get('precision', {}),
            enable_websocket=config_data.get('enable_websocket', True),
            enable_auto_reconnect=config_data.get(
                'enable_auto_reconnect', True),
            enable_heartbeat=config_data.get('enable_heartbeat', True),
            connect_timeout=config_data.get('connect_timeout', 30),
            request_timeout=config_data.get('request_timeout', 10),
            heartbeat_interval=config_data.get('heartbeat_interval', 30),
            max_retry_attempts=config_data.get('max_retry_attempts', 3),
            retry_delay=config_data.get('retry_delay', 1.0),
            extra_params=config_data.get('extra_params', {})
        )

    def _merge_config(self, user_config: ExchangeConfig, default_config: Dict[str, Any]) -> ExchangeConfig:
        """
        合并用户配置和默认配置

        Args:
            user_config: 用户配置
            default_config: 默认配置

        Returns:
            ExchangeConfig: 合并后的配置
        """
        # 用户配置优先，默认配置作为补充
        merged_config = default_config.copy()

        # 合并rate_limits
        if 'rate_limits' in default_config and user_config.rate_limits:
            merged_rate_limits = default_config['rate_limits'].copy()
            merged_rate_limits.update(user_config.rate_limits)
            user_config.rate_limits = merged_rate_limits
        elif 'rate_limits' in default_config and not user_config.rate_limits:
            user_config.rate_limits = default_config['rate_limits']

        # 合并precision
        if 'precision' in default_config and user_config.precision:
            merged_precision = default_config['precision'].copy()
            merged_precision.update(user_config.precision)
            user_config.precision = merged_precision
        elif 'precision' in default_config and not user_config.precision:
            user_config.precision = default_config['precision']

        # 合并symbol_mapping
        if 'symbol_mapping' in default_config and user_config.symbol_mapping:
            merged_symbol_mapping = default_config['symbol_mapping'].copy()
            merged_symbol_mapping.update(user_config.symbol_mapping)
            user_config.symbol_mapping = merged_symbol_mapping
        elif 'symbol_mapping' in default_config and not user_config.symbol_mapping:
            user_config.symbol_mapping = default_config['symbol_mapping']

        # 合并extra_params
        if 'extra_params' in default_config and user_config.extra_params:
            merged_extra_params = default_config['extra_params'].copy()
            merged_extra_params.update(user_config.extra_params)
            user_config.extra_params = merged_extra_params
        elif 'extra_params' in default_config and not user_config.extra_params:
            user_config.extra_params = default_config['extra_params']

        return user_config

    def get_registered_exchanges(self) -> List[str]:
        """
        获取已注册的交易所ID列表

        Returns:
            List[str]: 交易所ID列表
        """
        return list(self._registry.keys())

    def get_exchange_info(self, exchange_id: str) -> Optional[ExchangeRegistryEntry]:
        """
        获取交易所信息

        Args:
            exchange_id: 交易所ID

        Returns:
            ExchangeRegistryEntry: 交易所注册信息
        """
        return self._registry.get(exchange_id)

    def get_exchanges_by_type(self, exchange_type: ExchangeType) -> List[str]:
        """
        根据交易所类型获取交易所ID列表

        Args:
            exchange_type: 交易所类型

        Returns:
            List[str]: 交易所ID列表
        """
        return [
            exchange_id for exchange_id, entry in self._registry.items()
            if entry.exchange_type == exchange_type
        ]

    def get_exchanges_by_feature(self, feature: str) -> List[str]:
        """
        根据功能特性获取支持的交易所ID列表

        Args:
            feature: 功能特性名称

        Returns:
            List[str]: 支持该功能的交易所ID列表
        """
        return [
            exchange_id for exchange_id, entry in self._registry.items()
            if feature in entry.supported_features
        ]

    def clear_instances(self) -> None:
        """清理所有适配器实例"""
        self._instances.clear()
        self.logger.info("清理所有交易所适配器实例")

    def get_instance(self, exchange_id: str) -> Optional[ExchangeInterface]:
        """
        获取已创建的适配器实例

        Args:
            exchange_id: 交易所ID

        Returns:
            ExchangeInterface: 适配器实例，如果不存在返回None
        """
        return self._instances.get(exchange_id)

    def list_instances(self) -> Dict[str, ExchangeInterface]:
        """
        列出所有已创建的适配器实例

        Returns:
            Dict[str, ExchangeInterface]: 实例字典
        """
        return self._instances.copy()


# 全局工厂实例 - 延迟初始化
_exchange_factory = None


def get_exchange_factory() -> ExchangeFactory:
    """获取全局交易所工厂实例（延迟初始化）"""
    global _exchange_factory
    if _exchange_factory is None:
        _exchange_factory = ExchangeFactory()
    return _exchange_factory
