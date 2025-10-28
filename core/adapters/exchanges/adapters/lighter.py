"""
Lighter交易所适配器

基于MESA架构的Lighter适配器，提供统一的交易接口。
使用Lighter SDK进行API交互和WebSocket连接。
整合了分离的模块：lighter_base.py、lighter_rest.py、lighter_websocket.py
"""

import asyncio
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal
import yaml
import os

from ....logging import get_logger

from ..adapter import ExchangeAdapter
from ..interface import ExchangeConfig
from ..models import *
from ..subscription_manager import create_subscription_manager, DataType
from .lighter_base import LighterBase
from .lighter_rest import LighterRest
from .lighter_websocket import LighterWebSocket


class LighterAdapter(ExchangeAdapter):
    """Lighter交易所适配器 - 统一接口"""

    def __init__(self, config: ExchangeConfig, event_bus=None):
        super().__init__(config, event_bus)

        # 初始化各个模块
        config_dict = self._convert_config_to_dict(config)
        self._base = LighterBase(config_dict)
        self._rest = LighterRest(config_dict)
        self._websocket = LighterWebSocket(config_dict)

        # 共享数据缓存
        shared_position_cache = {}
        shared_order_cache = {}

        self._position_cache = shared_position_cache
        self._order_cache = shared_order_cache

        # 设置回调列表
        shared_position_callbacks = []
        shared_order_callbacks = []

        self._position_callbacks = shared_position_callbacks
        self._order_callbacks = shared_order_callbacks

        # 设置基础URL
        self.base_url = getattr(
            config, 'base_url', None) or self._base.base_url
        self.ws_url = getattr(config, 'ws_url', None) or self._base.ws_url

        # 符号映射
        self._symbol_mapping = getattr(config, 'symbol_mapping', {})

        # 连接状态
        self._connected = False
        self._authenticated = False

        # 缓存支持的交易对
        self._supported_symbols = []
        self._market_info = {}

        # 初始化订阅管理器
        try:
            config_dict = self._load_lighter_config()

            symbol_cache_service = self._get_symbol_cache_service()

            self._subscription_manager = create_subscription_manager(
                exchange_config=config_dict,
                symbol_cache_service=symbol_cache_service,
                logger=self.logger
            )

            if self.logger:
                self.logger.info(
                    f"✅ Lighter订阅管理器初始化成功，模式: {config_dict.get('subscription_mode', {}).get('mode', 'unknown')}")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"创建Lighter订阅管理器失败，使用默认配置: {e}")
            # 使用默认配置
            default_config = {
                'exchange_id': 'lighter',
                'subscription_mode': {
                    'mode': 'predefined',
                    'predefined': {
                        'symbols': ['BTC-USD', 'ETH-USD', 'SOL-USD'],
                        'data_types': {'ticker': True, 'orderbook': True, 'trades': False, 'user_data': False}
                    }
                }
            }

            symbol_cache_service = self._get_symbol_cache_service()
            self._subscription_manager = create_subscription_manager(
                exchange_config=default_config,
                symbol_cache_service=symbol_cache_service,
                logger=self.logger
            )

        self.logger.info("Lighter适配器初始化完成")

    def _convert_config_to_dict(self, config: ExchangeConfig) -> Dict[str, Any]:
        """
        将ExchangeConfig转换为字典

        Args:
            config: ExchangeConfig对象

        Returns:
            配置字典
        """
        config_dict = {
            "testnet": getattr(config, 'testnet', False),
            "api_key_private_key": getattr(config, 'api_key_private_key', ''),
            "account_index": getattr(config, 'account_index', 0),
            "api_key_index": getattr(config, 'api_key_index', 0),
        }

        # 添加可选配置
        if hasattr(config, 'api_url'):
            config_dict['api_url'] = config.api_url
        if hasattr(config, 'ws_url'):
            config_dict['ws_url'] = config.ws_url

        return config_dict

    def _load_lighter_config(self) -> Dict[str, Any]:
        """加载Lighter配置文件"""
        config_path = "config/exchanges/lighter_config.yaml"

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                if self.logger:
                    self.logger.info(f"✅ 加载Lighter配置文件: {config_path}")
                return config
        except FileNotFoundError:
            if self.logger:
                self.logger.warning(f"Lighter配置文件未找到: {config_path}")
            return {'exchange_id': 'lighter'}
        except Exception as e:
            if self.logger:
                self.logger.error(f"加载Lighter配置文件失败: {e}")
            return {'exchange_id': 'lighter'}

    def _get_symbol_cache_service(self):
        """获取符号缓存服务实例"""
        try:
            from core.services.symbol_manager.implementations.symbol_conversion_service import SymbolConversionService
            return SymbolConversionService.get_instance()
        except Exception as e:
            if self.logger:
                self.logger.warning(f"无法获取符号缓存服务: {e}")
            return None

    # ============= 连接管理 =============

    async def connect(self) -> bool:
        """
        建立连接

        Returns:
            是否连接成功
        """
        try:
            if self._connected:
                self.logger.info("已经连接到Lighter")
                return True

            # 初始化REST客户端
            await self._rest.initialize()

            # 建立WebSocket连接
            await self._websocket.connect()

            # 加载市场信息
            await self._load_market_info()

            self._connected = True
            self._authenticated = bool(self._rest.signer_client)

            self.logger.info("✅ 成功连接到Lighter交易所")
            return True

        except Exception as e:
            self.logger.error(f"连接Lighter失败: {e}")
            return False

    async def disconnect(self):
        """断开连接"""
        try:
            # 关闭WebSocket
            await self._websocket.disconnect()

            # 关闭REST客户端
            await self._rest.close()

            self._connected = False
            self._authenticated = False

            self.logger.info("已断开与Lighter的连接")

        except Exception as e:
            self.logger.error(f"断开Lighter连接时出错: {e}")

    async def _load_market_info(self):
        """加载市场信息"""
        try:
            exchange_info = await self._rest.get_exchange_info()

            if exchange_info and exchange_info.symbols:
                self._supported_symbols = [s['symbol']
                                           for s in exchange_info.symbols]
                self._market_info = {s['symbol']                                     : s for s in exchange_info.symbols}

                # 更新base模块的市场缓存
                self._base.update_markets_cache(exchange_info.symbols)

                # 同步到REST和WebSocket模块
                self._rest._markets_cache = self._base._markets_cache
                self._rest._symbol_to_market_index = self._base._symbol_to_market_index
                self._websocket._markets_cache = self._base._markets_cache
                self._websocket._symbol_to_market_index = self._base._symbol_to_market_index

                self.logger.info(f"加载了 {len(self._supported_symbols)} 个交易对")
        except Exception as e:
            self.logger.error(f"加载市场信息失败: {e}")

    def is_connected(self) -> bool:
        """
        检查是否已连接

        Returns:
            是否已连接
        """
        return self._connected

    # ============= 市场数据 =============

    async def get_exchange_info(self) -> ExchangeInfo:
        """
        获取交易所信息

        Returns:
            ExchangeInfo对象
        """
        return await self._rest.get_exchange_info()

    async def get_ticker(self, symbol: str) -> Optional[TickerData]:
        """
        获取ticker数据

        Args:
            symbol: 交易对符号

        Returns:
            TickerData对象
        """
        normalized_symbol = self._normalize_symbol(symbol)
        return await self._rest.get_ticker(normalized_symbol)

    async def get_orderbook(self, symbol: str, limit: int = 20) -> Optional[OrderBookData]:
        """
        获取订单簿

        Args:
            symbol: 交易对符号
            limit: 深度限制

        Returns:
            OrderBookData对象
        """
        normalized_symbol = self._normalize_symbol(symbol)
        return await self._rest.get_orderbook(normalized_symbol, limit)

    async def get_recent_trades(self, symbol: str, limit: int = 100) -> List[TradeData]:
        """
        获取最近成交

        Args:
            symbol: 交易对符号
            limit: 数量限制

        Returns:
            TradeData列表
        """
        normalized_symbol = self._normalize_symbol(symbol)
        return await self._rest.get_recent_trades(normalized_symbol, limit)

    # ============= 账户信息 =============

    async def get_account_balance(self) -> List[BalanceData]:
        """
        获取账户余额

        Returns:
            BalanceData列表
        """
        return await self._rest.get_account_balance()

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """
        获取活跃订单

        Args:
            symbol: 交易对符号（可选）

        Returns:
            OrderData列表
        """
        normalized_symbol = self._normalize_symbol(symbol) if symbol else None
        return await self._rest.get_open_orders(normalized_symbol)

    async def get_positions(self) -> List[PositionData]:
        """
        获取持仓信息

        Returns:
            PositionData列表
        """
        # 优先从缓存获取
        if self._position_cache:
            return list(self._position_cache.values())

        # 否则从REST API获取
        return await self._rest.get_positions()

    # ============= 交易功能 =============

    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        **kwargs
    ) -> Optional[OrderData]:
        """
        下单

        Args:
            symbol: 交易对符号
            side: 订单方向 ("buy" 或 "sell")
            order_type: 订单类型 ("limit" 或 "market")
            quantity: 数量
            price: 价格（限价单必需）
            **kwargs: 其他参数

        Returns:
            OrderData对象
        """
        normalized_symbol = self._normalize_symbol(symbol)
        return await self._rest.place_order(
            normalized_symbol, side, order_type, quantity, price, **kwargs
        )

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """
        取消订单

        Args:
            symbol: 交易对符号
            order_id: 订单ID

        Returns:
            是否成功
        """
        normalized_symbol = self._normalize_symbol(symbol)
        return await self._rest.cancel_order(normalized_symbol, order_id)

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> bool:
        """
        取消所有订单

        Args:
            symbol: 交易对符号（可选，为None时取消所有）

        Returns:
            是否成功
        """
        try:
            orders = await self.get_open_orders(symbol)

            results = []
            for order in orders:
                result = await self.cancel_order(order.symbol, order.order_id)
                results.append(result)

            return all(results) if results else True

        except Exception as e:
            self.logger.error(f"取消所有订单失败: {e}")
            return False

    # ============= WebSocket订阅 =============

    async def subscribe_ticker(self, symbol: str, callback: Optional[Callable] = None):
        """
        订阅ticker数据

        Args:
            symbol: 交易对符号
            callback: 数据回调函数
        """
        normalized_symbol = self._normalize_symbol(symbol)
        await self._websocket.subscribe_ticker(normalized_symbol, callback)

    async def subscribe_orderbook(self, symbol: str, callback: Optional[Callable] = None):
        """
        订阅订单簿

        Args:
            symbol: 交易对符号
            callback: 数据回调函数
        """
        normalized_symbol = self._normalize_symbol(symbol)
        await self._websocket.subscribe_orderbook(normalized_symbol, callback)

    async def subscribe_trades(self, symbol: str, callback: Optional[Callable] = None):
        """
        订阅成交数据

        Args:
            symbol: 交易对符号
            callback: 数据回调函数
        """
        normalized_symbol = self._normalize_symbol(symbol)
        await self._websocket.subscribe_trades(normalized_symbol, callback)

    async def subscribe_orders(self, callback: Optional[Callable] = None):
        """
        订阅订单更新

        Args:
            callback: 数据回调函数
        """
        if callback:
            self._order_callbacks.append(callback)
        await self._websocket.subscribe_orders(callback)

    async def subscribe_positions(self, callback: Optional[Callable] = None):
        """
        订阅持仓更新

        Args:
            callback: 数据回调函数
        """
        if callback:
            self._position_callbacks.append(callback)
        await self._websocket.subscribe_positions(callback)

    async def unsubscribe_ticker(self, symbol: str):
        """取消订阅ticker"""
        normalized_symbol = self._normalize_symbol(symbol)
        await self._websocket.unsubscribe_ticker(normalized_symbol)

    async def unsubscribe_orderbook(self, symbol: str):
        """取消订阅订单簿"""
        normalized_symbol = self._normalize_symbol(symbol)
        await self._websocket.unsubscribe_orderbook(normalized_symbol)

    async def unsubscribe_trades(self, symbol: str):
        """取消订阅成交"""
        normalized_symbol = self._normalize_symbol(symbol)
        await self._websocket.unsubscribe_trades(normalized_symbol)

    # ============= 杠杆和保证金 =============

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """
        设置杠杆

        Args:
            symbol: 交易对符号
            leverage: 杠杆倍数

        Returns:
            是否成功
        """
        # Lighter SDK中可能有对应的方法，这里先返回True
        self.logger.warning("Lighter适配器暂不支持设置杠杆")
        return True

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> bool:
        """
        设置保证金模式

        Args:
            symbol: 交易对符号
            margin_mode: 保证金模式 ("cross" 或 "isolated")

        Returns:
            是否成功
        """
        # Lighter支持保证金模式，但需要通过SDK实现
        self.logger.warning("Lighter适配器暂不支持设置保证金模式")
        return True

    # ============= 辅助方法 =============

    def _normalize_symbol(self, symbol: str) -> str:
        """
        标准化交易对符号

        Args:
            symbol: 原始符号

        Returns:
            标准化后的符号
        """
        if not symbol:
            return symbol

        # 先检查是否有自定义映射
        if symbol in self._symbol_mapping:
            return self._symbol_mapping[symbol]

        # 使用base模块的标准化方法
        return self._base.normalize_symbol(symbol)

    def get_supported_symbols(self) -> List[str]:
        """
        获取支持的交易对列表

        Returns:
            交易对符号列表
        """
        return self._supported_symbols.copy()

    def __repr__(self) -> str:
        """字符串表示"""
        return f"LighterAdapter(connected={self._connected}, symbols={len(self._supported_symbols)})"
