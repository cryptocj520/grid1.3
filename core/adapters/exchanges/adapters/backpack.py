"""
Backpack交易所适配器 - 重构版本

基于MESA架构的Backpack适配器，提供统一的交易接口。
使用ED25519签名方式直接调用Backpack API。
整合了分离的模块：backpack_base.py、backpack_rest.py、backpack_websocket.py
"""

import asyncio
import aiohttp
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal

from ....logging import get_logger

from ..adapter import ExchangeAdapter
from ..interface import ExchangeConfig
from ..models import *
from ..subscription_manager import create_subscription_manager, DataType
from .backpack_base import BackpackBase
from .backpack_rest import BackpackRest
from .backpack_websocket import BackpackWebSocket


class BackpackAdapter(ExchangeAdapter):
    """Backpack交易所适配器 - 统一接口"""

    def __init__(self, config: ExchangeConfig, event_bus=None):
        super().__init__(config, event_bus)

        # 初始化各个模块
        self._base = BackpackBase(config)
        self._rest = BackpackRest(config, self.logger)
        self._websocket = BackpackWebSocket(config, self.logger)

        # 🔥 关键修复：让REST和WebSocket共享持仓缓存
        # 这样WebSocket收到的持仓更新可以被REST API访问到
        shared_position_cache = {}
        self._rest._position_cache = shared_position_cache
        self._websocket._position_cache = shared_position_cache
        self._position_cache = shared_position_cache  # adapter自己也引用

        # 🔥 同时共享持仓回调列表
        shared_position_callbacks = []
        self._rest._position_callbacks = shared_position_callbacks
        self._websocket._position_callbacks = shared_position_callbacks
        self._position_callbacks = shared_position_callbacks

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

        # 会话管理
        self._session = None

        # 🚀 初始化订阅管理器 - 加载Backpack配置文件
        try:
            # 尝试加载YAML配置文件
            config_dict = self._load_backpack_config()

            # 🔥 修复：获取符号缓存服务实例
            symbol_cache_service = self._get_symbol_cache_service()

            self._subscription_manager = create_subscription_manager(
                exchange_config=config_dict,
                symbol_cache_service=symbol_cache_service,
                logger=self.logger
            )

            if self.logger:
                self.logger.info(
                    f"✅ Backpack订阅管理器初始化成功，模式: {config_dict.get('subscription_mode', {}).get('mode', 'unknown')}")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"创建Backpack订阅管理器失败，使用默认配置: {e}")
            # 使用默认配置
            default_config = {
                'exchange_id': 'backpack',
                'subscription_mode': {
                    'mode': 'predefined',
                    'predefined': {
                        'symbols': ['SOL_USDC_PERP', 'BTC_USDC_PERP', 'ETH_USDC_PERP'],
                        'data_types': {'ticker': True, 'orderbook': True, 'trades': False, 'user_data': False}
                    }
                }
            }
            # 🔥 修复：获取符号缓存服务实例
            symbol_cache_service = self._get_symbol_cache_service()

            self._subscription_manager = create_subscription_manager(
                exchange_config=default_config,
                symbol_cache_service=symbol_cache_service,
                logger=self.logger
            )

    def _load_backpack_config(self) -> Dict[str, Any]:
        """加载Backpack配置文件"""
        try:
            import yaml
            from pathlib import Path

            # 构造配置文件路径
            config_path = Path(__file__).parent.parent.parent.parent.parent / \
                "config" / "exchanges" / "backpack_config.yaml"

            if not config_path.exists():
                raise FileNotFoundError(f"配置文件不存在: {config_path}")

            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)

            # 返回backpack部分的配置
            backpack_config = config_data.get('backpack', {})
            backpack_config['exchange_id'] = 'backpack'

            if self.logger:
                self.logger.info(f"成功加载Backpack配置文件: {config_path}")

            return backpack_config

        except Exception as e:
            if self.logger:
                self.logger.error(f"加载Backpack配置文件失败: {e}")
            raise

    # === 核心连接方法 ===

    async def _do_connect(self) -> bool:
        """建立连接"""
        try:
            # 创建session
            if not self._session or self._session.closed:
                self._session = aiohttp.ClientSession()

            # 设置session给各模块使用
            self._rest.session = self._session
            if hasattr(self._websocket, '_session'):
                self._websocket._session = self._session

            # 连接REST API
            rest_connected = await self._rest.connect()
            if not rest_connected:
                self.logger.error("REST API连接失败")
                return False

            # 获取支持的交易对
            await self._fetch_supported_symbols()

            self._connected = True
            self.logger.info("Backpack适配器连接成功")
            return True

        except Exception as e:
            self.logger.error(f"连接失败: {e}")
            return False

    async def _do_disconnect(self) -> None:
        """断开连接"""
        try:
            # 断开WebSocket连接
            if self._websocket:
                await self._websocket.disconnect()

            # 断开REST连接
            if self._rest:
                await self._rest.disconnect()

            # 关闭session
            if self._session and not self._session.closed:
                await self._session.close()

            self._connected = False
            self._authenticated = False
            self.logger.info("Backpack适配器已断开连接")

        except Exception as e:
            self.logger.error(f"断开连接时出错: {e}")

    async def _do_authenticate(self) -> bool:
        """认证"""
        try:
            # 通过REST API进行认证
            auth_result = await self._rest.authenticate()
            self._authenticated = auth_result

            if self._authenticated:
                self.logger.info("Backpack认证成功")
            else:
                self.logger.warning("Backpack认证失败")

            return self._authenticated

        except Exception as e:
            self.logger.error(f"认证失败: {e}")
            return False

    async def _do_health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            # 检查REST API健康状态
            health_info = await self._rest.health_check()

            # 检查WebSocket连接状态
            ws_connected = self._websocket._is_connection_usable()

            return {
                "status": "healthy" if health_info.get("status") == "healthy" else "unhealthy",
                "rest_api": health_info,
                "websocket": {"connected": ws_connected},
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            self.logger.error(f"健康检查失败: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

    async def _do_heartbeat(self) -> None:
        """心跳检测"""
        try:
            # REST API心跳
            await self._rest.heartbeat()
            # WebSocket模块已有心跳检测机制
        except Exception as e:
            self.logger.warning(f"心跳检测失败: {e}")

    # === 交易所信息方法 ===

    async def get_exchange_info(self) -> ExchangeInfo:
        """获取交易所信息"""
        return await self._rest.get_exchange_info()

    async def get_supported_symbols(self) -> List[str]:
        """获取支持的交易对列表"""
        if not self._supported_symbols:
            await self._fetch_supported_symbols()
        return self._supported_symbols.copy()

    async def _fetch_supported_symbols(self) -> None:
        """获取支持的交易对"""
        try:
            self._supported_symbols = await self._rest.get_supported_symbols()
            # 同步到其他模块
            self._base._supported_symbols = self._supported_symbols
            self._websocket._supported_symbols = self._supported_symbols

            # 获取市场信息
            self._market_info = getattr(self._rest, '_market_info', {})
            self._base._market_info = self._market_info

            self.logger.info(f"成功获取 {len(self._supported_symbols)} 个交易对")
        except Exception as e:
            self.logger.error(f"获取支持交易对失败: {e}")
            # 使用默认列表
            self._supported_symbols = self._base.get_default_symbols()

    def _map_symbol(self, symbol: str) -> str:
        """映射符号到交易所格式"""
        return self._base._map_symbol(symbol)

    def _reverse_map_symbol(self, exchange_symbol: str) -> str:
        """反向映射交易所符号到标准格式"""
        return self._base._reverse_map_symbol(exchange_symbol)

    async def get_market_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取市场信息"""
        return await self._rest.get_market_info(symbol)

    # === 市场数据方法 ===

    async def get_ticker(self, symbol: str) -> TickerData:
        """获取单个交易对的ticker数据"""
        return await self._rest.get_ticker(symbol)

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """获取多个交易对的ticker数据"""
        return await self._rest.get_tickers(symbols)

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿数据 - 统一使用公开API"""
        try:
            # 使用公开API获取订单簿快照
            snapshot = await self.get_orderbook_snapshot(symbol)
            if not snapshot:
                return OrderBookData(
                    symbol=symbol,
                    bids=[],
                    asks=[],
                    timestamp=datetime.now(),
                    nonce=None,
                    raw_data={}
                )

            # 转换为OrderBookData格式
            bids = []
            asks = []

            for bid in snapshot.get('bids', []):
                if len(bid) >= 2:
                    bids.append(OrderBookLevel(
                        price=Decimal(str(bid[0])),
                        size=Decimal(str(bid[1]))
                    ))

            for ask in snapshot.get('asks', []):
                if len(ask) >= 2:
                    asks.append(OrderBookLevel(
                        price=Decimal(str(ask[0])),
                        size=Decimal(str(ask[1]))
                    ))

            return OrderBookData(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=datetime.now(),
                nonce=None,
                raw_data=snapshot
            )

        except Exception as e:
            if self.logger:
                self.logger.error(f"获取订单簿失败 {symbol}: {e}")
            return OrderBookData(
                symbol=symbol,
                bids=[],
                asks=[],
                timestamp=datetime.now(),
                nonce=None,
                raw_data={}
            )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OHLCVData]:
        """获取K线数据"""
        return await self._rest.get_ohlcv(symbol, timeframe, since, limit)

    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """获取成交数据"""
        return await self._rest.get_trades(symbol, since, limit)

    # === 账户方法 ===

    async def get_balances(self) -> List[BalanceData]:
        """获取账户余额"""
        return await self._rest.get_balances()

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """获取持仓信息"""
        return await self._rest.get_positions(symbols)

    # === 交易方法 ===

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> OrderData:
        """创建订单"""
        # 基本参数验证
        if not symbol or not side or not order_type:
            raise ValueError("订单参数不完整：symbol, side, order_type 不能为空")

        if amount <= 0:
            raise ValueError(f"订单数量必须大于0: {amount}")

        if order_type == OrderType.LIMIT and (price is None or price <= 0):
            raise ValueError(f"限价单必须指定有效价格: {price}")

        order = await self._rest.create_order(symbol, side, order_type, amount, price, params)

        # 触发订单创建事件
        if hasattr(self, '_handle_order_update'):
            await self._handle_order_update(order)

        return order

    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """取消订单"""
        order = await self._rest.cancel_order(order_id, symbol)

        # 触发订单更新事件
        if hasattr(self, '_handle_order_update'):
            await self._handle_order_update(order)

        return order

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """取消所有订单"""
        orders = await self._rest.cancel_all_orders(symbol)

        # 触发订单更新事件
        for order in orders:
            if hasattr(self, '_handle_order_update'):
                await self._handle_order_update(order)

        return orders

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """获取订单信息"""
        return await self._rest.get_order(order_id, symbol)

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        return await self._rest.get_open_orders(symbol)

    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OrderData]:
        """获取历史订单"""
        return await self._rest.get_order_history(symbol, since, limit)

    # === 设置方法 ===

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数"""
        return await self._rest.set_leverage(symbol, leverage)

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式"""
        return await self._rest.set_margin_mode(symbol, margin_mode)

    # === WebSocket订阅方法 ===

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅ticker数据流"""
        # 确保WebSocket连接
        await self._ensure_websocket_connection()

        # 委托给WebSocket模块
        await self._websocket.subscribe_ticker(symbol, callback)

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """订阅订单簿数据流"""
        # 确保WebSocket连接
        await self._ensure_websocket_connection()

        # 委托给WebSocket模块
        await self._websocket.subscribe_orderbook(symbol, callback)

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅成交数据流"""
        # 确保WebSocket连接
        await self._ensure_websocket_connection()

        # 委托给WebSocket模块
        await self._websocket.subscribe_trades(symbol, callback)

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """订阅用户数据流（订单更新）"""
        # 确保WebSocket连接
        await self._ensure_websocket_connection()

        # 委托给WebSocket模块
        await self._websocket.subscribe_user_data(callback)

    async def subscribe_position_updates(self, symbol: str, callback: Callable) -> None:
        """
        订阅持仓更新流（实时同步持仓）

        Args:
            symbol: 交易对
            callback: 持仓更新回调函数，接收参数：
                {
                    'symbol': str,
                    'size': Decimal,  # 带符号，正数=多仓，负数=空仓
                    'entry_price': Decimal,
                    'unrealized_pnl': Decimal,
                    'side': str  # 'Long' or 'Short'
                }
        """
        # 确保WebSocket连接
        await self._ensure_websocket_connection()

        # 委托给WebSocket模块
        await self._websocket.subscribe_position_updates(symbol, callback)

        if self.logger:
            self.logger.info(f"✅ BackpackAdapter: 已订阅持仓更新流 ({symbol})")

    async def batch_subscribe_tickers(
        self,
        symbols: Optional[List[str]] = None,
        callback: Optional[Callable[[str, TickerData], None]] = None
    ) -> None:
        """批量订阅ticker数据（支持硬编码和动态两种模式）"""
        try:
            # 确保WebSocket连接
            await self._ensure_websocket_connection()

            # 🚀 使用订阅管理器确定要订阅的交易对
            if symbols is None:
                # 没有提供symbols，使用订阅管理器
                if self._subscription_manager.mode.value == "predefined":
                    # 硬编码模式：使用配置文件中的交易对
                    symbols = self._subscription_manager.get_subscription_symbols()
                    if self.logger:
                        self.logger.info(
                            f"🔧 Backpack硬编码模式：使用配置文件中的 {len(symbols)} 个交易对")
                else:
                    # 动态模式：从市场发现交易对
                    symbols = await self._subscription_manager.discover_symbols(self.get_supported_symbols)
                    if self.logger:
                        self.logger.info(
                            f"🔧 Backpack动态模式：发现 {len(symbols)} 个交易对")

            # 检查是否应该订阅ticker数据
            if not self._subscription_manager.should_subscribe_data_type(DataType.TICKER):
                if self.logger:
                    self.logger.info("配置中禁用了ticker数据订阅，跳过")
                return

            if not symbols:
                if self.logger:
                    self.logger.warning("没有找到要订阅的交易对")
                return

            # 将订阅添加到管理器
            for symbol in symbols:
                self._subscription_manager.add_subscription(
                    symbol=symbol,
                    data_type=DataType.TICKER,
                    callback=callback
                )

            # 委托给WebSocket模块
            await self._websocket.batch_subscribe_tickers(symbols, callback)

            if self.logger:
                self.logger.info(f"✅ Backpack批量订阅ticker完成: {len(symbols)}个交易对")

        except Exception as e:
            if self.logger:
                self.logger.error(f"Backpack批量订阅ticker失败: {str(e)}")
            raise

    async def batch_subscribe_orderbooks(
        self,
        symbols: Optional[List[str]] = None,
        callback: Optional[Callable[[str, OrderBookData], None]] = None
    ) -> None:
        """批量订阅订单簿数据（支持硬编码和动态两种模式）"""
        try:
            # 确保WebSocket连接
            await self._ensure_websocket_connection()

            # 🚀 使用订阅管理器确定要订阅的交易对
            if symbols is None:
                # 没有提供symbols，使用订阅管理器
                if self._subscription_manager.mode.value == "predefined":
                    # 硬编码模式：使用配置文件中的交易对
                    symbols = self._subscription_manager.get_subscription_symbols()
                    if self.logger:
                        self.logger.info(
                            f"🔧 Backpack硬编码模式：使用配置文件中的 {len(symbols)} 个交易对")
                else:
                    # 动态模式：从市场发现交易对
                    symbols = await self._subscription_manager.discover_symbols(self.get_supported_symbols)
                    if self.logger:
                        self.logger.info(
                            f"🔧 Backpack动态模式：发现 {len(symbols)} 个交易对")

            # 检查是否应该订阅orderbook数据
            if not self._subscription_manager.should_subscribe_data_type(DataType.ORDERBOOK):
                if self.logger:
                    self.logger.info("配置中禁用了orderbook数据订阅，跳过")
                return

            if not symbols:
                if self.logger:
                    self.logger.warning("没有找到要订阅的交易对")
                return

            # 将订阅添加到管理器
            for symbol in symbols:
                self._subscription_manager.add_subscription(
                    symbol=symbol,
                    data_type=DataType.ORDERBOOK,
                    callback=callback
                )

            # 委托给WebSocket模块
            await self._websocket.batch_subscribe_orderbooks(symbols, callback)

            if self.logger:
                self.logger.info(
                    f"✅ Backpack批量订阅orderbook完成: {len(symbols)}个交易对")

        except Exception as e:
            if self.logger:
                self.logger.error(f"Backpack批量订阅orderbook失败: {str(e)}")
            raise

    async def batch_subscribe_all_tickers(
        self,
        callback: Optional[Callable[[str, TickerData], None]] = None
    ) -> None:
        """批量订阅所有ticker数据"""
        # 确保WebSocket连接
        await self._ensure_websocket_connection()

        # 委托给WebSocket模块
        await self._websocket.batch_subscribe_all_tickers(callback)

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        if self._websocket:
            await self._websocket.unsubscribe(symbol)

    async def unsubscribe_all(self) -> None:
        """取消所有订阅"""
        if self._websocket:
            await self._websocket.unsubscribe_all()

    def get_subscription_manager(self):
        """获取订阅管理器实例"""
        return self._subscription_manager

    def get_subscription_stats(self) -> Dict[str, Any]:
        """获取订阅统计信息"""
        return self._subscription_manager.get_subscription_stats()

    # === 其他原始备份脚本中的方法 ===

    async def _make_authenticated_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """发起需要认证的API请求 - 委托给REST模块"""
        return await self._rest._make_authenticated_request(method, endpoint, params, data)

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """获取原始ticker数据"""
        return await self._rest.fetch_ticker(symbol)

    async def fetch_all_tickers(self) -> List[Dict[str, Any]]:
        """获取所有原始ticker数据"""
        return await self._rest.fetch_all_tickers()

    async def fetch_orderbook(self, symbol: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """获取原始订单簿数据"""
        return await self._rest.fetch_orderbook(symbol, limit)

    async def get_orderbook_snapshot(self, symbol: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        获取订单簿完整快照 - 通过REST API

        Args:
            symbol: 交易对符号
            limit: 深度限制 (可选)

        Returns:
            Dict: 完整的订单簿快照数据
        """
        return await self._rest.get_orderbook_snapshot(symbol, limit)

    async def fetch_trades(self, symbol: str, since: Optional[int] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取原始交易数据"""
        return await self._rest.fetch_trades(symbol, since, limit)

    async def get_klines(self, symbol: str, interval: str, since: Optional[datetime] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取K线数据"""
        return await self._rest.get_klines(symbol, interval, since, limit)

    async def fetch_balances(self) -> Dict[str, Any]:
        """获取原始余额数据"""
        return await self._rest.fetch_balances()

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        time_in_force: str = "GTC",
        client_order_id: Optional[str] = None
    ) -> OrderData:
        """下单 - 委托给REST模块"""
        return await self._rest.place_order(symbol, side, order_type, quantity, price, time_in_force, client_order_id)

    async def cancel_order_by_id(self, symbol: str, order_id: Optional[str] = None, client_order_id: Optional[str] = None) -> bool:
        """通过ID取消订单"""
        return await self._rest.cancel_order_by_id(symbol, order_id, client_order_id)

    async def get_order_status(self, symbol: str, order_id: Optional[str] = None, client_order_id: Optional[str] = None) -> OrderData:
        """获取订单状态"""
        return await self._rest.get_order_status(symbol, order_id, client_order_id)

    async def get_recent_trades(self, symbol: str, limit: int = 500) -> List[Dict[str, Any]]:
        """获取最近成交"""
        return await self._rest.get_recent_trades(symbol, limit)

    # === 符号处理方法 ===

    def _normalize_backpack_symbol(self, symbol: str) -> str:
        """标准化Backpack符号格式"""
        return self._base._normalize_backpack_symbol(symbol)

    def get_symbol_info(self, symbol: str):
        """获取交易对信息"""
        return self._base.get_symbol_info(symbol)

    def is_valid_symbol(self, symbol: str) -> bool:
        """检查符号是否有效"""
        return self._base.is_valid_symbol(symbol)

    def get_default_symbols(self) -> List[str]:
        """获取默认支持的交易对"""
        return self._base.get_default_symbols()

    def validate_order_params(self, symbol: str, side: OrderSide, order_type: OrderType,
                              amount: Decimal, price: Optional[Decimal] = None) -> bool:
        """验证订单参数"""
        return self._base.validate_order_params(symbol, side, order_type, amount, price)

    def format_quantity(self, symbol: str, quantity: Decimal):
        """格式化数量精度"""
        symbol_info = self._base.get_symbol_info(symbol)
        return self._base.format_quantity(symbol, quantity, symbol_info)

    def format_price(self, symbol: str, price: Decimal):
        """格式化价格精度"""
        symbol_info = self._base.get_symbol_info(symbol)
        return self._base.format_price(symbol, price, symbol_info)

    def get_min_order_amount(self, symbol: str) -> Decimal:
        """获取最小订单数量"""
        return self._base.get_min_order_amount(symbol)

    def get_max_order_amount(self, symbol: str) -> Decimal:
        """获取最大订单数量"""
        return self._base.get_max_order_amount(symbol)

    def get_price_precision(self, symbol: str) -> int:
        """获取价格精度"""
        return self._base.get_price_precision(symbol)

    def get_qty_precision(self, symbol: str) -> int:
        """获取数量精度"""
        return self._base.get_qty_precision(symbol)

    def is_perpetual_contract(self, symbol: str) -> bool:
        """判断是否为永续合约"""
        return self._base.is_perpetual_contract(symbol)

    def extract_base_quote(self, symbol: str) -> tuple:
        """提取base和quote货币"""
        return self._base.extract_base_quote(symbol)

    def build_symbol(self, base: str, quote: str, contract_type: str = 'PERP') -> str:
        """构建符号"""
        return self._base.build_symbol(base, quote, contract_type)

    def get_contract_type(self, symbol: str) -> str:
        """获取合约类型"""
        return self._base.get_contract_type(symbol)

    # === 事件处理方法 ===

    async def _handle_ticker_update(self, ticker: TickerData) -> None:
        """处理ticker更新事件"""
        try:
            if self.event_bus:
                await self.event_bus.emit('ticker_update', {
                    'exchange': 'backpack',
                    'symbol': ticker.symbol,
                    'data': ticker
                })
        except Exception as e:
            self.logger.warning(f"处理ticker更新事件失败: {e}")

    async def _handle_orderbook_update(self, orderbook: OrderBookData) -> None:
        """处理订单簿更新事件"""
        try:
            if self.event_bus:
                await self.event_bus.emit('orderbook_update', {
                    'exchange': 'backpack',
                    'symbol': orderbook.symbol,
                    'data': orderbook
                })
        except Exception as e:
            self.logger.warning(f"处理订单簿更新事件失败: {e}")

    async def _handle_order_update(self, order: OrderData) -> None:
        """处理订单更新事件"""
        try:
            if self.event_bus:
                await self.event_bus.emit('order_update', {
                    'exchange': 'backpack',
                    'symbol': order.symbol,
                    'data': order
                })
        except Exception as e:
            self.logger.warning(f"处理订单更新事件失败: {e}")

    # === 属性和工具方法 ===

    async def get_market_status(self, symbol: str) -> Dict[str, Any]:
        """获取市场状态"""
        try:
            market_info = await self.get_market_info(symbol)
            return {
                "symbol": symbol,
                "status": "active" if market_info else "inactive",
                "info": market_info,
                "timestamp": datetime.now()
            }
        except Exception as e:
            return {
                "symbol": symbol,
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now()
            }

    @property
    def supported_symbols(self) -> List[str]:
        """支持的交易对列表（同步属性）"""
        return self._supported_symbols

    def is_connected(self) -> bool:
        """是否已连接"""
        return self._connected

    def is_authenticated(self) -> bool:
        """是否已认证"""
        return self._authenticated

    # === 内部方法 ===

    async def _ensure_websocket_connection(self) -> None:
        """确保WebSocket连接已建立"""
        if not self._websocket._is_connection_usable():
            await self._websocket.connect()

    def _get_symbol_cache_service(self):
        """获取符号缓存服务实例"""
        try:
            # 尝试从依赖注入容器获取符号缓存服务
            from ....di.container import get_container
            from ....services.symbol_manager.interfaces.symbol_cache import ISymbolCacheService

            container = get_container()
            symbol_cache_service = container.get(ISymbolCacheService)

            if self.logger:
                self.logger.info("✅ 获取符号缓存服务成功")
            return symbol_cache_service

        except Exception as e:
            if self.logger:
                self.logger.warning(f"⚠️ 获取符号缓存服务失败: {e}，返回None")
            return None

    def __repr__(self) -> str:
        return f"BackpackAdapter(connected={self.is_connected}, authenticated={self.is_authenticated})"
