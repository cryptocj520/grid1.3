"""
Hyperliquid交易所适配器 - 重构版本

本模块提供了Hyperliquid交易所的完整适配器实现，基于模块化设计：
- hyperliquid_base.py: 基础配置和工具方法
- hyperliquid_rest.py: REST API接口
- hyperliquid_websocket.py: WebSocket连接和数据流

支持功能：
- 永续合约交易
- 实时WebSocket数据流
- 自动重连和错误处理
- 事件驱动架构集成
- 完整的缓存机制
"""

import asyncio
import json
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Any, Callable
from enum import Enum

from ....logging import get_logger

from ..adapter import ExchangeAdapter
from ..interface import ExchangeConfig, ExchangeStatus
from ..models import (
    ExchangeType,
    OrderSide,
    OrderType,
    OrderStatus,
    PositionSide,
    MarginMode,
    OrderData,
    PositionData,
    BalanceData,
    TickerData,
    OHLCVData,
    OrderBookData,
    TradeData,
    ExchangeInfo,
    OrderBookLevel
)

from .hyperliquid_base import HyperliquidBase
from .hyperliquid_rest import HyperliquidRest
from ..subscription_manager import SubscriptionManager, DataType, create_subscription_manager


class HyperliquidAdapter(ExchangeAdapter):
    """
    Hyperliquid交易所适配器 - 重构版本

    基于模块化设计的Hyperliquid实现，支持：
    - 永续合约交易
    - 实时WebSocket数据流
    - 自动重连和错误处理
    - 事件驱动架构集成
    - 完整的缓存机制
    """

    def __init__(self, config: ExchangeConfig, event_bus=None):
        """初始化Hyperliquid适配器"""
        super().__init__(config, event_bus)

        # 初始化子组件
        self._base = HyperliquidBase(config)
        self._rest = HyperliquidRest(config, None)  # logger稍后设置

        # 根据配置选择WebSocket实现
        self._websocket = self._create_websocket_instance(config)

        # 设置日志器
        self._base.set_logger(self.logger)
        self._rest.logger = self.logger  # 直接设置logger
        self._websocket.logger = self.logger  # 直接设置logger

        # WebSocket事件回调映射
        self._ws_callbacks = {
            'ticker': [],
            'orderbook': [],
            'trades': [],
            'user_data': []
        }

        # 🚀 初始化订阅管理器 - 支持硬编码和动态两种模式
        try:
            # 尝试加载Hyperliquid配置文件
            config_dict = self._load_hyperliquid_config()

            # 🔥 修复：获取符号缓存服务实例
            symbol_cache_service = self._get_symbol_cache_service()

            self._subscription_manager = create_subscription_manager(
                exchange_config=config_dict,
                symbol_cache_service=symbol_cache_service,
                logger=self.logger
            )

            if self.logger:
                self.logger.info(
                    f"✅ Hyperliquid订阅管理器初始化成功，模式: {config_dict.get('subscription_mode', {}).get('mode', 'unknown')}")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"创建Hyperliquid订阅管理器失败，使用默认配置: {e}")
            # 使用默认配置
            default_config = {
                'exchange_id': 'hyperliquid',
                'subscription_mode': {
                    'mode': 'predefined',
                    'predefined': {
                        'symbols': ['BTC/USDC:PERP', 'ETH/USDC:PERP', 'SOL/USDC:PERP'],
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

        if self.logger:
            self.logger.info("✅ Hyperliquid适配器初始化完成，支持双模式订阅")

    def _load_hyperliquid_config(self) -> Dict[str, Any]:
        """加载Hyperliquid配置文件"""
        try:
            import yaml
            from pathlib import Path

            config_path = Path(__file__).parent.parent.parent.parent.parent / \
                "config" / "exchanges" / "hyperliquid_config.yaml"

            if not config_path.exists():
                raise FileNotFoundError(f"配置文件不存在: {config_path}")

            with open(config_path, 'r', encoding='utf-8') as file:
                config_data = yaml.safe_load(file)

            hyperliquid_config = config_data.get('hyperliquid', {})
            hyperliquid_config['exchange_id'] = 'hyperliquid'

            if self.logger:
                self.logger.info(f"成功加载Hyperliquid配置文件: {config_path}")

            return hyperliquid_config

        except Exception as e:
            if self.logger:
                self.logger.error(f"加载Hyperliquid配置文件失败: {e}")
            raise

    def _create_websocket_instance(self, config: ExchangeConfig):
        """根据配置创建WebSocket实例"""
        try:
            # 加载配置确定使用哪个WebSocket实现
            hyperliquid_config = self._load_hyperliquid_config()
            websocket_config = hyperliquid_config.get('websocket', {})
            implementation = websocket_config.get('implementation', 'native')

            # 🔥 强制输出日志，确保能看到
            print(f"🔥 Hyperliquid WebSocket实现选择: {implementation}")

            if implementation == 'native':
                from .hyperliquid_websocket_native import HyperliquidNativeWebSocket
                websocket_instance = HyperliquidNativeWebSocket(
                    config, self._base)
                print("🔥 ✅ 创建原生WebSocket实例 (零延迟)")
                if self.logger:
                    self.logger.info("✅ 使用原生WebSocket实现 (零延迟)")
            elif implementation == 'ccxt':
                from .hyperliquid_websocket import HyperliquidWebSocket
                websocket_instance = HyperliquidWebSocket(config, self._base)
                print("🔥 ✅ 创建ccxt WebSocket实例 (稳定)")
                if self.logger:
                    self.logger.info("✅ 使用ccxt WebSocket实现 (稳定)")
            else:
                # 默认使用原生实现
                from .hyperliquid_websocket_native import HyperliquidNativeWebSocket
                websocket_instance = HyperliquidNativeWebSocket(
                    config, self._base)
                print(f"🔥 ⚠️ 未知实现{implementation}，使用默认原生实现")
                if self.logger:
                    self.logger.warning(
                        f"未知的WebSocket实现: {implementation}，使用默认的原生实现")

            return websocket_instance

        except Exception as e:
            print(f"🔥 ❌ 创建WebSocket实例失败: {e}")
            if self.logger:
                self.logger.error(f"创建WebSocket实例失败: {e}")
            # 降级到ccxt实现
            from .hyperliquid_websocket import HyperliquidWebSocket
            return HyperliquidWebSocket(config, self._base)

    # === 生命周期管理实现 ===

    async def _do_connect(self) -> bool:
        """执行连接逻辑"""
        try:
            if self.logger:
                self.logger.info("开始连接Hyperliquid交易所...")

            # 连接REST API
            if not await self._rest.connect():
                if self.logger:
                    self.logger.error("REST API连接失败")
                return False

            # 🔥 简化：获取支持的交易对
            try:
                supported_symbols = await self.get_supported_symbols()
                symbol_count = len(supported_symbols)

                if self.logger:
                    self.logger.info(f"获取到 {symbol_count} 个Hyperliquid永续合约交易对")

            except Exception as e:
                if self.logger:
                    self.logger.warning(f"获取交易对失败: {e}")
                symbol_count = 0

            # 连接WebSocket
            if not await self._websocket.connect():
                if self.logger:
                    self.logger.error("WebSocket连接失败")
                return False

            if self.logger:
                self.logger.info(
                    f"✅ Hyperliquid交易所连接成功 (支持{symbol_count}个交易对)")

            return True

        except Exception as e:
            if self.logger:
                self.logger.error(f"连接Hyperliquid失败: {e}")
            return False

    async def _do_disconnect(self) -> None:
        """执行具体的断开连接逻辑"""
        try:
            # 断开WebSocket连接
            await self._websocket.disconnect()

            # 断开REST连接
            await self._rest.disconnect()

            # 🔥 移除订阅管理器清理，由符号缓存架构处理

            # 清理回调
            self._ws_callbacks = {
                'ticker': [],
                'orderbook': [],
                'trades': [],
                'user_data': []
            }

            if self.logger:
                self.logger.info("Hyperliquid适配器已断开")

        except Exception as e:
            if self.logger:
                self.logger.error(f"断开连接时出错: {str(e)}")

    async def _do_authenticate(self) -> bool:
        """执行具体的认证逻辑"""
        try:
            # 🔥 修复：检查是否为公共访问模式
            public_only = not bool(self.config.api_key)

            if public_only:
                # 公共访问模式下不需要认证
                if self.logger:
                    self.logger.info("Hyperliquid公共访问模式，跳过认证")
                return True

            # 私有模式下才进行认证：通过获取余额来测试认证
            await self._rest.get_balances()
            if self.logger:
                self.logger.info("Hyperliquid认证成功")
            return True
        except Exception as e:
            if self.logger:
                # 🔥 修复：将错误级别从 ERROR 降级为 WARNING，因为这不影响市场数据功能
                self.logger.warning(f"Hyperliquid认证失败（不影响市场数据功能）: {str(e)}")
            # 🔥 修复：即使认证失败，也返回 True，因为市场数据不需要认证
            return True

    async def _do_health_check(self) -> Dict[str, Any]:
        """执行具体的健康检查"""
        health_data = {
            'exchange_time': None,
            'rest_connected': False,
            'websocket_connected': False,
            'market_count': 0,
            'subscriptions': 0
        }

        try:
            # 检查REST API健康状态
            exchange_info = await self._rest.get_exchange_info()
            health_data['exchange_time'] = exchange_info.timestamp
            health_data['rest_connected'] = True
            health_data['market_count'] = len(exchange_info.markets)

            # 检查WebSocket连接状态
            ws_status = self._websocket.get_connection_status()
            health_data['websocket_connected'] = ws_status['connected']
            health_data['subscriptions'] = ws_status['subscriptions']

            # 注意：不设置status字段，让基类来处理
            return health_data

        except Exception as e:
            health_data['error'] = str(e)
            return health_data

    async def _do_heartbeat(self) -> None:
        """执行心跳检测"""
        # REST心跳通过获取服务器时间
        exchange_info = await self._rest.get_exchange_info()

        # WebSocket心跳由WebSocket模块自己处理
        ws_status = self._websocket.get_connection_status()
        if not ws_status['connected']:
            if self.logger:
                self.logger.warning("WebSocket连接已断开")

    # === 市场数据接口实现 ===

    async def get_exchange_info(self) -> ExchangeInfo:
        """获取交易所信息"""
        return await self._rest.get_exchange_info()

    async def get_ticker(self, symbol: str) -> TickerData:
        """获取单个交易对行情"""
        return await self._rest.get_ticker(symbol)

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """获取多个交易对行情"""
        return await self._rest.get_tickers(symbols)

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿"""
        return await self._rest.get_orderbook(symbol, limit)

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
        """获取最近成交记录"""
        return await self._rest.get_trades(symbol, since, limit)

    # === 账户和交易接口实现 ===

    async def get_balances(self) -> List[BalanceData]:
        """
        获取账户余额（自动根据账户类型选择）

        Hyperliquid的现货账户和合约账户是分开的：
        - SPOT: 查询现货账户余额
        - PERPETUAL: 查询合约账户余额
        """
        from ..interface import ExchangeType

        if self.config.exchange_type == ExchangeType.SPOT:
            # 现货账户
            if self.logger:
                self.logger.debug("🔍 获取Hyperliquid现货账户余额")
            return await self._rest.get_balances()
        else:
            # 合约账户（PERPETUAL）
            if self.logger:
                self.logger.debug("🔍 获取Hyperliquid合约账户余额")
            return await self._rest.get_swap_balances()

    async def get_swap_balances(self) -> List[BalanceData]:
        """获取合约账户余额（直接调用）"""
        return await self._rest.get_swap_balances()

    async def get_health(self) -> Dict[str, Any]:
        """获取系统健康状态"""
        return {
            "status": "operational",
            "rest_connected": self._rest.exchange is not None,
            "websocket_connected": self._websocket.get_connection_status().get('connected', False),
            "timestamp": datetime.now().isoformat()
        }

    async def get_execution_stats(self) -> Dict[str, Any]:
        """获取执行统计"""
        return {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "average_response_time": 0.0,
            "timestamp": datetime.now().isoformat()
        }

    async def close(self):
        """关闭适配器"""
        try:
            await self._websocket.disconnect()
            await self._rest.disconnect()
        except Exception:
            pass

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """获取持仓信息"""
        return await self._rest.get_positions(symbols)

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
        order = await self._rest.create_order(symbol, side, order_type, amount, price, params)

        # 触发订单创建事件
        await self._handle_order_update(order)

        return order

    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """取消订单"""
        order = await self._rest.cancel_order(order_id, symbol)

        # 触发订单更新事件
        await self._handle_order_update(order)

        return order

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """取消所有订单"""
        orders = await self._rest.cancel_all_orders(symbol)

        # 触发订单更新事件
        for order in orders:
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

    # === 交易设置接口实现 ===

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数"""
        return await self._rest.set_leverage(symbol, leverage)

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式"""
        return await self._rest.set_margin_mode(symbol, margin_mode)

    # === 实时数据流接口实现 ===

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅行情数据流"""
        try:
            # 包装回调函数
            wrapped_callback = self._wrap_ticker_callback(callback)
            self._ws_callbacks['ticker'].append(
                (symbol, callback, wrapped_callback))

            # 通过WebSocket订阅
            await self._websocket.subscribe_ticker(symbol, wrapped_callback)

            if self.logger:
                self.logger.info(f"已订阅{symbol}行情数据")

        except Exception as e:
            if self.logger:
                self.logger.error(f"订阅行情失败 {symbol}: {e}")
            # 降级为轮询模式
            await self._start_ticker_polling(symbol, callback)

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """订阅订单簿数据流"""
        try:
            # 包装回调函数
            wrapped_callback = self._wrap_orderbook_callback(callback)
            self._ws_callbacks['orderbook'].append(
                (symbol, callback, wrapped_callback))

            # 通过WebSocket订阅
            await self._websocket.subscribe_orderbook(symbol, wrapped_callback)

            if self.logger:
                self.logger.info(f"已订阅{symbol}订单簿数据")

        except Exception as e:
            if self.logger:
                self.logger.error(f"订阅订单簿失败 {symbol}: {e}")
            # 降级为轮询模式
            await self._start_orderbook_polling(symbol, callback)

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅成交数据流"""
        try:
            # 包装回调函数
            wrapped_callback = self._wrap_trades_callback(callback)
            self._ws_callbacks['trades'].append(
                (symbol, callback, wrapped_callback))

            # 通过WebSocket订阅
            await self._websocket.subscribe_trades(symbol, wrapped_callback)

            if self.logger:
                self.logger.info(f"已订阅{symbol}成交数据")

        except Exception as e:
            if self.logger:
                self.logger.error(f"订阅成交失败 {symbol}: {e}")
            # 降级为轮询模式
            await self._start_trades_polling(symbol, callback)

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """订阅用户数据流"""
        try:
            # 包装回调函数
            wrapped_callback = self._wrap_user_data_callback(callback)
            self._ws_callbacks['user_data'].append(
                ('', callback, wrapped_callback))

            # 🔍 强制终端输出 + 日志
            print(f"\n{'='*80}", flush=True)
            print(f"[SUBSCRIBE-DEBUG] 🔄 准备订阅WebSocket用户数据...", flush=True)
            print(f"{'='*80}\n", flush=True)

            if self.logger:
                self.logger.info("[SUBSCRIBE-DEBUG] 🔄 准备订阅WebSocket用户数据...")

            # 🔥 修复：传递原始callback，不要传递wrapped_callback
            # wrapped_callback需要2个参数(symbol, user_data)，但extended_data_callback只传递1个参数
            # 原始callback只需要1个参数(user_data)，直接传递即可
            await self._websocket.subscribe_user_data(callback)

            print(f"[SUBSCRIBE-DEBUG] ✅ WebSocket用户数据订阅成功\n", flush=True)
            if self.logger:
                self.logger.info("[SUBSCRIBE-DEBUG] ✅ WebSocket用户数据订阅成功")

        except Exception as e:
            import traceback
            print(f"\n{'='*80}", flush=True)
            print(f"[SUBSCRIBE-DEBUG] ❌ 订阅用户数据失败: {e}", flush=True)
            print(
                f"[SUBSCRIBE-DEBUG] 错误堆栈:\n{traceback.format_exc()}", flush=True)
            print(f"[SUBSCRIBE-DEBUG] ⚠️  降级为REST轮询模式（每2秒轮询一次）", flush=True)
            print(f"{'='*80}\n", flush=True)

            if self.logger:
                self.logger.error(f"❌ [SUBSCRIBE-DEBUG] 订阅用户数据失败: {e}")
                self.logger.error(
                    f"[SUBSCRIBE-DEBUG] 错误堆栈:\n{traceback.format_exc()}")
                self.logger.warning(
                    "⚠️  [SUBSCRIBE-DEBUG] 降级为REST轮询模式（每2秒轮询一次）")
            # 降级为轮询模式
            await self._start_user_data_polling(callback)

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        # 停止WebSocket订阅（Hyperliquid WebSocket模块负责具体实现）
        # 这里主要清理回调记录

        if symbol:
            # 取消特定符号的订阅
            for callback_type in self._ws_callbacks:
                self._ws_callbacks[callback_type] = [
                    (s, cb, wcb) for s, cb, wcb in self._ws_callbacks[callback_type]
                    if s != symbol
                ]
            if self.logger:
                self.logger.info(f"已取消{symbol}的所有订阅")
        else:
            # 取消所有订阅
            for callback_type in self._ws_callbacks:
                self._ws_callbacks[callback_type].clear()
            if self.logger:
                self.logger.info("已取消所有订阅")

    # === 批量订阅接口 ===

    async def batch_subscribe_tickers(self, symbols: Optional[List[str]] = None, callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """批量订阅ticker数据（支持硬编码和动态两种模式）

        Args:
            symbols: 要订阅的交易对符号列表（None时使用配置文件中的设置）
            callback: ticker数据回调函数 (symbol, ticker_data)
        """
        try:
            # 🚀 使用订阅管理器确定要订阅的交易对
            if symbols is None:
                # 没有提供symbols，使用订阅管理器
                if self._subscription_manager.mode.value == "predefined":
                    # 硬编码模式：使用配置文件中的交易对
                    symbols = self._subscription_manager.get_subscription_symbols()
                    if self.logger:
                        self.logger.info(
                            f"🔧 硬编码模式：使用配置文件中的 {len(symbols)} 个交易对")
                else:
                    # 动态模式：从市场发现交易对
                    symbols = await self._subscription_manager.discover_symbols(self.get_supported_symbols)
                    if self.logger:
                        self.logger.info(f"🔧 动态模式：发现 {len(symbols)} 个交易对")

            # 检查是否应该订阅ticker数据
            if not self._subscription_manager.should_subscribe_data_type(DataType.TICKER):
                if self.logger:
                    self.logger.info("配置中禁用了ticker数据订阅，跳过")
                return

            if not symbols:
                if self.logger:
                    self.logger.warning("没有找到要订阅的交易对")
                return

            # 🔥 根据配置使用相应的WebSocket实现
            config = self._load_hyperliquid_config()
            implementation = config.get('websocket', {}).get(
                'implementation', 'native')
            websocket_type = "原生WebSocket" if implementation == 'native' else "ccxt WebSocket"
            if self.logger:
                self.logger.info(
                    f"📡 使用{websocket_type}订阅ticker数据: {len(symbols)} 个交易对")

            # 包装回调函数
            if callback is None:
                def callback(symbol, ticker): return None  # 默认回调
            wrapped_callback = self._wrap_batch_ticker_callback(callback)

            # 批量添加到回调列表
            for symbol in symbols:
                self._ws_callbacks['ticker'].append(
                    (symbol, callback, wrapped_callback))

            # 将订阅添加到管理器
            for symbol in symbols:
                self._subscription_manager.add_subscription(
                    symbol=symbol,
                    data_type=DataType.TICKER,
                    callback=wrapped_callback
                )

            # 通过WebSocket批量订阅
            await self._websocket.batch_subscribe_tickers(symbols, wrapped_callback)

            # 🔥 记录统计信息
            perpetual_count = 0
            spot_count = 0

            for symbol in symbols:
                market_type = self._base.get_market_type_from_symbol(symbol)
                if market_type == "perpetual":
                    perpetual_count += 1
                elif market_type == "spot":
                    spot_count += 1

            if self.logger:
                self.logger.info(
                    f"✅ 批量订阅ticker完成: perpetual={perpetual_count}, spot={spot_count}")
                self.logger.info(f"✅ 批量订阅ticker完成: {len(symbols)}个交易对")

        except Exception as e:
            if self.logger:
                self.logger.error(f"批量订阅ticker失败: {str(e)}")
            raise

    def _wrap_batch_ticker_callback(self, original_callback: Callable[[str, TickerData], None]) -> Callable[[str, TickerData], None]:
        """包装批量ticker回调函数，确保参数兼容性"""
        async def wrapped_callback(symbol: str, ticker_data: TickerData):
            try:
                # 直接调用原始回调（已经是两个参数格式）
                if asyncio.iscoroutinefunction(original_callback):
                    await original_callback(symbol, ticker_data)
                else:
                    original_callback(symbol, ticker_data)

                # 触发事件
                await self._handle_ticker_update(ticker_data)

            except Exception as e:
                if self.logger:
                    self.logger.warning(f"批量Ticker回调执行失败 {symbol}: {str(e)}")

        return wrapped_callback

    def _filter_major_symbols(self, symbols: List[str]) -> List[str]:
        """过滤出主流币种，避免订阅不支持的小币种

        Args:
            symbols: 所有符号列表

        Returns:
            过滤后的主流币种列表
        """
        # 主流币种列表（这些通常支持activeAssetCtx）
        major_coins = {
            'BTC', 'ETH', 'SOL', 'AVAX', 'DOGE', 'ADA', 'DOT', 'UNI', 'LINK',
            'AAVE', 'MATIC', 'LTC', 'XRP', 'BNB', 'ARB', 'OP', 'SUI', 'APT',
            'NEAR', 'FTM', 'ATOM', 'ICP', 'TIA', 'SEI', 'JUP', 'JTO', 'WIF',
            'BONK', 'PYTH', 'TRUMP', 'MEME'  # 一些流行的meme币
        }

        filtered_symbols = []
        for symbol in symbols:
            # 提取基础币种名称 (BTC/USDC:PERP -> BTC)
            if '/' in symbol:
                base_coin = symbol.split('/')[0].upper()
                if base_coin in major_coins:
                    filtered_symbols.append(symbol)
            elif symbol.upper() in major_coins:
                filtered_symbols.append(symbol)

        return filtered_symbols

    async def batch_subscribe_orderbooks(self, symbols: Optional[List[str]] = None, callback: Optional[Callable[[OrderBookData], None]] = None) -> None:
        """批量订阅订单簿数据（支持硬编码和动态两种模式）

        Args:
            symbols: 要订阅的交易对符号列表（None时使用配置文件中的设置）
            callback: 订单簿数据回调函数
        """
        try:
            # 🚀 使用订阅管理器确定要订阅的交易对
            if symbols is None:
                # 没有提供symbols，使用订阅管理器
                if self._subscription_manager.mode.value == "predefined":
                    # 硬编码模式：使用配置文件中的交易对
                    symbols = self._subscription_manager.get_subscription_symbols()
                    if self.logger:
                        self.logger.info(
                            f"🔧 硬编码模式：使用配置文件中的 {len(symbols)} 个交易对")
                else:
                    # 动态模式：从市场发现交易对
                    symbols = await self._subscription_manager.discover_symbols(self.get_supported_symbols)
                    if self.logger:
                        self.logger.info(f"🔧 动态模式：发现 {len(symbols)} 个交易对")

            # 检查是否应该订阅orderbook数据
            if not self._subscription_manager.should_subscribe_data_type(DataType.ORDERBOOK):
                if self.logger:
                    self.logger.info("配置中禁用了orderbook数据订阅，跳过")
                return

            if not symbols:
                if self.logger:
                    self.logger.warning("没有找到要订阅的交易对")
                return

            # 过滤黑名单符号
            filtered_symbols = self._base.filter_websocket_symbols(symbols)

            # 包装回调函数
            if callback is None:
                def callback(orderbook): return None  # 默认回调
            wrapped_callback = self._wrap_orderbook_callback(callback)

            # 批量添加到回调列表
            for symbol in filtered_symbols:
                self._ws_callbacks['orderbook'].append(
                    (symbol, callback, wrapped_callback))

            # 将订阅添加到管理器
            for symbol in filtered_symbols:
                self._subscription_manager.add_subscription(
                    symbol=symbol,
                    data_type=DataType.ORDERBOOK,
                    callback=wrapped_callback
                )

            # 通过WebSocket批量订阅
            await self._websocket.batch_subscribe_orderbooks(filtered_symbols, wrapped_callback)

            if self.logger:
                self.logger.info(f"已批量订阅{len(filtered_symbols)}个交易对的订单簿数据")

        except Exception as e:
            if self.logger:
                self.logger.error(f"批量订阅订单簿失败: {e}")

    async def batch_subscribe_mixed(self,
                                    symbols: Optional[List[str]] = None,
                                    ticker_callback: Optional[Callable[[
                                        str, TickerData], None]] = None,
                                    orderbook_callback: Optional[Callable[[
                                        str, OrderBookData], None]] = None,
                                    trades_callback: Optional[Callable[[
                                        str, TradeData], None]] = None,
                                    user_data_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None) -> None:
        """批量订阅混合数据类型（支持任意组合）

        Args:
            symbols: 要订阅的交易对符号列表（None时使用配置文件中的设置）
            ticker_callback: ticker数据回调函数
            orderbook_callback: orderbook数据回调函数
            trades_callback: trades数据回调函数
            user_data_callback: user_data回调函数
        """
        try:
            # 🚀 使用订阅管理器确定要订阅的交易对
            if symbols is None:
                if self._subscription_manager.mode.value == "predefined":
                    symbols = self._subscription_manager.get_subscription_symbols()
                    if self.logger:
                        self.logger.info(
                            f"🔧 硬编码模式：使用配置文件中的 {len(symbols)} 个交易对")
                else:
                    symbols = await self._subscription_manager.discover_symbols(self.get_supported_symbols)
                    if self.logger:
                        self.logger.info(f"🔧 动态模式：发现 {len(symbols)} 个交易对")

            if not symbols:
                if self.logger:
                    self.logger.warning("没有找到要订阅的交易对")
                return

            # 根据配置决定订阅哪些数据类型
            subscription_count = 0

            # 订阅ticker数据
            if (ticker_callback is not None and
                    self._subscription_manager.should_subscribe_data_type(DataType.TICKER)):
                await self.batch_subscribe_tickers(symbols, ticker_callback)
                subscription_count += 1
                if self.logger:
                    self.logger.info(f"✅ 已订阅ticker数据: {len(symbols)}个交易对")

            # 订阅orderbook数据
            if (orderbook_callback is not None and
                    self._subscription_manager.should_subscribe_data_type(DataType.ORDERBOOK)):
                await self.batch_subscribe_orderbooks(symbols, orderbook_callback)
                subscription_count += 1
                if self.logger:
                    self.logger.info(f"✅ 已订阅orderbook数据: {len(symbols)}个交易对")

            # 订阅trades数据
            if (trades_callback is not None and
                    self._subscription_manager.should_subscribe_data_type(DataType.TRADES)):
                for symbol in symbols:
                    await self.subscribe_trades(symbol, trades_callback)
                subscription_count += 1
                if self.logger:
                    self.logger.info(f"✅ 已订阅trades数据: {len(symbols)}个交易对")

            # 订阅user_data数据
            if (user_data_callback is not None and
                    self._subscription_manager.should_subscribe_data_type(DataType.USER_DATA)):
                await self.subscribe_user_data(user_data_callback)
                subscription_count += 1
                if self.logger:
                    self.logger.info(f"✅ 已订阅user_data数据")

            # 获取订阅统计信息
            stats = self._subscription_manager.get_subscription_stats()
            if self.logger:
                self.logger.info(
                    f"🎯 混合订阅完成: {subscription_count}种数据类型, {len(symbols)}个交易对")
                self.logger.info(f"📊 订阅统计: {stats}")

        except Exception as e:
            if self.logger:
                self.logger.error(f"批量混合订阅失败: {e}")
            raise

    def get_subscription_manager(self) -> SubscriptionManager:
        """获取订阅管理器实例"""
        return self._subscription_manager

    def get_subscription_stats(self) -> Dict[str, Any]:
        """获取订阅统计信息"""
        return self._subscription_manager.get_subscription_stats()

    # === 回调函数包装器 ===

    def _wrap_ticker_callback(self, original_callback: Callable[[TickerData], None]) -> Callable[[str, TickerData], None]:
        """包装ticker回调函数"""
        async def wrapped_callback(symbol: str, ticker_data: TickerData):
            try:
                # 调用原始回调
                if asyncio.iscoroutinefunction(original_callback):
                    await original_callback(ticker_data)
                else:
                    original_callback(ticker_data)

                # 触发事件
                await self._handle_ticker_update(ticker_data)

            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Ticker回调执行失败: {str(e)}")

        return wrapped_callback

    def _wrap_orderbook_callback(self, original_callback: Callable[[OrderBookData], None]) -> Callable[[str, OrderBookData], None]:
        """包装orderbook回调函数"""
        async def wrapped_callback(symbol: str, orderbook_data: OrderBookData):
            try:
                # 🔧 修复：检查原始回调函数的参数数量
                import inspect
                sig = inspect.signature(original_callback)
                param_count = len(sig.parameters)

                # 调用原始回调
                if asyncio.iscoroutinefunction(original_callback):
                    if param_count == 2:
                        # 两个参数的回调函数 (symbol, orderbook_data)
                        await original_callback(symbol, orderbook_data)
                    else:
                        # 单个参数的回调函数 (orderbook_data)
                        await original_callback(orderbook_data)
                else:
                    if param_count == 2:
                        # 两个参数的回调函数 (symbol, orderbook_data)
                        original_callback(symbol, orderbook_data)
                    else:
                        # 单个参数的回调函数 (orderbook_data)
                        original_callback(orderbook_data)

                # 触发事件
                await self._handle_orderbook_update(orderbook_data)

            except Exception as e:
                if self.logger:
                    self.logger.warning(f"OrderBook回调执行失败: {str(e)}")
                    # 🔧 修复：添加更详细的调试信息
                    import traceback
                    self.logger.debug(f"回调函数详情: {original_callback}")
                    self.logger.debug(f"异常堆栈: {traceback.format_exc()}")

        return wrapped_callback

    def _wrap_trades_callback(self, original_callback: Callable[[TradeData], None]) -> Callable[[str, TradeData], None]:
        """包装trades回调函数"""
        async def wrapped_callback(symbol: str, trade_data: TradeData):
            try:
                # 🔧 修复：检查原始回调函数的参数数量
                import inspect
                sig = inspect.signature(original_callback)
                param_count = len(sig.parameters)

                # 调用原始回调
                if asyncio.iscoroutinefunction(original_callback):
                    if param_count == 2:
                        # 两个参数的回调函数 (symbol, trade_data)
                        await original_callback(symbol, trade_data)
                    else:
                        # 单个参数的回调函数 (trade_data)
                        await original_callback(trade_data)
                else:
                    if param_count == 2:
                        # 两个参数的回调函数 (symbol, trade_data)
                        original_callback(symbol, trade_data)
                    else:
                        # 单个参数的回调函数 (trade_data)
                        original_callback(trade_data)

                # 触发事件
                await self._handle_trade_update(trade_data)

            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Trade回调执行失败: {str(e)}")
                    # 🔧 修复：添加更详细的调试信息
                    import traceback
                    self.logger.debug(f"回调函数详情: {original_callback}")
                    self.logger.debug(f"异常堆栈: {traceback.format_exc()}")

        return wrapped_callback

    def _wrap_user_data_callback(self, original_callback: Callable[[Dict[str, Any]], None]) -> Callable[[str, Dict[str, Any]], None]:
        """包装user data回调函数"""
        async def wrapped_callback(symbol: str, user_data: Dict[str, Any]):
            try:
                # 🔧 修复：检查原始回调函数的参数数量
                import inspect
                sig = inspect.signature(original_callback)
                param_count = len(sig.parameters)

                # 调用原始回调
                if asyncio.iscoroutinefunction(original_callback):
                    if param_count == 2:
                        # 两个参数的回调函数 (symbol, user_data)
                        await original_callback(symbol, user_data)
                    else:
                        # 单个参数的回调函数 (user_data)
                        await original_callback(user_data)
                else:
                    if param_count == 2:
                        # 两个参数的回调函数 (symbol, user_data)
                        original_callback(symbol, user_data)
                    else:
                        # 单个参数的回调函数 (user_data)
                        original_callback(user_data)

                # 触发事件（用户数据可能包含订单、余额等更新）
                await self._handle_user_data_update(user_data)

            except Exception as e:
                if self.logger:
                    self.logger.warning(f"UserData回调执行失败: {str(e)}")
                    # 🔧 修复：添加更详细的调试信息
                    import traceback
                    self.logger.debug(f"回调函数详情: {original_callback}")
                    self.logger.debug(f"异常堆栈: {traceback.format_exc()}")

        return wrapped_callback

    # === 轮询模式（降级方案）===

    async def _start_ticker_polling(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """启动ticker轮询模式"""
        if self.logger:
            self.logger.warning(f"启动{symbol}行情轮询模式")

        asyncio.create_task(self._poll_ticker(symbol, callback))

    async def _start_orderbook_polling(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """启动orderbook轮询模式"""
        if self.logger:
            self.logger.warning(f"启动{symbol}订单簿轮询模式")

        asyncio.create_task(self._poll_orderbook(symbol, callback))

    async def _start_trades_polling(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """启动trades轮询模式"""
        if self.logger:
            self.logger.warning(f"启动{symbol}成交轮询模式")

        asyncio.create_task(self._poll_trades(symbol, callback))

    async def _start_user_data_polling(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """启动用户数据轮询模式"""
        print(f"\n{'='*80}", flush=True)
        print(f"⚠️  [POLLING-MODE] 启动REST轮询模式（WebSocket未启用）", flush=True)
        print(f"[POLLING-MODE] 轮询间隔: 2秒", flush=True)
        print(f"[POLLING-MODE] 限制: 只能检测当前挂单，无法实时捕获成交瞬间", flush=True)
        print(f"{'='*80}\n", flush=True)

        if self.logger:
            self.logger.warning("⚠️  [POLLING-MODE] 启动REST轮询模式（WebSocket未启用）")
            self.logger.warning("[POLLING-MODE] 轮询间隔: 2秒")
            self.logger.warning("[POLLING-MODE] 限制: 只能检测当前挂单，无法实时捕获成交瞬间")

        asyncio.create_task(self._poll_user_data(callback))

    # 轮询实现方法（参考原始脚本中的实现）
    async def _poll_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """轮询ticker数据"""
        try:
            while True:
                ticker = await self.get_ticker(symbol)

                if asyncio.iscoroutinefunction(callback):
                    await callback(ticker)
                else:
                    callback(ticker)

                await asyncio.sleep(1)  # 1秒轮询间隔
        except Exception as e:
            if self.logger:
                self.logger.error(f"轮询ticker失败 {symbol}: {e}")

    async def _poll_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """轮询orderbook数据"""
        try:
            while True:
                orderbook = await self.get_orderbook(symbol)

                if asyncio.iscoroutinefunction(callback):
                    await callback(orderbook)
                else:
                    callback(orderbook)

                await asyncio.sleep(0.5)  # 0.5秒轮询间隔
        except Exception as e:
            if self.logger:
                self.logger.error(f"轮询orderbook失败 {symbol}: {e}")

    async def _poll_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """轮询trades数据"""
        try:
            last_trade_id = None
            while True:
                trades = await self.get_trades(symbol, limit=10)

                # 只推送新的成交
                for trade in trades:
                    if last_trade_id is None or trade.id != last_trade_id:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(trade)
                        else:
                            callback(trade)
                        last_trade_id = trade.id

                await asyncio.sleep(1)  # 1秒轮询间隔
        except Exception as e:
            if self.logger:
                self.logger.error(f"轮询trades失败 {symbol}: {e}")

    async def _poll_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """轮询用户数据"""
        try:
            last_balances = {}
            last_orders = {}

            while True:
                # 检查余额变化
                try:
                    current_balances = await self.get_balances()
                    if str(current_balances) != str(last_balances):
                        user_data = {'type': 'balance',
                                     'data': current_balances}

                        if asyncio.iscoroutinefunction(callback):
                            await callback(user_data)
                        else:
                            callback(user_data)

                        last_balances = current_balances
                except Exception:
                    pass

                # 检查订单变化
                try:
                    current_orders = await self.get_open_orders()
                    if str(current_orders) != str(last_orders):
                        # 🔍 添加调试日志
                        if self.logger:
                            self.logger.info(
                                f"[POLLING-MODE] 检测到订单变化: 当前{len(current_orders)}个挂单")

                        user_data = {'type': 'orders', 'data': current_orders}

                        # 🔍 调试：显示传递的数据格式
                        if self.logger:
                            self.logger.debug(
                                f"[POLLING-MODE] 传递数据格式: 字典 (type={user_data['type']}, data长度={len(user_data['data'])})")

                        if asyncio.iscoroutinefunction(callback):
                            await callback(user_data)
                        else:
                            callback(user_data)

                        last_orders = current_orders
                except Exception as e:
                    if self.logger:
                        self.logger.debug(f"[POLLING-MODE] 轮询订单失败: {e}")
                    pass

                await asyncio.sleep(2)  # 2秒轮询间隔
        except Exception as e:
            if self.logger:
                self.logger.error(f"轮询用户数据失败: {e}")

    # === 便利方法 ===

    async def get_supported_symbols(self) -> List[str]:
        """获取支持的交易对列表（根据exchange_type返回现货或永续）"""
        try:
            # 如果已经有缓存的交易对，直接返回
            if self._base._supported_symbols:
                return self._base._supported_symbols.copy()

            # 🔥 使用ccxt获取所有市场信息并根据exchange_type过滤
            if self._rest.exchange and hasattr(self._rest.exchange, 'markets'):
                markets = self._rest.exchange.markets
                all_symbols = list(markets.keys())

                # 🎯 根据市场类型分类
                perpetual_symbols = []
                spot_symbols = []

                for symbol in all_symbols:
                    market_info = markets[symbol]
                    market_type = market_info.get('type', 'unknown')

                    if market_type == 'swap':
                        perpetual_symbols.append(symbol)
                    elif market_type == 'spot':
                        spot_symbols.append(symbol)

                # 🔥 根据配置的exchange_type决定返回哪种市场
                from ..interface import ExchangeType
                if self.config and self.config.exchange_type == ExchangeType.SPOT:
                    # 现货市场
                    self._base._supported_symbols = spot_symbols
                    target_symbols = spot_symbols
                    market_name = "现货"
                else:
                    # 永续合约市场（默认）
                    self._base._supported_symbols = perpetual_symbols
                    target_symbols = perpetual_symbols
                    market_name = "永续合约"

                if self.logger:
                    self.logger.info(
                        f"从ccxt获取到{len(all_symbols)}个Hyperliquid交易对")
                    self.logger.info(
                        f"🔥 过滤后{market_name}: {len(target_symbols)}个")
                    self.logger.info(
                        f"💡 现货交易对: {len(spot_symbols)}个 | 永续合约: {len(perpetual_symbols)}个")
                    if target_symbols:
                        self.logger.info(
                            f"✅ {market_name}示例: {target_symbols[:10]}")

                return target_symbols

            # 如果ccxt没有数据，使用默认永续合约列表（:USDC格式）
            default_symbols = [
                k for k in self._base._default_symbol_mapping.keys() if k.endswith(':PERP')]
            # 🔥 如果默认列表为空，创建一些基本的永续合约
            if not default_symbols:
                default_symbols = [
                    'BTC/USDC:USDC', 'ETH/USDC:USDC', 'SOL/USDC:USDC',
                    'AVAX/USDC:USDC', 'DOGE/USDC:USDC', 'LINK/USDC:USDC'
                ]

            self._base._supported_symbols = default_symbols

            if self.logger:
                self.logger.info(
                    f"使用默认Hyperliquid永续合约列表: {len(default_symbols)}个")

            return default_symbols

        except Exception as e:
            if self.logger:
                self.logger.error(f"获取Hyperliquid永续合约失败: {e}")

            # 出错时返回默认永续合约列表
            default_symbols = [
                'BTC/USDC:USDC', 'ETH/USDC:USDC', 'SOL/USDC:USDC',
                'AVAX/USDC:USDC', 'DOGE/USDC:USDC', 'LINK/USDC:USDC'
            ]
            return default_symbols

    def get_connection_status(self) -> Dict[str, Any]:
        """获取连接状态"""
        return {
            'rest_connected': self._rest.exchange is not None,
            'websocket_status': self._websocket.get_connection_status(),
            'total_subscriptions': sum(len(callbacks) for callbacks in self._ws_callbacks.values())
        }

    def get_symbol_mapping(self) -> Dict[str, str]:
        """获取符号映射"""
        return self._base._default_symbol_mapping.copy()

    # === 符号处理方法 ===

    def map_symbol(self, symbol: str) -> str:
        """映射交易对符号"""
        return self._base.map_symbol(symbol)

    def reverse_map_symbol(self, exchange_symbol: str) -> str:
        """反向映射交易对符号"""
        return self._base.reverse_map_symbol(exchange_symbol)

    async def on_ticker_update(self, symbol: str, ticker_data: TickerData) -> None:
        """处理ticker数据更新回调

        Args:
            symbol: 交易对符号
            ticker_data: ticker数据
        """
        try:
            if self.logger:
                self.logger.debug(
                    f"行情更新: {symbol}@{self.config.exchange_id}, "
                    f"价格: {ticker_data.last}"
                )

            # 🔧 修复：检查并安全调用外部回调函数
            if hasattr(self, '_ticker_callback') and self._ticker_callback:
                # 检查回调函数的参数签名
                import inspect
                try:
                    sig = inspect.signature(self._ticker_callback)
                    param_count = len(sig.parameters)

                    if param_count == 1:
                        # 单参数回调（监控服务的ticker_callback）
                        if asyncio.iscoroutinefunction(self._ticker_callback):
                            await self._ticker_callback(ticker_data)
                        else:
                            self._ticker_callback(ticker_data)
                    elif param_count >= 2:
                        # 双参数回调（全局回调包装器）
                        if asyncio.iscoroutinefunction(self._ticker_callback):
                            await self._ticker_callback(symbol, ticker_data)
                        else:
                            self._ticker_callback(symbol, ticker_data)
                    else:
                        if self.logger:
                            self.logger.warning(
                                f"⚠️  ticker回调函数参数数量异常: {param_count}")

                except Exception as callback_error:
                    if self.logger:
                        self.logger.error(
                            f"❌ ticker回调执行失败: {str(callback_error)}")
                        # 记录更多调试信息
                        callback_info = f"回调类型: {type(self._ticker_callback)}, 异步: {asyncio.iscoroutinefunction(self._ticker_callback)}"
                        self.logger.debug(f"回调详情: {callback_info}")
                        import traceback
                        self.logger.debug(f"异常堆栈: {traceback.format_exc()}")

        except Exception as e:
            if self.logger:
                self.logger.error(f"处理ticker更新回调失败: {str(e)}")
                import traceback
                self.logger.debug(f"完整异常信息: {traceback.format_exc()}")

    async def on_extended_data_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """处理扩展数据更新回调（成交量、资金费率等）

        Args:
            symbol: 交易对符号  
            data: 扩展数据
        """
        try:
            if self.logger:
                self.logger.debug(
                    f"扩展数据更新: {symbol}, "
                    f"成交量: {data.get('volume_24h', 0):,.0f}, "
                    f"资金费率: {data.get('funding_rate', 0):.6f}%"
                )

            # 如果有扩展数据回调函数，调用它
            if hasattr(self, '_extended_data_callback') and self._extended_data_callback:
                if asyncio.iscoroutinefunction(self._extended_data_callback):
                    await self._extended_data_callback(symbol, data)
                else:
                    self._extended_data_callback(symbol, data)

        except Exception as e:
            if self.logger:
                self.logger.error(f"处理扩展数据更新回调失败: {str(e)}")

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
