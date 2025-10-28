"""
Hyperliquid WebSocket模块 - 基于ccxt WebSocket的实现

使用ccxt统一接口处理Hyperliquid WebSocket连接和数据订阅
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable, Tuple
from decimal import Decimal
import ccxt.pro as ccxt

from ..interface import ExchangeConfig
from ..models import (
    TickerData, OrderBookData, TradeData, OrderBookLevel,
    OrderSide
)
from .hyperliquid_base import HyperliquidBase

# 导入统计配置读取器
from core.infrastructure.stats_config import get_exchange_stats_frequency, get_exchange_stats_summary


class HyperliquidWebSocket:
    """Hyperliquid WebSocket客户端 - 基于ccxt实现"""

    def __init__(self, config: ExchangeConfig, base_instance):
        """初始化WebSocket适配器

        Args:
            config: 交易所配置
            base_instance: HyperliquidBase实例，用于符号转换等操作
        """
        self.config = config
        self._base = base_instance
        self.logger = None

        # ccxt WebSocket 相关
        self._ccxt_exchange = None
        self._ccxt_connected = False
        self._ccxt_tasks = set()  # 修改为set类型，匹配后续使用

        # 订阅管理
        # (sub_type, symbol, callback)
        self._subscriptions: List[Tuple[str, str, Callable]] = []
        self._active_subscriptions = set()  # 跟踪已激活的订阅

        # 控制标志
        self._should_stop = False
        self._reconnecting = False

        # 缓存
        self._latest_orderbooks: Dict[str, Dict[str, Any]] = {}
        self._asset_ctx_cache = {}
        self._extended_data_callbacks = []

        # 统计配置
        self._stats_config = None
        self._symbol_count = None
        self._init_stats_config()

        # 连接状态
        self._reconnect_attempts = 0

        # 初始化连接状态监控
        self._init_connection_monitoring()

    def _init_stats_config(self) -> None:
        """初始化统计配置"""
        try:
            self._stats_config = get_exchange_stats_frequency(
                'hyperliquid', self._symbol_count)
            if self.logger:
                summary = get_exchange_stats_summary(
                    'hyperliquid', self._symbol_count)
                self.logger.info(f"🔥 Hyperliquid统计配置已加载: {summary}")
        except Exception as e:
            self._stats_config = {
                'message_stats_frequency': 1000,
                'callback_stats_frequency': 500,
                'orderbook_stats_frequency': 500,
                'global_callback_frequency': 500
            }
            if self.logger:
                self.logger.warning(f"统计配置加载失败，使用默认配置: {e}")

    def update_symbol_count(self, symbol_count: int) -> None:
        """更新币种数量，重新计算统计配置"""
        self._symbol_count = symbol_count
        old_config = self._stats_config.copy() if self._stats_config else {}
        self._init_stats_config()

        if old_config != self._stats_config and self.logger:
            self.logger.info(f"🔄 Hyperliquid统计配置已更新 (币种数量: {symbol_count})")

    def _get_stats_frequency(self, stat_type: str) -> int:
        """获取指定类型的统计频率"""
        if not self._stats_config:
            default_freq = {
                'message_stats_frequency': 1000,
                'callback_stats_frequency': 500,
                'orderbook_stats_frequency': 500,
                'global_callback_frequency': 500
            }
            return default_freq.get(stat_type, 100)
        return self._stats_config.get(stat_type, 100)

    # === 连接管理 ===

    async def connect(self) -> bool:
        """连接ccxt WebSocket"""
        try:
            if self._ccxt_connected:
                if self.logger:
                    self.logger.info("ccxt WebSocket已连接")
                return True

            if self.logger:
                self.logger.info("开始连接Hyperliquid ccxt WebSocket")

            success = await self._connect_ccxt_websocket()

            if success:
                if self.logger:
                    self.logger.info("✅ Hyperliquid ccxt WebSocket连接成功")
                return True
            else:
                if self.logger:
                    self.logger.error("❌ Hyperliquid ccxt WebSocket连接失败")
                return False

        except Exception as e:
            if self.logger:
                self.logger.error(f"连接Hyperliquid ccxt WebSocket失败: {str(e)}")
            return False

    async def disconnect(self) -> None:
        """断开ccxt WebSocket连接"""
        if self.logger:
            self.logger.info("正在断开Hyperliquid ccxt WebSocket连接...")

        self._should_stop = True

        # 清理ccxt WebSocket任务
        await self._cleanup_ccxt_tasks()

        # 清理数据
        self._subscriptions.clear()
        self._latest_orderbooks.clear()
        self._active_subscriptions.clear()

        if self.logger:
            self.logger.info("Hyperliquid ccxt WebSocket已断开")

    # === 订阅功能 ===

    async def subscribe_ticker(self, symbol: str, callback: Callable[[str, TickerData], None]) -> None:
        """订阅ticker数据"""
        self._subscriptions.append(('ticker', symbol, callback))

        if self._ccxt_connected:
            await self._ccxt_watch_ticker(symbol, callback)

        if self.logger:
            self.logger.info(f"订阅ticker: {symbol}")

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[str, OrderBookData], None]) -> None:
        """订阅orderbook数据"""
        self._subscriptions.append(('orderbook', symbol, callback))

        if self._ccxt_connected:
            await self._ccxt_watch_orderbook(symbol, callback)

        if self.logger:
            self.logger.info(f"订阅orderbook: {symbol}")

    async def subscribe_trades(self, symbol: str, callback: Callable[[str, TradeData], None]) -> None:
        """订阅trades数据"""
        self._subscriptions.append(('trades', symbol, callback))

        # ccxt trades订阅可以在这里实现
        if self.logger:
            self.logger.info(f"订阅trades: {symbol}")

    async def batch_subscribe_tickers(self, symbols: List[str], callback: Callable[[str, TickerData], None]) -> None:
        """批量订阅ticker"""
        filtered_symbols = self._base.filter_symbols_by_market_type(symbols)

        if not filtered_symbols:
            if self.logger:
                enabled_markets = self._base.get_enabled_markets()
                self.logger.warning(
                    f"没有符合启用市场类型的符号可订阅。启用的市场: {enabled_markets}")
            return

        # 🔥 修复：设置全局ticker回调，与Backpack和EdgeX保持一致
        if callback:
            self.ticker_callback = callback

        for symbol in filtered_symbols:
            await self.subscribe_ticker(symbol, callback)

        if self.logger:
            self.logger.info(f"✅ 批量订阅ticker完成: {len(filtered_symbols)}个符号")

    async def batch_subscribe_orderbooks(self, symbols: List[str], callback: Callable[[str, OrderBookData], None]) -> None:
        """批量订阅orderbook"""
        filtered_symbols = self._base.filter_symbols_by_market_type(symbols)

        if not filtered_symbols:
            if self.logger:
                enabled_markets = self._base.get_enabled_markets()
                self.logger.warning(
                    f"没有符合启用市场类型的符号可订阅。启用的市场: {enabled_markets}")
            return

        for symbol in filtered_symbols:
            await self.subscribe_orderbook(symbol, callback)

        if self.logger:
            self.logger.info(f"✅ 批量订阅orderbook完成: {len(filtered_symbols)}个符号")

    async def subscribe_funding_rates(self, symbols: List[str]) -> bool:
        """订阅资金费率数据"""
        try:
            if not self._ccxt_exchange:
                await self._init_ccxt_exchange()

            success_count = 0

            for symbol in symbols:
                try:
                    # 启动资金费率监听任务
                    task_name = f"ccxt_funding_rate_{symbol}"
                    if task_name not in self._ccxt_tasks:
                        task = asyncio.create_task(
                            self._ccxt_watch_funding_rate(symbol))
                        self._ccxt_tasks.add(task)
                        success_count += 1

                    if self.logger:
                        self.logger.info(f"开始监听资金费率: {symbol}")

                except Exception as e:
                    if self.logger:
                        self.logger.error(f"订阅资金费率失败 {symbol}: {e}")

            return success_count > 0

        except Exception as e:
            if self.logger:
                self.logger.error(f"订阅资金费率失败: {e}")
            return False

    async def get_current_funding_rates(self, symbols: List[str] = None) -> Dict[str, Any]:
        """获取当前资金费率（一次性获取）"""
        return await self._ccxt_fetch_funding_rates(symbols)

    async def get_funding_rate(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取单个交易对的资金费率"""
        return await self._ccxt_fetch_funding_rate(symbol)

    # === ccxt WebSocket 实现 ===

    async def _init_ccxt_exchange(self) -> bool:
        """初始化ccxt exchange实例"""
        try:
            if self._ccxt_exchange:
                return True

            # 🔥 修复：添加钱包地址配置
            exchange_config = {
                'apiKey': self.config.api_key,
                'secret': self.config.api_secret,
                'sandbox': False,
                'enableRateLimit': True,
            }

            # Hyperliquid 需要钱包地址用于私有API调用
            if self.config.wallet_address:
                exchange_config['walletAddress'] = self.config.wallet_address
                if self.logger:
                    self.logger.info(
                        f"✅ 配置钱包地址: {self.config.wallet_address[:10]}...{self.config.wallet_address[-6:]}")

            self._ccxt_exchange = ccxt.hyperliquid(exchange_config)

            if self.logger:
                self.logger.info("✅ ccxt Hyperliquid exchange实例已初始化")

            # 🔥 关键修复：加载市场信息
            if self.logger:
                self.logger.info("🔄 正在加载Hyperliquid市场信息...")

            await self._ccxt_exchange.load_markets()

            if self.logger:
                markets_count = len(self._ccxt_exchange.markets)
                self.logger.info(
                    f"✅ Hyperliquid市场信息加载完成，共 {markets_count} 个市场")

            return True

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 初始化ccxt exchange失败: {str(e)}")
            return False

    async def _connect_ccxt_websocket(self) -> bool:
        """连接ccxt WebSocket"""
        try:
            if not self._ccxt_exchange:
                if not await self._init_ccxt_exchange():
                    return False

            self._ccxt_connected = True

            if self.logger:
                self.logger.info("✅ ccxt WebSocket连接已准备就绪")

            return True

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 连接ccxt WebSocket失败: {str(e)}")
            return False

    async def _ccxt_watch_ticker(self, symbol: str, callback: Callable[[str, TickerData], None]) -> None:
        """使用ccxt WebSocket订阅ticker数据"""
        try:
            if not self._ccxt_connected:
                if not await self._connect_ccxt_websocket():
                    return

            ccxt_symbol = self._convert_to_ccxt_symbol(symbol)
            if not ccxt_symbol:
                if self.logger:
                    self.logger.warning(f"无法转换符号格式: {symbol}")
                return

            # 验证市场是否存在
            if ccxt_symbol not in self._ccxt_exchange.markets:
                if self.logger:
                    self.logger.warning(
                        f"市场不存在: {ccxt_symbol} (从 {symbol} 转换而来)")
                    self.logger.debug(
                        f"可用市场示例: {list(self._ccxt_exchange.markets.keys())[:10]}")
                return

            task_key = f"ticker_{symbol}"
            if task_key not in self._ccxt_tasks:
                task = asyncio.create_task(
                    self._ccxt_ticker_loop(ccxt_symbol)
                )
                self._ccxt_tasks.add(task)
                task.add_done_callback(self._ccxt_tasks.discard)

                if self.logger:
                    self.logger.info(
                        f"🎯 ccxt ticker订阅已启动: {symbol} -> {ccxt_symbol}")

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ ccxt ticker订阅失败 {symbol}: {str(e)}")

    async def _ccxt_watch_orderbook(self, symbol: str, callback: Callable[[str, OrderBookData], None]) -> None:
        """使用ccxt WebSocket订阅orderbook数据"""
        try:
            if not self._ccxt_connected:
                if not await self._connect_ccxt_websocket():
                    return

            ccxt_symbol = self._convert_to_ccxt_symbol(symbol)
            if not ccxt_symbol:
                if self.logger:
                    self.logger.warning(f"无法转换符号格式: {symbol}")
                return

            # 验证市场是否存在
            if ccxt_symbol not in self._ccxt_exchange.markets:
                if self.logger:
                    self.logger.warning(
                        f"市场不存在: {ccxt_symbol} (从 {symbol} 转换而来)")
                return

            task_key = f"orderbook_{symbol}"
            if task_key not in self._ccxt_tasks:
                task = asyncio.create_task(
                    self._ccxt_orderbook_loop(ccxt_symbol)
                )
                self._ccxt_tasks.add(task)
                task.add_done_callback(self._ccxt_tasks.discard)

                if self.logger:
                    self.logger.info(
                        f"🎯 ccxt orderbook订阅已启动: {symbol} -> {ccxt_symbol}")

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ ccxt orderbook订阅失败 {symbol}: {str(e)}")

    async def _ccxt_ticker_loop(self, symbol: str):
        """ccxt ticker数据循环"""
        try:
            # 注意：这里的symbol参数已经是ccxt格式的符号了，无需再次转换
            if self.logger:
                self.logger.debug(f"[CCXT] 开始ticker循环: {symbol}")

            while not self._should_stop and self._ccxt_connected:
                try:
                    ticker = await self._ccxt_exchange.watch_ticker(symbol)

                    # 🔥 修复：调用存储在_subscriptions中的回调函数
                    # 遍历所有订阅，找到匹配的ticker订阅
                    for sub_type, sub_symbol, callback in self._subscriptions:
                        if sub_type == 'ticker':
                            # 将订阅的原始符号转换为ccxt格式进行比较
                            ccxt_sub_symbol = self._convert_to_ccxt_symbol(
                                sub_symbol)
                            if ccxt_sub_symbol == symbol:
                                if callback:
                                    # 转换为统一格式（使用原始符号）
                                    unified_ticker = self._convert_ccxt_ticker_to_standard(
                                        ticker, sub_symbol)
                                    if unified_ticker:
                                        await self._safe_callback_with_symbol(callback, sub_symbol, unified_ticker)

                                        # 🔥 修复：调用全局ticker_callback，与Backpack和EdgeX保持一致
                                        if hasattr(self, 'ticker_callback') and self.ticker_callback:
                                            await self._safe_callback_with_symbol(self.ticker_callback, sub_symbol, unified_ticker)

                                        # 也保持原有的扩展数据回调
                                        await self._base.extended_data_callback('ticker', unified_ticker)

                except Exception as e:
                    self.logger.error(f"[CCXT] ticker循环错误 {symbol}: {e}")
                    await asyncio.sleep(1)

        except Exception as e:
            self.logger.error(f"启动ticker循环失败 {symbol}: {e}")

    async def _ccxt_orderbook_loop(self, symbol: str):
        """ccxt orderbook数据循环"""
        try:
            # 注意：这里的symbol参数已经是ccxt格式的符号了，无需再次转换
            if self.logger:
                self.logger.debug(f"[CCXT] 开始orderbook循环: {symbol}")

            while not self._should_stop and self._ccxt_connected:
                try:
                    orderbook = await self._ccxt_exchange.watch_order_book(symbol)

                    # 转换为统一格式
                    unified_orderbook = self._convert_ccxt_orderbook_to_standard(
                        orderbook, symbol)
                    if unified_orderbook:
                        # 缓存数据
                        self._cache_orderbook_data(symbol, unified_orderbook)

                        # 触发orderbook回调
                        await self._base.extended_data_callback('orderbook', unified_orderbook)

                except Exception as e:
                    self.logger.error(f"[CCXT] orderbook循环错误 {symbol}: {e}")
                    await asyncio.sleep(1)

        except Exception as e:
            self.logger.error(f"启动orderbook循环失败 {symbol}: {e}")

    async def _ccxt_watch_funding_rate(self, symbol: str) -> None:
        """使用ccxt监听资金费率"""
        try:
            if not self._ccxt_exchange:
                await self._init_ccxt_exchange()

            ccxt_symbol = self._convert_to_ccxt_symbol(symbol)
            if not ccxt_symbol:
                return

            if self.logger:
                self.logger.debug(f"[CCXT] 开始监听 {symbol} 的资金费率")

            while not self._should_stop and self._ccxt_connected:
                try:
                    funding_rate = await self._ccxt_exchange.watch_funding_rate(ccxt_symbol)

                    # 转换为统一格式
                    unified_funding = self._convert_from_ccxt_funding_rate(
                        funding_rate, symbol)
                    if unified_funding:
                        # 触发资金费率回调
                        await self._base.extended_data_callback('funding_rate', unified_funding)

                except Exception as e:
                    self.logger.error(f"[CCXT] 监听 {symbol} 资金费率错误: {e}")
                    await asyncio.sleep(5)

        except Exception as e:
            self.logger.error(f"启动资金费率监听失败 {symbol}: {e}")

    async def _ccxt_watch_trades(self, symbol: str):
        """使用ccxt监听交易数据"""
        try:
            if not self._ccxt_exchange:
                await self._init_ccxt_exchange()

            ccxt_symbol = self._convert_to_ccxt_symbol(symbol)
            if not ccxt_symbol:
                return

            self.logger.debug(f"[CCXT] 开始监听 {symbol} 的交易数据")

            while not self._should_stop and self._ccxt_connected:
                try:
                    trades = await self._ccxt_exchange.watch_trades(ccxt_symbol)

                    # 转换为统一格式
                    for trade in trades:
                        unified_trade = self._convert_trade_from_ccxt(
                            trade, symbol)
                        if unified_trade:
                            # 触发交易数据回调
                            await self._base.extended_data_callback('trade', unified_trade)

                except Exception as e:
                    self.logger.error(f"[CCXT] 监听 {symbol} 交易数据错误: {e}")
                    await asyncio.sleep(1)

        except Exception as e:
            self.logger.error(f"[CCXT] 交易数据监听失败 {symbol}: {e}")

    async def _ccxt_watch_balance(self):
        """使用ccxt监听账户余额"""
        try:
            if not self._ccxt_exchange:
                await self._init_ccxt_exchange()

            self.logger.debug("[CCXT] 开始监听账户余额")

            while not self._should_stop and self._ccxt_connected:
                try:
                    balance = await self._ccxt_exchange.watch_balance()

                    # 转换为统一格式
                    unified_balance = self._convert_balance_from_ccxt(balance)
                    if unified_balance:
                        # 触发余额数据回调
                        await self._base.extended_data_callback('balance', unified_balance)

                except Exception as e:
                    self.logger.error(f"[CCXT] 监听账户余额错误: {e}")
                    await asyncio.sleep(1)

        except Exception as e:
            self.logger.error(f"[CCXT] 账户余额监听失败: {e}")

    async def _ccxt_watch_orders(self, symbol: str = None):
        """使用ccxt监听订单状态"""
        try:
            print(f"\n{'='*80}", flush=True)
            print(
                f"[WS-INIT-DEBUG] 🚀 _ccxt_watch_orders 任务启动！symbol={symbol}", flush=True)
            print(f"{'='*80}\n", flush=True)

            self.logger.info(
                f"[WS-INIT-DEBUG] 🚀 _ccxt_watch_orders 任务启动！symbol={symbol}")

            if not self._ccxt_exchange:
                self.logger.info("[WS-INIT-DEBUG] 初始化ccxt exchange...")
                await self._init_ccxt_exchange()
                self.logger.info(
                    f"[WS-INIT-DEBUG] ✅ ccxt exchange已初始化: {type(self._ccxt_exchange)}")

            ccxt_symbol = self._convert_to_ccxt_symbol(
                symbol) if symbol else None

            self.logger.info(f"[WS-INIT-DEBUG] 转换后的symbol: {ccxt_symbol}")
            self.logger.info(
                f"[WS-INIT-DEBUG] _ccxt_connected状态: {self._ccxt_connected}")
            self.logger.info(
                f"[WS-INIT-DEBUG] _should_stop状态: {self._should_stop}")
            self.logger.info(f"[CCXT] 开始监听订单状态 {symbol or '全部'}")

            if not self._ccxt_connected:
                print(f"\n[WS-INIT-DEBUG] ❌ CCXT未连接，无法监听订单！\n", flush=True)
                self.logger.error("[WS-INIT-DEBUG] ❌ CCXT未连接，无法监听订单！")
                return

            # 如果连接成功，打印状态
            print(f"[WS-INIT-DEBUG] ✅ CCXT已连接，准备进入监听循环...\n", flush=True)

            while not self._should_stop and self._ccxt_connected:
                try:
                    # 🔍 添加调试日志：开始监听
                    self.logger.debug(
                        f"[WS-ORDER-DEBUG] 正在监听订单更新，symbol={ccxt_symbol}")

                    orders = await self._ccxt_exchange.watch_orders(ccxt_symbol)

                    # 🔍 添加调试日志：收到订单数据
                    self.logger.info(
                        f"[WS-ORDER-DEBUG] 收到watch_orders返回数据，类型={type(orders)}, 数量={len(orders) if orders else 0}")

                    # 🔥 修复：将订单列表作为整体传递，而不是单个订单
                    # 格式化为grid_engine_impl期望的列表格式
                    if orders:
                        # 🔍 打印每个订单的详细信息
                        for idx, order in enumerate(orders):
                            self.logger.info(
                                f"[WS-ORDER-DEBUG] 订单{idx+1}: id={order.get('id')}, status={order.get('status')}, side={order.get('side')}, amount={order.get('amount')}")

                        # 🔥 修复：_convert_order_from_ccxt返回的是字典，不是OrderData对象
                        order_dicts = []
                        for order in orders:
                            unified_order = self._convert_order_from_ccxt(
                                order)
                            if unified_order:  # unified_order已经是字典，直接使用
                                order_dicts.append(unified_order)
                                # 🔍 记录转换后的数据
                                self.logger.debug(
                                    f"[WS-ORDER-DEBUG] 转换后的订单: id={unified_order.get('id')}, status={unified_order.get('status')}")

                        if order_dicts:
                            self.logger.info(
                                f"[WS-ORDER-DEBUG] 准备回调，订单数量={len(order_dicts)}")
                            # 🔍 打印第一个订单的完整数据（用于调试）
                            if order_dicts:
                                sample_order = order_dicts[0]
                                self.logger.info(
                                    f"[WS-ORDER-DEBUG] 订单样本数据: {sample_order}")
                            # 直接传递订单字典列表
                            await self._base.extended_data_callback('order', order_dicts)
                            self.logger.info(f"[WS-ORDER-DEBUG] ✅ 订单回调已触发")
                        else:
                            self.logger.warning(
                                f"[WS-ORDER-DEBUG] ⚠️  order_dicts为空，无法触发回调")
                    else:
                        self.logger.debug(f"[WS-ORDER-DEBUG] orders为空，继续监听")

                except Exception as e:
                    self.logger.error(f"[CCXT] 监听订单状态错误: {e}")
                    import traceback
                    self.logger.error(
                        f"[WS-ORDER-DEBUG] 错误堆栈:\n{traceback.format_exc()}")
                    await asyncio.sleep(1)

        except Exception as e:
            self.logger.error(f"[CCXT] 订单状态监听失败: {e}")

    def _convert_to_ccxt_symbol(self, symbol: str) -> str:
        """将标准符号格式转换为ccxt格式"""
        try:
            # 如果symbol是基础币种名称（如BTC、ETH），需要转换为ccxt格式
            if '/' not in symbol and ':' not in symbol:
                # 基础币种，转换为Hyperliquid标准格式
                return f"{symbol}/USDC:USDC"

            # 如果是已经标准格式的符号，进行格式转换
            if '/' in symbol and ':' in symbol:
                parts = symbol.split('/')
                if len(parts) == 2:
                    base_part = parts[0]
                    quote_part = parts[1]

                    # 处理不同的输入格式
                    if ':' in quote_part:
                        # 格式: BTC/USDC:USDC 或 BTC/USDC:PERP
                        quote_currency, contract_type = quote_part.split(':')

                        # 对于Hyperliquid，统一转换为ccxt格式
                        if contract_type in ['PERP', 'USDC']:
                            return f"{base_part}/USDC:USDC"
                        else:
                            return f"{base_part}/{quote_currency}"
                    else:
                        # 格式: BTC/USDC，添加合约类型
                        return f"{base_part}/{quote_part}:{quote_part}"

            # 如果格式不匹配，直接返回原始符号
            return symbol

        except Exception as e:
            if self.logger:
                self.logger.error(f"符号转换失败 {symbol}: {str(e)}")
            return symbol

    def _convert_ccxt_ticker_to_standard(self, ccxt_ticker: Dict[str, Any], original_symbol: str) -> TickerData:
        """将ccxt ticker数据转换为标准格式"""
        try:
            return TickerData(
                symbol=original_symbol,
                bid=self._base._safe_decimal(ccxt_ticker.get('bid')),
                ask=self._base._safe_decimal(ccxt_ticker.get('ask')),
                last=self._base._safe_decimal(ccxt_ticker.get('last')),
                open=self._base._safe_decimal(ccxt_ticker.get('open')),
                high=self._base._safe_decimal(ccxt_ticker.get('high')),
                low=self._base._safe_decimal(ccxt_ticker.get('low')),
                close=self._base._safe_decimal(ccxt_ticker.get('close')),
                volume=self._base._safe_decimal(ccxt_ticker.get('baseVolume')),
                quote_volume=self._base._safe_decimal(
                    ccxt_ticker.get('quoteVolume')),
                change=self._base._safe_decimal(ccxt_ticker.get('change')),
                percentage=self._base._safe_decimal(
                    ccxt_ticker.get('percentage')),
                timestamp=datetime.now(),
                exchange_timestamp=self._base._parse_timestamp(
                    ccxt_ticker.get('timestamp')),
                raw_data=ccxt_ticker
            )

        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"ccxt ticker转换失败 {original_symbol}: {str(e)}")
            return TickerData(symbol=original_symbol, timestamp=datetime.now())

    def _convert_ccxt_orderbook_to_standard(self, ccxt_orderbook: Dict[str, Any], original_symbol: str) -> OrderBookData:
        """将ccxt orderbook数据转换为标准格式"""
        try:
            # 转换买盘
            bids = []
            for bid in ccxt_orderbook.get('bids', []):
                if len(bid) >= 2:
                    bids.append(OrderBookLevel(
                        price=self._base._safe_decimal(bid[0]),
                        size=self._base._safe_decimal(bid[1])
                    ))

            # 转换卖盘
            asks = []
            for ask in ccxt_orderbook.get('asks', []):
                if len(ask) >= 2:
                    asks.append(OrderBookLevel(
                        price=self._base._safe_decimal(ask[0]),
                        size=self._base._safe_decimal(ask[1])
                    ))

            return OrderBookData(
                symbol=original_symbol,
                bids=bids,
                asks=asks,
                timestamp=datetime.now(),
                exchange_timestamp=self._base._parse_timestamp(
                    ccxt_orderbook.get('timestamp')),
                raw_data=ccxt_orderbook
            )

        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"ccxt orderbook转换失败 {original_symbol}: {str(e)}")
            return OrderBookData(symbol=original_symbol, bids=[], asks=[], timestamp=datetime.now())

    def _convert_trade_from_ccxt(self, trade: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        """将ccxt交易数据转换为统一格式"""
        try:
            return {
                'symbol': symbol,
                'trade_id': trade.get('id'),
                'price': float(trade.get('price', 0)),
                'amount': float(trade.get('amount', 0)),
                'side': trade.get('side'),  # 'buy' or 'sell'
                'timestamp': trade.get('timestamp'),
                'datetime': trade.get('datetime'),
                'info': trade
            }
        except Exception as e:
            if self.logger:
                self.logger.error(f"转换交易数据错误: {e}")
            return None

    def _convert_balance_from_ccxt(self, balance: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """将ccxt余额数据转换为统一格式"""
        try:
            return {
                'timestamp': balance.get('timestamp'),
                'datetime': balance.get('datetime'),
                'balances': {
                    asset: {
                        'free': float(info.get('free', 0)),
                        'used': float(info.get('used', 0)),
                        'total': float(info.get('total', 0))
                    }
                    for asset, info in balance.get('info', {}).items()
                    if isinstance(info, dict)
                },
                'info': balance
            }
        except Exception as e:
            if self.logger:
                self.logger.error(f"转换余额数据错误: {e}")
            return None

    def _convert_order_from_ccxt(self, order: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """将ccxt订单数据转换为统一格式"""
        try:
            return {
                'id': order.get('id'),
                'symbol': order.get('symbol'),
                'side': order.get('side'),
                'amount': float(order.get('amount', 0)),
                'price': float(order.get('price', 0)),
                'filled': float(order.get('filled', 0)),
                'remaining': float(order.get('remaining', 0)),
                'status': order.get('status'),
                'timestamp': order.get('timestamp'),
                'datetime': order.get('datetime'),
                'info': order
            }
        except Exception as e:
            if self.logger:
                self.logger.error(f"转换订单数据错误: {e}")
            return None

    def _convert_from_ccxt_funding_rate(self, funding_rate: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
        """将ccxt资金费率数据转换为统一格式"""
        try:
            return {
                'symbol': symbol,
                'funding_rate': funding_rate.get('fundingRate'),
                'next_funding_time': funding_rate.get('fundingTimestamp'),
                'timestamp': funding_rate.get('timestamp'),
                'info': funding_rate
            }
        except Exception as e:
            if self.logger:
                self.logger.error(f"转换资金费率数据失败 {symbol}: {e}")
            return None

    async def _cleanup_ccxt_tasks(self) -> None:
        """清理ccxt WebSocket任务"""
        try:
            for task in self._ccxt_tasks:
                if not task.done():
                    task.cancel()

            # 等待所有任务完成
            if self._ccxt_tasks:
                await asyncio.gather(*self._ccxt_tasks, return_exceptions=True)

            self._ccxt_tasks.clear()

        except Exception as e:
            if self.logger:
                self.logger.error(f"清理ccxt任务失败: {e}")

    # === 工具方法 ===

    async def _safe_callback_with_symbol(self, callback: Callable, symbol: str, data: Any) -> None:
        """安全调用回调函数"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(symbol, data)
            else:
                callback(symbol, data)
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 回调函数执行失败 {symbol}: {str(e)}")

    async def _ccxt_fetch_funding_rate(self, symbol: str) -> Optional[Dict[str, Any]]:
        """使用ccxt获取单个交易对的资金费率"""
        try:
            if not self._ccxt_exchange:
                await self._init_ccxt_exchange()

            # 转换为ccxt格式的交易对
            ccxt_symbol = self._convert_to_ccxt_symbol(symbol)
            if not ccxt_symbol:
                return None

            funding_rate = await self._ccxt_exchange.fetch_funding_rate(ccxt_symbol)

            # 转换为统一格式
            return {
                'symbol': symbol,
                'funding_rate': funding_rate.get('fundingRate'),
                'next_funding_time': funding_rate.get('fundingTimestamp'),
                'timestamp': funding_rate.get('timestamp'),
                'info': funding_rate
            }

        except Exception as e:
            if self.logger:
                self.logger.error(f"获取资金费率失败 {symbol}: {e}")
            return None

    async def _ccxt_fetch_funding_rates(self, symbols: List[str] = None) -> Dict[str, Any]:
        """使用ccxt获取多个交易对的资金费率"""
        try:
            if not self._ccxt_exchange:
                await self._init_ccxt_exchange()

            # 如果没有指定symbols，使用已订阅的symbols
            if not symbols:
                # 使用 _active_subscriptions 跟踪已订阅的符号
                symbols = list(self._active_subscriptions)

            # 转换为ccxt格式
            ccxt_symbols = []
            for symbol in symbols:
                ccxt_symbol = self._convert_to_ccxt_symbol(symbol)
                if ccxt_symbol:
                    ccxt_symbols.append(ccxt_symbol)

            if not ccxt_symbols:
                return {}

            funding_rates = await self._ccxt_exchange.fetch_funding_rates(ccxt_symbols)

            # 转换回我们的格式
            result = {}
            for ccxt_symbol, rate_data in funding_rates.items():
                original_symbol = self._convert_to_ccxt_symbol(
                    ccxt_symbol)  # 反向转换回标准格式
                if original_symbol:
                    result[original_symbol] = {
                        'symbol': original_symbol,
                        'funding_rate': rate_data.get('fundingRate'),
                        'next_funding_time': rate_data.get('fundingTimestamp'),
                        'timestamp': rate_data.get('timestamp'),
                        'info': rate_data
                    }

            return result

        except Exception as e:
            if self.logger:
                self.logger.error(f"获取资金费率失败: {e}")
            return {}

    # 连接管理功能
    def _init_connection_monitoring(self):
        """初始化连接监控"""
        self._connection_status = {
            'connected': False,
            'last_ping': None,
            'last_pong': None,
            'reconnect_count': 0,
            'last_reconnect': None,
            'health_check_interval': 30,
            'ping_timeout': 10
        }

    async def _monitor_connection_health(self):
        """监控连接健康状态"""
        while not self._should_stop:
            try:
                if self._ccxt_exchange and self._ccxt_connected:
                    # 检查连接状态
                    await self._check_connection_health()

                await asyncio.sleep(self._connection_status['health_check_interval'])

            except Exception as e:
                self.logger.error(f"连接健康监控失败: {e}")
                await asyncio.sleep(30)

    async def _check_connection_health(self):
        """检查连接健康状态"""
        try:
            current_time = datetime.now().timestamp()

            # 检查最后一次ping时间
            if self._connection_status['last_ping']:
                ping_timeout = self._connection_status['ping_timeout']
                time_since_ping = current_time - \
                    self._connection_status['last_ping']

                if time_since_ping > ping_timeout:
                    self.logger.warning(
                        f"连接超时 - 距离最后ping: {time_since_ping:.1f}秒")
                    await self._handle_connection_timeout()

            # 发送心跳检查
            await self._send_heartbeat()

        except Exception as e:
            self.logger.error(f"检查连接健康失败: {e}")

    async def _send_heartbeat(self):
        """发送心跳检查"""
        try:
            if self._ccxt_exchange and self._ccxt_connected:
                # 记录ping时间
                self._connection_status['last_ping'] = datetime.now(
                ).timestamp()

                # 尝试获取交易所状态作为心跳
                try:
                    await self._ccxt_exchange.fetch_status()
                    self._connection_status['last_pong'] = datetime.now(
                    ).timestamp()

                except Exception as e:
                    self.logger.warning(f"心跳检查失败: {e}")

        except Exception as e:
            self.logger.error(f"发送心跳失败: {e}")

    async def _handle_connection_timeout(self):
        """处理连接超时"""
        try:
            self.logger.warning("检测到连接超时，开始重连...")

            # 标记连接断开
            self._ccxt_connected = False
            self._connection_status['connected'] = False

            # 触发重连
            await self._reconnect()

        except Exception as e:
            self.logger.error(f"处理连接超时失败: {e}")

    async def _reconnect(self):
        """重新连接"""
        try:
            # 增加重连次数
            self._connection_status['reconnect_count'] += 1
            reconnect_count = self._connection_status['reconnect_count']

            self.logger.info(f"开始第 {reconnect_count} 次重连...")

            # 计算退避时间
            backoff_time = min(30, 2 ** (reconnect_count - 1))

            # 如果不是第一次重连，等待退避时间
            if reconnect_count > 1:
                self.logger.info(f"等待 {backoff_time} 秒后重连...")
                await asyncio.sleep(backoff_time)

            # 关闭现有连接
            await self._close_ccxt_connection()

            # 重新初始化连接
            await self._init_ccxt_exchange()

            if self._ccxt_connected:
                self.logger.info(f"重连成功 (第 {reconnect_count} 次)")
                self._connection_status['connected'] = True
                self._connection_status['last_reconnect'] = datetime.now(
                ).timestamp()

                # 重置重连计数
                self._connection_status['reconnect_count'] = 0

                # 重新启动监听任务
                await self._restart_monitoring_tasks()

            else:
                self.logger.error(f"重连失败 (第 {reconnect_count} 次)")

        except Exception as e:
            self.logger.error(f"重连失败: {e}")

    async def _restart_monitoring_tasks(self):
        """重新启动监听任务"""
        try:
            # 清理现有任务
            await self._cleanup_ccxt_tasks()

            # 重新启动ticker和orderbook监听
            if hasattr(self, '_monitored_symbols') and self._monitored_symbols:
                await self.subscribe_ticker_data(self._monitored_symbols)
                await self.subscribe_orderbook_data(self._monitored_symbols)

        except Exception as e:
            self.logger.error(f"重新启动监听任务失败: {e}")

    async def _close_ccxt_connection(self):
        """关闭ccxt连接"""
        try:
            if self._ccxt_exchange:
                await self._ccxt_exchange.close()
                self._ccxt_exchange = None

            self._ccxt_connected = False
            self._connection_status['connected'] = False

        except Exception as e:
            self.logger.error(f"关闭ccxt连接失败: {e}")

    def get_connection_status(self) -> Dict[str, Any]:
        """获取连接状态信息"""
        return {
            'connected': self._ccxt_connected,
            'connection_info': self._connection_status.copy() if hasattr(self, '_connection_status') else {},
            'task_count': len(self._ccxt_tasks),
            'subscriptions': len(self._ccxt_tasks),  # 兼容旧的属性名
            'exchange_type': 'ccxt',
            'exchange_id': 'hyperliquid',
            'active_subscriptions': len(self._active_subscriptions),
            'ticker_subscriptions': len([t for t in self._ccxt_tasks if 'ticker' in str(t)]),
            'orderbook_subscriptions': len([t for t in self._ccxt_tasks if 'orderbook' in str(t)]),
            'reconnect_attempts': self._reconnect_attempts,
            'enabled_markets': self._base.get_enabled_markets() if hasattr(self._base, 'get_enabled_markets') else [],
            'market_priority': getattr(self._base, 'market_priority', []),
            'default_market': getattr(self._base, 'default_market', 'perpetual')
        }

    def is_healthy(self) -> bool:
        """检查连接是否健康"""
        if not self._ccxt_connected:
            return False

        # 检查最后一次pong响应时间
        if self._connection_status['last_pong']:
            current_time = datetime.now().timestamp()
            time_since_pong = current_time - \
                self._connection_status['last_pong']

            # 如果超过2分钟没有收到pong，认为连接不健康
            if time_since_pong > 120:
                return False

        return True

    async def _safe_callback(self, callback_func, data: Any):
        """安全的回调调用"""
        try:
            if callback_func:
                if asyncio.iscoroutinefunction(callback_func):
                    await callback_func(data)
                else:
                    callback_func(data)
        except Exception as e:
            self.logger.error(f"回调执行失败: {e}")

    async def start_monitoring(self, symbols: List[str]):
        """启动监控（更新版本）"""
        try:
            # 保存监控的符号列表
            self._monitored_symbols = symbols

            # 初始化缓存
            self._init_cache()

            # 初始化连接监控
            self._init_connection_monitoring()

            # 启动ccxt WebSocket连接
            await self._init_ccxt_exchange()

            if self._ccxt_connected:
                # 启动监听任务
                await self.subscribe_ticker_data(symbols)
                await self.subscribe_orderbook_data(symbols)

                # 启动连接健康监控
                health_task = asyncio.create_task(
                    self._monitor_connection_health())
                self._ccxt_tasks.add(health_task)
                health_task.add_done_callback(self._ccxt_tasks.discard)

                # 启动定期缓存清理
                cleanup_task = asyncio.create_task(
                    self._periodic_cache_cleanup())
                self._ccxt_tasks.add(cleanup_task)
                cleanup_task.add_done_callback(self._ccxt_tasks.discard)

                self.logger.info(
                    f"Hyperliquid WebSocket 监控已启动，监听 {len(symbols)} 个符号")

        except Exception as e:
            self.logger.error(f"启动监控失败: {e}")
            raise

    async def stop_monitoring(self):
        """停止监控（更新版本）"""
        try:
            self._should_stop = True

            # 清理任务
            await self._cleanup_ccxt_tasks()

            # 关闭连接
            await self._close_ccxt_connection()

            # 清理缓存
            if hasattr(self, '_orderbook_cache'):
                self._orderbook_cache.clear()
            if hasattr(self, '_asset_context_cache'):
                self._asset_context_cache.clear()

            self.logger.info("Hyperliquid WebSocket 监控已停止")

        except Exception as e:
            self.logger.error(f"停止监控失败: {e}")

    # 缓存和数据处理功能
    def _init_cache(self):
        """初始化缓存"""
        self._orderbook_cache = {}
        self._asset_context_cache = {}
        self._last_cache_update = {}

    def _cache_orderbook_data(self, symbol: str, orderbook_data: Dict[str, Any]):
        """缓存订单簿数据"""
        try:
            self._orderbook_cache[symbol] = {
                'data': orderbook_data,
                'timestamp': datetime.now().timestamp()
            }
            self._last_cache_update[symbol] = datetime.now()

        except Exception as e:
            self.logger.error(f"缓存订单簿数据失败 {symbol}: {e}")

    def _get_cached_orderbook(self, symbol: str, max_age_seconds: int = 60) -> Optional[Dict[str, Any]]:
        """获取缓存的订单簿数据"""
        try:
            if symbol not in self._orderbook_cache:
                return None

            cached_data = self._orderbook_cache[symbol]
            current_time = datetime.now().timestamp()

            if current_time - cached_data['timestamp'] > max_age_seconds:
                # 缓存过期，删除
                del self._orderbook_cache[symbol]
                return None

            return cached_data['data']

        except Exception as e:
            self.logger.error(f"获取缓存订单簿数据失败 {symbol}: {e}")
            return None

    def _cache_asset_context(self, symbol: str, context_data: Dict[str, Any]):
        """缓存资产上下文数据"""
        try:
            self._asset_context_cache[symbol] = {
                'data': context_data,
                'timestamp': datetime.now().timestamp()
            }

        except Exception as e:
            self.logger.error(f"缓存资产上下文失败 {symbol}: {e}")

    def _get_cached_asset_context(self, symbol: str, max_age_seconds: int = 300) -> Optional[Dict[str, Any]]:
        """获取缓存的资产上下文数据"""
        try:
            if symbol not in self._asset_context_cache:
                return None

            cached_data = self._asset_context_cache[symbol]
            current_time = datetime.now().timestamp()

            if current_time - cached_data['timestamp'] > max_age_seconds:
                # 缓存过期，删除
                del self._asset_context_cache[symbol]
                return None

            return cached_data['data']

        except Exception as e:
            self.logger.error(f"获取缓存资产上下文失败 {symbol}: {e}")
            return None

    def _clean_expired_cache(self):
        """清理过期缓存"""
        try:
            current_time = datetime.now().timestamp()

            # 清理过期的订单簿缓存
            expired_orderbooks = []
            for symbol, cached_data in self._orderbook_cache.items():
                if current_time - cached_data['timestamp'] > 60:  # 1分钟过期
                    expired_orderbooks.append(symbol)

            for symbol in expired_orderbooks:
                del self._orderbook_cache[symbol]

            # 清理过期的资产上下文缓存
            expired_contexts = []
            for symbol, cached_data in self._asset_context_cache.items():
                if current_time - cached_data['timestamp'] > 300:  # 5分钟过期
                    expired_contexts.append(symbol)

            for symbol in expired_contexts:
                del self._asset_context_cache[symbol]

            if expired_orderbooks or expired_contexts:
                self.logger.debug(
                    f"清理过期缓存 - 订单簿: {len(expired_orderbooks)}, 资产上下文: {len(expired_contexts)}")

        except Exception as e:
            self.logger.error(f"清理过期缓存失败: {e}")

    # 符号提取和验证工具
    def _extract_symbols_from_message(self, message: Dict[str, Any]) -> List[str]:
        """从消息中提取符号列表"""
        symbols = []
        try:
            if 'data' in message:
                data = message['data']
                if isinstance(data, dict):
                    # 尝试从不同字段提取符号
                    for field in ['symbol', 'coin', 'market', 'pair']:
                        if field in data:
                            symbols.append(data[field])
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            for field in ['symbol', 'coin', 'market', 'pair']:
                                if field in item:
                                    symbols.append(item[field])

        except Exception as e:
            self.logger.error(f"提取符号失败: {e}")

        return list(set(symbols))  # 去重

    def _validate_symbol(self, symbol: str) -> bool:
        """验证符号格式"""
        try:
            if not symbol or not isinstance(symbol, str):
                return False

            # 基本长度检查
            if len(symbol) < 2 or len(symbol) > 20:
                return False

            # 基本格式检查（允许字母、数字、连字符、下划线）
            import re
            if not re.match(r'^[A-Za-z0-9\-_]+$', symbol):
                return False

            return True

        except Exception as e:
            self.logger.error(f"验证符号失败 {symbol}: {e}")
            return False

    def _normalize_symbol(self, symbol: str) -> str:
        """标准化符号格式"""
        try:
            if not symbol:
                return symbol

            # 转换为大写
            normalized = symbol.upper()

            # 移除多余的空格
            normalized = normalized.strip()

            # 标准化分隔符（将下划线转换为连字符）
            normalized = normalized.replace('_', '-')

            return normalized

        except Exception as e:
            self.logger.error(f"标准化符号失败 {symbol}: {e}")
            return symbol

    async def _periodic_cache_cleanup(self):
        """定期清理缓存"""
        while not self._should_stop:
            try:
                self._clean_expired_cache()
                await asyncio.sleep(60)  # 每分钟清理一次

            except Exception as e:
                self.logger.error(f"定期缓存清理失败: {e}")
                await asyncio.sleep(60)

    # 公共接口方法
    async def subscribe_trades(self, symbols: List[str]):
        """订阅交易数据"""
        if not symbols:
            return

        self.logger.info(f"[CCXT] 订阅交易数据: {symbols}")

        # 为每个符号创建独立的监听任务
        for symbol in symbols:
            task = asyncio.create_task(self._ccxt_watch_trades(symbol))
            self._ccxt_tasks.add(task)
            task.add_done_callback(self._ccxt_tasks.discard)

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None] = None, symbols: List[str] = None):
        """
        订阅用户数据（余额、订单）

        Args:
            callback: 用户数据回调函数
            symbols: 要订阅的交易对列表（可选）
        """
        self.logger.info("[CCXT] 订阅用户数据")

        # 注册订单回调到base实例
        if callback:
            self._base.register_callback('order', callback)
            self.logger.info("✅ 已注册订单回调函数")

        # 🔥 Hyperliquid不支持watchBalance()，跳过余额WebSocket监听
        # 余额数据可通过REST API查询（已在balance_monitor.py中实现）
        self.logger.info("ℹ️  Hyperliquid不支持余额WebSocket，将使用REST API查询余额")

        # 订阅订单状态
        if symbols:
            for symbol in symbols:
                order_task = asyncio.create_task(
                    self._ccxt_watch_orders(symbol))
                self._ccxt_tasks.add(order_task)
                order_task.add_done_callback(self._ccxt_tasks.discard)
        else:
            # 订阅全部订单
            order_task = asyncio.create_task(self._ccxt_watch_orders())
            self._ccxt_tasks.add(order_task)
            order_task.add_done_callback(self._ccxt_tasks.discard)

    async def unsubscribe_trades(self, symbols: List[str]):
        """取消订阅交易数据"""
        self.logger.info(f"[CCXT] 取消订阅交易数据: {symbols}")
        # 由于ccxt任务是独立的，我们需要重新启动监听任务
        # 这里可以实现更精细的取消订阅逻辑

    async def unsubscribe_user_data(self):
        """取消订阅用户数据"""
        self.logger.info("[CCXT] 取消订阅用户数据")
        # 由于ccxt任务是独立的，我们需要重新启动监听任务
        # 这里可以实现更精细的取消订阅逻辑

    # 数据查询方法
    async def get_latest_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取最新的ticker数据"""
        try:
            if not self._ccxt_exchange:
                await self._init_ccxt_exchange()

            ccxt_symbol = self._convert_to_ccxt_symbol(symbol)
            if not ccxt_symbol:
                return None

            ticker = await self._ccxt_exchange.fetch_ticker(ccxt_symbol)
            return self._convert_ccxt_ticker_to_standard(ticker, symbol)

        except Exception as e:
            self.logger.error(f"获取最新ticker失败 {symbol}: {e}")
            return None

    async def get_latest_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取最新的orderbook数据"""
        try:
            # 先尝试从缓存获取
            cached_orderbook = self._get_cached_orderbook(symbol)
            if cached_orderbook:
                return cached_orderbook

            # 如果缓存没有，从交易所获取
            if not self._ccxt_exchange:
                await self._init_ccxt_exchange()

            ccxt_symbol = self._convert_to_ccxt_symbol(symbol)
            if not ccxt_symbol:
                return None

            orderbook = await self._ccxt_exchange.fetch_order_book(ccxt_symbol)
            converted_orderbook = self._convert_ccxt_orderbook_to_standard(
                orderbook, symbol)

            # 缓存数据
            if converted_orderbook:
                self._cache_orderbook_data(symbol, converted_orderbook)

            return converted_orderbook

        except Exception as e:
            self.logger.error(f"获取最新orderbook失败 {symbol}: {e}")
            return None

    async def get_latest_trades(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        """获取最新的交易数据"""
        try:
            if not self._ccxt_exchange:
                await self._init_ccxt_exchange()

            ccxt_symbol = self._convert_to_ccxt_symbol(symbol)
            if not ccxt_symbol:
                return []

            trades = await self._ccxt_exchange.fetch_trades(ccxt_symbol, limit=limit)

            # 转换为统一格式
            converted_trades = []
            for trade in trades:
                converted_trade = self._convert_trade_from_ccxt(trade, symbol)
                if converted_trade:
                    converted_trades.append(converted_trade)

            return converted_trades

        except Exception as e:
            self.logger.error(f"获取最新交易数据失败 {symbol}: {e}")
            return []

    async def get_account_balance(self) -> Optional[Dict[str, Any]]:
        """获取账户余额"""
        try:
            if not self._ccxt_exchange:
                await self._init_ccxt_exchange()

            balance = await self._ccxt_exchange.fetch_balance()
            return self._convert_balance_from_ccxt(balance)

        except Exception as e:
            self.logger.error(f"获取账户余额失败: {e}")
            return None

    async def get_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        """获取未完成订单"""
        try:
            if not self._ccxt_exchange:
                await self._init_ccxt_exchange()

            ccxt_symbol = self._convert_to_ccxt_symbol(
                symbol) if symbol else None
            orders = await self._ccxt_exchange.fetch_open_orders(ccxt_symbol)

            # 转换为统一格式
            converted_orders = []
            for order in orders:
                converted_order = self._convert_order_from_ccxt(order)
                if converted_order:
                    converted_orders.append(converted_order)

            return converted_orders

        except Exception as e:
            self.logger.error(f"获取未完成订单失败: {e}")
            return []

    # 状态和统计方法
    def get_subscription_stats(self) -> Dict[str, Any]:
        """获取订阅统计信息"""
        return {
            'total_tasks': len(self._ccxt_tasks),
            'connection_status': self.get_connection_status(),
            'health_status': self.is_healthy(),
            'cache_stats': {
                'orderbook_cache_size': len(getattr(self, '_orderbook_cache', {})),
                'asset_context_cache_size': len(getattr(self, '_asset_context_cache', {}))
            },
            'monitored_symbols': len(getattr(self, '_monitored_symbols', [])),
            'exchange_info': {
                'exchange_id': 'hyperliquid',
                'implementation': 'ccxt',
                'features': ['ticker', 'orderbook', 'trades', 'balance', 'orders', 'funding_rate']
            }
        }

    def get_performance_metrics(self) -> Dict[str, Any]:
        """获取性能指标"""
        connection_info = self._connection_status

        return {
            'connection_health': self.is_healthy(),
            'reconnect_count': connection_info.get('reconnect_count', 0),
            'last_ping': connection_info.get('last_ping'),
            'last_pong': connection_info.get('last_pong'),
            'last_reconnect': connection_info.get('last_reconnect'),
            'uptime_seconds': (
                datetime.now().timestamp() - connection_info.get('last_reconnect', 0)
                if connection_info.get('last_reconnect')
                else None
            ),
            'task_count': len(self._ccxt_tasks),
            'cache_hit_ratio': self._calculate_cache_hit_ratio()
        }

    def _calculate_cache_hit_ratio(self) -> float:
        """计算缓存命中率"""
        # 这里可以实现缓存命中率的计算逻辑
        # 现在返回一个默认值
        return 0.85

    async def subscribe_ticker_data(self, symbols: List[str]):
        """订阅ticker数据"""
        if not symbols:
            return

        self.logger.info(f"[CCXT] 开始订阅ticker数据: {len(symbols)} 个符号")

        # 启动ccxt WebSocket连接
        if not await self._init_ccxt_exchange():
            return

        # 为每个符号创建独立的ticker循环任务
        for symbol in symbols:
            task = asyncio.create_task(self._ccxt_ticker_loop(symbol))
            self._ccxt_tasks.add(task)
            task.add_done_callback(self._ccxt_tasks.discard)

    async def subscribe_orderbook_data(self, symbols: List[str]):
        """订阅orderbook数据"""
        if not symbols:
            return

        self.logger.info(f"[CCXT] 开始订阅orderbook数据: {len(symbols)} 个符号")

        # 启动ccxt WebSocket连接
        if not await self._init_ccxt_exchange():
            return

        # 为每个符号创建独立的orderbook循环任务
        for symbol in symbols:
            task = asyncio.create_task(self._ccxt_orderbook_loop(symbol))
            self._ccxt_tasks.add(task)
            task.add_done_callback(self._ccxt_tasks.discard)

        self.logger.info(
            f"✅ Hyperliquid ccxt orderbook订阅完成: {len(symbols)} 个符号")
