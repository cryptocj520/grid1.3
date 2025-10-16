"""
Hyperliquid原生WebSocket实现 - 解决ccxt延迟问题

使用原生WebSocket + allMids/l2Book订阅，实现真正的实时数据推送
与hyperliquid_websocket.py功能完全一致，只是底层实现不同
"""

import asyncio
import json
import time
import websockets
import httpx
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable, Set, Tuple
from decimal import Decimal

from ..interface import ExchangeConfig
from ..models import TickerData, OrderBookData, TradeData, OrderBookLevel, OrderSide
from .hyperliquid_base import HyperliquidBase

# 导入统计配置读取器
from core.infrastructure.stats_config import get_exchange_stats_frequency, get_exchange_stats_summary


class HyperliquidNativeWebSocket:
    """Hyperliquid原生WebSocket客户端 - 零延迟实现"""

    def __init__(self, config: ExchangeConfig, base_instance: HyperliquidBase):
        self.config = config
        self._base = base_instance
        self.logger = base_instance.logger  # 🔥 修复：使用基础实例的logger
        
        # WebSocket连接
        self._ws_connection = None
        self._ws_connected = False
        self._should_stop = False
        self._reconnecting = False
        self._reconnect_attempts = 0
        
        # 🔥 修复：任务管理
        self._message_handler_task = None
        self._heartbeat_task = None
        
        # 订阅管理
        self._subscriptions: List[Tuple[str, str, Callable]] = []  # (sub_type, symbol, callback)
        self._subscribed_symbols: Set[str] = set()
        self._active_subscriptions = set()
        
        # 🔥 关键：全局回调设置
        self.ticker_callback = None
        self.orderbook_callback = None
        self.trades_callback = None
        
        # 连接状态监控
        self._last_heartbeat = 0
        self._last_ping_time = 0
        self._last_pong_time = 0
        
        # 缓存
        self._ticker_cache: Dict[str, TickerData] = {}
        self._orderbook_cache: Dict[str, OrderBookData] = {}
        self._latest_orderbooks: Dict[str, Dict[str, Any]] = {}
        self._asset_ctx_cache = {}
        
        # 连接参数
        self._ping_interval = 30  # 30秒ping间隔
        self._pong_timeout = 60   # 60秒无pong响应则重连
        
        # 统计配置
        self._stats_config = None
        self._symbol_count = None
        self._init_stats_config()
        
        # 初始化连接状态监控
        self._init_connection_monitoring()
        
        # REST API客户端
        self._http_client = httpx.AsyncClient(timeout=10.0)
        self._base_url = "https://api.hyperliquid.xyz"
        
        # 控制标志
        self._native_tasks = set()
        
    def _init_stats_config(self) -> None:
        """初始化统计配置"""
        try:
            self._stats_config = get_exchange_stats_frequency('hyperliquid', self._symbol_count)
            if self.logger:
                summary = get_exchange_stats_summary('hyperliquid', self._symbol_count)
                self.logger.info(f"🔥 Hyperliquid Native统计配置已加载: {summary}")
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
            self.logger.info(f"🔄 Hyperliquid Native统计配置已更新 (币种数量: {symbol_count})")
    
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
        """连接到Hyperliquid WebSocket"""
        try:
            if self._ws_connected:
                if self.logger:
                    self.logger.info("WebSocket已连接，跳过重复连接")
                return True
                
            if self.logger:
                self.logger.info(f"开始连接Hyperliquid原生WebSocket: {self._base.ws_url}")
                
            # 🔥 增加连接超时和重试逻辑
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    if self.logger:
                        self.logger.info(f"🔄 连接尝试 {attempt + 1}/{max_retries}")
                    
                    # 连接WebSocket (增加超时)
                    self._ws_connection = await asyncio.wait_for(
                        websockets.connect(
                            self._base.ws_url,
                            ping_interval=None,  # 使用自定义ping
                            ping_timeout=None,
                            close_timeout=10
                        ),
                        timeout=15  # 15秒超时
                    )
                    
                    self._ws_connected = True
                    self._last_heartbeat = time.time()
                    self._last_ping_time = time.time()
                    self._last_pong_time = time.time()
                    
                    # 🔥 修复：保存任务引用，防止垃圾回收
                    self._message_handler_task = asyncio.create_task(self._message_handler())
                    self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                    
                    if self.logger:
                        self.logger.info("✅ Hyperliquid原生WebSocket连接成功")
                        
                    return True
                    
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning(f"连接超时，尝试 {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay)
                    continue
                    
                except Exception as e:
                    if self.logger:
                        self.logger.warning(f"连接失败: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay)
                    continue
                    
            if self.logger:
                self.logger.error("所有连接尝试都失败了")
            return False
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"连接异常: {e}")
            return False

    async def disconnect(self) -> None:
        """断开WebSocket连接"""
        if self.logger:
            self.logger.info("正在断开Hyperliquid原生WebSocket连接...")
        
        self._should_stop = True
        
        # 🔥 修复：正确取消任务
        if self._message_handler_task and not self._message_handler_task.done():
            self._message_handler_task.cancel()
            try:
                await self._message_handler_task
            except asyncio.CancelledError:
                pass
        
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        # 关闭WebSocket连接
        if self._ws_connection:
            await self._ws_connection.close()
            self._ws_connection = None
        
        self._ws_connected = False
        
        # 清理数据
        self._subscriptions.clear()
        self._subscribed_symbols.clear()
        self._active_subscriptions.clear()
        
        # 清理缓存
        self._ticker_cache.clear()
        self._orderbook_cache.clear()
        self._latest_orderbooks.clear()
        
        # 关闭HTTP客户端
        if self._http_client:
            await self._http_client.aclose()
        
        if self.logger:
            self.logger.info("Hyperliquid原生WebSocket已断开")

    # === 订阅功能 ===

    async def subscribe_ticker(self, symbol: str, callback: Callable[[str, TickerData], None]) -> None:
        """订阅ticker数据"""
        self._subscriptions.append(('ticker', symbol, callback))
        self._subscribed_symbols.add(symbol)
        
        if self._ws_connected:
            await self._subscribe_allmids()
        
        if self.logger:
            self.logger.info(f"订阅ticker: {symbol}")

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[str, OrderBookData], None]) -> None:
        """订阅orderbook数据"""
        self._subscriptions.append(('orderbook', symbol, callback))
        self._subscribed_symbols.add(symbol)
        
        if self._ws_connected:
            await self._subscribe_l2book(symbol)
        
        if self.logger:
            self.logger.info(f"订阅orderbook: {symbol}")

    async def subscribe_trades(self, symbol: str, callback: Callable[[str, TradeData], None]) -> None:
        """订阅trades数据"""
        self._subscriptions.append(('trades', symbol, callback))
        self._subscribed_symbols.add(symbol)
        
        if self._ws_connected:
            await self._subscribe_trades(symbol)
        
        if self.logger:
            self.logger.info(f"订阅trades: {symbol}")

    async def batch_subscribe_tickers(self, symbols: List[str], callback: Callable[[str, TickerData], None]) -> None:
        """批量订阅ticker数据 - 使用allMids"""
        if not symbols:
            if self.logger:
                self.logger.warning("🚫 批量订阅ticker: 符号列表为空")
            return
            
        if self.logger:
            self.logger.info(f"📋 开始批量订阅ticker: {len(symbols)} 个符号")
            
        filtered_symbols = self._base.filter_symbols_by_market_type(symbols)
        
        if not filtered_symbols:
            if self.logger:
                enabled_markets = self._base.get_enabled_markets()
                self.logger.warning(f"🚫 没有符合启用市场类型的符号可订阅。启用的市场: {enabled_markets}")
                self.logger.warning(f"🚫 原始符号: {symbols}")
            return
        
        if self.logger:
            self.logger.info(f"✅ 过滤后的符号: {len(filtered_symbols)} 个")
            self.logger.info(f"📝 符号列表: {filtered_symbols}")
        
        # 🔥 关键：设置全局回调
        if callback:
            self.ticker_callback = callback
            if self.logger:
                self.logger.info("✅ 设置全局ticker回调成功")
        else:
            if self.logger:
                self.logger.warning("⚠️ 未提供ticker回调函数")
            
        # 保存订阅的符号
        self._subscribed_symbols.update(filtered_symbols)
        
        # 添加到订阅列表
        for symbol in filtered_symbols:
            self._subscriptions.append(('ticker', symbol, callback))
        
        if self.logger:
            self.logger.info(f"📊 已保存 {len(self._subscribed_symbols)} 个订阅符号")
        
        # 🔥 关键：检查WebSocket连接状态
        if not self._ws_connected:
            if self.logger:
                self.logger.error("❌ WebSocket未连接，无法发送订阅请求")
            return
            
        # 发送allMids订阅请求
        if self.logger:
            self.logger.info("📡 发送allMids订阅请求...")
            
        try:
            await self._subscribe_allmids()
            if self.logger:
                self.logger.info(f"✅ 批量订阅ticker完成: {len(filtered_symbols)}个符号 (使用allMids数据流)")
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 发送allMids订阅请求失败: {e}")
            raise

    async def batch_subscribe_orderbooks(self, symbols: List[str], callback: Callable[[str, OrderBookData], None]) -> None:
        """批量订阅orderbook"""
        if not symbols:
            return
            
        filtered_symbols = self._base.filter_symbols_by_market_type(symbols)
        
        if not filtered_symbols:
            if self.logger:
                enabled_markets = self._base.get_enabled_markets()
                self.logger.warning(f"没有符合启用市场类型的符号可订阅。启用的市场: {enabled_markets}")
            return
        
        # 设置全局回调
        if callback:
            self.orderbook_callback = callback
            
        # 保存订阅的符号
        self._subscribed_symbols.update(filtered_symbols)
        
        # 添加到订阅列表
        for symbol in filtered_symbols:
            self._subscriptions.append(('orderbook', symbol, callback))
        
        # 为每个符号发送l2Book订阅请求
        if self._ws_connected:
            for symbol in filtered_symbols:
                await self._subscribe_l2book(symbol)
            
        if self.logger:
            self.logger.info(f"✅ 批量订阅orderbook完成: {len(filtered_symbols)}个符号")

    async def subscribe_funding_rates(self, symbols: List[str]) -> bool:
        """订阅资金费率数据"""
        try:
            success_count = 0
            
            for symbol in symbols:
                try:
                    # 启动资金费率监听任务
                    task = asyncio.create_task(self._native_watch_funding_rate(symbol))
                    self._native_tasks.add(task)
                    task.add_done_callback(self._native_tasks.discard)
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
        return await self._native_fetch_funding_rates(symbols)

    async def get_funding_rate(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取单个交易对的资金费率"""
        return await self._native_fetch_funding_rate(symbol)

    # === 原生WebSocket订阅实现 ===

    async def _subscribe_allmids(self) -> None:
        """订阅allMids数据流"""
        try:
            if not self._ws_connected:
                return
                
            # Hyperliquid allMids订阅消息
            subscribe_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "allMids"
                }
            }
            
            await self._ws_connection.send(json.dumps(subscribe_msg))
            
            if self.logger:
                self.logger.info("🎯 已订阅Hyperliquid allMids数据流")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅allMids失败: {e}")

    async def _subscribe_l2book(self, symbol: str) -> None:
        """订阅l2Book数据流"""
        try:
            if not self._ws_connected:
                return
                
            # 转换为Hyperliquid格式
            hyperliquid_symbol = self._convert_to_hyperliquid_symbol(symbol)
            
            # Hyperliquid l2Book订阅消息
            subscribe_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "l2Book",
                    "coin": hyperliquid_symbol
                }
            }
            
            await self._ws_connection.send(json.dumps(subscribe_msg))
            
            if self.logger:
                self.logger.info(f"🎯 已订阅Hyperliquid l2Book: {symbol} -> {hyperliquid_symbol}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅l2Book失败 {symbol}: {e}")

    async def _subscribe_trades(self, symbol: str) -> None:
        """订阅交易数据流"""
        try:
            if not self._ws_connected:
                return
                
            # 转换为Hyperliquid格式
            hyperliquid_symbol = self._convert_to_hyperliquid_symbol(symbol)
            
            # Hyperliquid trades订阅消息
            subscribe_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "trades",
                    "coin": hyperliquid_symbol
                }
            }
            
            await self._ws_connection.send(json.dumps(subscribe_msg))
            
            if self.logger:
                self.logger.info(f"🎯 已订阅Hyperliquid trades: {symbol} -> {hyperliquid_symbol}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅trades失败 {symbol}: {e}")

    async def _message_handler(self) -> None:
        """WebSocket消息处理器"""
        try:
            async for message in self._ws_connection:
                # 更新心跳时间
                self._last_heartbeat = time.time()
                
                try:
                    data = json.loads(message)
                    await self._process_message(data)
                except json.JSONDecodeError:
                    if self.logger:
                        self.logger.warning(f"无效JSON消息: {message}")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"处理消息失败: {e}")
                        
        except websockets.exceptions.ConnectionClosed:
            if self.logger:
                self.logger.warning("WebSocket连接已断开")
            self._ws_connected = False
            # 🔥 修复：在连接断开时触发重连，增加更好的错误处理
            if not self._should_stop:
                # 异步调度重连，避免阻塞
                asyncio.create_task(self._safe_reconnect("connection_closed"))
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"消息处理器异常: {e}")
            self._ws_connected = False
            # 🔥 修复：在异常时也尝试重连，增加更好的错误处理
            if not self._should_stop:
                # 异步调度重连，避免阻塞
                asyncio.create_task(self._safe_reconnect("message_handler_exception"))

    async def _safe_reconnect(self, reason: str) -> None:
        """安全重连包装器"""
        try:
            if self.logger:
                self.logger.info(f"🔄 触发重连 (原因: {reason})")
            await self._reconnect()
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 安全重连失败: {e}")
            # 强制重置重连状态，避免卡死
            self._reconnecting = False

    async def _process_message(self, data: Dict[str, Any]) -> None:
        """处理WebSocket消息"""
        try:
            # 处理心跳响应
            if data.get("channel") == "pong":
                self._last_pong_time = time.time()
                if self.logger:
                    self.logger.debug("🏓 收到心跳响应")
                return
                
            # 处理allMids数据
            if data.get("channel") == "allMids":
                await self._handle_allmids_data(data.get("data", {}))
                return
                
            # 处理l2Book数据
            if data.get("channel") == "l2Book":
                await self._handle_l2book_data(data.get("data", {}))
                return
                
            # 处理trades数据
            if data.get("channel") == "trades":
                await self._handle_trades_data(data.get("data", {}))
                return
                
            # 处理订阅确认
            if data.get("channel") == "subscriptionResponse":
                if self.logger:
                    self.logger.debug(f"订阅确认: {data}")
                return
                
            # 处理其他消息类型
            if self.logger:
                self.logger.debug(f"未知消息类型: {data}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"处理消息失败: {e}")

    async def _handle_allmids_data(self, data: Dict[str, Any]) -> None:
        """处理allMids数据"""
        try:
            # allMids数据格式: {"BTC-USD": {"mid": "50000.0", "bid": "49999.0", "ask": "50001.0"}}
            mids = data.get("mids", {})
            
            # 🔥 调试信息：记录数据接收情况（改为DEBUG级别避免过多输出）
            if self.logger and len(mids) > 0:
                self.logger.debug(f"🔥 收到allMids数据: {len(mids)}个符号, 订阅符号: {len(self._subscribed_symbols)}个")
                self.logger.debug(f"🔥 ticker_callback设置: {self.ticker_callback is not None}")
                
                # 🔥 调试信息：显示前几个符号格式
                symbol_examples = list(mids.keys())[:5]
                self.logger.debug(f"🔥 前5个符号格式示例: {symbol_examples}")
                
                # 🔥 调试信息：显示订阅符号的示例
                subscribed_examples = list(self._subscribed_symbols)[:5]
                self.logger.debug(f"🔥 前5个订阅符号示例: {subscribed_examples}")
            
            processed_count = 0
            filtered_count = 0
            for symbol, mid_data in mids.items():
                # 转换符号格式
                standard_symbol = self._convert_from_hyperliquid_symbol(symbol)
                
                # 🔥 调试信息：记录符号转换
                if self.logger and not symbol.startswith('@'):
                    self.logger.debug(f"🔄 符号转换: {symbol} -> {standard_symbol}")
                
                # 只处理我们订阅的符号
                if standard_symbol not in self._subscribed_symbols:
                    filtered_count += 1
                    continue
                    
                # 转换为标准TickerData格式
                ticker = self._convert_allmids_to_ticker(symbol, mid_data)
                if ticker:
                    processed_count += 1
                    
                    # 🔥 调试信息：记录处理的符号
                    if self.logger:
                        self.logger.debug(f"📊 处理ticker数据: {standard_symbol} -> {ticker.last}")
                    
                    # 🔥 关键：修复全局回调调用（与CCXT版本保持一致）
                    if hasattr(self, 'ticker_callback') and self.ticker_callback:
                        await self._safe_callback_with_symbol(self.ticker_callback, standard_symbol, ticker)
                    
                    # 调用具体的ticker回调
                    await self._trigger_ticker_callbacks(standard_symbol, ticker)
                    
                    # 🔥 修复：安全调用扩展数据回调
                    if hasattr(self._base, 'extended_data_callback'):
                        await self._base.extended_data_callback('ticker', ticker)
            
            # 🔥 调试信息：记录处理结果
            if self.logger:
                self.logger.debug(f"🔥 处理allMids数据完成: 处理={processed_count}个符号, 过滤={filtered_count}个符号")
                        
        except Exception as e:
            if self.logger:
                self.logger.error(f"处理allMids数据失败: {e}")

    async def _handle_l2book_data(self, data: Dict[str, Any]) -> None:
        """处理l2Book数据"""
        try:
            coin = data.get("coin")
            levels = data.get("levels", [])
            
            if not coin or not levels:
                return
                
            # 转换为标准符号
            standard_symbol = self._convert_from_hyperliquid_symbol(coin)
            if standard_symbol not in self._subscribed_symbols:
                return
                
            # 转换为标准OrderBookData格式
            orderbook = self._convert_l2book_to_orderbook(coin, levels)
            if orderbook:
                # 缓存数据
                self._cache_orderbook_data(standard_symbol, orderbook)
                
                # 调用orderbook回调
                if self.orderbook_callback:
                    await self._safe_callback_with_symbol(self.orderbook_callback, standard_symbol, orderbook)
                
                # 调用具体的orderbook回调
                await self._trigger_orderbook_callbacks(standard_symbol, orderbook)
                
                # 🔥 修复：安全调用扩展数据回调
                if hasattr(self._base, 'extended_data_callback'):
                    await self._base.extended_data_callback('orderbook', orderbook)
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"处理l2Book数据失败: {e}")

    async def _handle_trades_data(self, data: Dict[str, Any]) -> None:
        """处理trades数据"""
        try:
            coin = data.get("coin")
            trades = data.get("trades", [])
            
            if not coin or not trades:
                return
                
            # 转换为标准符号
            standard_symbol = self._convert_from_hyperliquid_symbol(coin)
            if standard_symbol not in self._subscribed_symbols:
                return
                
            # 转换每个交易数据
            for trade_data in trades:
                trade = self._convert_trade_data(coin, trade_data)
                if trade:
                    # 调用trades回调
                    if self.trades_callback:
                        await self._safe_callback_with_symbol(self.trades_callback, standard_symbol, trade)
                    
                    # 调用具体的trades回调
                    await self._trigger_trades_callbacks(standard_symbol, trade)
                    
                    # 🔥 修复：安全调用扩展数据回调
                    if hasattr(self._base, 'extended_data_callback'):
                        await self._base.extended_data_callback('trade', trade)
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"处理trades数据失败: {e}")

    # === 数据转换方法 ===

    def _convert_allmids_to_ticker(self, symbol: str, mid_data: Any) -> Optional[TickerData]:
        """将allMids数据转换为TickerData"""
        try:
            # 标准化符号
            standard_symbol = self._convert_from_hyperliquid_symbol(symbol)
            
            # 🔥 修复：处理不同的数据格式
            if isinstance(mid_data, str):
                # 如果mid_data是字符串，则它就是价格
                mid_price = self._safe_decimal(mid_data)
                bid_price = None
                ask_price = None
            elif isinstance(mid_data, dict):
                # 如果mid_data是字典，则解析各个字段
                mid_price = self._safe_decimal(mid_data.get("mid"))
                bid_price = self._safe_decimal(mid_data.get("bid"))
                ask_price = self._safe_decimal(mid_data.get("ask"))
            else:
                # 如果是其他类型，尝试直接转换为价格
                mid_price = self._safe_decimal(mid_data)
                bid_price = None
                ask_price = None
            
            # 创建TickerData
            ticker = TickerData(
                symbol=standard_symbol,
                last=mid_price,
                bid=bid_price,
                ask=ask_price,
                timestamp=datetime.now(),
                exchange_timestamp=datetime.now(),
                
                # 从allMids数据中可能缺少的字段，使用默认值
                high=None,
                low=None,
                volume=None,
                change=None,
                percentage=None,
                
                # 技术指标
                bid_size=None,
                ask_size=None,
                open=None,
                close=mid_price,
                
                # 时间戳
                received_timestamp=datetime.now(),
                processed_timestamp=datetime.now(),
                sent_timestamp=datetime.now(),
                
                # 原始数据
                raw_data={"symbol": symbol, "mid_data": mid_data}
            )
            
            return ticker
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"转换allMids数据失败 {symbol}: {e}")
            return None

    def _convert_l2book_to_orderbook(self, symbol: str, levels: List[List[Dict[str, Any]]]) -> Optional[OrderBookData]:
        """将l2Book数据转换为OrderBookData"""
        try:
            # 标准化符号
            standard_symbol = self._convert_from_hyperliquid_symbol(symbol)
            
            # 转换买盘
            bids = []
            if len(levels) > 0:
                for level in levels[0]:
                    price = self._safe_decimal(level.get("px"))
                    size = self._safe_decimal(level.get("sz"))
                    if price and size:
                        bids.append(OrderBookLevel(price=price, size=size))
            
            # 转换卖盘
            asks = []
            if len(levels) > 1:
                for level in levels[1]:
                    price = self._safe_decimal(level.get("px"))
                    size = self._safe_decimal(level.get("sz"))
                    if price and size:
                        asks.append(OrderBookLevel(price=price, size=size))
            
            return OrderBookData(
                symbol=standard_symbol,
                bids=bids,
                asks=asks,
                timestamp=datetime.now(),
                exchange_timestamp=datetime.now(),
                raw_data={"coin": symbol, "levels": levels}
            )
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"转换l2Book数据失败 {symbol}: {e}")
            return None

    def _convert_trade_data(self, symbol: str, trade_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """将交易数据转换为标准格式"""
        try:
            # 标准化符号
            standard_symbol = self._convert_from_hyperliquid_symbol(symbol)
            
            return {
                'symbol': standard_symbol,
                'trade_id': trade_data.get('id'),
                'price': float(trade_data.get('px', 0)),
                'amount': float(trade_data.get('sz', 0)),
                'side': 'buy' if trade_data.get('side') == 'B' else 'sell',
                'timestamp': trade_data.get('time'),
                'info': trade_data
            }
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"转换交易数据失败 {symbol}: {e}")
            return None

    def _convert_from_hyperliquid_symbol(self, hyperliquid_symbol: str) -> str:
        """从Hyperliquid格式转换为标准格式"""
        # 🔥 修复：对于数字符号（如@1, @10），暂时跳过处理
        # 这些符号需要通过元数据映射，我们暂时忽略它们
        if hyperliquid_symbol.startswith('@'):
            return hyperliquid_symbol  # 直接返回，会被后续逻辑过滤掉
        
        # 🔥 修复：处理永续合约符号格式
        # 对于Hyperliquid，我们知道永续合约符号格式是基础币种名称（如BTC, ETH, SOL）
        # 需要映射到标准格式：BTC -> BTC/USDC:USDC
        
        # 🔥 处理标准格式 BTC-USD -> BTC/USDC:USDC
        if '-' in hyperliquid_symbol:
            base, quote = hyperliquid_symbol.split('-')
            if quote == 'USD':
                return f"{base}/USDC:USDC"  # 🔥 修复：改为:USDC以匹配订阅符号格式
        
        # 🔥 关键修复：处理简单币种符号（如BTC, ETH, SOL）
        # 对于永续合约，这些符号应该映射为 BTC/USDC:USDC 格式
        if '/' not in hyperliquid_symbol and ':' not in hyperliquid_symbol and '-' not in hyperliquid_symbol:
            # 这是一个简单的基础币种符号，映射为永续合约格式
            # 根据调试信息，我们的订阅符号是 BTC/USDC:USDC 格式
            if self.logger:
                self.logger.debug(f"🔄 映射基础币种符号: {hyperliquid_symbol} -> {hyperliquid_symbol}/USDC:USDC")
            return f"{hyperliquid_symbol}/USDC:USDC"
        
        # 🔥 备用逻辑：使用HyperliquidBase的reverse_map_symbol方法
        if hasattr(self._base, 'reverse_map_symbol'):
            mapped_symbol = self._base.reverse_map_symbol(hyperliquid_symbol)
            if mapped_symbol != hyperliquid_symbol:  # 如果有映射结果
                return mapped_symbol
        
        # 如果没有其他映射，直接返回原符号
        return hyperliquid_symbol

    def _convert_to_hyperliquid_symbol(self, standard_symbol: str) -> str:
        """将标准格式转换为Hyperliquid格式"""
        # 🔥 修复：使用HyperliquidBase的map_symbol方法
        if hasattr(self._base, 'map_symbol'):
            return self._base.map_symbol(standard_symbol)
        
        # 🔥 备用逻辑：BTC/USDC:PERP -> BTC-USD
        if '/' in standard_symbol:
            base = standard_symbol.split('/')[0]
            return f"{base}-USD"
        return standard_symbol

    def _safe_decimal(self, value: Any) -> Optional[Decimal]:
        """安全转换为Decimal"""
        try:
            if value is None or value == '':
                return None
            return Decimal(str(value))
        except (ValueError, TypeError):
            return None

    # === 回调触发方法 ===

    async def _trigger_ticker_callbacks(self, symbol: str, ticker: TickerData) -> None:
        """触发ticker回调"""
        for sub_type, sub_symbol, callback in self._subscriptions:
            if sub_type == 'ticker' and sub_symbol == symbol:
                await self._safe_callback_with_symbol(callback, symbol, ticker)

    async def _trigger_orderbook_callbacks(self, symbol: str, orderbook: OrderBookData) -> None:
        """触发orderbook回调"""
        for sub_type, sub_symbol, callback in self._subscriptions:
            if sub_type == 'orderbook' and sub_symbol == symbol:
                await self._safe_callback_with_symbol(callback, symbol, orderbook)

    async def _trigger_trades_callbacks(self, symbol: str, trade: Dict[str, Any]) -> None:
        """触发trades回调"""
        for sub_type, sub_symbol, callback in self._subscriptions:
            if sub_type == 'trades' and sub_symbol == symbol:
                await self._safe_callback_with_symbol(callback, symbol, trade)

    async def _safe_callback_with_symbol(self, callback: Callable, symbol: str, data: Any) -> None:
        """安全的回调调用"""
        try:
            if callback:
                if asyncio.iscoroutinefunction(callback):
                    await callback(symbol, data)
                else:
                    callback(symbol, data)
        except Exception as e:
            if self.logger:
                self.logger.error(f"回调执行失败: {e}")

    # === 心跳和重连 ===

    async def _heartbeat_loop(self) -> None:
        """心跳检测循环 - 增强版本"""
        if self.logger:
            self.logger.info("💓 启动Hyperliquid心跳检测循环")
            
        while not self._should_stop:
            try:
                # 🔥 修复：即使连接断开也继续检测，以便触发重连
                await asyncio.sleep(5)  # 每5秒检查一次
                
                if self._should_stop:
                    break
                
                current_time = time.time()
                
                # 🔥 修复：检查连接状态
                if not self._ws_connected:
                    if self.logger:
                        self.logger.warning("⚠️ 心跳检测发现连接断开，触发重连...")
                    # 使用安全重连方法
                    asyncio.create_task(self._safe_reconnect("heartbeat_disconnected"))
                    await asyncio.sleep(10)  # 等待10秒后继续检测
                    continue
                
                # 🔥 修复：检查连接是否真正可用
                if not self._is_connection_alive():
                    if self.logger:
                        self.logger.warning("⚠️ 心跳检测发现连接不可用，触发重连...")
                    self._ws_connected = False
                    asyncio.create_task(self._safe_reconnect("heartbeat_connection_dead"))
                    await asyncio.sleep(10)  # 等待10秒后继续检测
                    continue
                
                # 发送ping
                if current_time - self._last_ping_time > self._ping_interval:
                    await self._send_ping()
                    self._last_ping_time = current_time
                
                # 检查pong响应
                pong_timeout = current_time - self._last_pong_time
                if pong_timeout > self._pong_timeout:
                    if self.logger:
                        self.logger.warning(f"⚠️ 心跳超时: {pong_timeout:.1f}s无pong响应，触发重连...")
                    self._ws_connected = False
                    asyncio.create_task(self._safe_reconnect("heartbeat_pong_timeout"))
                    await asyncio.sleep(10)  # 等待10秒后继续检测
                    continue
                
                # 检查数据接收超时
                silence_time = current_time - self._last_heartbeat
                if silence_time > 90:  # 90秒无数据
                    if self.logger:
                        self.logger.warning(f"⚠️ 数据接收超时: {silence_time:.1f}s无数据，触发重连...")
                    self._ws_connected = False
                    asyncio.create_task(self._safe_reconnect("heartbeat_data_timeout"))
                    await asyncio.sleep(10)  # 等待10秒后继续检测
                    continue
                    
            except asyncio.CancelledError:
                if self.logger:
                    self.logger.info("💓 心跳检测循环被取消")
                break
            except Exception as e:
                if self.logger:
                    self.logger.error(f"❌ 心跳检测异常: {e}")
                await asyncio.sleep(5)
                
        if self.logger:
            self.logger.info("💓 心跳检测循环已退出")

    def _is_connection_alive(self) -> bool:
        """检查连接是否真正可用"""
        try:
            if not self._ws_connection:
                return False
            if self._ws_connection.closed:
                return False
            # 可以添加更多连接状态检查
            return True
        except Exception:
            return False

    async def _send_ping(self) -> None:
        """发送心跳ping - 增强版本"""
        try:
            if self._ws_connected and self._ws_connection and not self._ws_connection.closed:
                ping_msg = {
                    "method": "ping",
                    "id": int(time.time() * 1000)
                }
                await self._ws_connection.send(json.dumps(ping_msg))
                
                if self.logger:
                    self.logger.debug("🏓 发送心跳ping")
            else:
                if self.logger:
                    self.logger.warning("⚠️ 无法发送ping，连接不可用")
                # 🔥 修复：连接不可用时标记断开并触发重连
                self._ws_connected = False
                asyncio.create_task(self._safe_reconnect("ping_connection_unavailable"))
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 发送心跳失败: {e}")
            # 🔥 修复：ping失败时标记断开并触发重连
            self._ws_connected = False
            asyncio.create_task(self._safe_reconnect("ping_send_failed"))

    async def _reconnect(self) -> None:
        """重连逻辑 - 增强版本"""
        # 🔥 修复：使用更严格的重连状态检查
        if self._reconnecting:
            if self.logger:
                self.logger.debug("重连已在进行中，跳过")
            return
        
        if self._should_stop:
            if self.logger:
                self.logger.debug("系统正在停止，跳过重连")
            return
            
        self._reconnecting = True
        self._reconnect_attempts += 1
        
        try:
            if self.logger:
                self.logger.info(f"🔄 开始重连尝试 #{self._reconnect_attempts}")
                
            # 🔥 修复：强制清理所有现有任务和连接
            await self._force_cleanup()
            
            # 🔥 修复：重置连接状态
            self._ws_connected = False
                
            # 等待后重连
            delay = min(2 ** min(self._reconnect_attempts, 6), 60)  # 最大60秒延迟
            if self.logger:
                self.logger.info(f"🔄 等待 {delay}s 后重连...")
            await asyncio.sleep(delay)
            
            # 🔥 修复：检查是否应该停止
            if self._should_stop:
                if self.logger:
                    self.logger.info("系统正在停止，取消重连")
                return
            
            # 重新连接
            if self.logger:
                self.logger.info("🔄 正在重新建立连接...")
            success = await self.connect()
            
            if success:
                # 重新订阅
                if self.logger:
                    self.logger.info("🔄 正在重新订阅...")
                await self._resubscribe_all()
                    
                self._reconnect_attempts = 0
                if self.logger:
                    self.logger.info("✅ 重连成功")
            else:
                if self.logger:
                    self.logger.error("❌ 重连失败 - connect返回False")
                # 不抛出异常，让外层逻辑继续尝试
                    
        except asyncio.CancelledError:
            if self.logger:
                self.logger.warning("⚠️ 重连被取消")
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 重连异常: {e}")
            # 🔥 修复：不抛出异常，让系统继续尝试
        finally:
            # 🔥 修复：确保重连状态总是被重置
            self._reconnecting = False

    async def _force_cleanup(self) -> None:
        """强制清理所有连接和任务"""
        try:
            # 停止现有任务
            cleanup_tasks = []
            
            if self._message_handler_task and not self._message_handler_task.done():
                self._message_handler_task.cancel()
                cleanup_tasks.append(self._message_handler_task)
            
            if self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
                cleanup_tasks.append(self._heartbeat_task)
                
            # 等待任务取消完成
            if cleanup_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*cleanup_tasks, return_exceptions=True),
                        timeout=3.0
                    )
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("任务取消超时")
                
            # 关闭WebSocket连接
            if self._ws_connection:
                try:
                    await asyncio.wait_for(self._ws_connection.close(), timeout=3.0)
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("WebSocket关闭超时")
                except Exception as e:
                    if self.logger:
                        self.logger.debug(f"WebSocket关闭异常: {e}")
                finally:
                    self._ws_connection = None
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"强制清理异常: {e}")
            # 重置连接对象，确保下次能够重新连接
            self._ws_connection = None

    async def _resubscribe_all(self) -> None:
        """重新订阅所有数据"""
        try:
            # 重新订阅ticker数据
            ticker_symbols = set()
            for sub_type, symbol, callback in self._subscriptions:
                if sub_type == 'ticker':
                    ticker_symbols.add(symbol)
            
            if ticker_symbols:
                await self._subscribe_allmids()
            
            # 重新订阅orderbook数据
            orderbook_symbols = set()
            for sub_type, symbol, callback in self._subscriptions:
                if sub_type == 'orderbook':
                    orderbook_symbols.add(symbol)
            
            for symbol in orderbook_symbols:
                await self._subscribe_l2book(symbol)
            
            # 重新订阅trades数据
            trades_symbols = set()
            for sub_type, symbol, callback in self._subscriptions:
                if sub_type == 'trades':
                    trades_symbols.add(symbol)
            
            for symbol in trades_symbols:
                await self._subscribe_trades(symbol)
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"重新订阅失败: {e}")

    # === 资金费率相关 ===

    async def _native_watch_funding_rate(self, symbol: str) -> None:
        """使用原生方法监听资金费率"""
        try:
            if self.logger:
                self.logger.debug(f"[Native] 开始监听 {symbol} 的资金费率")
            
            while not self._should_stop:
                try:
                    funding_rate = await self._native_fetch_funding_rate(symbol)
                    
                    if funding_rate:
                        # 🔥 修复：安全调用扩展数据回调
                        if hasattr(self._base, 'extended_data_callback'):
                            await self._base.extended_data_callback('funding_rate', funding_rate)
                        
                    # 每5分钟检查一次
                    await asyncio.sleep(300)
                        
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"[Native] 监听 {symbol} 资金费率错误: {e}")
                    await asyncio.sleep(30)
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"启动资金费率监听失败 {symbol}: {e}")

    async def _native_fetch_funding_rate(self, symbol: str) -> Optional[Dict[str, Any]]:
        """使用原生方法获取单个交易对的资金费率"""
        try:
            # 转换为Hyperliquid格式
            hyperliquid_symbol = self._convert_to_hyperliquid_symbol(symbol)
            
            # 使用REST API获取资金费率
            url = f"{self._base_url}/info"
            payload = {"type": "metaAndAssetCtxs"}
            
            response = await self._http_client.post(url, json=payload)
            
            if response.status_code != 200:
                return None
                
            data = response.json()
            
            # 解析资金费率
            if isinstance(data, list) and len(data) >= 2:
                universe_data = data[0]
                asset_ctxs = data[1]
                
                if isinstance(universe_data, dict) and "universe" in universe_data:
                    universe = universe_data["universe"]
                    
                    # 查找特定币种
                    for i, coin_data in enumerate(universe):
                        if isinstance(coin_data, dict) and coin_data.get("name") == hyperliquid_symbol:
                            if i < len(asset_ctxs):
                                coin_ctx = asset_ctxs[i]
                                
                                funding_rate = coin_ctx.get("funding")
                                if funding_rate is not None:
                                    return {
                                        'symbol': symbol,
                                        'funding_rate': float(funding_rate),
                                        'timestamp': time.time() * 1000,
                                        'info': coin_ctx
                                    }
            
            return None
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取资金费率失败 {symbol}: {e}")
            return None

    async def _native_fetch_funding_rates(self, symbols: List[str] = None) -> Dict[str, Any]:
        """使用原生方法获取多个交易对的资金费率"""
        try:
            # 如果没有指定symbols，使用已订阅的symbols
            if not symbols:
                symbols = list(self._subscribed_symbols)
                
            # 使用REST API获取所有资金费率
            url = f"{self._base_url}/info"
            payload = {"type": "metaAndAssetCtxs"}
            
            response = await self._http_client.post(url, json=payload)
            
            if response.status_code != 200:
                return {}
                
            data = response.json()
            
            results = {}
            
            # 解析资金费率
            if isinstance(data, list) and len(data) >= 2:
                universe_data = data[0]
                asset_ctxs = data[1]
                
                if isinstance(universe_data, dict) and "universe" in universe_data:
                    universe = universe_data["universe"]
                    
                    # 遍历所有币种
                    for i, coin_data in enumerate(universe):
                        if not isinstance(coin_data, dict) or "name" not in coin_data:
                            continue
                            
                        hyperliquid_symbol = coin_data["name"]
                        standard_symbol = self._convert_from_hyperliquid_symbol(hyperliquid_symbol)
                        
                        # 只处理我们需要的符号
                        if symbols and standard_symbol not in symbols:
                            continue
                            
                        # 获取该币种的资金费率
                        if i < len(asset_ctxs):
                            coin_ctx = asset_ctxs[i]
                            
                            funding_rate = coin_ctx.get("funding")
                            if funding_rate is not None:
                                results[standard_symbol] = {
                                    'symbol': standard_symbol,
                                    'funding_rate': float(funding_rate),
                                    'timestamp': time.time() * 1000,
                                    'info': coin_ctx
                                }
            
            return results
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"批量获取资金费率失败: {e}")
            return {}

    # === 数据查询方法 ===

    async def get_latest_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取最新的ticker数据"""
        try:
            # 从缓存获取
            if symbol in self._ticker_cache:
                return self._ticker_cache[symbol]
            
            # 如果缓存没有，尝试从REST API获取
            hyperliquid_symbol = self._convert_to_hyperliquid_symbol(symbol)
            
            url = f"{self._base_url}/info"
            payload = {"type": "allMids"}
            
            response = await self._http_client.post(url, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                
                # 解析ticker数据
                if isinstance(data, dict) and hyperliquid_symbol in data:
                    mid_data = data[hyperliquid_symbol]
                    ticker = self._convert_allmids_to_ticker(hyperliquid_symbol, mid_data)
                    if ticker:
                        self._ticker_cache[symbol] = ticker
                        return ticker
                        
            return None
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取最新ticker失败 {symbol}: {e}")
            return None

    async def get_latest_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取最新的orderbook数据"""
        try:
            # 先从缓存获取
            cached_orderbook = self._get_cached_orderbook(symbol)
            if cached_orderbook:
                return cached_orderbook
                
            # 如果缓存没有，从REST API获取
            hyperliquid_symbol = self._convert_to_hyperliquid_symbol(symbol)
            
            url = f"{self._base_url}/info"
            payload = {
                "type": "l2Book",
                "coin": hyperliquid_symbol
            }
            
            response = await self._http_client.post(url, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                
                if isinstance(data, dict) and "levels" in data:
                    levels = data["levels"]
                    orderbook = self._convert_l2book_to_orderbook(hyperliquid_symbol, levels)
                    if orderbook:
                        self._cache_orderbook_data(symbol, orderbook)
                        return orderbook
                        
            return None
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取最新orderbook失败 {symbol}: {e}")
            return None

    async def get_latest_trades(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        """获取最新的交易数据"""
        try:
            # 原生实现暂时返回空列表
            # 可以通过REST API实现，但Hyperliquid的公共API可能不支持历史交易查询
            return []
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取最新交易数据失败 {symbol}: {e}")
            return []

    async def get_account_balance(self) -> Optional[Dict[str, Any]]:
        """获取账户余额"""
        try:
            if not self.config.api_key:
                return None
                
            url = f"{self._base_url}/info"
            payload = {
                "type": "clearinghouseState",
                "user": self.config.api_key
            }
            
            response = await self._http_client.post(url, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                
                # 转换为统一格式
                return {
                    'timestamp': time.time() * 1000,
                    'balances': data.get('balances', {}),
                    'info': data
                }
                
            return None
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取账户余额失败: {e}")
            return None

    async def get_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        """获取未完成订单"""
        try:
            # 原生实现暂时返回空列表
            # 需要私有API支持
            return []
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取未完成订单失败: {e}")
            return []

    # === 缓存管理 ===

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

    def _cache_orderbook_data(self, symbol: str, orderbook_data: Dict[str, Any]):
        """缓存订单簿数据"""
        try:
            self._latest_orderbooks[symbol] = {
                'data': orderbook_data,
                'timestamp': time.time()
            }
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"缓存订单簿数据失败 {symbol}: {e}")

    def _get_cached_orderbook(self, symbol: str, max_age_seconds: int = 60) -> Optional[Dict[str, Any]]:
        """获取缓存的订单簿数据"""
        try:
            if symbol not in self._latest_orderbooks:
                return None
                
            cached_data = self._latest_orderbooks[symbol]
            current_time = time.time()
            
            if current_time - cached_data['timestamp'] > max_age_seconds:
                # 缓存过期，删除
                del self._latest_orderbooks[symbol]
                return None
                
            return cached_data['data']
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取缓存订单簿数据失败 {symbol}: {e}")
            return None

    async def _cleanup_native_tasks(self) -> None:
        """清理原生任务"""
        try:
            for task in self._native_tasks:
                if not task.done():
                    task.cancel()
                    
            # 等待所有任务完成
            if self._native_tasks:
                await asyncio.gather(*self._native_tasks, return_exceptions=True)
                
            self._native_tasks.clear()
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"清理原生任务失败: {e}")

    # === 状态和统计方法 ===

    def is_connected(self) -> bool:
        """检查连接状态"""
        return self._ws_connected

    def get_subscribed_symbols(self) -> Set[str]:
        """获取已订阅的符号"""
        return self._subscribed_symbols.copy()

    def get_connection_status(self) -> Dict[str, Any]:
        """获取连接状态信息"""
        return {
            'connected': self._ws_connected,
            'connection_info': self._connection_status.copy() if hasattr(self, '_connection_status') else {},
            'task_count': len(self._native_tasks),
            'subscriptions': len(self._subscriptions),
            'exchange_type': 'native_websocket',
            'exchange_id': 'hyperliquid',
            'active_subscriptions': len(self._active_subscriptions),
            'ticker_subscriptions': len([s for s in self._subscriptions if s[0] == 'ticker']),
            'orderbook_subscriptions': len([s for s in self._subscriptions if s[0] == 'orderbook']),
            'reconnect_attempts': self._reconnect_attempts,
            'enabled_markets': self._base.get_enabled_markets() if hasattr(self._base, 'get_enabled_markets') else [],
            'market_priority': getattr(self._base, 'market_priority', []),
            'default_market': getattr(self._base, 'default_market', 'perpetual')
        }

    def is_healthy(self) -> bool:
        """检查连接是否健康"""
        if not self._ws_connected:
            return False
            
        # 检查最后一次pong响应时间
        if self._last_pong_time:
            current_time = time.time()
            time_since_pong = current_time - self._last_pong_time
            
            # 如果超过2分钟没有收到pong，认为连接不健康
            if time_since_pong > 120:
                return False
                
        return True

    def get_subscription_stats(self) -> Dict[str, Any]:
        """获取订阅统计信息"""
        return {
            'total_tasks': len(self._native_tasks),
            'connection_status': self.get_connection_status(),
            'health_status': self.is_healthy(),
            'cache_stats': {
                'orderbook_cache_size': len(self._latest_orderbooks),
                'ticker_cache_size': len(self._ticker_cache)
            },
            'monitored_symbols': len(self._subscribed_symbols),
            'exchange_info': {
                'exchange_id': 'hyperliquid',
                'implementation': 'native_websocket',
                'features': ['ticker', 'orderbook', 'trades', 'funding_rate']
            }
        }

    def get_performance_metrics(self) -> Dict[str, Any]:
        """获取性能指标"""
        return {
            'connection_health': self.is_healthy(),
            'reconnect_count': self._reconnect_attempts,
            'last_ping': self._last_ping_time,
            'last_pong': self._last_pong_time,
            'last_heartbeat': self._last_heartbeat,
            'task_count': len(self._native_tasks),
            'subscribed_symbols': len(self._subscribed_symbols),
            'implementation': 'native_websocket'
        }

    # === 兼容性方法 ===

    async def start_monitoring(self, symbols: List[str]):
        """启动监控（兼容性方法）"""
        try:
            # 连接WebSocket
            await self.connect()
            
            # 批量订阅ticker数据
            if hasattr(self, 'ticker_callback') and self.ticker_callback:
                await self.batch_subscribe_tickers(symbols, self.ticker_callback)
            
            # 批量订阅orderbook数据
            if hasattr(self, 'orderbook_callback') and self.orderbook_callback:
                await self.batch_subscribe_orderbooks(symbols, self.orderbook_callback)
                
            if self.logger:
                self.logger.info(f"Hyperliquid Native WebSocket 监控已启动，监听 {len(symbols)} 个符号")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"启动监控失败: {e}")
            raise

    async def stop_monitoring(self):
        """停止监控（兼容性方法）"""
        try:
            await self.disconnect()
            
            if self.logger:
                self.logger.info("Hyperliquid Native WebSocket 监控已停止")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"停止监控失败: {e}")