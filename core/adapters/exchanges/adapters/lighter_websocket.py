"""
Lighter交易所适配器 - WebSocket模块

封装Lighter SDK的WebSocket功能，提供实时数据流
"""

from typing import Dict, Any, Optional, List, Callable
from decimal import Decimal
import asyncio
import logging
import json

try:
    import lighter
    from lighter import WsClient
    LIGHTER_AVAILABLE = True
except ImportError:
    LIGHTER_AVAILABLE = False
    logging.warning("lighter SDK未安装")

from .lighter_base import LighterBase
from ..models import TickerData, OrderBookData, TradeData, OrderData, PositionData, OrderBookLevel

logger = logging.getLogger(__name__)


class LighterWebSocket(LighterBase):
    """Lighter WebSocket客户端"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化Lighter WebSocket客户端

        Args:
            config: 配置字典
        """
        if not LIGHTER_AVAILABLE:
            raise ImportError("lighter SDK未安装，无法使用Lighter WebSocket")

        super().__init__(config)

        # WebSocket客户端
        self.ws_client: Optional[WsClient] = None
        self._ws_task: Optional[asyncio.Task] = None
        # 保存事件循环引用
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None

        # 订阅的市场和账户
        self._subscribed_markets: List[int] = []
        self._subscribed_accounts: List[int] = []

        # 数据缓存
        self._order_books: Dict[str, OrderBookData] = {}
        self._account_data: Dict[str, Any] = {}

        # 回调函数
        self._ticker_callbacks: List[Callable] = []
        self._orderbook_callbacks: List[Callable] = []
        self._trade_callbacks: List[Callable] = []
        self._order_callbacks: List[Callable] = []
        self._order_fill_callbacks: List[Callable] = []  # 🔥 新增：订单成交回调
        self._position_callbacks: List[Callable] = []

        # 连接状态
        self._connected = False
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 10

        logger.info("Lighter WebSocket客户端初始化完成")

    # ============= 连接管理 =============

    async def connect(self):
        """建立WebSocket连接"""
        try:
            if self._connected:
                logger.warning("WebSocket已连接")
                return

            # 🔥 保存事件循环引用（用于线程安全的回调调度）
            self._event_loop = asyncio.get_event_loop()

            # 注意：lighter的WsClient是同步的，需要在单独的线程中运行
            # 这里我们先不启动，等待订阅后再启动
            self._connected = True
            logger.info("Lighter WebSocket准备就绪")

        except Exception as e:
            logger.error(f"WebSocket连接失败: {e}")
            raise

    async def disconnect(self):
        """断开WebSocket连接"""
        try:
            self._connected = False

            if self._ws_task and not self._ws_task.done():
                self._ws_task.cancel()
                try:
                    await self._ws_task
                except asyncio.CancelledError:
                    pass

            self.ws_client = None
            logger.info("WebSocket已断开")

        except Exception as e:
            logger.error(f"断开WebSocket时出错: {e}")

    async def reconnect(self):
        """重新连接WebSocket"""
        logger.info("尝试重新连接WebSocket...")

        await self.disconnect()
        await asyncio.sleep(min(self._reconnect_attempts * 2, 30))

        try:
            await self.connect()
            await self._resubscribe_all()
            self._reconnect_attempts = 0
            logger.info("WebSocket重连成功")
        except Exception as e:
            self._reconnect_attempts += 1
            logger.error(
                f"WebSocket重连失败 (尝试 {self._reconnect_attempts}/{self._max_reconnect_attempts}): {e}")

            if self._reconnect_attempts < self._max_reconnect_attempts:
                asyncio.create_task(self.reconnect())

    async def _resubscribe_all(self):
        """重新订阅所有频道"""
        # 重新订阅市场数据
        for market_index in self._subscribed_markets.copy():
            await self.subscribe_orderbook(market_index)

        # 重新订阅账户数据
        for account_index in self._subscribed_accounts.copy():
            await self.subscribe_account(account_index)

    # ============= 订阅管理 =============

    async def subscribe_ticker(self, symbol: str, callback: Optional[Callable] = None):
        """
        订阅ticker数据

        Args:
            symbol: 交易对符号
            callback: 数据回调函数
        """
        market_index = self.get_market_index(symbol)
        if market_index is None:
            logger.warning(f"未找到交易对 {symbol} 的市场索引")
            return

        if callback:
            self._ticker_callbacks.append(callback)

        await self.subscribe_orderbook(market_index, symbol)

    async def subscribe_orderbook(self, market_index_or_symbol, symbol: Optional[str] = None):
        """
        订阅订单簿

        Args:
            market_index_or_symbol: 市场索引或交易对符号
            symbol: 交易对符号（如果第一个参数是市场索引）
        """
        if isinstance(market_index_or_symbol, str):
            symbol = market_index_or_symbol
            market_index = self.get_market_index(symbol)
            if market_index is None:
                logger.warning(f"未找到交易对 {symbol} 的市场索引")
                return
        else:
            market_index = market_index_or_symbol
            if symbol is None:
                symbol = self._get_symbol_from_market_index(market_index)

        if market_index not in self._subscribed_markets:
            self._subscribed_markets.append(market_index)
            logger.info(f"已订阅订单簿: {symbol} (market_index={market_index})")

            # 如果WsClient已创建，需要重新创建以包含新的订阅
            await self._recreate_ws_client()

    async def subscribe_trades(self, symbol: str, callback: Optional[Callable] = None):
        """
        订阅成交数据

        Args:
            symbol: 交易对符号
            callback: 数据回调函数
        """
        if callback:
            self._trade_callbacks.append(callback)

        # Lighter的订单簿更新中包含成交信息
        await self.subscribe_orderbook(symbol)

    async def subscribe_account(self, account_index: Optional[int] = None):
        """
        订阅账户数据

        Args:
            account_index: 账户索引（默认使用配置中的账户）
        """
        if account_index is None:
            account_index = self.account_index

        if account_index not in self._subscribed_accounts:
            self._subscribed_accounts.append(account_index)
            logger.info(f"已订阅账户数据: account_index={account_index}")

            # 如果WsClient已创建，需要重新创建以包含新的订阅
            await self._recreate_ws_client()

    async def subscribe_orders(self, callback: Optional[Callable] = None):
        """
        订阅订单更新

        Args:
            callback: 数据回调函数
        """
        if callback:
            self._order_callbacks.append(callback)

        await self.subscribe_account()

    async def subscribe_order_fills(self, callback: Callable) -> None:
        """
        订阅订单成交（专门监控FILLED状态的订单）

        Args:
            callback: 订单成交回调函数，参数为OrderData
        """
        if callback:
            self._order_fill_callbacks.append(callback)

        await self.subscribe_account()

    async def subscribe_positions(self, callback: Optional[Callable] = None):
        """
        订阅持仓更新

        Args:
            callback: 数据回调函数
        """
        if callback:
            self._position_callbacks.append(callback)

        await self.subscribe_account()

    async def unsubscribe_ticker(self, symbol: str):
        """取消订阅ticker"""
        market_index = self.get_market_index(symbol)
        if market_index and market_index in self._subscribed_markets:
            self._subscribed_markets.remove(market_index)
            await self._recreate_ws_client()

    async def unsubscribe_orderbook(self, symbol: str):
        """取消订阅订单簿"""
        await self.unsubscribe_ticker(symbol)

    async def unsubscribe_trades(self, symbol: str):
        """取消订阅成交"""
        await self.unsubscribe_ticker(symbol)

    # ============= WebSocket客户端管理 =============

    async def _recreate_ws_client(self):
        """重新创建WebSocket客户端（当订阅变化时）"""
        if not self._subscribed_markets and not self._subscribed_accounts:
            logger.info("没有订阅，跳过创建WsClient")
            return

        try:
            # 先关闭旧的客户端
            if self._ws_task and not self._ws_task.done():
                self._ws_task.cancel()
                try:
                    await self._ws_task
                except asyncio.CancelledError:
                    pass

            # 创建新的WsClient
            # 🔥 从ws_url中提取host（去掉协议和路径）
            ws_host = self.ws_url.replace("wss://", "").replace("ws://", "")
            # 如果URL中包含路径，去掉路径（SDK会自动添加/stream）
            if "/" in ws_host:
                ws_host = ws_host.split("/")[0]

            self.ws_client = WsClient(
                host=ws_host,
                path="/stream",  # 明确指定path
                order_book_ids=self._subscribed_markets,
                account_ids=self._subscribed_accounts,
                on_order_book_update=self._on_order_book_update,
                on_account_update=self._on_account_update,
            )

            # 在单独的线程中运行（因为lighter的WsClient是同步的）
            self._ws_task = asyncio.create_task(self._run_ws_client())

            logger.info(
                f"✅ WebSocket已连接 - account: {self._subscribed_accounts[0] if self._subscribed_accounts else 'N/A'}")

        except Exception as e:
            logger.error(f"创建WebSocket客户端失败: {e}")

    async def _run_ws_client(self):
        """在异步任务中运行同步的WsClient"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.ws_client.run)
            logger.warning("⚠️ WebSocket客户端run()方法退出了")
        except asyncio.CancelledError:
            logger.info("WebSocket任务已取消")
        except Exception as e:
            logger.error(f"❌ WebSocket运行出错: {e}", exc_info=True)
            # 尝试重连
            asyncio.create_task(self.reconnect())

    # ============= 消息处理 =============

    def _on_order_book_update(self, market_id: str, order_book: Dict[str, Any]):
        """
        订单簿更新回调

        Args:
            market_id: 市场ID
            order_book: 订单簿数据
        """
        try:
            market_index = int(market_id)
            symbol = self._get_symbol_from_market_index(market_index)

            if not symbol:
                logger.warning(f"未找到market_index={market_index}对应的符号")
                return

            # 解析订单簿
            order_book_data = self._parse_order_book(symbol, order_book)

            # 缓存
            self._order_books[symbol] = order_book_data

            # 触发回调
            self._trigger_orderbook_callbacks(order_book_data)

            # 从订单簿中提取ticker数据
            if self._ticker_callbacks:
                ticker = self._extract_ticker_from_orderbook(
                    symbol, order_book, order_book_data)
                if ticker:
                    self._trigger_ticker_callbacks(ticker)

        except Exception as e:
            logger.error(f"处理订单簿更新失败: {e}")

    def _on_account_update(self, account_id: str, account: Dict[str, Any]):
        """
        账户数据更新回调

        Args:
            account_id: 账户ID
            account: 账户数据
        """
        try:
            # 缓存账户数据
            self._account_data[account_id] = account

            # 解析交易数据（Lighter使用'trades'而不是'orders'）
            if "trades" in account and account["trades"]:
                trades_data = account["trades"]

                # trades可能是字典{market_id: [trade_list]}或直接是数字（表示交易数量）
                if isinstance(trades_data, dict):
                    for market_id, trade_list in trades_data.items():
                        if isinstance(trade_list, list):
                            for trade_info in trade_list:
                                order = self._parse_trade_as_order(trade_info)
                                if order:
                                    logger.info(
                                        f"✅ 订单成交: id={order.id[:16] if order.id else 'N/A'}..., "
                                        f"价格: {order.average}, 数量: {order.filled}")

                                    # 触发订单成交回调
                                    if self._order_fill_callbacks:
                                        self._trigger_order_fill_callbacks(
                                            order)

            # 保留对'orders'的兼容（如果未来API改变）
            elif "orders" in account:
                orders = self._parse_orders(account["orders"])

                for order in orders:
                    # 触发通用订单回调
                    if self._order_callbacks:
                        self._trigger_order_callbacks(order)

                    # 识别订单成交并触发专门的成交回调
                    if self._order_fill_callbacks and order.status and "FILLED" in order.status.upper():
                        logger.info(
                            f"✅ 订单成交: {order.id[:16]}..., 价格: {order.average}, 数量: {order.filled}")
                        self._trigger_order_fill_callbacks(order)

            # 解析持仓更新
            if "positions" in account and self._position_callbacks:
                positions = self._parse_positions(account["positions"])
                for position in positions:
                    self._trigger_position_callbacks(position)

        except Exception as e:
            logger.error(f"❌ 处理账户更新失败: {e}", exc_info=True)

    # ============= 数据解析 =============

    def _parse_order_book(self, symbol: str, order_book: Dict[str, Any]) -> OrderBookData:
        """解析订单簿数据"""
        bids = []
        asks = []

        if "bids" in order_book:
            for bid in order_book["bids"]:
                if isinstance(bid, (list, tuple)) and len(bid) >= 2:
                    bids.append(OrderBookLevel(
                        price=self._safe_decimal(bid[0]),
                        size=self._safe_decimal(bid[1])
                    ))

        if "asks" in order_book:
            for ask in order_book["asks"]:
                if isinstance(ask, (list, tuple)) and len(ask) >= 2:
                    asks.append(OrderBookLevel(
                        price=self._safe_decimal(ask[0]),
                        size=self._safe_decimal(ask[1])
                    ))

        return OrderBookData(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=int(asyncio.get_event_loop().time() * 1000),
            exchange="lighter"
        )

    def _extract_ticker_from_orderbook(self, symbol: str, raw_data: Dict[str, Any], order_book: OrderBookData) -> Optional[TickerData]:
        """从订单簿中提取ticker数据"""
        try:
            best_bid = order_book.bids[0].price if order_book.bids else Decimal(
                "0")
            best_ask = order_book.asks[0].price if order_book.asks else Decimal(
                "0")

            # 最新价格取中间价
            last_price = (best_bid + best_ask) / \
                2 if best_bid > 0 and best_ask > 0 else best_bid or best_ask

            return TickerData(
                symbol=symbol,
                last_price=last_price,
                bid_price=best_bid,
                ask_price=best_ask,
                volume_24h=self._safe_decimal(raw_data.get("volume_24h", 0)),
                high_24h=self._safe_decimal(
                    raw_data.get("high_24h", last_price)),
                low_24h=self._safe_decimal(
                    raw_data.get("low_24h", last_price)),
                timestamp=int(asyncio.get_event_loop().time() * 1000),
                exchange="lighter"
            )
        except Exception as e:
            logger.error(f"提取ticker数据失败: {e}")
            return None

    def _parse_orders(self, orders_data: Dict[str, Any]) -> List[OrderData]:
        """解析订单列表"""
        orders = []
        for market_index_str, order_list in orders_data.items():
            try:
                market_index = int(market_index_str)
                symbol = self._get_symbol_from_market_index(market_index)

                for order_info in order_list:
                    orders.append(self._parse_order(order_info, symbol))
            except Exception as e:
                logger.error(f"解析订单失败: {e}")

        return orders

    def _parse_order(self, order_info: Dict[str, Any], symbol: str) -> OrderData:
        """解析单个订单"""
        # 🔥 计算成交均价：根据Lighter SDK数据结构
        filled_base = self._safe_decimal(
            order_info.get("filled_base_amount", 0))
        filled_quote = self._safe_decimal(
            order_info.get("filled_quote_amount", 0))

        # 计算平均成交价 = 成交金额 / 成交数量
        average_price = None
        if filled_base > 0 and filled_quote > 0:
            average_price = filled_quote / filled_base

        order_data = OrderData(
            order_id=str(order_info.get("order_index", "")),
            client_order_id=str(order_info.get("client_order_index", "")),
            symbol=symbol,
            side=self._parse_order_side(order_info.get("is_ask", False)),
            order_type=self._parse_order_type(order_info.get("type", 0)),
            quantity=self._safe_decimal(
                order_info.get("initial_base_amount", 0)),
            price=self._safe_decimal(order_info.get("price", 0)),
            filled_quantity=filled_base,
            status=self._parse_order_status(
                order_info.get("status", "unknown")),
            timestamp=self._parse_timestamp(order_info.get("timestamp")),
            exchange="lighter"
        )

        # 🔥 设置成交均价（如果有）
        if average_price:
            order_data.average = average_price

        return order_data

    def _parse_trade_as_order(self, trade_info: Dict[str, Any]) -> Optional[OrderData]:
        """
        将trade数据解析为OrderData（用于WebSocket订单成交通知）

        Lighter WebSocket中，交易成交数据在'trades'键中
        """
        try:
            # 🔥 获取市场ID（注意是market_id不是market_index）
            market_id = trade_info.get("market_id")
            if market_id is None:
                logger.warning(f"交易数据缺少market_id: {trade_info}")
                return None

            symbol = self._get_symbol_from_market_index(market_id)

            # 🔥 解析交易数据（使用Lighter WebSocket的实际字段名）
            # size: 交易数量（字符串）
            # price: 交易价格（字符串）
            # usd_amount: 交易金额（字符串）
            size_str = trade_info.get("size", "0")
            price_str = trade_info.get("price", "0")

            base_amount = self._safe_decimal(size_str)
            average_price = self._safe_decimal(price_str)

            # 🔥 构造OrderData（使用正确的字段名）
            from ..models import OrderSide, OrderType, OrderStatus

            order_data = OrderData(
                id=str(trade_info.get("tx_hash", "")),  # 使用tx_hash作为订单ID
                client_id="",
                symbol=symbol,
                side=OrderSide.SELL if trade_info.get(
                    "is_maker_ask", False) else OrderSide.BUY,
                type=OrderType.MARKET,  # 交易已成交，视为市价单
                amount=base_amount,
                price=average_price if average_price else Decimal("0"),
                filled=base_amount,  # 交易全部成交
                remaining=Decimal("0"),  # 已全部成交
                cost=self._safe_decimal(
                    trade_info.get("usd_amount", "0")),  # 成交金额
                average=average_price,  # 平均成交价
                status=OrderStatus.FILLED,  # 交易即成交
                timestamp=self._parse_timestamp(trade_info.get("timestamp")),
                updated=self._parse_timestamp(trade_info.get("timestamp")),
                fee=None,  # 手续费信息（可选）
                trades=[],  # 成交记录（空列表）
                params={},  # 额外参数（空字典）
                raw_data=trade_info  # 保存原始trade数据
            )

            return order_data

        except Exception as e:
            logger.error(f"解析交易数据失败: {e}", exc_info=True)
            return None

    def _parse_positions(self, positions_data: Dict[str, Any]) -> List[PositionData]:
        """解析持仓列表"""
        positions = []
        for market_index_str, position_info in positions_data.items():
            try:
                market_index = int(market_index_str)
                symbol = self._get_symbol_from_market_index(market_index)

                position_size = self._safe_decimal(
                    position_info.get("position", 0))
                if position_size == 0:
                    continue

                positions.append(PositionData(
                    symbol=symbol,
                    side="long" if position_size > 0 else "short",
                    size=abs(position_size),
                    entry_price=self._safe_decimal(
                        position_info.get("avg_entry_price", 0)),
                    unrealized_pnl=self._safe_decimal(
                        position_info.get("unrealized_pnl", 0)),
                    realized_pnl=self._safe_decimal(
                        position_info.get("realized_pnl", 0)),
                    liquidation_price=self._safe_decimal(
                        position_info.get("liquidation_price")),
                    leverage=Decimal("1"),
                    exchange="lighter"
                ))
            except Exception as e:
                logger.error(f"解析持仓失败: {e}")

        return positions

    def _get_symbol_from_market_index(self, market_index: int) -> str:
        """从市场索引获取符号"""
        market_info = self._markets_cache.get(market_index)
        if market_info:
            return market_info.get("symbol", "")
        return f"MARKET_{market_index}"

    # ============= 回调触发 =============

    def _trigger_ticker_callbacks(self, ticker: TickerData):
        """触发ticker回调"""
        for callback in self._ticker_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(ticker))
                else:
                    callback(ticker)
            except Exception as e:
                logger.error(f"ticker回调执行失败: {e}")

    def _trigger_orderbook_callbacks(self, orderbook: OrderBookData):
        """触发订单簿回调"""
        for callback in self._orderbook_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(orderbook))
                else:
                    callback(orderbook)
            except Exception as e:
                logger.error(f"订单簿回调执行失败: {e}")

    def _trigger_trade_callbacks(self, trade: TradeData):
        """触发成交回调"""
        for callback in self._trade_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(trade))
                else:
                    callback(trade)
            except Exception as e:
                logger.error(f"成交回调执行失败: {e}")

    def _trigger_order_callbacks(self, order: OrderData):
        """触发订单回调"""
        for callback in self._order_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(order))
                else:
                    callback(order)
            except Exception as e:
                logger.error(f"订单回调执行失败: {e}")

    def _trigger_order_fill_callbacks(self, order: OrderData):
        """触发订单成交回调（线程安全）"""
        for callback in self._order_fill_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # 🔥 WebSocket在同步线程中运行，需要线程安全地调度协程
                    if self._event_loop and self._event_loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            callback(order), self._event_loop)
                    else:
                        logger.warning("⚠️ 事件循环未运行，无法调度异步回调")
                else:
                    callback(order)
            except Exception as e:
                logger.error(f"订单成交回调执行失败: {e}")

    def _trigger_position_callbacks(self, position: PositionData):
        """触发持仓回调"""
        for callback in self._position_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(position))
                else:
                    callback(position)
            except Exception as e:
                logger.error(f"持仓回调执行失败: {e}")

    def get_cached_orderbook(self, symbol: str) -> Optional[OrderBookData]:
        """获取缓存的订单簿"""
        return self._order_books.get(symbol)
