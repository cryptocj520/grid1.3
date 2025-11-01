"""
Lighter交易所适配器 - WebSocket模块

封装Lighter SDK的WebSocket功能，提供实时数据流
"""

from typing import Dict, Any, Optional, List, Callable
from decimal import Decimal
from datetime import datetime
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

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    logging.warning("websockets库未安装，无法使用直接订阅功能")

from .lighter_base import LighterBase
from ..models import (
    TickerData, OrderBookData, TradeData, OrderData, PositionData,
    OrderBookLevel, OrderStatus, OrderSide, OrderType
)

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

        # WebSocket客户端（SDK的WsClient，用于订阅account_all）
        self.ws_client: Optional[WsClient] = None
        self._ws_task: Optional[asyncio.Task] = None

        # 直接WebSocket连接（用于订阅account_all_orders）
        self._direct_ws = None
        self._direct_ws_task: Optional[asyncio.Task] = None

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

        # 🔥 确保logger有文件handler，写入ExchangeAdapter.log
        self._setup_logger()

        logger.info("Lighter WebSocket客户端初始化完成")

    def _setup_logger(self):
        """设置logger的文件handler"""
        from logging.handlers import RotatingFileHandler
        from pathlib import Path

        # 确保logs目录存在
        Path("logs").mkdir(parents=True, exist_ok=True)

        # 检查是否已有文件handler
        has_file_handler = any(
            isinstance(h, RotatingFileHandler) and 'ExchangeAdapter.log' in str(
                h.baseFilename)
            for h in logger.handlers
        )

        if not has_file_handler:
            # 添加文件handler
            file_handler = RotatingFileHandler(
                'logs/ExchangeAdapter.log',
                maxBytes=10*1024*1024,  # 10MB
                backupCount=3,
                encoding='utf-8'
            )
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.setLevel(logging.INFO)  # 确保logger级别至少是INFO
            logger.info("✅ ExchangeAdapter.log 文件handler已配置")

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

            # 关闭SDK的WebSocket任务
            if self._ws_task and not self._ws_task.done():
                self._ws_task.cancel()
                try:
                    await self._ws_task
                except asyncio.CancelledError:
                    pass

            self.ws_client = None

            # 关闭直接WebSocket连接
            if self._direct_ws_task and not self._direct_ws_task.done():
                self._direct_ws_task.cancel()
                try:
                    await self._direct_ws_task
                except asyncio.CancelledError:
                    pass

            if self._direct_ws:
                try:
                    await self._direct_ws.close()
                except:
                    pass
                self._direct_ws = None

            logger.info("✅ WebSocket已断开（包括直接订阅）")

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

        使用直接WebSocket连接订阅account_all_orders频道
        这样可以接收挂单状态推送，而不仅仅是成交推送

        Args:
            callback: 数据回调函数
        """
        if callback:
            self._order_callbacks.append(callback)

        # 🔥 启动直接订阅account_all_orders（包含挂单状态）
        await self._subscribe_account_all_orders()

        # 🔥 同时保持account_all订阅（用于成交推送）
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
            if not self.ws_url:
                logger.error("❌ WebSocket URL未配置，无法创建WebSocket客户端")
                return

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

        根据Lighter WebSocket文档，account数据包含:
        - orders: {market_index: [Order]} - 订单列表
        - trades: {market_index: [Trade]} - 成交列表
        - positions: {market_index: Position} - 持仓数据

        Args:
            account_id: 账户ID
            account: 账户数据
        """
        try:
            # 缓存账户数据
            self._account_data[account_id] = account

            logger.debug(
                f"📥 收到账户更新: account_id={account_id}, keys={list(account.keys())}")

            # 🔥 解析订单数据（根据Lighter WebSocket文档）
            if "orders" in account and account["orders"]:
                orders_data = account["orders"]

                # orders是字典: {market_index: [Order]}
                if isinstance(orders_data, dict):
                    for market_index, order_list in orders_data.items():
                        if isinstance(order_list, list):
                            for order_info in order_list:
                                logger.debug(f"🔍 订单完整数据: {order_info}")

                                order = self._parse_order_from_ws(order_info)
                                if order:
                                    logger.debug(
                                        f"📝 订单更新: id={order.id}, "
                                        f"状态={order.status}, 价格={order.price}, "
                                        f"数量={order.amount}, 已成交={order.filled}")

                                    # 触发通用订单回调
                                    if self._order_callbacks:
                                        self._trigger_order_callbacks(order)

                                    # 如果是FILLED状态，触发订单成交回调
                                    if self._order_fill_callbacks and order.status == OrderStatus.FILLED:
                                        logger.info(
                                            f"✅ 订单成交: id={order.id}, "
                                            f"成交价={order.average}, 成交量={order.filled}")
                                        self._trigger_order_fill_callbacks(
                                            order)

            # 🔥 解析成交数据（Trade列表，Lighter通过这个推送订单成交）
            if "trades" in account and account["trades"]:
                trades_data = account["trades"]
                if isinstance(trades_data, dict):
                    for market_index, trade_list in trades_data.items():
                        if isinstance(trade_list, list):
                            logger.info(
                                f"📊 市场{market_index}收到{len(trade_list)}个成交记录")

                            # 🔥 遍历每个trade，解析为OrderData并触发回调
                            for trade_info in trade_list:
                                logger.info(
                                    f"🔍 收到成交数据keys: {list(trade_info.keys())}")
                                logger.debug(f"🔍 成交完整数据: {trade_info}")

                                # 解析trade为OrderData
                                order = self._parse_trade_as_order(trade_info)
                                if order:
                                    logger.info(
                                        f"💰 订单成交: id={order.id}, "
                                        f"价格={order.average}, 数量={order.filled}, "
                                        f"方向={order.side.value}")

                                    # 触发订单回调
                                    if self._order_callbacks:
                                        self._trigger_order_callbacks(order)

                                    # 触发订单成交回调
                                    if self._order_fill_callbacks:
                                        self._trigger_order_fill_callbacks(
                                            order)

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
                # Lighter WebSocket返回字典格式：{'price': '...', 'size': '...'}
                if isinstance(bid, dict):
                    bids.append(OrderBookLevel(
                        price=self._safe_decimal(bid.get('price', 0)),
                        size=self._safe_decimal(bid.get('size', 0))
                    ))
                # 兼容列表/元组格式：['price', 'size']
                elif isinstance(bid, (list, tuple)) and len(bid) >= 2:
                    bids.append(OrderBookLevel(
                        price=self._safe_decimal(bid[0]),
                        size=self._safe_decimal(bid[1])
                    ))

        if "asks" in order_book:
            for ask in order_book["asks"]:
                # Lighter WebSocket返回字典格式：{'price': '...', 'size': '...'}
                if isinstance(ask, dict):
                    asks.append(OrderBookLevel(
                        price=self._safe_decimal(ask.get('price', 0)),
                        size=self._safe_decimal(ask.get('size', 0))
                    ))
                # 兼容列表/元组格式：['price', 'size']
                elif isinstance(ask, (list, tuple)) and len(ask) >= 2:
                    asks.append(OrderBookLevel(
                        price=self._safe_decimal(ask[0]),
                        size=self._safe_decimal(ask[1])
                    ))

        return OrderBookData(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=datetime.now(),
            nonce=None
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
                timestamp=datetime.now(),
                bid=best_bid,
                ask=best_ask,
                last=last_price,
                volume=self._safe_decimal(raw_data.get("volume_24h", 0)),
                high=self._safe_decimal(
                    raw_data.get("high_24h", last_price)),
                low=self._safe_decimal(
                    raw_data.get("low_24h", last_price))
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

    def _parse_order_from_ws(self, order_info: Dict[str, Any]) -> Optional[OrderData]:
        """
        解析WebSocket推送的Order JSON

        ⚠️ 根据Lighter官方Go结构文档，实际字段名是缩写形式：
        - "i":  OrderIndex (int64) - 订单ID
        - "u":  ClientOrderIndex (int64) - 客户端订单ID  
        - "is": InitialBaseAmount (int64) - 初始数量（单位1e5）
        - "rs": RemainingBaseAmount (int64) - 剩余数量（单位1e5）
        - "p":  Price (uint32) - 价格（需要除以price_multiplier）
        - "ia": IsAsk (uint8) - 是否卖单 (0=buy, 1=sell)
        - "st": Status (uint8) - 状态码 (0=Failed, 1=Pending, 2=Executed, 3=Pending-Final)
        """
        try:
            from ..models import OrderSide, OrderType, OrderStatus

            # 🔥 使用实际的缩写字段名
            order_index = order_info.get("i")  # OrderIndex
            client_order_index = order_info.get("u")  # ClientOrderIndex

            if order_index is None:
                logger.warning(
                    f"⚠️ 订单数据缺少OrderIndex(i): keys={list(order_info.keys())}")
                return None

            order_id = str(order_index)

            # 获取市场索引和符号（假设字段名是"m"）
            # TODO: 需要确认market_index的实际字段名
            market_index = order_info.get("m")
            symbol = self._get_symbol_from_market_index(
                market_index) if market_index else "UNKNOWN"

            # 🔥 解析数量（使用缩写字段，数量单位是1e5）
            initial_amount_raw = order_info.get("is", 0)  # InitialBaseAmount
            remaining_amount_raw = order_info.get(
                "rs", 0)  # RemainingBaseAmount

            initial_amount = self._safe_decimal(
                initial_amount_raw) / Decimal("100000")
            remaining_amount = self._safe_decimal(
                remaining_amount_raw) / Decimal("100000")
            filled_amount = initial_amount - remaining_amount

            # 暂时无法从Order结构直接获取filled_quote，设置为0
            filled_quote = Decimal("0")

            # 计算成交均价（如果有成交且有价格）
            average_price = None
            price_raw = order_info.get("p", 0)  # Price (uint32)
            # 根据市场的price_multiplier调整，这里假设是10
            price = self._safe_decimal(price_raw) / Decimal("10")
            if filled_amount > 0 and price > 0:
                average_price = price  # 近似使用订单价格

            # 🔥 解析订单方向（使用缩写字段）
            is_ask = order_info.get("ia", 0)  # IsAsk (uint8: 0=buy, 1=sell)
            side = OrderSide.SELL if is_ask else OrderSide.BUY

            # 🔥 解析订单状态（使用缩写字段，状态是数字）
            status_code = order_info.get("st", 1)  # Status (uint8)
            if status_code == 2:  # Executed
                status = OrderStatus.FILLED
            elif status_code == 0:  # Failed
                status = OrderStatus.CANCELED
            elif status_code == 1 or status_code == 3:  # Pending / Pending-Final
                status = OrderStatus.OPEN
            else:
                status = OrderStatus.PENDING

            # 构造OrderData
            return OrderData(
                id=order_id,                                    # ✅ OrderIndex的字符串形式
                client_id=str(
                    client_order_index) if client_order_index else "",
                symbol=symbol,
                side=side,
                type=OrderType.LIMIT,
                amount=initial_amount,
                filled=filled_amount,
                remaining=remaining_amount,
                price=price,
                average=average_price,
                cost=filled_quote,
                status=status,
                timestamp=datetime.now(),
                updated=None,
                fee=None,
                trades=[],
                params={},
                raw_data=order_info
            )

        except Exception as e:
            logger.error(f"解析WebSocket订单失败: {e}", exc_info=True)
            return None

    def _parse_order(self, order_info: Dict[str, Any], symbol: str) -> OrderData:
        """解析单个订单（兼容旧版本）"""
        # 🔥 使用新的解析方法
        result = self._parse_order_from_ws(order_info)
        if result:
            return result

        # 降级处理
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

        Trade JSON格式（根据文档）:
        {
            "trade_id": INTEGER,
            "tx_hash": STRING,
            "market_id": INTEGER,
            "size": STRING,
            "price": STRING,
            "ask_id": INTEGER,        # 卖单订单ID
            "bid_id": INTEGER,        # 买单订单ID
            "ask_account_id": INTEGER, # 卖方账户ID
            "bid_account_id": INTEGER, # 买方账户ID
            "is_maker_ask": BOOLEAN   # maker是卖方(true)还是买方(false)
        }
        """
        try:
            # 🔥 获取市场ID
            market_id = trade_info.get("market_id")
            if market_id is None:
                logger.warning(f"交易数据缺少market_id: {trade_info}")
                return None

            symbol = self._get_symbol_from_market_index(market_id)

            # 🔥 判断当前账户是买方还是卖方
            ask_account_id = trade_info.get("ask_account_id")
            bid_account_id = trade_info.get("bid_account_id")

            # 根据账户ID判断是买还是卖
            is_sell = (ask_account_id == self.account_index)
            is_buy = (bid_account_id == self.account_index)

            if not (is_sell or is_buy):
                # 这个trade不属于当前账户
                return None

            # 🔥 获取正确的订单ID（ask_id或bid_id）
            order_id = trade_info.get(
                "ask_id") if is_sell else trade_info.get("bid_id")
            if order_id is None:
                logger.warning(f"交易数据缺少订单ID: {trade_info}")
                return None

            # 解析交易数量和价格
            size_str = trade_info.get("size", "0")
            price_str = trade_info.get("price", "0")
            usd_amount_str = trade_info.get("usd_amount", "0")

            base_amount = self._safe_decimal(size_str)
            trade_price = self._safe_decimal(price_str)
            usd_amount = self._safe_decimal(usd_amount_str)

            # 🔥 构造OrderData
            from ..models import OrderSide, OrderType, OrderStatus

            order_data = OrderData(
                id=str(order_id),  # ✅ 使用ask_id或bid_id作为订单ID
                client_id="",
                symbol=symbol,
                side=OrderSide.SELL if is_sell else OrderSide.BUY,
                type=OrderType.LIMIT,  # Trade可能来自限价单
                amount=base_amount,
                price=trade_price,
                filled=base_amount,  # 交易全部成交
                remaining=Decimal("0"),  # 已全部成交
                cost=usd_amount,  # 成交金额
                average=trade_price,  # 成交价
                status=OrderStatus.FILLED,  # 已成交
                timestamp=self._parse_timestamp(trade_info.get("timestamp")),
                updated=self._parse_timestamp(trade_info.get("timestamp")),
                fee=None,
                trades=[],
                params={},
                raw_data=trade_info
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
        """触发ticker回调（线程安全）"""
        for callback in self._ticker_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # 🔥 WebSocket在同步线程中运行，需要线程安全地调度协程
                    if self._event_loop and self._event_loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            callback(ticker), self._event_loop)
                    else:
                        logger.debug("⚠️ 事件循环未运行，跳过ticker回调")
                else:
                    callback(ticker)
            except Exception as e:
                logger.error(f"ticker回调执行失败: {e}", exc_info=True)

    def _trigger_orderbook_callbacks(self, orderbook: OrderBookData):
        """触发订单簿回调（线程安全）"""
        for callback in self._orderbook_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # 🔥 WebSocket在同步线程中运行，需要线程安全地调度协程
                    if self._event_loop and self._event_loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            callback(orderbook), self._event_loop)
                    else:
                        logger.debug("⚠️ 事件循环未运行，跳过订单簿回调")
                else:
                    callback(orderbook)
            except Exception as e:
                logger.error(f"订单簿回调执行失败: {e}")

    def _trigger_trade_callbacks(self, trade: TradeData):
        """触发成交回调（线程安全）"""
        for callback in self._trade_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # 🔥 WebSocket在同步线程中运行，需要线程安全地调度协程
                    if self._event_loop and self._event_loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            callback(trade), self._event_loop)
                    else:
                        logger.debug("⚠️ 事件循环未运行，跳过成交回调")
                else:
                    callback(trade)
            except Exception as e:
                logger.error(f"成交回调执行失败: {e}")

    def _trigger_order_callbacks(self, order: OrderData):
        """触发订单回调（线程安全）"""
        for callback in self._order_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # 🔥 WebSocket在同步线程中运行，需要线程安全地调度协程
                    if self._event_loop and self._event_loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            callback(order), self._event_loop)
                    else:
                        logger.debug("⚠️ 事件循环未运行，跳过订单回调")
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
        """触发持仓回调（线程安全）"""
        for callback in self._position_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # 🔥 WebSocket在同步线程中运行，需要线程安全地调度协程
                    if self._event_loop and self._event_loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            callback(position), self._event_loop)
                    else:
                        logger.debug("⚠️ 事件循环未运行，跳过持仓回调")
                else:
                    callback(position)
            except Exception as e:
                logger.error(f"持仓回调执行失败: {e}")

    # ============= 直接订阅account_all_orders =============

    async def _subscribe_account_all_orders(self):
        """
        直接订阅account_all_orders频道

        根据Lighter WebSocket文档，订阅account_all_orders需要：
        1. 建立WebSocket连接到 wss://mainnet.zklighter.elliot.ai/stream
        2. 发送订阅消息，包含auth token
        3. 接收订单推送（包括挂单状态）
        """
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("⚠️ websockets库未安装，无法直接订阅订单")
            return

        if self._direct_ws_task and not self._direct_ws_task.done():
            logger.info("⚠️ 直接订阅任务已在运行")
            return

        # 启动直接订阅任务
        self._direct_ws_task = asyncio.create_task(
            self._run_direct_ws_subscription())
        logger.info("🚀 已启动直接订阅account_all_orders任务")

    async def _run_direct_ws_subscription(self):
        """运行直接WebSocket订阅"""
        try:
            # 检查SignerClient是否可用
            if not self.signer_client:
                logger.error("❌ SignerClient未初始化，无法订阅订单")
                return

            # 🔥 生成auth token（有效期1小时）
            # create_auth_token_with_expiry需要过期时间戳（秒级），不是相对秒数
            import time
            expiry_timestamp = int(time.time()) + 3600  # 当前时间+1小时

            result = self.signer_client.create_auth_token_with_expiry(
                expiry_timestamp)
            # 返回的是元组 (token, None)
            auth_token = result[0] if isinstance(result, tuple) else result

            logger.info(f"✅ 生成认证token (过期时间: {expiry_timestamp})")

            # 连接WebSocket
            ws_url = self.ws_url
            logger.info(f"🔗 连接WebSocket: {ws_url}")

            # 🔥 配置WebSocket参数：设置合理的ping/pong间隔和超时
            async with websockets.connect(
                ws_url,
                ping_interval=30,      # 每30秒发送一次ping（减少网络开销）
                ping_timeout=30,       # 等待pong响应的超时时间为30秒
                close_timeout=10       # 关闭连接的超时时间为10秒
            ) as ws:
                self._direct_ws = ws

                # 发送订阅消息
                subscribe_msg = {
                    "type": "subscribe",
                    "channel": f"account_all_orders/{self.account_index}",
                    "auth": auth_token
                }
                await ws.send(json.dumps(subscribe_msg))
                logger.info(
                    f"✅ 已订阅频道: account_all_orders/{self.account_index}")

                # 持续接收消息
                async for message in ws:
                    try:
                        data = json.loads(message)
                        await self._handle_direct_ws_message(data)
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ JSON解析失败: {e}")
                    except Exception as e:
                        logger.error(f"❌ 处理消息失败: {e}", exc_info=True)

        except websockets.exceptions.ConnectionClosedError as e:
            # WebSocket连接关闭（如心跳超时）
            logger.warning(f"⚠️ WebSocket连接已关闭: {e}，5秒后重连...")
            await asyncio.sleep(5)
            # 自动重连
            self._direct_ws_task = asyncio.create_task(
                self._run_direct_ws_subscription())
        except Exception as e:
            logger.error(f"❌ 直接WebSocket订阅失败: {e}", exc_info=True)
            # 10秒后重连（异常情况）
            await asyncio.sleep(10)
            if not self._direct_ws_task or self._direct_ws_task.done():
                self._direct_ws_task = asyncio.create_task(
                    self._run_direct_ws_subscription())

    async def _handle_direct_ws_message(self, data: Dict[str, Any]):
        """
        处理直接WebSocket消息

        根据文档，account_all_orders返回：
        {
            "channel": "account_all_orders:{ACCOUNT_ID}",
            "orders": {
                "{MARKET_INDEX}": [Order]
            },
            "type": "update/account_all_orders"
        }
        """
        try:
            msg_type = data.get("type", "")
            channel = data.get("channel", "")

            logger.debug(
                f"📥 收到直接WebSocket推送: channel={channel}, type={msg_type}")

            # 处理订单更新
            if msg_type == "update/account_all_orders" and "orders" in data:
                orders_data = data["orders"]

                if isinstance(orders_data, dict):
                    for market_index, order_list in orders_data.items():
                        if isinstance(order_list, list):
                            for order_info in order_list:
                                logger.debug(f"🔍 订单完整数据: {order_info}")

                                # 解析订单（使用完整的Order JSON格式）
                                order = self._parse_order_from_direct_ws(
                                    order_info)
                                if order:
                                    logger.debug(
                                        f"📝 订单推送: id={order.id}, "
                                        f"状态={order.status.value}, 价格={order.price}, "
                                        f"数量={order.amount}, 已成交={order.filled}")

                                    # 触发订单回调
                                    if self._order_callbacks:
                                        for callback in self._order_callbacks:
                                            if asyncio.iscoroutinefunction(callback):
                                                await callback(order)
                                            else:
                                                callback(order)

                                    # 如果是成交状态，触发成交回调
                                    if order.status == OrderStatus.FILLED and self._order_fill_callbacks:
                                        for callback in self._order_fill_callbacks:
                                            if asyncio.iscoroutinefunction(callback):
                                                await callback(order)
                                            else:
                                                callback(order)

        except Exception as e:
            logger.error(f"❌ 处理直接WebSocket消息失败: {e}", exc_info=True)

    def _parse_order_from_direct_ws(self, order_info: Dict[str, Any]) -> Optional[OrderData]:
        """
        解析来自account_all_orders的订单数据

        根据文档，Order JSON格式：
        {
            "order_index": INTEGER,
            "client_order_index": INTEGER,
            "market_index": INTEGER,
            "initial_base_amount": STRING,
            "price": STRING,
            "remaining_base_amount": STRING,
            "filled_base_amount": STRING,
            "filled_quote_amount": STRING,
            "is_ask": BOOL,
            "status": STRING,  # "open", "filled", "canceled"
            ...
        }
        """
        try:
            # 获取市场符号
            market_index = order_info.get("market_index")
            if market_index is None:
                return None

            symbol = self._get_symbol_from_market_index(market_index)

            # 订单ID
            order_index = order_info.get("order_index")
            order_id = str(order_index) if order_index is not None else ""

            # 数量和价格
            initial_amount = self._safe_decimal(
                order_info.get("initial_base_amount", "0"))
            remaining_amount = self._safe_decimal(
                order_info.get("remaining_base_amount", "0"))
            filled_amount = self._safe_decimal(
                order_info.get("filled_base_amount", "0"))
            price = self._safe_decimal(order_info.get("price", "0"))

            # 成交金额和均价
            filled_quote = self._safe_decimal(
                order_info.get("filled_quote_amount", "0"))
            average_price = filled_quote / filled_amount if filled_amount > 0 else None

            # 方向
            is_ask = order_info.get("is_ask", False)
            side = OrderSide.SELL if is_ask else OrderSide.BUY

            # 状态
            status_str = order_info.get("status", "unknown").lower()
            if status_str == "filled":
                status = OrderStatus.FILLED
            elif status_str == "canceled" or status_str == "cancelled":
                status = OrderStatus.CANCELED
            elif status_str == "open":
                status = OrderStatus.OPEN
            else:
                status = OrderStatus.OPEN  # 默认为OPEN

            # 创建OrderData
            return OrderData(
                id=order_id,
                client_id=str(order_info.get("client_order_index", "")),
                symbol=symbol,
                side=side,
                type=OrderType.LIMIT,
                amount=initial_amount,
                price=price,
                filled=filled_amount,
                remaining=remaining_amount,
                cost=filled_quote,
                average=average_price,
                status=status,
                timestamp=self._parse_timestamp(order_info.get("timestamp")),
                updated=None,
                fee=None,
                trades=[],
                params={},
                raw_data=order_info
            )

        except Exception as e:
            logger.error(f"解析订单失败: {e}", exc_info=True)
            return None

    def get_cached_orderbook(self, symbol: str) -> Optional[OrderBookData]:
        """获取缓存的订单簿"""
        return self._order_books.get(symbol)
