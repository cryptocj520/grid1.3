"""
Lighter交易所适配器 - REST API模块

封装Lighter SDK的REST API功能，提供市场数据、账户信息和交易功能

⚠️  Lighter交易所特殊说明：

1. Market ID从0开始（不是1）：
   - market_id=0: ETH
   - market_id=1: BTC
   - market_id=2: SOL
   
2. 动态价格精度（关键特性）：
   - 不同交易对使用不同的价格精度
   - 价格乘数公式: price_int = price_usd × (10 ** price_decimals)
   - 数量使用1e5: base_amount = quantity × 100000 (实际测试确认)
   
   示例：
   - ETH (2位小数): $4127.39 × 100 = 412739
   - BTC (1位小数): $114357.8 × 10 = 1143578
   - SOL (3位小数): $199.058 × 1000 = 199058
   - DOGE (6位小数): $0.202095 × 1000000 = 202095
   
3. 必须使用order_books() API：
   - 获取完整市场列表必须用order_books()
   - order_book_details(market_id) 只返回活跃市场
   - 不能通过循环遍历market_id来发现市场
   
这些设计是Lighter作为Layer 2 DEX的优化选择，与传统CEX不同！
"""

from typing import Dict, Any, Optional, List
from decimal import Decimal
from datetime import datetime
import asyncio
import logging

try:
    import lighter
    from lighter import Configuration, ApiClient, SignerClient
    from lighter.api import AccountApi, OrderApi, TransactionApi, CandlestickApi, FundingApi
    LIGHTER_AVAILABLE = True
except ImportError:
    LIGHTER_AVAILABLE = False
    logging.warning(
        "lighter SDK未安装。请执行: pip install git+https://github.com/elliottech/lighter-python.git")

from .lighter_base import LighterBase
from ..models import (
    TickerData, OrderBookData, TradeData, BalanceData,
    OrderData, PositionData, ExchangeInfo, OrderBookLevel, OrderSide, OrderType, OrderStatus
)

# 配置 logger 输出到文件
logger = logging.getLogger(__name__)
if not logger.handlers:
    import os
    from logging.handlers import RotatingFileHandler
    from pathlib import Path

    # 确保日志目录存在
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    # 添加文件处理器
    log_file = log_dir / "ExchangeAdapter.log"
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5*1024*1024,  # 5MB
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.WARNING)  # 只记录 WARNING 及以上级别
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.WARNING)


class LighterRest(LighterBase):
    """Lighter REST API封装类"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化Lighter REST客户端

        Args:
            config: 配置字典
        """
        if not LIGHTER_AVAILABLE:
            raise ImportError("lighter SDK未安装，无法使用Lighter适配器")

        super().__init__(config)

        # 初始化客户端
        self.api_client: Optional[ApiClient] = None
        self.signer_client: Optional[SignerClient] = None

        # API实例
        self.account_api: Optional[AccountApi] = None
        self.order_api: Optional[OrderApi] = None
        self.transaction_api: Optional[TransactionApi] = None
        self.candlestick_api: Optional[CandlestickApi] = None
        self.funding_api: Optional[FundingApi] = None

        # 连接状态
        self._connected = False

        # 🔥 初始化markets字典（用于WebSocket共享）
        self.markets = {}

        # 🔥 市场信息缓存（避免频繁调用API触发429限流）
        # 这是关键修复！没有这个缓存会导致批量下单时触发429
        self._market_info_cache = {}  # {symbol: {info, timestamp}}

        # 🔥 初始化WebSocket模块（用于订单成交监控）
        try:
            from .lighter_websocket import LighterWebSocket
            self._websocket = LighterWebSocket(config)
            logger.info("✅ Lighter WebSocket模块已初始化")
        except Exception as e:
            logger.warning(f"⚠️ Lighter WebSocket初始化失败: {e}")
            self._websocket = None

        logger.info("Lighter REST客户端初始化完成")

    def is_connected(self) -> bool:
        """检查是否已连接"""
        return self._connected

    async def connect(self):
        """连接（调用initialize）"""
        await self.initialize()

    async def initialize(self):
        """初始化API客户端"""
        try:
            # 创建API客户端
            configuration = Configuration(host=self.base_url)
            self.api_client = ApiClient(configuration=configuration)

            # 创建各种API实例
            self.account_api = AccountApi(self.api_client)
            self.order_api = OrderApi(self.api_client)
            self.transaction_api = TransactionApi(self.api_client)
            self.candlestick_api = CandlestickApi(self.api_client)
            self.funding_api = FundingApi(self.api_client)

            # 如果配置了私钥，创建签名客户端
            if self.api_key_private_key:
                self.signer_client = SignerClient(
                    url=self.base_url,
                    private_key=self.api_key_private_key,
                    account_index=self.account_index,
                    api_key_index=self.api_key_index,
                )

                # 检查客户端
                err = self.signer_client.check_client()
                if err is not None:
                    error_msg = self.parse_error(err)
                    logger.error(f"SignerClient检查失败: {error_msg}")
                    raise Exception(f"SignerClient初始化失败: {error_msg}")

            self._connected = True
            logger.info("Lighter REST客户端连接成功")

            # 加载市场信息
            await self._load_markets()

            # 🔥 连接WebSocket（如果已初始化）
            if self._websocket:
                try:
                    logger.info(
                        f"🔗 准备连接WebSocket - markets数量: {len(self.markets)}, _markets_cache数量: {len(self._markets_cache)}")

                    # 共享markets信息给WebSocket
                    self._websocket.markets = self.markets
                    self._websocket._markets_cache = getattr(
                        self, '_markets_cache', {})

                    await self._websocket.connect()
                    logger.info("✅ Lighter WebSocket已连接")
                except Exception as ws_err:
                    logger.warning(
                        f"⚠️ Lighter WebSocket连接失败: {ws_err}，将使用fallback方案")
                    import traceback
                    logger.debug(f"WebSocket错误详情:\n{traceback.format_exc()}")
            else:
                logger.warning("⚠️ WebSocket模块未初始化")

        except Exception as e:
            logger.error(f"Lighter REST客户端初始化失败: {e}")
            raise

    async def close(self):
        """关闭连接"""
        try:
            # 🔥 断开WebSocket
            if self._websocket:
                try:
                    await self._websocket.disconnect()
                except Exception as ws_err:
                    logger.warning(f"断开WebSocket时出错: {ws_err}")

            if self.signer_client:
                await self.signer_client.close()
            if self.api_client:
                await self.api_client.close()
            self._connected = False
            logger.info("Lighter REST客户端已关闭")
        except Exception as e:
            logger.error(f"关闭Lighter REST客户端时出错: {e}")

    async def disconnect(self):
        """断开连接（调用close）"""
        await self.close()

    # ============= 市场数据 =============

    async def _load_markets(self):
        """
        加载市场信息

        ⚠️  重要说明：
        - Lighter的market_id从0开始，不是从1开始
        - ETH的market_id是0（这是最重要的市场）
        - 必须使用order_books() API获取完整列表，不能用循环遍历market_id
        - order_book_details(market_id) 只返回有交易的市场，可能漏掉不活跃的市场

        市场ID示例：
        - market_id=0: ETH (价格精度: 2位小数, 乘数: 100)
        - market_id=1: BTC (价格精度: 1位小数, 乘数: 10)
        - market_id=2: SOL (价格精度: 3位小数, 乘数: 1000)
        """
        try:
            # 获取订单簿列表（包含市场信息）
            # ⚠️ 必须使用此API，它会返回所有市场包括market_id=0的ETH
            response = await self.order_api.order_books()

            if hasattr(response, 'order_books'):
                markets = []
                for order_book_info in response.order_books:
                    if hasattr(order_book_info, 'symbol') and hasattr(order_book_info, 'market_id'):
                        market_info = {
                            "market_id": order_book_info.market_id,  # 使用API返回的真实 market_id
                            "symbol": order_book_info.symbol,
                        }
                        markets.append(market_info)

                        # 🔥 同时填充 self.markets 字典（用于WebSocket）
                        self.markets[order_book_info.symbol] = market_info

                self.update_markets_cache(markets)
                logger.info(f"加载了 {len(markets)} 个市场")

        except Exception as e:
            logger.error(f"加载市场信息失败: {e}")

    async def get_exchange_info(self) -> ExchangeInfo:
        """
        获取交易所信息

        Returns:
            ExchangeInfo对象
        """
        try:
            # 获取订单簿信息
            response = await self.order_api.order_books()

            symbols = []
            if hasattr(response, 'order_books'):
                for ob in response.order_books:
                    if hasattr(ob, 'symbol') and hasattr(ob, 'market_id'):
                        symbols.append({
                            "symbol": ob.symbol,
                            "market_id": ob.market_id,  # 使用 market_id 而不是 index
                            "base_asset": ob.symbol.split('-')[0] if '-' in ob.symbol else "",
                            "quote_asset": ob.symbol.split('-')[1] if '-' in ob.symbol else "USD",
                            "status": getattr(ob, 'status', 'trading'),
                        })

            # 创建 ExchangeInfo 对象
            info = ExchangeInfo(
                name="Lighter",
                id="lighter",
                type=None,
                supported_features=[],
                rate_limits={},
                precision={},
                fees={},
                markets={s['symbol']: s for s in symbols},
                status="online",
                timestamp=datetime.now()
            )
            info.symbols = symbols  # 添加 symbols 属性以保持兼容性
            return info

        except Exception as e:
            logger.error(f"获取交易所信息失败: {e}")
            raise

    async def get_ticker(self, symbol: str) -> Optional[TickerData]:
        """
        获取ticker数据

        Args:
            symbol: 交易对符号

        Returns:
            TickerData对象
        """
        try:
            market_id = self.get_market_index(symbol)
            if market_id is None:
                logger.warning(f"未找到交易对 {symbol} 的市场ID")
                return None

            # 获取市场统计信息（包含价格信息）
            response = await self.order_api.order_book_details(market_id=market_id)

            if not response or not hasattr(response, 'order_book_details') or not response.order_book_details:
                return None

            detail = response.order_book_details[0]

            # 解析ticker数据（基于实际API返回字段）
            # 🔥 修复：不使用0作为默认值，避免返回无效价格（多交易所兼容性）
            last_price = self._safe_decimal(
                getattr(detail, 'last_trade_price', None))
            daily_high = self._safe_decimal(
                getattr(detail, 'daily_price_high', None))
            daily_low = self._safe_decimal(
                getattr(detail, 'daily_price_low', None))
            daily_volume = self._safe_decimal(
                getattr(detail, 'daily_base_token_volume', None))

            # 尝试获取最佳买卖价（从订单簿）
            bid_price = last_price
            ask_price = last_price
            try:
                orderbook_response = await self.order_api.order_book_orders(
                    market_id=market_id, limit=1)
                if orderbook_response.bids:
                    bid_price = self._safe_decimal(
                        orderbook_response.bids[0].price)
                if orderbook_response.asks:
                    ask_price = self._safe_decimal(
                        orderbook_response.asks[0].price)
            except Exception as e:
                logger.debug(f"无法获取订单簿最佳价格: {e}")

            return TickerData(
                symbol=symbol,
                last=last_price,
                bid=bid_price,
                ask=ask_price,
                volume=daily_volume,
                high=daily_high,
                low=daily_low,
                timestamp=datetime.now()
            )

        except Exception as e:
            logger.error(f"获取ticker失败 {symbol}: {e}")
            return None

    async def get_orderbook(self, symbol: str, limit: int = 20) -> Optional[OrderBookData]:
        """
        获取订单簿

        Args:
            symbol: 交易对符号
            limit: 深度限制

        Returns:
            OrderBookData对象
        """
        try:
            market_id = self.get_market_index(symbol)
            if market_id is None:
                logger.warning(f"未找到交易对 {symbol} 的市场ID")
                return None

            # 使用 order_book_orders 获取订单簿深度
            response = await self.order_api.order_book_orders(market_id=market_id, limit=limit)

            if not response:
                return None

            # 解析买单和卖单
            # Lighter返回完整订单对象：{price, remaining_base_amount, order_id, ...}
            bids = []
            asks = []

            if hasattr(response, 'bids') and response.bids:
                for bid in response.bids[:limit]:
                    # 提取 price 和 remaining_base_amount
                    price = self._safe_decimal(getattr(bid, 'price', 0))
                    quantity = self._safe_decimal(
                        getattr(bid, 'remaining_base_amount', 0))

                    if price > 0 and quantity > 0:
                        bids.append(OrderBookLevel(
                            price=price,
                            size=quantity
                        ))

            if hasattr(response, 'asks') and response.asks:
                for ask in response.asks[:limit]:
                    # 提取 price 和 remaining_base_amount
                    price = self._safe_decimal(getattr(ask, 'price', 0))
                    quantity = self._safe_decimal(
                        getattr(ask, 'remaining_base_amount', 0))

                    if price > 0 and quantity > 0:
                        asks.append(OrderBookLevel(
                            price=price,
                            size=quantity
                        ))

            return OrderBookData(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=datetime.now(),
                nonce=None
            )

        except Exception as e:
            logger.error(f"获取订单簿失败 {symbol}: {e}")
            return None

    async def get_recent_trades(self, symbol: str, limit: int = 100) -> List[TradeData]:
        """
        获取最近成交

        Args:
            symbol: 交易对符号
            limit: 数量限制

        Returns:
            TradeData列表
        """
        try:
            market_id = self.get_market_index(symbol)
            if market_id is None:
                logger.warning(f"未找到交易对 {symbol} 的市场ID")
                return []

            # 获取最近成交
            response = await self.order_api.recent_trades(market_id=market_id, limit=limit)

            trades = []
            if hasattr(response, 'trades') and response.trades:
                for trade in response.trades:
                    price = self._safe_decimal(trade.price) if hasattr(
                        trade, 'price') else Decimal("0")
                    amount = self._safe_decimal(trade.size) if hasattr(
                        trade, 'size') else Decimal("0")
                    cost = price * amount

                    # 解析成交方向
                    is_ask = getattr(trade, 'is_ask', False)
                    side = OrderSide.SELL if is_ask else OrderSide.BUY

                    trades.append(TradeData(
                        id=str(getattr(trade, 'trade_id', '')),
                        symbol=symbol,
                        side=side,
                        amount=amount,
                        price=price,
                        cost=cost,
                        fee=None,
                        timestamp=self._parse_timestamp(
                            getattr(trade, 'timestamp', None)) or datetime.now(),
                        order_id=str(getattr(trade, 'order_id', '')),
                        raw_data={'trade': trade}
                    ))

            return trades

        except Exception as e:
            logger.error(f"获取最近成交失败 {symbol}: {e}")
            return []

    # ============= 账户信息 =============

    async def get_account_balance(self) -> List[BalanceData]:
        """
        获取账户余额

        Returns:
            BalanceData列表
        """
        if not self.signer_client:
            logger.error("未配置SignerClient，无法获取账户信息")
            return []

        try:
            # 获取账户信息
            response = await self.account_api.account(by="index", value=str(self.account_index))

            balances = []

            # 解析 DetailedAccounts 结构
            if hasattr(response, 'accounts') and response.accounts:
                # 获取第一个账户（通常就是查询的账户）
                account = response.accounts[0]

                # 获取可用余额和抵押品（USDC余额）
                available_balance = self._safe_decimal(
                    getattr(account, 'available_balance', 0))
                collateral = self._safe_decimal(
                    getattr(account, 'collateral', 0))

                # 计算锁定余额（抵押品 - 可用余额）
                locked = max(collateral - available_balance, Decimal("0"))

                # USDC 余额（Lighter是合约交易所，只有USDC保证金）
                if collateral > 0:
                    from datetime import datetime
                    balances.append(BalanceData(
                        currency="USDC",
                        free=available_balance,
                        used=locked,
                        total=collateral,
                        usd_value=collateral,
                        timestamp=datetime.now(),
                        raw_data={'account': account}
                    ))

                # 注意：Lighter是合约交易所，持仓不是余额
                # 持仓应该通过 get_positions() 方法查询

            return balances

        except Exception as e:
            logger.error(f"获取账户余额失败: {e}")
            import traceback
            traceback.print_exc()
            return []

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """
        获取活跃订单

        Args:
            symbol: 交易对符号（可选，为None时获取所有）

        Returns:
            OrderData列表
        """
        if not self.signer_client:
            logger.error("未配置SignerClient，无法获取订单信息")
            return []

        try:
            # 🔥 修复：Lighter 需要使用专门的订单查询 API，而不是 account API
            # 生成认证令牌
            import time
            expiry_timestamp = int(time.time()) + 3600  # 1小时后过期
            auth_result = self.signer_client.create_auth_token_with_expiry(
                expiry_timestamp)

            # SDK 返回元组 (token, error)
            if isinstance(auth_result, tuple):
                auth_token, error = auth_result
                if error:
                    logger.error(f"生成认证令牌失败: {error}")
                    return []
            else:
                auth_token = auth_result

            # 获取 market_id
            market_id = None
            if symbol:
                market_id = self.get_market_index(symbol)
                if market_id is None:
                    logger.warning(f"未找到交易对 {symbol} 的市场索引")
                    return []

            # 使用 account_active_orders API（SDK 方法是异步的，直接 await）
            response = await self.order_api.account_active_orders(
                account_index=self.account_index,
                market_id=market_id if market_id is not None else 255,  # 255 = 所有市场
                auth=auth_token
            )

            orders = []

            # 🔥 account_active_orders 返回 orders 列表，不是 accounts
            if hasattr(response, 'orders') and response.orders:
                logger.info(f"🔍 REST API返回 {len(response.orders)} 个活跃订单")

                for order_info in response.orders:
                    order_symbol = self._get_symbol_from_market_index(
                        getattr(order_info, 'market_index', None))

                    # 如果指定了symbol，过滤
                    if symbol and order_symbol != symbol:
                        continue

                    orders.append(self._parse_order(order_info, order_symbol))
            else:
                logger.info(f"✅ REST API确认无活跃订单")

            return orders

        except Exception as e:
            logger.error(f"获取活跃订单失败: {e}")
            return []

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """
        获取单个订单信息（网格系统关键方法）

        Args:
            order_id: 订单ID
            symbol: 交易对符号

        Returns:
            OrderData对象

        Raises:
            Exception: 如果订单不存在或查询失败
        """
        if not self.signer_client:
            logger.error("未配置SignerClient，无法获取订单信息")
            raise Exception("未配置SignerClient")

        try:
            # 获取所有活跃订单
            open_orders = await self.get_open_orders(symbol)

            # 在活跃订单中查找
            for order in open_orders:
                if order.id == order_id:
                    logger.debug(f"找到订单: {order_id}, 状态={order.status.value}")
                    return order

            # 如果在活跃订单中没找到，尝试从历史订单查找
            # 注意：Lighter可能没有直接的单订单查询API
            logger.warning(f"订单 {order_id} 不在活跃订单列表中，可能已成交或取消")

            # 返回一个占位符OrderData，表示订单可能已完成
            # 网格系统会根据订单不在open_orders中判断其已成交
            return OrderData(
                id=order_id,
                client_id=None,
                symbol=symbol,
                side=OrderSide.BUY,  # 占位符
                type=OrderType.LIMIT,  # 占位符
                amount=Decimal("0"),
                price=None,
                filled=Decimal("0"),
                remaining=Decimal("0"),
                cost=Decimal("0"),
                average=None,
                status=OrderStatus.FILLED,  # 假设已成交
                timestamp=datetime.now(),
                updated=None,
                fee=None,
                trades=[],
                params={},
                raw_data={}
            )

        except Exception as e:
            logger.error(f"获取订单 {order_id} 失败: {e}")
            raise

    async def get_order_history(self, symbol: Optional[str] = None, limit: int = 100) -> List[OrderData]:
        """
        获取历史订单（已完成/取消）

        Args:
            symbol: 交易对符号（可选）
            limit: 返回数量限制

        Returns:
            OrderData列表
        """
        if not self.signer_client:
            logger.error("未配置SignerClient，无法获取订单历史")
            return []

        try:
            # 生成认证令牌
            import lighter
            auth_token, err = self.signer_client.create_auth_token_with_expiry(
                lighter.SignerClient.DEFAULT_10_MIN_AUTH_EXPIRY
            )
            if err:
                logger.error(f"生成认证令牌失败: {err}")
                return []

            # 获取市场ID（如果指定了symbol）
            market_id = None
            if symbol:
                market_id = self.get_market_index(symbol)

            # 获取历史订单
            response = await self.order_api.account_inactive_orders(
                account_index=self.account_index,
                limit=limit,
                auth=auth_token,
                market_id=market_id if market_id is not None else 255  # 255表示所有市场
            )

            orders = []
            if hasattr(response, 'orders') and response.orders:
                for order_info in response.orders:
                    order_symbol = self._get_symbol_from_market_index(
                        getattr(order_info, 'market_index', None))

                    orders.append(self._parse_order(order_info, order_symbol))

            return orders

        except Exception as e:
            logger.error(f"获取历史订单失败: {e}")
            import traceback
            traceback.print_exc()
            return []

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """
        获取持仓信息

        Args:
            symbols: 交易对符号列表（Lighter会忽略，返回所有持仓）

        Returns:
            PositionData列表
        """
        if not self.signer_client:
            logger.error("未配置SignerClient，无法获取持仓信息")
            return []

        try:
            # 获取账户信息（包含持仓）
            response = await self.account_api.account(by="index", value=str(self.account_index))

            positions = []
            # 解析 DetailedAccounts 结构
            if hasattr(response, 'accounts') and response.accounts:
                account = response.accounts[0]

                if hasattr(account, 'positions') and account.positions:

                    for idx, position_info in enumerate(account.positions):
                        symbol = position_info.symbol if hasattr(
                            position_info, 'symbol') else ""

                        # 获取持仓数据
                        position_raw = position_info.position if hasattr(
                            position_info, 'position') else 0

                        position_size = self._safe_decimal(position_raw) if hasattr(
                            position_info, 'position') else Decimal("0")

                        if position_size == 0:
                            continue

                        from datetime import datetime
                        from ..models import PositionSide, MarginMode

                        # 🔥 Lighter持仓方向定义（与传统CEX一致）
                        # 正数 = 多头 (LONG) | 负数 = 空头 (SHORT)
                        # ✅ 测试验证：BUY订单成交后，position返回正数，表示做多
                        position_side = PositionSide.LONG if position_size > 0 else PositionSide.SHORT

                        positions.append(PositionData(
                            symbol=symbol,
                            side=position_side,
                            size=abs(position_size),
                            entry_price=self._safe_decimal(
                                getattr(position_info, 'avg_entry_price', 0)),
                            mark_price=None,  # Lighter不提供标记价格
                            current_price=None,  # 需要单独查询
                            unrealized_pnl=self._safe_decimal(
                                getattr(position_info, 'unrealized_pnl', 0)),
                            realized_pnl=self._safe_decimal(
                                getattr(position_info, 'realized_pnl', 0)),
                            percentage=None,  # 可以计算
                            leverage=int(
                                getattr(position_info, 'leverage', 1)),
                            margin_mode=MarginMode.CROSS if getattr(
                                position_info, 'margin_mode', 0) == 0 else MarginMode.ISOLATED,
                            margin=self._safe_decimal(
                                getattr(position_info, 'allocated_margin', 0)),
                            liquidation_price=self._safe_decimal(getattr(position_info, 'liquidation_price', 0)) if getattr(
                                position_info, 'liquidation_price', '0') != '0' else None,
                            timestamp=datetime.now(),
                            raw_data={'position_info': position_info}
                        ))

            # 🔥 如果指定了symbols，只返回匹配的持仓
            if symbols:
                positions = [p for p in positions if p.symbol in symbols]

            return positions

        except Exception as e:
            logger.error(f"获取持仓信息失败: {e}")
            return []

    # ============= 交易功能 =============

    def _validate_order_preconditions(self) -> bool:
        """验证下单前置条件"""
        if not self.signer_client:
            logger.error("❌ 未配置SignerClient，无法下单")
            logger.error(
                "   请检查lighter_config.yaml中的api_key_private_key和account_index")
            return False
        return True

    def _extract_price_decimals(self, market_details) -> int:
        """从市场详情中提取价格精度"""
        if hasattr(market_details, 'order_book_details') and market_details.order_book_details:
            return market_details.order_book_details[0].price_decimals
        return 1  # 默认值

    async def _get_market_info(self, symbol: str) -> Optional[Dict]:
        """
        获取市场信息（索引、价格精度、乘数）

        🔥 使用缓存机制避免频繁API调用触发429限流
        这是关键修复！批量下单时必须使用缓存

        Returns:
            包含 market_index, price_decimals, price_multiplier 的字典，或 None
        """
        import time

        # 🔥 检查缓存（5分钟有效期）
        # 注意：缓存的是市场的静态配置（市场索引、价格精度），不是动态数据
        # 这些配置基本不会变化，所以可以缓存较长时间
        if symbol in self._market_info_cache:
            cache_entry = self._market_info_cache[symbol]
            cache_age = time.time() - cache_entry['timestamp']
            if cache_age < 300:  # 缓存5分钟内有效（300秒）
                logger.debug(f"✅ 使用缓存的市场信息: {symbol} (缓存年龄: {cache_age:.1f}秒)")
                return cache_entry['info']

        try:
            market_index = self.get_market_index(symbol)
            logger.debug(f"✅ 获取market_index: {market_index}")

            if market_index is None:
                logger.error(f"❌ 未找到交易对 {symbol} 的市场索引")
                logger.error(
                    f"   可用市场: {list(self.markets.keys()) if self.markets else '未加载'}")
                return None

            # 获取市场详情，动态获取价格精度
            logger.debug(f"🔍 获取市场详情: market_id={market_index}")
            market_details = await self.order_api.order_book_details(market_id=market_index)
            price_decimals = self._extract_price_decimals(market_details)

            logger.debug(f"✅ 获取价格精度成功: price_decimals={price_decimals}")

            price_multiplier = Decimal(10 ** price_decimals)
            logger.debug(
                f"{symbol} 价格精度: {price_decimals}位小数, 乘数: {price_multiplier}")

            market_info = {
                'market_index': market_index,
                'price_decimals': price_decimals,
                'price_multiplier': price_multiplier
            }

            # 🔥 缓存市场信息（关键！）
            self._market_info_cache[symbol] = {
                'info': market_info,
                'timestamp': time.time()
            }
            logger.debug(f"💾 已缓存市场信息: {symbol}")

            return market_info

        except Exception as e:
            logger.error(f"获取市场信息失败: {e}")
            return None

    async def _calculate_slippage_protection_price(
        self,
        symbol: str,
        side: str,
        provided_price: Optional[Decimal] = None
    ) -> Optional[Decimal]:
        """
        计算市价单的滑点保护价格（万分之1 = 0.01%）

        Args:
            symbol: 交易对符号
            side: 订单方向
            provided_price: 用户提供的价格（如果有）

        Returns:
            滑点保护价格，或 None
        """
        if provided_price:
            return provided_price

        try:
            orderbook = await self.get_orderbook(symbol)
            if not orderbook or not orderbook.bids or not orderbook.asks:
                logger.error(f"无法获取{symbol}的订单簿，市价单需要价格")
                return None

            is_sell = (side.lower() == "sell")
            if is_sell:
                # 卖单：使用买1价格并减少万分之1
                base_price = orderbook.bids[0].price
                protection_price = base_price * Decimal("0.9999")
            else:
                # 买单：使用卖1价格并增加万分之1
                base_price = orderbook.asks[0].price
                protection_price = base_price * Decimal("1.0001")

            logger.debug(
                f"市价单滑点保护价格: {protection_price} (基准: {base_price}, 滑点: 0.01%)")
            return protection_price

        except Exception as e:
            logger.error(f"计算滑点保护价格失败: {e}")
            return None

    def _convert_market_order_params(
        self,
        market_info: Dict,
        quantity: Decimal,
        avg_execution_price: Decimal,
        side: str,
        **kwargs
    ) -> Dict:
        """转换市价单参数为Lighter格式"""
        # 🔥 先对价格应用精度规则（与限价单保持一致）
        price_decimals = market_info['price_decimals']
        if price_decimals == 0:
            quantize_precision = Decimal("1")
        else:
            quantize_precision = Decimal(10) ** (-price_decimals)

        avg_execution_price_rounded = avg_execution_price.quantize(
            quantize_precision)

        # 🔥 Lighter的数量单位是1e5而不是1e6（实际测试发现放大10倍问题）
        base_amount_int = int(quantity * Decimal("100000"))
        avg_price_int = int(avg_execution_price_rounded *
                            market_info['price_multiplier'])
        is_ask = (side.lower() == "sell")

        logger.debug(f"  symbol参数中的market_index={market_info['market_index']}")
        logger.debug(
            f"  价格精度: {market_info['price_decimals']}位小数, 乘数: {market_info['price_multiplier']}")
        logger.debug(
            f"  avg_execution_price={avg_execution_price} -> 四舍五入后={avg_execution_price_rounded}, avg_price_int={avg_price_int}")
        logger.debug(
            f"  reduce_only={kwargs.get('reduce_only', False)}")

        return {
            'market_index': market_info['market_index'],
            'client_order_index': kwargs.get("client_order_id",
                                             int(asyncio.get_event_loop().time() * 1000)),
            'base_amount': base_amount_int,
            'avg_execution_price': avg_price_int,
            'is_ask': is_ask,
            'reduce_only': kwargs.get("reduce_only", False)
        }

    def _convert_limit_order_params(
        self,
        market_info: Dict,
        quantity: Decimal,
        price: Decimal,
        side: str,
        **kwargs
    ) -> Dict:
        """转换限价单参数为Lighter格式"""
        import lighter

        # 🔥 根据price_decimals动态调整价格精度（直接使用quantize避免浮点误差）
        # 例如：price_decimals=1 -> quantize(Decimal("0.1"))
        #      price_decimals=2 -> quantize(Decimal("0.01"))
        price_decimals = market_info['price_decimals']
        if price_decimals == 0:
            quantize_precision = Decimal("1")
        else:
            quantize_precision = Decimal(10) ** (-price_decimals)

        price_rounded = price.quantize(quantize_precision)

        # 🔥 Lighter的数量单位是1e5而不是1e6（与市价单保持一致）
        base_amount_int = int(quantity * Decimal("100000"))
        price_int = int(price_rounded * market_info['price_multiplier'])
        is_ask = (side.lower() == "sell")

        # 🔍 简化日志：只在 DEBUG 级别输出详细参数
        logger.debug(f"Lighter限价单参数: market_id={market_info['market_index']}, "
                     f"price={price_rounded}, quantity={quantity}, "
                     f"base_amount={base_amount_int}, is_ask={is_ask}")

        time_in_force = kwargs.get("time_in_force", "GTT")
        tif_map = {
            "IOC": lighter.SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
            "GTT": lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            "POST_ONLY": lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
        }

        return {
            'market_index': market_info['market_index'],
            'client_order_index': kwargs.get("client_order_id",
                                             int(asyncio.get_event_loop().time() * 1000)),
            'base_amount': base_amount_int,
            'price': price_int,
            'is_ask': is_ask,
            'order_type': lighter.SignerClient.ORDER_TYPE_LIMIT,
            'time_in_force': tif_map.get(time_in_force,
                                         lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME),
            'reduce_only': kwargs.get("reduce_only", False),
            'trigger_price': 0
        }

    async def _execute_market_order(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        provided_price: Optional[Decimal],
        market_info: Dict,
        **kwargs
    ) -> Optional[OrderData]:
        """执行市价单"""
        # 🔥 生成唯一的 client_order_id（确保整个流程使用同一个值）
        if "client_order_id" not in kwargs:
            kwargs["client_order_id"] = int(
                asyncio.get_event_loop().time() * 1000)

        # 计算滑点保护价格
        avg_execution_price = await self._calculate_slippage_protection_price(
            symbol, side, provided_price
        )
        if not avg_execution_price:
            return None

        # 转换参数
        params = self._convert_market_order_params(
            market_info, quantity, avg_execution_price, side, **kwargs
        )

        # 执行下单
        try:
            tx, tx_hash, err = await self.signer_client.create_market_order(**params)

            # 处理结果
            return await self._handle_order_result(
                tx, tx_hash, err, symbol, side, "market",
                quantity, avg_execution_price, **kwargs
            )
        except Exception as e:
            logger.error(f"执行市价单失败: {e}")
            return None

    async def _execute_limit_order(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal],
        market_info: Dict,
        **kwargs
    ) -> Optional[OrderData]:
        """执行限价单"""
        if not price:
            logger.error("限价单必须指定价格")
            return None

        # 🔥 生成唯一的 client_order_id（确保整个流程使用同一个值）
        if "client_order_id" not in kwargs:
            kwargs["client_order_id"] = int(
                asyncio.get_event_loop().time() * 1000)

        # 转换参数
        params = self._convert_limit_order_params(
            market_info, quantity, price, side, **kwargs
        )

        # 执行下单
        try:
            import lighter
            tx, tx_hash, err = await self.signer_client.create_order(**params)

            # 🔥 处理结果（使用调整后的价格，与_convert_limit_order_params保持一致）
            price_decimals = market_info['price_decimals']
            if price_decimals == 0:
                quantize_precision = Decimal("1")
            else:
                quantize_precision = Decimal(10) ** (-price_decimals)

            price_rounded = price.quantize(quantize_precision)

            return await self._handle_order_result(
                tx, tx_hash, err, symbol, side, "limit",
                quantity, price_rounded, **kwargs
            )
        except Exception as e:
            logger.error(f"执行限价单失败: {e}")
            return None

    async def _handle_order_result(
        self,
        tx,
        tx_hash,
        err,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal,
        price: Decimal,
        **kwargs
    ) -> Optional[OrderData]:
        """
        处理下单结果

        ⚠️ 重要：Lighter下单API返回的是transaction hash，不是order_id
        真正的order_id需要从WebSocket推送或REST查询中获取
        """
        # 检查错误
        if err:
            error_msg = self.parse_error(err) if err else "未知错误"
            logger.error(f"❌ Lighter下单失败: {error_msg}")
            logger.error(f"   订单类型: {order_type}, 方向: {side}, 数量: {quantity}")
            if order_type == "market":
                logger.error(f"   市价单保护价格: {price}")
            else:
                logger.error(f"   限价单价格: {price}")
            return None

        # 检查返回值
        if not tx and not tx_hash:
            logger.error(f"❌ Lighter下单失败: tx和tx_hash都为空（无错误信息）")
            logger.error(f"   这可能是钱包未授权或gas不足")
            return None

        # 🔥 提取transaction hash（这不是order_id！）
        tx_hash_str = str(tx_hash.tx_hash) if tx_hash and hasattr(
            tx_hash, 'tx_hash') else str(tx_hash)
        logger.info(f"✅ Lighter下单成功: tx_hash={tx_hash_str}")

        # 🔥 Lighter特殊处理：REST API无法立即查询到新下的订单
        # 原因：
        # 1. Lighter是Layer 2，订单需要时间上链
        # 2. account_api.account()不返回订单列表（只返回账户信息）
        # 3. order_api.account_active_orders()需要复杂的认证token
        #
        # 解决方案：
        # 1. 返回带tx_hash的临时OrderData
        # 2. 依赖WebSocket推送真正的order_id和状态
        # 3. 网格系统通过WebSocket回调更新订单信息
        logger.info(f"⚠️ Lighter下单成功，等待WebSocket推送真正的order_id")
        logger.info(f"   tx_hash: {tx_hash_str}")
        logger.info(f"   将通过WebSocket回调获取order_id和订单状态")

        # 🔥 Lighter特殊处理：使用 client_order_id 作为订单ID
        # 原因：
        # 1. 下单返回的是 tx_hash（128字符），无法直接查询订单
        # 2. WebSocket 推送包含 client_order_index，可以用它匹配订单
        # 3. 引擎用 client_order_id 存储订单，WebSocket 推送时通过 client_order_index 匹配
        from datetime import datetime

        client_order_id_str = str(kwargs.get("client_order_id", int(
            asyncio.get_event_loop().time() * 1000)))

        logger.info(
            f"🔑 Lighter订单标识: client_order_id={client_order_id_str}, tx_hash={tx_hash_str[:16]}...")

        return OrderData(
            # 🔥 使用 client_order_id（而不是 tx_hash）
            id=client_order_id_str,
            client_id=client_order_id_str,                       # 保持一致
            symbol=symbol,
            side=OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL,
            type=OrderType.MARKET if order_type == "market" else OrderType.LIMIT,
            amount=quantity,
            price=price,
            filled=Decimal("0"),
            remaining=quantity,
            cost=Decimal("0"),
            average=None,
            status=OrderStatus.PENDING,
            timestamp=datetime.now(),
            updated=None,
            fee=None,
            trades=[],
            params=kwargs,
            raw_data={'tx': tx, 'tx_hash': tx_hash, 'tx_hash_str': tx_hash_str}
        )

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
        下单（主流程编排）

        Args:
            symbol: 交易对符号
            side: 订单方向 ("buy" 或 "sell")
            order_type: 订单类型 ("limit" 或 "market")
            quantity: 数量
            price: 价格（限价单必需）
            **kwargs: 其他参数

        Returns:
            OrderData对象

        ⚠️  Lighter价格精度说明：
        - Lighter使用动态价格乘数，不同交易对精度不同
        - 价格乘数公式: price_int = price_usd × (10 ** price_decimals)
        - 数量始终使用1e6: base_amount = quantity × 1000000

        示例：
        - ETH (price_decimals=2): $4127.39 × 100 = 412739
        - BTC (price_decimals=1): $114357.8 × 10 = 1143578
        - SOL (price_decimals=3): $199.058 × 1000 = 199058
        - DOGE (price_decimals=6): $0.202095 × 1000000 = 202095

        注意：这与大多数交易所不同！
        - 大多数CEX使用固定的1e8或1e6
        - Lighter根据价格大小动态选择精度，以优化Layer 2性能
        """
        # 🔥 nonce冲突已通过grid_engine_impl.py中的串行下单解决
        # 串行下单确保了nonce自然递增，无需在此添加延迟

        logger.debug(
            f"📝 开始下单: symbol={symbol}, side={side}, type={order_type}, qty={quantity}")

        try:
            # 1. 验证前置条件
            if not self._validate_order_preconditions():
                return None

            # 2. 获取市场信息（索引、价格精度、乘数）
            market_info = await self._get_market_info(symbol)
            if not market_info:
                return None

            # 3. 根据订单类型执行下单
            if order_type.lower() == "market":
                return await self._execute_market_order(
                    symbol, side, quantity, price, market_info, **kwargs
                )
            else:
                return await self._execute_limit_order(
                    symbol, side, quantity, price, market_info, **kwargs
                )

        except Exception as e:
            logger.error(f"下单失败 {symbol}: {e}")
            return None

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """
        取消订单

        Args:
            symbol: 交易对符号
            order_id: 订单ID

        Returns:
            是否成功
        """
        if not self.signer_client:
            logger.error("未配置SignerClient，无法取消订单")
            return False

        try:
            market_index = self.get_market_index(symbol)
            if market_index is None:
                logger.error(f"未找到交易对 {symbol} 的市场索引")
                return False

            # 🔥 检查order_id是否为tx_hash（128字符十六进制）
            if len(order_id) > 20:  # tx_hash通常是128字符，order_index是整数
                logger.warning(
                    f"⚠️ 订单ID似乎是tx_hash（长度{len(order_id)}），无法直接取消。"
                    f"需要等待WebSocket更新为真实的order_index"
                )
                # 尝试从挂单列表中查找真实的order_index
                try:
                    orders = await self.get_open_orders(symbol)
                    for order in orders:
                        # 通过client_order_id或其他方式匹配
                        # 暂时返回False，等待WebSocket更新
                        pass
                except Exception as e:
                    logger.error(f"查询挂单失败: {e}")
                return False

            # 取消订单
            tx, tx_hash, err = await self.signer_client.cancel_order(
                market_index=market_index,
                order_index=int(order_id),
            )

            if err:
                logger.error(f"取消订单失败: {self.parse_error(err)}")
                return False

            return True

        except Exception as e:
            logger.error(f"取消订单失败 {symbol}/{order_id}: {e}")
            return False

    async def place_market_order(
            self,
            symbol: str,
            side: OrderSide,
            quantity: Decimal,
            reduce_only: bool = False) -> Optional[OrderData]:
        """
        下市价单（便捷方法）

        Args:
            symbol: 交易对符号
            side: 订单方向
            quantity: 数量
            reduce_only: 只减仓模式（平仓专用，不会开新仓或加仓）

        Returns:
            订单数据 或 None
        """
        logger.debug(
            f"🚀 place_market_order被调用: symbol={symbol}, side={side}, qty={quantity}, reduce_only={reduce_only}")

        # 转换OrderSide枚举为字符串
        side_str = "buy" if side == OrderSide.BUY else "sell"

        logger.debug(f"   转换side: {side} → {side_str}")

        return await self.place_order(
            symbol=symbol,
            side=side_str,  # 🔥 修复：传递字符串而不是枚举
            order_type="market",  # 🔥 修复：传递字符串
            quantity=quantity,
            reduce_only=reduce_only  # 🔥 新增：只减仓模式
        )

    # ============= 辅助方法 =============

    def _get_symbol_from_market_index(self, market_index: int) -> str:
        """从市场索引获取符号"""
        market_info = self._markets_cache.get(market_index)
        if market_info:
            return market_info.get("symbol", "")
        return ""

    def _parse_order(self, order_info: Any, symbol: str) -> OrderData:
        """
        解析订单信息

        根据Lighter API文档，Order对象包含:
        - order_index: INTEGER (真正的订单ID)
        - order_id: STRING (order_index的字符串形式)
        - client_order_index: INTEGER
        - client_order_id: STRING
        """
        from datetime import datetime

        # 🔥 获取真正的订单ID (优先使用order_index，然后是order_id)
        order_index = getattr(order_info, 'order_index', None)
        order_id_str = getattr(order_info, 'order_id', None)

        # 如果有order_index，使用它；否则使用order_id
        final_order_id = order_id_str if order_id_str else (
            str(order_index) if order_index is not None else '')

        logger.debug(
            f"解析订单: order_index={order_index}, order_id={order_id_str}, final_id={final_order_id}")

        # 解析数量信息
        initial_amount = self._safe_decimal(
            getattr(order_info, 'initial_base_amount', 0))
        filled_amount = self._safe_decimal(
            getattr(order_info, 'filled_base_amount', 0))
        remaining_amount = self._safe_decimal(
            getattr(order_info, 'remaining_base_amount', 0))

        # 解析价格
        price = self._safe_decimal(getattr(order_info, 'price', 0))
        filled_quote = self._safe_decimal(
            getattr(order_info, 'filled_quote_amount', 0))

        # 计算平均价格
        average_price = filled_quote / filled_amount if filled_amount > 0 else None

        return OrderData(
            # ✅ 使用真正的order_id（order_index的字符串形式）
            id=final_order_id,
            client_id=str(getattr(order_info, 'client_order_id', '')),
            symbol=symbol,
            side=self._parse_order_side(getattr(order_info, 'is_ask', False)),
            type=self._parse_order_type(getattr(order_info, 'type', 'limit')),
            amount=initial_amount,
            price=price if price > 0 else None,
            filled=filled_amount,
            remaining=remaining_amount,
            cost=filled_quote,
            average=average_price,
            status=self._parse_order_status(
                getattr(order_info, 'status', 'unknown')),
            timestamp=self._parse_timestamp(
                getattr(order_info, 'timestamp', None)) or datetime.now(),
            updated=None,
            fee=None,
            trades=[],
            params={},
            raw_data={'order_info': order_info}
        )

    def _parse_order_side(self, is_ask: bool) -> OrderSide:
        """解析订单方向"""
        return OrderSide.SELL if is_ask else OrderSide.BUY

    def _parse_order_type(self, order_type_str: str) -> OrderType:
        """解析订单类型"""
        type_mapping = {
            'market': OrderType.MARKET,
            'limit': OrderType.LIMIT,
            'stop-limit': OrderType.STOP_LIMIT,
            'stop_limit': OrderType.STOP_LIMIT,
            'stop-market': OrderType.STOP,
            'stop_market': OrderType.STOP,
            'stop': OrderType.STOP,
        }
        return type_mapping.get(order_type_str.lower().replace('_', '-'), OrderType.LIMIT)

    def _parse_order_status(self, status_str: str) -> OrderStatus:
        """解析订单状态"""
        status_mapping = {
            'pending': OrderStatus.PENDING,
            'open': OrderStatus.OPEN,
            'filled': OrderStatus.FILLED,
            'canceled': OrderStatus.CANCELED,
            'canceled-too-much-slippage': OrderStatus.CANCELED,
            'expired': OrderStatus.EXPIRED,
            'rejected': OrderStatus.REJECTED,
        }
        return status_mapping.get(status_str.lower(), OrderStatus.UNKNOWN)
