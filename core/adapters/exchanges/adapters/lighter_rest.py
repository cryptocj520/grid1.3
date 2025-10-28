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
   - 数量始终使用1e6: base_amount = quantity × 1000000
   
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

logger = logging.getLogger(__name__)


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
            last_price = self._safe_decimal(
                getattr(detail, 'last_trade_price', 0))
            daily_high = self._safe_decimal(
                getattr(detail, 'daily_price_high', 0))
            daily_low = self._safe_decimal(
                getattr(detail, 'daily_price_low', 0))
            daily_volume = self._safe_decimal(
                getattr(detail, 'daily_base_token_volume', 0))

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
            # 获取账户信息（包含活跃订单）
            response = await self.account_api.account(by="index", value=str(self.account_index))

            orders = []
            if hasattr(response, 'accounts') and response.accounts:
                account = response.accounts[0]
                if hasattr(account, 'orders') and account.orders:
                    for order_info in account.orders:
                        order_symbol = self._get_symbol_from_market_index(
                            getattr(order_info, 'market_index', None))

                        # 如果指定了symbol，过滤
                        if symbol and order_symbol != symbol:
                            continue

                        orders.append(self._parse_order(
                            order_info, order_symbol))

            return orders

        except Exception as e:
            logger.error(f"获取活跃订单失败: {e}")
            return []

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

    async def get_positions(self) -> List[PositionData]:
        """
        获取持仓信息

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
                    for position_info in account.positions:
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

                        positions.append(PositionData(
                            symbol=symbol,
                            side=PositionSide.LONG if position_size > 0 else PositionSide.SHORT,
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

            return positions

        except Exception as e:
            logger.error(f"获取持仓信息失败: {e}")
            return []

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
        logger.info(
            f"📝 开始下单: symbol={symbol}, side={side}, type={order_type}, qty={quantity}")

        if not self.signer_client:
            logger.error("❌ 未配置SignerClient，无法下单")
            logger.error(
                "   请检查lighter_config.yaml中的api_key_private_key和account_index")
            return None

        try:
            market_index = self.get_market_index(symbol)
            logger.info(f"✅ 获取market_index: {market_index}")

            if market_index is None:
                logger.error(f"❌ 未找到交易对 {symbol} 的市场索引")
                logger.error(
                    f"   可用市场: {list(self.markets.keys()) if self.markets else '未加载'}")
                return None

            # ⚠️ 获取市场详情，动态获取价格精度
            # Lighter的每个交易对有不同的price_decimals（1-6位小数）
            logger.info(f"🔍 获取市场详情: market_id={market_index}")
            market_details = await self.order_api.order_book_details(market_id=market_index)
            price_decimals = 1  # 默认值
            if hasattr(market_details, 'order_book_details') and market_details.order_book_details:
                price_decimals = market_details.order_book_details[0].price_decimals

            logger.info(f"✅ 获取价格精度成功: price_decimals={price_decimals}")

            # ⚠️ 计算价格乘数：10^price_decimals
            # ETH(2位)=100, BTC(1位)=10, SOL(3位)=1000, DOGE(6位)=1000000
            price_multiplier = Decimal(10 ** price_decimals)
            logger.info(
                f"{symbol} 价格精度: {price_decimals}位小数, 乘数: {price_multiplier}")

            # 转换参数
            is_ask = (side.lower() == "sell")
            client_order_index = kwargs.get("client_order_id", int(
                asyncio.get_event_loop().time() * 1000))

            # 根据订单类型下单
            if order_type.lower() == "market":
                # 市价单 - 需要指定最差接受价格（滑点保护）
                if price:
                    # 如果传入了价格，直接使用
                    avg_execution_price = price
                else:
                    # 否则获取当前市场价格并添加万分之1滑点保护
                    orderbook = await self.get_orderbook(symbol)
                    if not orderbook or not orderbook.bids or not orderbook.asks:
                        logger.error(f"无法获取{symbol}的订单簿，市价单需要价格")
                        return None

                    # 根据方向选择价格并添加滑点（万分之1 = 0.01%）
                    if is_ask:  # 卖单，使用买1价格并减少万分之1
                        base_price = orderbook.bids[0].price
                        avg_execution_price = base_price * Decimal("0.9999")
                    else:  # 买单，使用卖1价格并增加万分之1
                        base_price = orderbook.asks[0].price
                        avg_execution_price = base_price * Decimal("1.0001")

                    logger.info(
                        f"市价单滑点保护价格: {avg_execution_price} (基准: {base_price}, 滑点: 0.01%)")

                # 计算实际参数
                base_amount_int = int(quantity * Decimal("1000000"))
                avg_price_int = int(avg_execution_price * price_multiplier)

                logger.info(f"🔍 Lighter市价单参数:")
                logger.info(f"  symbol={symbol}, market_index={market_index}")
                logger.info(
                    f"  价格精度: {price_decimals}位小数, 乘数: {price_multiplier}")
                logger.info(
                    f"  quantity={quantity}, base_amount={base_amount_int}")
                logger.info(
                    f"  avg_execution_price={avg_execution_price}, avg_price_int={avg_price_int}")
                logger.info(
                    f"  is_ask={is_ask}, reduce_only={kwargs.get('reduce_only', False)}")

                tx, tx_hash, err = await self.signer_client.create_market_order(
                    market_index=market_index,
                    client_order_index=client_order_index,
                    base_amount=base_amount_int,
                    avg_execution_price=avg_price_int,
                    is_ask=is_ask,
                    reduce_only=kwargs.get("reduce_only", False),
                )
            else:
                # 限价单
                if not price:
                    logger.error("限价单必须指定价格")
                    return None

                import lighter
                time_in_force = kwargs.get("time_in_force", "GTT")
                tif_map = {
                    "IOC": lighter.SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                    "GTT": lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                    "POST_ONLY": lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
                }

                # 根据price_decimals动态调整价格精度
                # 例如：BTC (price_decimals=1) 需要0.1的倍数
                #      ETH (price_decimals=2) 需要0.01的倍数
                price_precision = Decimal(10 ** (-price_decimals))
                price_rounded = (
                    price / price_precision).quantize(Decimal("1")) * price_precision

                base_amount_int = int(quantity * Decimal("1000000"))
                # 使用动态价格乘数：10^price_decimals
                price_int = int(price_rounded * price_multiplier)

                print(f"\n🔍 Lighter下单参数详情:")
                print(f"  symbol={symbol}, market_index={market_index}")
                print(f"  价格精度: {price_decimals}位小数, 乘数: {price_multiplier}")
                print(f"  原始价格: {price}, 调整后价格: {price_rounded}")
                print(f"  quantity={quantity}, base_amount={base_amount_int}")
                print(f"  price_int={price_int} (价格×{price_multiplier})")
                print(f"  is_ask={is_ask}, order_type=LIMIT")
                print(f"  time_in_force={kwargs.get('time_in_force', 'GTT')}")

                tx, tx_hash, err = await self.signer_client.create_order(
                    market_index=market_index,
                    client_order_index=client_order_index,
                    base_amount=base_amount_int,
                    price=price_int,
                    is_ask=is_ask,
                    order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
                    time_in_force=tif_map.get(
                        time_in_force, lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME),
                    reduce_only=kwargs.get("reduce_only", False),
                    trigger_price=0,
                )

            # 检查错误
            if err:
                error_msg = self.parse_error(err) if err else "未知错误"
                logger.error(f"❌ Lighter下单失败: {error_msg}")
                logger.error(
                    f"   订单类型: {order_type}, 方向: {side}, 数量: {quantity}")
                if order_type.lower() == "market" and 'avg_execution_price' in locals():
                    logger.error(f"   市价单保护价格: {avg_execution_price}")
                elif price:
                    logger.error(f"   限价单价格: {price}")
                return None

            # 检查返回值
            if not tx and not tx_hash:
                logger.error(f"❌ Lighter下单失败: tx和tx_hash都为空（无错误信息）")
                logger.error(f"   这可能是钱包未授权或gas不足")
                return None

            logger.info(f"✅ Lighter下单成功: tx_hash={tx_hash}")

            # 返回订单数据
            from datetime import datetime
            order_id_val = str(tx_hash.tx_hash) if tx_hash and hasattr(
                tx_hash, 'tx_hash') else str(client_order_index)

            return OrderData(
                id=order_id_val,
                client_id=str(client_order_index),
                symbol=symbol,
                side=OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL,
                type=OrderType.MARKET if order_type.lower() == "market" else OrderType.LIMIT,
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
                raw_data={'tx': tx, 'tx_hash': tx_hash}
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
            quantity: Decimal) -> Optional[OrderData]:
        """
        下市价单（便捷方法）

        Args:
            symbol: 交易对符号
            side: 订单方向
            quantity: 数量

        Returns:
            订单数据 或 None
        """
        logger.info(
            f"🚀 place_market_order被调用: symbol={symbol}, side={side}, qty={quantity}")

        # 转换OrderSide枚举为字符串
        side_str = "buy" if side == OrderSide.BUY else "sell"

        logger.info(f"   转换side: {side} → {side_str}")

        return await self.place_order(
            symbol=symbol,
            side=side_str,  # 🔥 修复：传递字符串而不是枚举
            order_type="market",  # 🔥 修复：传递字符串
            quantity=quantity
        )

    # ============= 辅助方法 =============

    def _get_symbol_from_market_index(self, market_index: int) -> str:
        """从市场索引获取符号"""
        market_info = self._markets_cache.get(market_index)
        if market_info:
            return market_info.get("symbol", "")
        return ""

    def _parse_order(self, order_info: Any, symbol: str) -> OrderData:
        """解析订单信息"""
        from datetime import datetime

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
            id=str(getattr(order_info, 'order_id', '')),
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
