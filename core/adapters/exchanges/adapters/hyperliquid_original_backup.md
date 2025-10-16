"""
Hyperliquid交易所适配器

基于MESA架构的Hyperliquid交易所适配器实现，
提供完整的永续合约交易功能和实时数据流。
"""

import asyncio
import ccxt
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal

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


class HyperliquidAdapter(ExchangeAdapter):
    """
    Hyperliquid交易所适配器

    基于ccxt库的Hyperliquid实现，支持：
    - 永续合约交易
    - 实时WebSocket数据流
    - 自动重连和错误处理
    - 事件驱动架构集成
    """

    def __init__(self, config: ExchangeConfig, event_bus=None):
        """初始化Hyperliquid适配器"""
        super().__init__(config, event_bus)
        self.exchange: Optional[ccxt.hyperliquid] = None
        self._ws_client = None
        self._subscriptions: Dict[str, List[Callable]] = {}

        # 符号映射（通用格式 -> Hyperliquid格式）
        self._default_symbol_mapping = {
            "BTC/USDC:PERP": "BTC-USD",
            "ETH/USDC:PERP": "ETH-USD",
            "SOL/USDC:PERP": "SOL-USD",
            "AVAX/USDC:PERP": "AVAX-USD"
        }

        # 合并用户配置的符号映射
        if config.symbol_mapping:
            self._default_symbol_mapping.update(config.symbol_mapping)

    # === 生命周期管理实现 ===

    async def _do_connect(self) -> bool:
        """执行具体的连接逻辑"""
        try:
            # 创建ccxt交易所实例
            exchange_config = {
                'privateKey': self.config.api_key,  # Hyperliquid使用私钥
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'spot',  # 使用现货类型访问永续合约
                }
            }

            # 如果有钱包地址，添加到配置
            if self.config.wallet_address:
                exchange_config['walletAddress'] = self.config.wallet_address

            # 测试网配置
            if self.config.testnet:
                exchange_config['sandbox'] = True
                if self.config.base_url:
                    exchange_config['urls'] = {'api': self.config.base_url}

            self.exchange = ccxt.hyperliquid(exchange_config)

            # 加载市场信息
            await asyncio.get_event_loop().run_in_executor(
                None, self.exchange.load_markets
            )

            self.logger.info(
                f"Hyperliquid连接成功，加载 {len(self.exchange.markets)} 个市场")
            return True

        except Exception as e:
            self.logger.error(f"Hyperliquid连接失败: {str(e)}")
            return False

    async def _do_disconnect(self) -> None:
        """执行具体的断开连接逻辑"""
        if self._ws_client:
            await self._close_websocket()

        if self.exchange:
            # ccxt没有显式的close方法，只需清理引用
            self.exchange = None

    async def _do_authenticate(self) -> bool:
        """执行具体的认证逻辑"""
        try:
            # Hyperliquid通过私钥自动认证，测试API访问
            await self._execute_with_retry(
                self._fetch_account_balance,
                operation_name="authentication_test"
            )
            return True

        except Exception as e:
            self.logger.error(f"Hyperliquid认证失败: {str(e)}")
            return False

    async def _do_health_check(self) -> Dict[str, Any]:
        """执行具体的健康检查"""
        health_data = {
            'exchange_time': None,
            'market_count': 0,
            'api_accessible': False
        }

        try:
            # 检查API访问
            timestamp = await asyncio.get_event_loop().run_in_executor(
                None, self.exchange.fetch_time
            )
            health_data['exchange_time'] = datetime.fromtimestamp(
                timestamp / 1000)
            health_data['api_accessible'] = True

            # 检查市场数据
            if self.exchange.markets:
                health_data['market_count'] = len(self.exchange.markets)

            return health_data

        except Exception as e:
            health_data['error'] = str(e)
            return health_data

    async def _do_heartbeat(self) -> None:
        """执行心跳检测"""
        # 简单的时间检查作为心跳
        await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_time
        )

    # === 市场数据接口实现 ===

    async def get_exchange_info(self) -> ExchangeInfo:
        """获取交易所信息"""
        return ExchangeInfo(
            name="Hyperliquid",
            id="hyperliquid",
            type=ExchangeType.PERPETUAL,
            supported_features=[
                "spot_trading", "perpetual_trading", "websocket",
                "orderbook", "ticker", "ohlcv", "user_stream"
            ],
            rate_limits=self.config.rate_limits,
            precision=self.config.precision,
            fees={},  # TODO: 获取实际费率
            markets=self.exchange.markets if self.exchange else {},
            status="operational",
            timestamp=datetime.now()
        )

    async def get_ticker(self, symbol: str) -> TickerData:
        """获取单个交易对行情"""
        mapped_symbol = self._map_symbol(symbol)

        ticker_data = await self._execute_with_retry(
            self._fetch_ticker,
            mapped_symbol,
            operation_name="get_ticker"
        )

        return self._parse_ticker(ticker_data, symbol)

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """获取多个交易对行情"""
        if symbols:
            # 获取指定交易对行情
            tasks = [self.get_ticker(symbol) for symbol in symbols]
            return await asyncio.gather(*tasks)
        else:
            # 获取所有交易对行情
            tickers_data = await self._execute_with_retry(
                self._fetch_all_tickers,
                operation_name="get_tickers"
            )

            return [
                self._parse_ticker(
                    ticker_data, self._reverse_map_symbol(market_symbol))
                for market_symbol, ticker_data in tickers_data.items()
            ]

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿"""
        mapped_symbol = self._map_symbol(symbol)

        orderbook_data = await self._execute_with_retry(
            self._fetch_orderbook,
            mapped_symbol,
            limit,
            operation_name="get_orderbook"
        )

        return self._parse_orderbook(orderbook_data, symbol)

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OHLCVData]:
        """获取K线数据"""
        mapped_symbol = self._map_symbol(symbol)
        since_timestamp = int(since.timestamp() * 1000) if since else None

        ohlcv_data = await self._execute_with_retry(
            self._fetch_ohlcv,
            mapped_symbol,
            timeframe,
            since_timestamp,
            limit,
            operation_name="get_ohlcv"
        )

        return [
            self._parse_ohlcv(candle, symbol, timeframe)
            for candle in ohlcv_data
        ]

    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """获取最近成交记录"""
        mapped_symbol = self._map_symbol(symbol)
        since_timestamp = int(since.timestamp() * 1000) if since else None

        trades_data = await self._execute_with_retry(
            self._fetch_trades,
            mapped_symbol,
            since_timestamp,
            limit,
            operation_name="get_trades"
        )

        return [
            self._parse_trade(trade, symbol)
            for trade in trades_data
        ]

    # === 账户和交易接口实现 ===

    async def get_balances(self) -> List[BalanceData]:
        """获取账户余额"""
        balance_data = await self._execute_with_retry(
            self._fetch_account_balance,
            operation_name="get_balances"
        )

        return [
            self._parse_balance(currency, balance_info)
            for currency, balance_info in balance_data.items()
            if balance_info.get('total', 0) > 0
        ]

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """获取持仓信息"""
        positions_data = await self._execute_with_retry(
            self._fetch_positions,
            operation_name="get_positions"
        )

        positions = []
        for position_info in positions_data:
            position = self._parse_position(position_info)

            # 过滤指定符号
            if symbols is None or position.symbol in symbols:
                positions.append(position)

        return positions

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
        mapped_symbol = self._map_symbol(symbol)

        order_data = await self._execute_with_retry(
            self._place_order,
            mapped_symbol,
            order_type.value,
            side.value,
            float(amount),
            float(price) if price else None,
            params or {},
            operation_name="create_order"
        )

        order = self._parse_order(order_data, symbol)

        # 触发订单创建事件
        await self._handle_order_update(order)

        return order

    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """取消订单"""
        mapped_symbol = self._map_symbol(symbol)

        order_data = await self._execute_with_retry(
            self._cancel_single_order,
            order_id,
            mapped_symbol,
            operation_name="cancel_order"
        )

        order = self._parse_order(order_data, symbol)

        # 触发订单更新事件
        await self._handle_order_update(order)

        return order

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """取消所有订单"""
        if symbol:
            mapped_symbol = self._map_symbol(symbol)
            orders_data = await self._execute_with_retry(
                self._cancel_orders_by_symbol,
                mapped_symbol,
                operation_name="cancel_all_orders"
            )
        else:
            orders_data = await self._execute_with_retry(
                self._cancel_all_open_orders,
                operation_name="cancel_all_orders"
            )

        orders = []
        for order_data in orders_data:
            order = self._parse_order(
                order_data, symbol or order_data.get('symbol', ''))
            orders.append(order)

            # 触发订单更新事件
            await self._handle_order_update(order)

        return orders

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """获取订单信息"""
        mapped_symbol = self._map_symbol(symbol)

        order_data = await self._execute_with_retry(
            self._fetch_order_info,
            order_id,
            mapped_symbol,
            operation_name="get_order"
        )

        return self._parse_order(order_data, symbol)

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        if symbol:
            mapped_symbol = self._map_symbol(symbol)
            orders_data = await self._execute_with_retry(
                self._fetch_open_orders_by_symbol,
                mapped_symbol,
                operation_name="get_open_orders"
            )
        else:
            orders_data = await self._execute_with_retry(
                self._fetch_all_open_orders,
                operation_name="get_open_orders"
            )

        return [
            self._parse_order(order_data, symbol or self._reverse_map_symbol(
                order_data.get('symbol', '')))
            for order_data in orders_data
        ]

    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OrderData]:
        """获取历史订单"""
        if symbol:
            mapped_symbol = self._map_symbol(symbol)
        else:
            mapped_symbol = None

        since_timestamp = int(since.timestamp() * 1000) if since else None

        orders_data = await self._execute_with_retry(
            self._fetch_order_history,
            mapped_symbol,
            since_timestamp,
            limit,
            operation_name="get_order_history"
        )

        return [
            self._parse_order(order_data, symbol or self._reverse_map_symbol(
                order_data.get('symbol', '')))
            for order_data in orders_data
        ]

    # === 交易设置接口实现 ===

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数"""
        mapped_symbol = self._map_symbol(symbol)

        result = await self._execute_with_retry(
            self._set_position_leverage,
            mapped_symbol,
            leverage,
            operation_name="set_leverage"
        )

        return result

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式"""
        mapped_symbol = self._map_symbol(symbol)

        result = await self._execute_with_retry(
            self._set_position_margin_mode,
            mapped_symbol,
            margin_mode,
            operation_name="set_margin_mode"
        )

        return result

    # === 实时数据流接口实现 ===

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅行情数据流"""
        try:
            # 使用CCXT的WebSocket功能（如果支持）
            if hasattr(self.exchange, 'watch_ticker'):
                asyncio.create_task(self._watch_ticker(symbol, callback))
            else:
                self.logger.warning(f"Hyperliquid不支持WebSocket行情订阅，使用轮询模式")
                asyncio.create_task(self._poll_ticker(symbol, callback))
        except Exception as e:
            self.logger.error(f"订阅行情失败 {symbol}: {e}")

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """订阅订单簿数据流"""
        try:
            if hasattr(self.exchange, 'watch_order_book'):
                asyncio.create_task(self._watch_orderbook(symbol, callback))
            else:
                self.logger.warning(f"Hyperliquid不支持WebSocket订单簿订阅，使用轮询模式")
                asyncio.create_task(self._poll_orderbook(symbol, callback))
        except Exception as e:
            self.logger.error(f"订阅订单簿失败 {symbol}: {e}")

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅成交数据流"""
        try:
            if hasattr(self.exchange, 'watch_trades'):
                asyncio.create_task(self._watch_trades(symbol, callback))
            else:
                self.logger.warning(f"Hyperliquid不支持WebSocket成交订阅，使用轮询模式")
                asyncio.create_task(self._poll_trades(symbol, callback))
        except Exception as e:
            self.logger.error(f"订阅成交失败 {symbol}: {e}")

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """订阅用户数据流"""
        try:
            if hasattr(self.exchange, 'watch_balance') or hasattr(self.exchange, 'watch_orders'):
                asyncio.create_task(self._watch_user_data(callback))
            else:
                self.logger.warning(f"Hyperliquid不支持WebSocket用户数据订阅，使用轮询模式")
                asyncio.create_task(self._poll_user_data(callback))
        except Exception as e:
            self.logger.error(f"订阅用户数据失败: {e}")

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        # 标记停止轮询
        if not hasattr(self, '_stop_polling'):
            self._stop_polling = set()

        if symbol:
            self._stop_polling.add(symbol)
        else:
            self._stop_polling.add('ALL')

    # === WebSocket/轮询实现方法 ===

    async def _watch_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """WebSocket行情监听"""
        try:
            while symbol not in getattr(self, '_stop_polling', set()) and 'ALL' not in getattr(self, '_stop_polling', set()):
                ticker_data = await asyncio.get_event_loop().run_in_executor(
                    None, self.exchange.watch_ticker, symbol
                )
                ticker = self._parse_ticker(ticker_data, symbol)
                await self._safe_callback(callback, ticker)
        except Exception as e:
            self.logger.error(f"WebSocket行情监听失败 {symbol}: {e}")

    async def _watch_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """WebSocket订单簿监听"""
        try:
            while symbol not in getattr(self, '_stop_polling', set()) and 'ALL' not in getattr(self, '_stop_polling', set()):
                orderbook_data = await asyncio.get_event_loop().run_in_executor(
                    None, self.exchange.watch_order_book, symbol
                )
                orderbook = self._parse_orderbook(orderbook_data, symbol)
                await self._safe_callback(callback, orderbook)
        except Exception as e:
            self.logger.error(f"WebSocket订单簿监听失败 {symbol}: {e}")

    async def _watch_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """WebSocket成交监听"""
        try:
            while symbol not in getattr(self, '_stop_polling', set()) and 'ALL' not in getattr(self, '_stop_polling', set()):
                trades_data = await asyncio.get_event_loop().run_in_executor(
                    None, self.exchange.watch_trades, symbol
                )
                for trade_data in trades_data:
                    trade = self._parse_trade(trade_data, symbol)
                    await self._safe_callback(callback, trade)
        except Exception as e:
            self.logger.error(f"WebSocket成交监听失败 {symbol}: {e}")

    async def _watch_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """WebSocket用户数据监听"""
        try:
            while 'ALL' not in getattr(self, '_stop_polling', set()):
                # 监听余额变化
                if hasattr(self.exchange, 'watch_balance'):
                    balance_data = await asyncio.get_event_loop().run_in_executor(
                        None, self.exchange.watch_balance
                    )
                    await self._safe_callback(callback, {'type': 'balance', 'data': balance_data})

                # 监听订单变化
                if hasattr(self.exchange, 'watch_orders'):
                    orders_data = await asyncio.get_event_loop().run_in_executor(
                        None, self.exchange.watch_orders
                    )
                    await self._safe_callback(callback, {'type': 'orders', 'data': orders_data})

                await asyncio.sleep(1)  # 防止过于频繁的检查
        except Exception as e:
            self.logger.error(f"WebSocket用户数据监听失败: {e}")

    # 轮询模式实现
    async def _poll_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """轮询行情数据"""
        try:
            while symbol not in getattr(self, '_stop_polling', set()) and 'ALL' not in getattr(self, '_stop_polling', set()):
                ticker = await self.get_ticker(symbol)
                await self._safe_callback(callback, ticker)
                await asyncio.sleep(1)  # 1秒轮询间隔
        except Exception as e:
            self.logger.error(f"轮询行情失败 {symbol}: {e}")

    async def _poll_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """轮询订单簿数据"""
        try:
            while symbol not in getattr(self, '_stop_polling', set()) and 'ALL' not in getattr(self, '_stop_polling', set()):
                orderbook = await self.get_orderbook(symbol)
                await self._safe_callback(callback, orderbook)
                await asyncio.sleep(0.5)  # 0.5秒轮询间隔
        except Exception as e:
            self.logger.error(f"轮询订单簿失败 {symbol}: {e}")

    async def _poll_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """轮询成交数据"""
        try:
            last_trade_id = None
            while symbol not in getattr(self, '_stop_polling', set()) and 'ALL' not in getattr(self, '_stop_polling', set()):
                trades = await self.get_trades(symbol, limit=10)
                # 只推送新的成交
                for trade in trades:
                    if last_trade_id is None or trade.id != last_trade_id:
                        await self._safe_callback(callback, trade)
                        last_trade_id = trade.id
                await asyncio.sleep(1)  # 1秒轮询间隔
        except Exception as e:
            self.logger.error(f"轮询成交失败 {symbol}: {e}")

    async def _poll_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """轮询用户数据"""
        try:
            last_balances = {}
            last_orders = {}

            while 'ALL' not in getattr(self, '_stop_polling', set()):
                # 检查余额变化
                try:
                    current_balances = await self.get_balances()
                    if str(current_balances) != str(last_balances):
                        await self._safe_callback(callback, {'type': 'balance', 'data': current_balances})
                        last_balances = current_balances
                except Exception:
                    pass

                # 检查订单变化
                try:
                    current_orders = await self.get_open_orders()
                    if str(current_orders) != str(last_orders):
                        await self._safe_callback(callback, {'type': 'orders', 'data': current_orders})
                        last_orders = current_orders
                except Exception:
                    pass

                await asyncio.sleep(2)  # 2秒轮询间隔
        except Exception as e:
            self.logger.error(f"轮询用户数据失败: {e}")

    async def _safe_callback(self, callback: Callable, data: Any) -> None:
        """安全调用回调函数"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            self.logger.error(f"回调函数执行失败: {e}")

    # === 私有方法实现 ===

    def _map_symbol(self, symbol: str) -> str:
        """映射交易对符号"""
        return self._default_symbol_mapping.get(symbol, symbol)

    def _reverse_map_symbol(self, exchange_symbol: str) -> str:
        """反向映射交易对符号"""
        reverse_mapping = {v: k for k,
                           v in self._default_symbol_mapping.items()}
        return reverse_mapping.get(exchange_symbol, exchange_symbol)

    # ccxt API调用方法
    async def _fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """获取行情数据"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_ticker, symbol
        )

    async def _fetch_all_tickers(self) -> Dict[str, Any]:
        """获取所有行情数据"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_tickers
        )

    async def _fetch_orderbook(self, symbol: str, limit: Optional[int]) -> Dict[str, Any]:
        """获取订单簿"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_order_book, symbol, limit
        )

    async def _fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[int],
        limit: Optional[int]
    ) -> List[List[float]]:
        """获取K线数据"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_ohlcv, symbol, timeframe, since, limit
        )

    async def _fetch_trades(
        self,
        symbol: str,
        since: Optional[int],
        limit: Optional[int]
    ) -> List[Dict[str, Any]]:
        """获取成交数据"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_trades, symbol, since, limit
        )

    async def _fetch_account_balance(self) -> Dict[str, Any]:
        """获取账户余额"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_balance
        )

    async def _fetch_positions(self) -> List[Dict[str, Any]]:
        """获取持仓信息"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_positions
        )

    async def _place_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: float,
        price: Optional[float],
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """下单"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.create_order, symbol, order_type, side, amount, price, params
        )

    async def _cancel_single_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """取消单个订单"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.cancel_order, order_id, symbol
        )

    async def _cancel_orders_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """取消指定交易对的所有订单"""
        orders = await self._fetch_open_orders_by_symbol(symbol)
        results = []
        for order in orders:
            try:
                result = await self._cancel_single_order(order['id'], symbol)
                results.append(result)
            except Exception as e:
                self.logger.error(f"取消订单失败 {order['id']}: {str(e)}")
        return results

    async def _cancel_all_open_orders(self) -> List[Dict[str, Any]]:
        """取消所有开放订单"""
        orders = await self._fetch_all_open_orders()
        results = []
        for order in orders:
            try:
                result = await self._cancel_single_order(order['id'], order['symbol'])
                results.append(result)
            except Exception as e:
                self.logger.error(f"取消订单失败 {order['id']}: {str(e)}")
        return results

    async def _fetch_order_info(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """获取订单信息"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_order, order_id, symbol
        )

    async def _fetch_open_orders_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """获取指定交易对的开放订单"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_open_orders, symbol
        )

    async def _fetch_all_open_orders(self) -> List[Dict[str, Any]]:
        """获取所有开放订单"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_open_orders
        )

    async def _fetch_order_history(
        self,
        symbol: Optional[str],
        since: Optional[int],
        limit: Optional[int]
    ) -> List[Dict[str, Any]]:
        """获取历史订单"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_orders, symbol, since, limit
        )

    async def _set_position_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置持仓杠杆"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.set_leverage, leverage, symbol
        )

    async def _set_position_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置持仓保证金模式"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.set_margin_mode, margin_mode, symbol
        )

    # 数据解析方法
    def _parse_ticker(self, ticker_data: Dict[str, Any], symbol: str) -> TickerData:
        """解析行情数据"""
        return TickerData(
            symbol=symbol,
            bid=self._safe_decimal(ticker_data.get('bid')),
            ask=self._safe_decimal(ticker_data.get('ask')),
            last=self._safe_decimal(ticker_data.get('last')),
            open=self._safe_decimal(ticker_data.get('open')),
            high=self._safe_decimal(ticker_data.get('high')),
            low=self._safe_decimal(ticker_data.get('low')),
            close=self._safe_decimal(ticker_data.get('close')),
            volume=self._safe_decimal(ticker_data.get('baseVolume')),
            quote_volume=self._safe_decimal(ticker_data.get('quoteVolume')),
            change=self._safe_decimal(ticker_data.get('change')),
            percentage=self._safe_decimal(ticker_data.get('percentage')),
            timestamp=datetime.fromtimestamp(
                ticker_data.get('timestamp', 0) / 1000),
            raw_data=ticker_data
        )

    def _parse_orderbook(self, orderbook_data: Dict[str, Any], symbol: str) -> OrderBookData:
        """解析订单簿数据"""
        bids = [
            OrderBookLevel(
                price=self._safe_decimal(bid[0]),
                size=self._safe_decimal(bid[1])
            )
            for bid in orderbook_data.get('bids', [])
        ]

        asks = [
            OrderBookLevel(
                price=self._safe_decimal(ask[0]),
                size=self._safe_decimal(ask[1])
            )
            for ask in orderbook_data.get('asks', [])
        ]

        return OrderBookData(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=datetime.fromtimestamp(
                orderbook_data.get('timestamp', 0) / 1000),
            nonce=orderbook_data.get('nonce'),
            raw_data=orderbook_data
        )

    def _parse_ohlcv(self, candle: List[float], symbol: str, timeframe: str) -> OHLCVData:
        """解析K线数据"""
        return OHLCVData(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=datetime.fromtimestamp(candle[0] / 1000),
            open=self._safe_decimal(candle[1]),
            high=self._safe_decimal(candle[2]),
            low=self._safe_decimal(candle[3]),
            close=self._safe_decimal(candle[4]),
            volume=self._safe_decimal(candle[5]),
            quote_volume=None,
            trades_count=None,
            raw_data={'candle': candle}
        )

    def _parse_trade(self, trade_data: Dict[str, Any], symbol: str) -> TradeData:
        """解析成交数据"""
        return TradeData(
            id=str(trade_data.get('id', '')),
            symbol=symbol,
            side=OrderSide.BUY if trade_data.get(
                'side') == 'buy' else OrderSide.SELL,
            amount=self._safe_decimal(trade_data.get('amount')),
            price=self._safe_decimal(trade_data.get('price')),
            cost=self._safe_decimal(trade_data.get('cost')),
            fee=trade_data.get('fee'),
            timestamp=datetime.fromtimestamp(
                trade_data.get('timestamp', 0) / 1000),
            order_id=trade_data.get('order'),
            raw_data=trade_data
        )

    def _parse_balance(self, currency: str, balance_info: Dict[str, Any]) -> BalanceData:
        """解析余额数据"""
        return BalanceData(
            currency=currency,
            free=self._safe_decimal(balance_info.get('free')),
            used=self._safe_decimal(balance_info.get('used')),
            total=self._safe_decimal(balance_info.get('total')),
            usd_value=None,
            timestamp=datetime.now(),
            raw_data=balance_info
        )

    def _parse_position(self, position_info: Dict[str, Any]) -> PositionData:
        """解析持仓数据"""
        symbol = self._reverse_map_symbol(position_info.get('symbol', ''))
        side = PositionSide.LONG if position_info.get(
            'side') == 'long' else PositionSide.SHORT

        return PositionData(
            symbol=symbol,
            side=side,
            size=self._safe_decimal(position_info.get('contracts', 0)),
            entry_price=self._safe_decimal(position_info.get('entryPrice')),
            mark_price=self._safe_decimal(position_info.get('markPrice')),
            current_price=self._safe_decimal(position_info.get('markPrice')),
            unrealized_pnl=self._safe_decimal(
                position_info.get('unrealizedPnl')),
            realized_pnl=self._safe_decimal(position_info.get('realizedPnl')),
            percentage=self._safe_decimal(position_info.get('percentage')),
            leverage=self._safe_int(position_info.get('leverage', 1)),
            margin_mode=MarginMode.CROSS if position_info.get(
                'marginType') == 'cross' else MarginMode.ISOLATED,
            margin=self._safe_decimal(position_info.get('initialMargin')),
            liquidation_price=self._safe_decimal(
                position_info.get('liquidationPrice')),
            timestamp=datetime.now(),
            raw_data=position_info
        )

    def _parse_order(self, order_data: Dict[str, Any], symbol: str) -> OrderData:
        """解析订单数据"""
        # 映射订单状态
        status_mapping = {
            'open': OrderStatus.OPEN,
            'closed': OrderStatus.FILLED,
            'canceled': OrderStatus.CANCELED,
            'cancelled': OrderStatus.CANCELED,
            'rejected': OrderStatus.REJECTED,
            'expired': OrderStatus.EXPIRED
        }

        status = status_mapping.get(
            order_data.get('status'), OrderStatus.UNKNOWN)

        # 映射订单类型
        type_mapping = {
            'market': OrderType.MARKET,
            'limit': OrderType.LIMIT,
            'stop': OrderType.STOP,
            'stop_limit': OrderType.STOP_LIMIT,
            'take_profit': OrderType.TAKE_PROFIT,
            'take_profit_limit': OrderType.TAKE_PROFIT_LIMIT
        }

        order_type = type_mapping.get(order_data.get('type'), OrderType.LIMIT)

        return OrderData(
            id=str(order_data.get('id', '')),
            client_id=order_data.get('clientOrderId'),
            symbol=symbol,
            side=OrderSide.BUY if order_data.get(
                'side') == 'buy' else OrderSide.SELL,
            type=order_type,
            amount=self._safe_decimal(order_data.get('amount')),
            price=self._safe_decimal(order_data.get('price')),
            filled=self._safe_decimal(order_data.get('filled')),
            remaining=self._safe_decimal(order_data.get('remaining')),
            cost=self._safe_decimal(order_data.get('cost')),
            average=self._safe_decimal(order_data.get('average')),
            status=status,
            timestamp=datetime.fromtimestamp(
                order_data.get('timestamp', 0) / 1000),
            updated=datetime.fromtimestamp(order_data.get(
                'lastTradeTimestamp', 0) / 1000) if order_data.get('lastTradeTimestamp') else None,
            fee=order_data.get('fee'),
            trades=order_data.get('trades', []),
            params={},
            raw_data=order_data
        )
