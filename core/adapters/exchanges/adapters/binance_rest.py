"""
Binance交易所REST API模块 - 重构版

包含Binance交易所的REST API接口实现
使用 ccxt 库进行API调用，支持期货和现货交易
"""

import asyncio
import ccxt
import hmac
import hashlib
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from decimal import Decimal

from .binance_base import BinanceBase
from ..models import (
    TickerData, OrderBookData, TradeData, BalanceData, OrderData,
    PositionData, OHLCVData, ExchangeInfo, ExchangeType,
    OrderSide, OrderType, OrderStatus
)


class BinanceRest(BinanceBase):
    """Binance REST API接口实现"""
    
    def __init__(self, config, logger=None):
        super().__init__(config)
        self.logger = logger
        self.exchange = None
        
        # API限制配置
        self.rate_limit_orders = 1200  # 1分钟内最大订单数
        self.rate_limit_requests = 2400  # 1分钟内最大请求数
        
        # 重试配置
        self.max_retries = 3
        self.retry_delay = 1.0
        
    async def initialize(self) -> bool:
        """初始化CCXT交易所实例"""
        try:
            # 创建ccxt交易所实例
            self.exchange = ccxt.binance(self.ccxt_config)
            
            # 加载市场信息
            await asyncio.get_event_loop().run_in_executor(
                None, self.exchange.load_markets
            )
            
            # 缓存市场信息
            self._market_info = self.exchange.markets
            
            # 测试API连接
            await asyncio.get_event_loop().run_in_executor(
                None, self.exchange.fetch_time
            )
            
            if self.logger:
                self.logger.info(f"✅ Binance REST初始化成功，加载 {len(self.exchange.markets)} 个市场")
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Binance REST初始化失败: {str(e)}")
            return False
    
    async def close(self):
        """关闭连接"""
        if self.exchange:
            # ccxt没有显式的close方法，只需清理引用
            self.exchange = None
    
    async def _execute_with_retry(self, func, *args, **kwargs):
        """带重试的API调用"""
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, func, *args, **kwargs
                )
                return result
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    if self.logger:
                        self.logger.warning(f"API调用失败 (尝试 {attempt + 1}/{self.max_retries}): {str(e)}")
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    if self.logger:
                        self.logger.error(f"API调用最终失败: {str(e)}")
        
        raise last_error
    
    # ==================== 市场数据接口 ====================
    
    async def get_exchange_info(self) -> ExchangeInfo:
        """获取交易所信息"""
        try:
            # 获取服务器时间
            server_time = await self._execute_with_retry(self.exchange.fetch_time)
            
            return ExchangeInfo(
                name="Binance Futures",
                id="binance",
                type=ExchangeType.FUTURES,
                supported_features=[
                    "spot_trading", "futures_trading", "websocket",
                    "orderbook", "ticker", "ohlcv", "user_stream"
                ],
                rate_limits={
                    'orders': self.rate_limit_orders,
                    'requests': self.rate_limit_requests
                },
                precision={},  # 从市场数据中获取
                fees={},  # TODO: 获取实际费率
                markets=self._market_info,
                status="operational",
                timestamp=datetime.fromtimestamp(server_time / 1000)
            )
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取交易所信息失败: {str(e)}")
            raise
    
    async def get_ticker(self, symbol: str) -> TickerData:
        """获取单个行情数据"""
        try:
            mapped_symbol = self.map_symbol_to_binance(symbol)
            ticker_data = await self._execute_with_retry(
                self.exchange.fetch_ticker, mapped_symbol
            )
            return self.parse_ticker(ticker_data, symbol)
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取行情失败 {symbol}: {str(e)}")
            raise
    
    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """获取多个行情数据"""
        try:
            if symbols:
                # 并发获取指定符号的行情
                tasks = [self.get_ticker(symbol) for symbol in symbols]
                return await asyncio.gather(*tasks)
            else:
                # 获取所有行情
                tickers_data = await self._execute_with_retry(
                    self.exchange.fetch_tickers
                )
                
                result = []
                for market_symbol, ticker_data in tickers_data.items():
                    symbol = self.map_symbol_from_binance(market_symbol)
                    result.append(self.parse_ticker(ticker_data, symbol))
                
                return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取多个行情失败: {str(e)}")
            raise
    
    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿"""
        try:
            mapped_symbol = self.map_symbol_to_binance(symbol)
            orderbook_data = await self._execute_with_retry(
                self.exchange.fetch_order_book, mapped_symbol, limit
            )
            return self.parse_orderbook(orderbook_data, symbol)
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取订单簿失败 {symbol}: {str(e)}")
            raise
    
    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OHLCVData]:
        """获取K线数据"""
        try:
            mapped_symbol = self.map_symbol_to_binance(symbol)
            since_timestamp = int(since.timestamp() * 1000) if since else None
            
            ohlcv_data = await self._execute_with_retry(
                self.exchange.fetch_ohlcv,
                mapped_symbol,
                timeframe,
                since_timestamp,
                limit
            )
            
            result = []
            for candle in ohlcv_data:
                ohlcv = OHLCVData(
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
                result.append(ohlcv)
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取K线数据失败 {symbol}: {str(e)}")
            raise
    
    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """获取成交数据"""
        try:
            mapped_symbol = self.map_symbol_to_binance(symbol)
            since_timestamp = int(since.timestamp() * 1000) if since else None
            
            trades_data = await self._execute_with_retry(
                self.exchange.fetch_trades,
                mapped_symbol,
                since_timestamp,
                limit
            )
            
            return [self.parse_trade(trade, symbol) for trade in trades_data]
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取成交数据失败 {symbol}: {str(e)}")
            raise
    
    # ==================== 账户接口 ====================
    
    async def get_balances(self) -> List[BalanceData]:
        """获取账户余额"""
        try:
            balance_data = await self._execute_with_retry(
                self.exchange.fetch_balance
            )
            
            result = []
            for currency, balance_info in balance_data.items():
                if currency in ['free', 'used', 'total', 'info']:
                    continue
                
                if balance_info.get('total', 0) > 0:
                    balance = BalanceData(
                        currency=currency,
                        free=self._safe_decimal(balance_info.get('free')),
                        used=self._safe_decimal(balance_info.get('used')),
                        total=self._safe_decimal(balance_info.get('total')),
                        usd_value=None,
                        timestamp=datetime.now(),
                        raw_data=balance_info
                    )
                    result.append(balance)
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取账户余额失败: {str(e)}")
            raise
    
    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """获取持仓信息"""
        try:
            positions_data = await self._execute_with_retry(
                self.exchange.fetch_positions
            )
            
            result = []
            for position_info in positions_data:
                # 只返回有持仓的合约
                if float(position_info.get('contracts', 0)) == 0:
                    continue
                
                position = self.parse_position(position_info)
                
                # 过滤指定符号
                if symbols is None or position.symbol in symbols:
                    result.append(position)
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取持仓信息失败: {str(e)}")
            raise
    
    # ==================== 交易接口 ====================
    
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
        try:
            mapped_symbol = self.map_symbol_to_binance(symbol)
            
            # 准备订单参数
            order_params = params or {}
            
            order_data = await self._execute_with_retry(
                self.exchange.create_order,
                mapped_symbol,
                self.ORDER_TYPE_MAPPING.get(order_type, 'LIMIT'),
                side.value.lower(),
                float(amount),
                float(price) if price else None,
                order_params
            )
            
            return self.parse_order(order_data, symbol)
        except Exception as e:
            if self.logger:
                self.logger.error(f"创建订单失败 {symbol}: {str(e)}")
            raise
    
    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """取消订单"""
        try:
            mapped_symbol = self.map_symbol_to_binance(symbol)
            
            order_data = await self._execute_with_retry(
                self.exchange.cancel_order,
                order_id,
                mapped_symbol
            )
            
            return self.parse_order(order_data, symbol)
        except Exception as e:
            if self.logger:
                self.logger.error(f"取消订单失败 {order_id}: {str(e)}")
            raise
    
    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """取消所有订单"""
        try:
            if symbol:
                # 取消指定符号的所有订单
                mapped_symbol = self.map_symbol_to_binance(symbol)
                open_orders = await self._execute_with_retry(
                    self.exchange.fetch_open_orders, mapped_symbol
                )
            else:
                # 取消所有订单
                open_orders = await self._execute_with_retry(
                    self.exchange.fetch_open_orders
                )
            
            result = []
            for order in open_orders:
                try:
                    cancelled_order = await self._execute_with_retry(
                        self.exchange.cancel_order,
                        order['id'],
                        order['symbol']
                    )
                    result.append(self.parse_order(
                        cancelled_order, 
                        symbol or self.map_symbol_from_binance(order['symbol'])
                    ))
                except Exception as e:
                    if self.logger:
                        self.logger.warning(f"取消订单失败 {order['id']}: {str(e)}")
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"取消所有订单失败: {str(e)}")
            raise
    
    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """获取订单信息"""
        try:
            mapped_symbol = self.map_symbol_to_binance(symbol)
            
            order_data = await self._execute_with_retry(
                self.exchange.fetch_order,
                order_id,
                mapped_symbol
            )
            
            return self.parse_order(order_data, symbol)
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取订单信息失败 {order_id}: {str(e)}")
            raise
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        try:
            if symbol:
                mapped_symbol = self.map_symbol_to_binance(symbol)
                orders_data = await self._execute_with_retry(
                    self.exchange.fetch_open_orders, mapped_symbol
                )
            else:
                orders_data = await self._execute_with_retry(
                    self.exchange.fetch_open_orders
                )
            
            result = []
            for order_data in orders_data:
                order_symbol = symbol or self.map_symbol_from_binance(order_data.get('symbol', ''))
                result.append(self.parse_order(order_data, order_symbol))
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取开放订单失败: {str(e)}")
            raise
    
    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OrderData]:
        """获取历史订单"""
        try:
            mapped_symbol = self.map_symbol_to_binance(symbol) if symbol else None
            since_timestamp = int(since.timestamp() * 1000) if since else None
            
            orders_data = await self._execute_with_retry(
                self.exchange.fetch_orders,
                mapped_symbol,
                since_timestamp,
                limit
            )
            
            result = []
            for order_data in orders_data:
                order_symbol = symbol or self.map_symbol_from_binance(order_data.get('symbol', ''))
                result.append(self.parse_order(order_data, order_symbol))
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取历史订单失败: {str(e)}")
            raise
    
    # ==================== 设置接口 ====================
    
    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数"""
        try:
            mapped_symbol = self.map_symbol_to_binance(symbol)
            
            # Binance期货设置杠杆
            result = await self._execute_with_retry(
                self.exchange.set_leverage,
                leverage,
                mapped_symbol
            )
            
            if self.logger:
                self.logger.info(f"✅ 设置杠杆成功 {symbol}: {leverage}x")
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"设置杠杆失败 {symbol}: {str(e)}")
            raise
    
    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式"""
        try:
            mapped_symbol = self.map_symbol_to_binance(symbol)
            
            # Binance期货设置保证金模式
            result = await self._execute_with_retry(
                self.exchange.set_margin_mode,
                margin_mode,
                mapped_symbol
            )
            
            if self.logger:
                self.logger.info(f"✅ 设置保证金模式成功 {symbol}: {margin_mode}")
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"设置保证金模式失败 {symbol}: {str(e)}")
            raise
    
    # ==================== 健康检查 ====================
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        health_data = {
            'api_accessible': False,
            'exchange_time': None,
            'market_count': 0,
            'server_time_diff': None
        }
        
        try:
            # 检查API访问
            start_time = time.time()
            server_time = await self._execute_with_retry(self.exchange.fetch_time)
            end_time = time.time()
            
            health_data['api_accessible'] = True
            health_data['exchange_time'] = datetime.fromtimestamp(server_time / 1000)
            health_data['server_time_diff'] = (end_time - start_time) * 1000  # 毫秒
            
            # 检查市场数据
            if self._market_info:
                health_data['market_count'] = len(self._market_info)
            
            return health_data
        except Exception as e:
            health_data['error'] = str(e)
            if self.logger:
                self.logger.error(f"健康检查失败: {str(e)}")
            return health_data
    
    async def heartbeat(self) -> bool:
        """心跳检查"""
        try:
            await self._execute_with_retry(self.exchange.fetch_time)
            return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"心跳检查失败: {str(e)}")
            return False 