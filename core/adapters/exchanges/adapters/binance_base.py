"""
Binance交易所基础模块 - 重构版

包含Binance交易所的基础配置、数据解析等公共功能
重构：遵循MESA架构，简化符号映射，支持统一符号转换服务
"""

import time
import decimal
from typing import Dict, List, Optional, Any, Union
from decimal import Decimal
from datetime import datetime
from enum import Enum

from ..models import (
    TickerData, OrderBookData, TradeData, BalanceData, OrderData, 
    OrderSide, OrderType, OrderStatus, PositionData, PositionSide,
    MarginMode, OrderBookLevel, ExchangeInfo, ExchangeType
)


class BinanceMarketType(Enum):
    """Binance市场类型"""
    SPOT = "spot"
    FUTURES = "future"
    DELIVERY = "delivery"


class BinanceSymbolInfo:
    """Binance交易对信息类"""
    
    def __init__(self, symbol: str, base: str, quote: str, market_type: str = "future"):
        self.symbol = symbol
        self.base = base
        self.quote = quote
        self.market_type = market_type
        self.min_qty = Decimal('0.001')
        self.max_qty = Decimal('1000000')
        self.min_price = Decimal('0.01')
        self.max_price = Decimal('1000000')
        self.price_precision = 8
        self.qty_precision = 8
        self.step_size = Decimal('0.001')
        self.tick_size = Decimal('0.01')


class BinanceBase:
    """Binance基础功能类 - 重构版"""
    
    # 默认配置
    DEFAULT_BASE_URL = "https://fapi.binance.com"  # 期货API
    DEFAULT_SPOT_URL = "https://api.binance.com"   # 现货API
    DEFAULT_WS_URL = "wss://fstream.binance.com/ws"  # 期货WebSocket
    DEFAULT_SPOT_WS_URL = "wss://stream.binance.com:9443/ws"  # 现货WebSocket
    
    # 测试网地址
    TESTNET_BASE_URL = "https://testnet.binancefuture.com"
    TESTNET_WS_URL = "wss://stream.binancefuture.com/ws"
    
    # 支持的时间周期
    SUPPORTED_TIMEFRAMES = {
        '1m': '1m',
        '3m': '3m',
        '5m': '5m',
        '15m': '15m',
        '30m': '30m',
        '1h': '1h',
        '2h': '2h',
        '4h': '4h',
        '6h': '6h',
        '8h': '8h',
        '12h': '12h',
        '1d': '1d',
        '3d': '3d',
        '1w': '1w',
        '1M': '1M'
    }
    
    # 订单类型映射
    ORDER_TYPE_MAPPING = {
        OrderType.MARKET: 'MARKET',
        OrderType.LIMIT: 'LIMIT',
        OrderType.STOP: 'STOP',
        OrderType.STOP_LIMIT: 'STOP_MARKET',  # Binance的STOP_MARKET对应我们的STOP_LIMIT
        OrderType.TAKE_PROFIT: 'TAKE_PROFIT',
        OrderType.TAKE_PROFIT_LIMIT: 'TAKE_PROFIT_MARKET'  # Binance的TAKE_PROFIT_MARKET对应我们的TAKE_PROFIT_LIMIT
    }
    
    # 订单状态映射
    ORDER_STATUS_MAPPING = {
        'NEW': OrderStatus.OPEN,
        'PARTIALLY_FILLED': OrderStatus.OPEN,  # 部分成交仍然是OPEN状态
        'FILLED': OrderStatus.FILLED,
        'CANCELED': OrderStatus.CANCELED,
        'REJECTED': OrderStatus.REJECTED,
        'EXPIRED': OrderStatus.EXPIRED
    }
    
    def __init__(self, config=None):
        self.config = config
        self.logger = None
        
        # 基础配置
        self._setup_urls()
        
        # 市场类型配置
        self.market_type = getattr(config, 'market_type', BinanceMarketType.FUTURES.value)
        self.testnet = getattr(config, 'testnet', False)
        
        # 🔥 重构：简化符号映射
        self._setup_legacy_symbol_mapping()
        
        # 支持的交易对和映射
        self._supported_symbols = []
        self._market_info = {}
        
        # CCXT兼容配置
        self.ccxt_config = self._setup_ccxt_config()
        
    def _setup_urls(self):
        """设置API URL"""
        if self.config:
            # 优先使用配置中的URL
            self.base_url = getattr(self.config, 'base_url', None)
            self.ws_url = getattr(self.config, 'ws_url', None)
            
            # 根据测试网配置调整
            if getattr(self.config, 'testnet', False):
                self.base_url = self.base_url or self.TESTNET_BASE_URL
                self.ws_url = self.ws_url or self.TESTNET_WS_URL
            else:
                self.base_url = self.base_url or self.DEFAULT_BASE_URL
                self.ws_url = self.ws_url or self.DEFAULT_WS_URL
        else:
            self.base_url = self.DEFAULT_BASE_URL
            self.ws_url = self.DEFAULT_WS_URL
    
    def _setup_legacy_symbol_mapping(self):
        """
        设置遗留符号映射（已弃用）
        
        @deprecated: 建议使用统一的符号转换服务
        """
        # 默认符号映射（通用格式 -> Binance永续合约格式）
        self._symbol_mapping = {
            "BTC": "BTC/USDT:USDT",
            "ETH": "ETH/USDT:USDT", 
            "SOL": "SOL/USDT:USDT",
            "AVAX": "AVAX/USDT:USDT",
            "BTC/USDC:PERP": "BTC/USDT:USDT",
            "ETH/USDC:PERP": "ETH/USDT:USDT", 
            "SOL/USDC:PERP": "SOL/USDT:USDT",
            "AVAX/USDC:PERP": "AVAX/USDT:USDT"
        }
        
        # 只保留配置中明确定义的映射
        if self.config and hasattr(self.config, 'symbol_mapping') and self.config.symbol_mapping:
            self._symbol_mapping.update(self.config.symbol_mapping)
    
    def _setup_ccxt_config(self) -> Dict[str, Any]:
        """设置CCXT配置"""
        config = {
            'enableRateLimit': True,
            'options': {
                'defaultType': 'swap',  # 默认使用永续合约（swap）
                'adjustForTimeDifference': True,
                'recvWindow': 60000
            }
        }
        
        if self.config:
            config['apiKey'] = getattr(self.config, 'api_key', '')
            config['secret'] = getattr(self.config, 'api_secret', '')
            
            if self.testnet:
                config['sandbox'] = True
                config['urls'] = {'api': self.base_url}
        
        return config
    
    def set_logger(self, logger):
        """设置日志器"""
        self.logger = logger
    
    def _normalize_binance_symbol(self, symbol: str) -> str:
        """标准化Binance符号格式"""
        # Binance符号格式通常是 BTCUSDT, ETHUSDT 等
        return symbol.upper()
    
    def map_symbol_to_binance(self, symbol: str) -> str:
        """映射通用符号到Binance格式"""
        return self._symbol_mapping.get(symbol, symbol)
    
    def map_symbol_from_binance(self, binance_symbol: str) -> str:
        """反向映射Binance符号到通用格式"""
        reverse_mapping = {v: k for k, v in self._symbol_mapping.items()}
        return reverse_mapping.get(binance_symbol, binance_symbol)
    
    def _safe_decimal(self, value: Any) -> Optional[Decimal]:
        """安全转换为Decimal类型"""
        if value is None:
            return None
        try:
            if isinstance(value, str) and value == '':
                return None
            return Decimal(str(value))
        except (decimal.InvalidOperation, ValueError, TypeError):
            if self.logger:
                self.logger.warning(f"无法转换为Decimal: {value}")
            return None
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """安全转换为int类型"""
        if value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            if self.logger:
                self.logger.warning(f"无法转换为int: {value}")
            return None
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """安全转换为float类型"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            if self.logger:
                self.logger.warning(f"无法转换为float: {value}")
            return None
    
    def parse_ticker(self, ticker_data: Dict[str, Any], symbol: str) -> TickerData:
        """解析行情数据"""
        return TickerData(
            symbol=symbol,
            bid=self._safe_decimal(ticker_data.get('bidPrice')),
            ask=self._safe_decimal(ticker_data.get('askPrice')),
            last=self._safe_decimal(ticker_data.get('lastPrice')),
            open=self._safe_decimal(ticker_data.get('openPrice')),
            high=self._safe_decimal(ticker_data.get('highPrice')),
            low=self._safe_decimal(ticker_data.get('lowPrice')),
            close=self._safe_decimal(ticker_data.get('lastPrice')),
            volume=self._safe_decimal(ticker_data.get('volume')),
            quote_volume=self._safe_decimal(ticker_data.get('quoteVolume')),
            change=self._safe_decimal(ticker_data.get('priceChange')),
            percentage=self._safe_decimal(ticker_data.get('priceChangePercent')),
            timestamp=datetime.fromtimestamp(ticker_data.get('closeTime', 0) / 1000),
            raw_data=ticker_data
        )
    
    def parse_orderbook(self, orderbook_data: Dict[str, Any], symbol: str) -> OrderBookData:
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
            timestamp=datetime.fromtimestamp(orderbook_data.get('T', 0) / 1000),
            nonce=orderbook_data.get('lastUpdateId'),
            raw_data=orderbook_data
        )
    
    def parse_trade(self, trade_data: Dict[str, Any], symbol: str) -> TradeData:
        """解析成交数据"""
        return TradeData(
            id=str(trade_data.get('id', '')),
            symbol=symbol,
            side=OrderSide.BUY if trade_data.get('isBuyerMaker') == False else OrderSide.SELL,
            amount=self._safe_decimal(trade_data.get('qty')),
            price=self._safe_decimal(trade_data.get('price')),
            cost=self._safe_decimal(trade_data.get('quoteQty')),
            fee=None,  # Binance trade数据中不包含fee
            timestamp=datetime.fromtimestamp(trade_data.get('time', 0) / 1000),
            order_id=None,
            raw_data=trade_data
        )
    
    def parse_balance(self, currency: str, balance_info: Dict[str, Any]) -> BalanceData:
        """解析余额数据"""
        return BalanceData(
            currency=currency,
            free=self._safe_decimal(balance_info.get('availableBalance')),
            used=self._safe_decimal(balance_info.get('crossUnPnl')),  # 使用未实现盈亏作为已用余额
            total=self._safe_decimal(balance_info.get('balance')),
            usd_value=None,
            timestamp=datetime.now(),
            raw_data=balance_info
        )
    
    def parse_position(self, position_info: Dict[str, Any]) -> PositionData:
        """解析持仓数据"""
        symbol = self.map_symbol_from_binance(position_info.get('symbol', ''))
        side = PositionSide.LONG if float(position_info.get('positionAmt', 0)) > 0 else PositionSide.SHORT
        
        return PositionData(
            symbol=symbol,
            side=side,
            size=self._safe_decimal(abs(float(position_info.get('positionAmt', 0)))),
            entry_price=self._safe_decimal(position_info.get('entryPrice')),
            mark_price=self._safe_decimal(position_info.get('markPrice')),
            current_price=self._safe_decimal(position_info.get('markPrice')),
            unrealized_pnl=self._safe_decimal(position_info.get('unRealizedProfit')),
            realized_pnl=None,  # Binance position数据中不直接包含
            percentage=self._safe_decimal(position_info.get('roe')),
            leverage=self._safe_int(position_info.get('leverage', 1)),
            margin_mode=MarginMode.CROSS if position_info.get('marginType') == 'cross' else MarginMode.ISOLATED,
            margin=self._safe_decimal(position_info.get('isolatedMargin')),
            liquidation_price=self._safe_decimal(position_info.get('liquidationPrice')),
            timestamp=datetime.now(),
            raw_data=position_info
        )
    
    def parse_order(self, order_data: Dict[str, Any], symbol: str) -> OrderData:
        """解析订单数据"""
        # 状态映射
        status = self.ORDER_STATUS_MAPPING.get(
            order_data.get('status'), OrderStatus.UNKNOWN)
        
        # 类型映射
        order_type_str = order_data.get('type', 'LIMIT')
        order_type = OrderType.LIMIT
        for ot, binance_type in self.ORDER_TYPE_MAPPING.items():
            if binance_type == order_type_str:
                order_type = ot
                break
        
        return OrderData(
            id=str(order_data.get('orderId', '')),
            client_id=order_data.get('clientOrderId'),
            symbol=symbol,
            side=OrderSide.BUY if order_data.get('side') == 'BUY' else OrderSide.SELL,
            type=order_type,
            amount=self._safe_decimal(order_data.get('origQty')),
            price=self._safe_decimal(order_data.get('price')),
            filled=self._safe_decimal(order_data.get('executedQty')),
            remaining=self._safe_decimal(
                float(order_data.get('origQty', 0)) - float(order_data.get('executedQty', 0))
            ),
            cost=self._safe_decimal(order_data.get('cumQuote')),
            average=self._safe_decimal(order_data.get('avgPrice')),
            status=status,
            timestamp=datetime.fromtimestamp(order_data.get('time', 0) / 1000),
            updated=datetime.fromtimestamp(order_data.get('updateTime', 0) / 1000),
            fee=None,  # 需要单独查询
            trades=[],
            params={},
            raw_data=order_data
        )
    
    def validate_symbol(self, symbol: str) -> bool:
        """验证交易对格式"""
        if not symbol:
            return False
        
        # 基本格式检查
        if len(symbol) < 6:  # 至少6个字符，如BTCUSDT
            return False
        
        return True
    
    def get_precision_info(self, symbol: str) -> Dict[str, int]:
        """获取交易对精度信息"""
        # 从市场信息中获取精度，或使用默认值
        if symbol in self._market_info:
            market = self._market_info[symbol]
            return {
                'price_precision': market.get('quotePrecision', 8),
                'amount_precision': market.get('baseAssetPrecision', 8)
            }
        
        # 默认精度
        return {
            'price_precision': 8,
            'amount_precision': 8
        }
    
    def format_price(self, price: Decimal, symbol: str) -> str:
        """格式化价格"""
        precision = self.get_precision_info(symbol)['price_precision']
        return f"{price:.{precision}f}"
    
    def format_amount(self, amount: Decimal, symbol: str) -> str:
        """格式化数量"""
        precision = self.get_precision_info(symbol)['amount_precision']
        return f"{amount:.{precision}f}" 