"""
EdgeX基础工具类 - 重构版

包含EdgeX交易所的基础配置、数据解析等公共功能
重构：简化符号映射，推荐使用统一符号转换服务
"""

import time
from decimal import Decimal
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

from ..models import (
    TickerData, OrderBookData, TradeData, BalanceData, OrderData, 
    OrderSide, OrderType, OrderStatus, PositionData, PositionSide,
    MarginMode, OrderBookLevel
)


class EdgeXBase:
    """EdgeX基础工具类 - 重构版"""
    
    DEFAULT_BASE_URL = "https://pro.edgex.exchange/"
    DEFAULT_WS_URL = "wss://quote.edgex.exchange/api/v1/public/ws"
    
    def __init__(self, config=None):
        self.config = config
        self.logger = None
        
        # 实际支持的交易对（将从API动态获取）
        self._supported_symbols = []
        self._contract_mappings = {}  # contract_id -> symbol
        self._symbol_contract_mappings = {}  # symbol -> contract_id
        
        # 🔥 重构：简化符号映射配置
        self._setup_legacy_symbol_mapping()
        
    def _setup_legacy_symbol_mapping(self):
        """
        设置遗留符号映射（已弃用）
        
        @deprecated: 建议使用统一的符号转换服务
        """
        # 符号映射（通用格式 -> EdgeX格式）
        self._default_symbol_mapping = {
            "BTC/USDC:PERP": "BTC_USDC",
            "ETH/USDC:PERP": "ETH_USDC", 
            "SOL/USDC:PERP": "SOL_USDC",
            "AVAX/USDC:PERP": "AVAX_USDC"
        }
        
        # 合并用户配置的符号映射
        if self.config and self.config.symbol_mapping:
            self._default_symbol_mapping.update(self.config.symbol_mapping)

    def _safe_decimal(self, value: Any) -> Decimal:
        """安全转换为Decimal"""
        try:
            if value is None:
                return Decimal('0')
            return Decimal(str(value))
        except (ValueError, TypeError):
            return Decimal('0')

    def _safe_int(self, value: Any) -> Optional[int]:
        """安全转换为整数
        
        将各种类型的值安全转换为整数，如果转换失败返回None
        用于处理交易笔数、合约ID等整数字段
        """
        try:
            if value is None:
                return None
            if isinstance(value, (int, float)):
                return int(value)
            if isinstance(value, str):
                # 处理数字字符串，包括小数点的情况
                if value.strip() == '':
                    return None
                return int(float(value))
            if isinstance(value, Decimal):
                return int(value)
            return None
        except (ValueError, TypeError, OverflowError):
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """安全转换为float"""
        try:
            if value is None:
                return None
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_str(self, value: Any) -> str:
        """安全转换为str"""
        if value is None:
            return ""
        return str(value)

    def _map_symbol(self, symbol: str) -> str:
        """
        映射交易对符号
        
        @deprecated: 建议使用统一的符号转换服务
        """
        if not hasattr(self, '_deprecation_logged_map'):
            if self.logger:
                self.logger.warning("⚠️ _map_symbol方法已弃用，建议使用统一的符号转换服务")
            self._deprecation_logged_map = True
        
        return self._default_symbol_mapping.get(symbol, symbol)

    def _reverse_map_symbol(self, exchange_symbol: str) -> str:
        """
        反向映射交易对符号
        
        @deprecated: 建议使用统一的符号转换服务
        """
        if not hasattr(self, '_deprecation_logged_reverse'):
            if self.logger:
                self.logger.warning("⚠️ _reverse_map_symbol方法已弃用，建议使用统一的符号转换服务")
            self._deprecation_logged_reverse = True
        
        reverse_mapping = {v: k for k, v in self._default_symbol_mapping.items()}
        return reverse_mapping.get(exchange_symbol, exchange_symbol)

    def _normalize_symbol(self, symbol: str) -> str:
        """
        标准化交易对符号
        
        @deprecated: 建议使用统一的符号转换服务
        """
        # 应用符号映射
        if hasattr(self, 'symbol_mapping') and symbol in self.symbol_mapping:
            return self.symbol_mapping[symbol]

        # 标准格式转换为EdgeX格式
        # 例如: BTC/USDT -> BTC_USDC (EdgeX使用USDC)
        if '/' in symbol:
            base, quote = symbol.split('/')
            if quote == 'USDT':
                quote = 'USDC'  # EdgeX主要使用USDC
            symbol = f"{base}_{quote}"
        if ':' in symbol:
            # 处理期货合约格式
            symbol = symbol.replace(':', '_')

        return symbol.upper()

    def _normalize_contract_symbol(self, symbol: str) -> str:
        """将EdgeX合约symbol转换为标准格式"""
        # EdgeX返回类似 "BTCUSDT", "ETHUSDT", "SOLUSDT" 等格式
        # 标准化为 "BTC_USDT" 格式
        if "/" in symbol:
            return symbol.replace("/", "_")
        elif symbol.endswith("USDT"):
            # 处理BTCUSDT格式
            base = symbol[:-4]  # 移除USDT
            return f"{base}_USDT"
        elif symbol.endswith("USDC"):
            # 处理BTCUSDC格式
            base = symbol[:-4]  # 移除USDC
            return f"{base}_USDC"
        else:
            # 对于其他格式，保持原样
            return symbol

    def _parse_timestamp(self, timestamp: Any, unit: str = 'ms') -> Optional[datetime]:
        """解析时间戳"""
        if timestamp is None:
            return None
        
        try:
            timestamp_int = int(timestamp)
            
            if unit == 'ms':
                return datetime.fromtimestamp(timestamp_int / 1000)
            elif unit == 'us':
                return datetime.fromtimestamp(timestamp_int / 1000000)
            else:
                return datetime.fromtimestamp(timestamp_int)
                
        except (ValueError, TypeError, OSError):
            return None

    def _parse_order_side(self, side: str) -> OrderSide:
        """解析订单方向"""
        if side and side.lower() in ['buy', 'bid']:
            return OrderSide.BUY
        elif side and side.lower() in ['sell', 'ask']:
            return OrderSide.SELL
        else:
            return OrderSide.BUY

    def _parse_order_type(self, order_type: str) -> OrderType:
        """解析订单类型"""
        if order_type and order_type.lower() == 'limit':
            return OrderType.LIMIT
        elif order_type and order_type.lower() == 'market':
            return OrderType.MARKET
        else:
            return OrderType.LIMIT

    def _parse_order_status(self, status: str) -> OrderStatus:
        """解析订单状态"""
        if not status:
            return OrderStatus.PENDING
        
        status_lower = status.lower()
        
        if status_lower in ['new', 'open', 'pending']:
            return OrderStatus.OPEN
        elif status_lower in ['filled', 'closed']:
            return OrderStatus.FILLED
        elif status_lower in ['canceled', 'cancelled']:
            return OrderStatus.CANCELLED
        elif status_lower in ['partially_filled', 'partial']:
            return OrderStatus.PARTIALLY_FILLED
        elif status_lower in ['rejected', 'failed']:
            return OrderStatus.REJECTED
        else:
            return OrderStatus.PENDING

    def _parse_position_side(self, side: str) -> PositionSide:
        """解析持仓方向"""
        if side and side.lower() in ['long', 'buy']:
            return PositionSide.LONG
        elif side and side.lower() in ['short', 'sell']:
            return PositionSide.SHORT
        else:
            return PositionSide.LONG

    def _parse_margin_mode(self, mode: str) -> MarginMode:
        """解析保证金模式"""
        if mode and mode.lower() == 'cross':
            return MarginMode.CROSS
        elif mode and mode.lower() == 'isolated':
            return MarginMode.ISOLATED
        else:
            return MarginMode.CROSS

    def _parse_ticker(self, data: Dict[str, Any], symbol: str) -> TickerData:
        """解析行情数据"""
        # 解析交易所时间戳
        exchange_timestamp = None
        if 'timestamp' in data:
            try:
                timestamp_ms = int(data['timestamp'])
                exchange_timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
            except (ValueError, TypeError):
                pass
        elif 'ts' in data:
            try:
                timestamp_ms = int(data['ts'])
                exchange_timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
            except (ValueError, TypeError):
                pass

        return TickerData(
            symbol=symbol,
            last=self._safe_decimal(data.get('last')),
            bid=self._safe_decimal(data.get('bid')),
            ask=self._safe_decimal(data.get('ask')),
            bid_volume=self._safe_decimal(data.get('bidSize')),
            ask_volume=self._safe_decimal(data.get('askSize')),
            high=self._safe_decimal(data.get('high')),
            low=self._safe_decimal(data.get('low')),
            volume=self._safe_decimal(data.get('volume')),
            quote_volume=self._safe_decimal(data.get('quoteVolume')),
            open=self._safe_decimal(data.get('open')),
            close=self._safe_decimal(data.get('close')),
            change=self._safe_decimal(data.get('change')),
            percentage=self._safe_decimal(data.get('percentage')),
            timestamp=datetime.now(),
            exchange_timestamp=exchange_timestamp,
            info=data
        )

    def _parse_orderbook(self, data: Dict[str, Any], symbol: str) -> OrderBookData:
        """解析订单簿数据"""
        bids = []
        asks = []
        
        # 解析买单
        for bid in data.get('bids', []):
            if len(bid) >= 2:
                bids.append(OrderBookLevel(
                    price=self._safe_decimal(bid[0]),
                    size=self._safe_decimal(bid[1])
                ))
        
        # 解析卖单
        for ask in data.get('asks', []):
            if len(ask) >= 2:
                asks.append(OrderBookLevel(
                    price=self._safe_decimal(ask[0]),
                    size=self._safe_decimal(ask[1])
                ))
        
        return OrderBookData(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=datetime.now(),
            exchange_timestamp=self._parse_timestamp(data.get('timestamp')),
            info=data
        )

    def _parse_trade(self, data: Dict[str, Any], symbol: str) -> TradeData:
        """解析交易数据"""
        return TradeData(
            id=self._safe_str(data.get('id')),
            symbol=symbol,
            side=self._parse_order_side(data.get('side')),
            amount=self._safe_decimal(data.get('amount')),
            price=self._safe_decimal(data.get('price')),
            cost=self._safe_decimal(data.get('cost')),
            timestamp=datetime.now(),
            exchange_timestamp=self._parse_timestamp(data.get('timestamp')),
            info=data
        )

    def _parse_balance(self, data: Dict[str, Any], currency: str) -> BalanceData:
        """解析余额数据"""
        return BalanceData(
            currency=currency,
            free=self._safe_decimal(data.get('free')),
            used=self._safe_decimal(data.get('used')),
            total=self._safe_decimal(data.get('total')),
            info=data
        )

    def _parse_order(self, data: Dict[str, Any]) -> OrderData:
        """解析订单数据"""
        return OrderData(
            id=self._safe_str(data.get('id')),
            symbol=self._safe_str(data.get('symbol')),
            side=self._parse_order_side(data.get('side')),
            type=self._parse_order_type(data.get('type')),
            amount=self._safe_decimal(data.get('amount')),
            price=self._safe_decimal(data.get('price')),
            filled=self._safe_decimal(data.get('filled')),
            remaining=self._safe_decimal(data.get('remaining')),
            cost=self._safe_decimal(data.get('cost')),
            status=self._parse_order_status(data.get('status')),
            timestamp=self._parse_timestamp(data.get('timestamp')),
            info=data
        )

    def _parse_position(self, data: Dict[str, Any]) -> PositionData:
        """解析持仓数据"""
        return PositionData(
            symbol=self._safe_str(data.get('symbol')),
            side=self._parse_position_side(data.get('side')),
            size=self._safe_decimal(data.get('size')),
            entry_price=self._safe_decimal(data.get('entryPrice')),
            mark_price=self._safe_decimal(data.get('markPrice')),
            unrealized_pnl=self._safe_decimal(data.get('unrealizedPnl')),
            realized_pnl=self._safe_decimal(data.get('realizedPnl')),
            margin_mode=self._parse_margin_mode(data.get('marginMode')),
            info=data
        )

    def get_supported_symbols(self) -> List[str]:
        """获取支持的交易对列表"""
        return self._supported_symbols.copy()

    def is_symbol_supported(self, symbol: str) -> bool:
        """检查交易对是否支持"""
        return symbol in self._supported_symbols

    def get_contract_mapping(self, symbol: str) -> Optional[str]:
        """获取符号的合约映射"""
        return self._symbol_contract_mappings.get(symbol)

    def get_symbol_by_contract(self, contract_id: str) -> Optional[str]:
        """通过合约ID获取符号"""
        return self._contract_mappings.get(contract_id)

    def set_logger(self, logger):
        """设置日志器"""
        self.logger = logger

    def get_logger(self):
        """获取日志器"""
        return self.logger 