"""
Backpack基础功能模块 - 重构版

包含Backpack交易所的基础配置、数据解析等公共功能
重构：简化符号映射，推荐使用统一符号转换服务
"""

import time
import decimal
from typing import Dict, List, Optional, Any, Union
from decimal import Decimal
from datetime import datetime

from ..models import (
    TickerData, OrderBookData, TradeData, BalanceData, OrderData, 
    OrderSide, OrderType, OrderStatus, PositionData, PositionSide,
    MarginMode, OrderBookLevel
)


class BackpackSymbolInfo:
    """Backpack交易对信息类"""
    
    def __init__(self, symbol: str, base: str, quote: str, contract_type: str = "PERPETUAL"):
        self.symbol = symbol
        self.base = base
        self.quote = quote
        self.contract_type = contract_type
        self.min_qty = Decimal('0.001')
        self.max_qty = Decimal('1000000')
        self.min_price = Decimal('0.01')
        self.max_price = Decimal('1000000')
        self.price_precision = 8
        self.qty_precision = 8


class BackpackBase:
    """Backpack基础功能类 - 重构版"""
    
    # 默认配置
    DEFAULT_BASE_URL = "https://api.backpack.exchange/"
    DEFAULT_WS_URL = "wss://ws.backpack.exchange/"
    
    # WebSocket订阅黑名单
    # 这些交易对会导致WebSocket错误: {'code': 4005, 'message': 'Invalid market'}
    WEBSOCKET_BLACKLIST = {
        'FRAG_USDC_PERP',
        'KBONK_USDC_PERP', 
        'KPEPE_USDC_PERP'
    }
    
    def __init__(self, config=None):
        self.config = config
        self.logger = None
        
        # 基础配置
        self.base_url = getattr(config, 'base_url', None) or self.DEFAULT_BASE_URL
        self.ws_url = getattr(config, 'ws_url', None) or self.DEFAULT_WS_URL
        
        # 确保URL以正确的格式结尾
        if not self.base_url.endswith('/'):
            self.base_url += '/'
            
        # 支持的交易对和映射
        self._supported_symbols = []
        self._market_info = {}
        
        # 🔥 重构：简化符号映射
        self._setup_legacy_symbol_mapping()
        
    def _setup_legacy_symbol_mapping(self):
        """
        设置遗留符号映射（已弃用）
        
        @deprecated: 建议使用统一的符号转换服务
        """
        self._symbol_mapping = {}
        
        # 只保留配置中明确定义的映射
        if self.config and hasattr(self.config, 'symbol_mapping') and self.config.symbol_mapping:
            self._symbol_mapping.update(self.config.symbol_mapping)
    
    def filter_websocket_symbols(self, symbols: List[str]) -> List[str]:
        """过滤掉WebSocket黑名单中的交易对"""
        filtered = [s for s in symbols if s not in self.WEBSOCKET_BLACKLIST]
        filtered_count = len(symbols) - len(filtered)
        if filtered_count > 0 and self.logger:
            self.logger.info(f"🚫 过滤掉 {filtered_count} 个黑名单交易对: {', '.join(self.WEBSOCKET_BLACKLIST & set(symbols))}")
        return filtered
    
    def is_websocket_blacklisted(self, symbol: str) -> bool:
        """检查交易对是否在WebSocket黑名单中"""
        return symbol in self.WEBSOCKET_BLACKLIST
    
    def _normalize_backpack_symbol(self, symbol: str) -> str:
        """标准化Backpack符号格式"""
        # Backpack返回的符号格式可能是 "SOL_USDC_PERP" 或 "BTC_USDC_PERP"
        # 保持原格式或转换为标准格式
        return symbol.upper()
    
    def _map_symbol(self, symbol: str) -> str:
        """
        映射交易对符号
        
        @deprecated: 建议使用统一的符号转换服务
        """
        if not hasattr(self, '_deprecation_logged_map'):
            if self.logger:
                self.logger.warning("⚠️ _map_symbol方法已弃用，建议使用统一的符号转换服务")
            self._deprecation_logged_map = True
        
        # 首先检查是否有显式映射
        if symbol in self._symbol_mapping:
            return self._symbol_mapping[symbol]
        
        # 对于永续合约，Backpack需要保留_PERP后缀
        # 直接返回完整符号，保留_PERP后缀
        return symbol
    
    def _reverse_map_symbol(self, exchange_symbol: str) -> str:
        """
        反向映射交易对符号
        
        @deprecated: 建议使用统一的符号转换服务
        """
        if not hasattr(self, '_deprecation_logged_reverse'):
            if self.logger:
                self.logger.warning("⚠️ _reverse_map_symbol方法已弃用，建议使用统一的符号转换服务")
            self._deprecation_logged_reverse = True
        
        # 首先检查显式映射
        reverse_mapping = {v: k for k, v in self._symbol_mapping.items()}
        if exchange_symbol in reverse_mapping:
            return reverse_mapping[exchange_symbol]
        
        # 现在Backpack返回的符号已经包含_PERP后缀
        # 所以不需要额外添加后缀，直接返回原符号
        return exchange_symbol
    
    def _safe_decimal(self, value: Any) -> Optional[Decimal]:
        """安全转换为Decimal"""
        if value is None:
            return None
        try:
            if isinstance(value, str):
                if value.strip() == '' or value.strip() == 'null':
                    return None
                return Decimal(value)
            elif isinstance(value, (int, float)):
                return Decimal(str(value))
            elif isinstance(value, Decimal):
                return value
            else:
                return None
        except (ValueError, TypeError, decimal.InvalidOperation):
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """安全转换为float"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        """安全转换为int"""
        if value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _safe_str(self, value: Any) -> str:
        """安全转换为str"""
        if value is None:
            return ""
        return str(value)

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

    def _parse_ticker_data(self, data: Dict[str, Any], symbol: str) -> TickerData:
        """解析ticker数据"""
        return TickerData(
            symbol=symbol,
            last=self._safe_decimal(data.get('price')),
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
            timestamp=self._parse_timestamp(data.get('timestamp')),
            exchange_timestamp=self._parse_timestamp(data.get('timestamp')),
            info=data
        )

    def _parse_orderbook_data(self, data: Dict[str, Any], symbol: str) -> OrderBookData:
        """解析orderbook数据"""
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
            timestamp=self._parse_timestamp(data.get('timestamp')),
            exchange_timestamp=self._parse_timestamp(data.get('timestamp')),
            info=data
        )

    def _parse_trade_data(self, data: Dict[str, Any], symbol: str) -> TradeData:
        """解析trade数据"""
        return TradeData(
            id=self._safe_str(data.get('id')),
            symbol=symbol,
            side=self._parse_order_side(data.get('side')),
            amount=self._safe_decimal(data.get('amount')),
            price=self._safe_decimal(data.get('price')),
            cost=self._safe_decimal(data.get('cost')),
            timestamp=self._parse_timestamp(data.get('timestamp')),
            exchange_timestamp=self._parse_timestamp(data.get('timestamp')),
            info=data
        )

    def _parse_balance_data(self, data: Dict[str, Any], currency: str) -> BalanceData:
        """解析balance数据"""
        return BalanceData(
            currency=currency,
            free=self._safe_decimal(data.get('free')),
            used=self._safe_decimal(data.get('used')),
            total=self._safe_decimal(data.get('total')),
            info=data
        )

    def _parse_order_data(self, data: Dict[str, Any]) -> OrderData:
        """解析order数据"""
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

    def _parse_position_data(self, data: Dict[str, Any]) -> PositionData:
        """解析position数据"""
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

    def format_quantity(self, symbol: str, quantity: Decimal, symbol_info: Optional[BackpackSymbolInfo] = None) -> str:
        """
        格式化数量精度，返回字符串
        
        Args:
            symbol: 交易对符号
            quantity: 原始数量
            symbol_info: 交易对信息（包含精度）
            
        Returns:
            格式化后的数量字符串
        """
        # 获取精度
        if symbol_info:
            precision = symbol_info.qty_precision
        else:
            precision = 8  # 默认精度
        
        # 根据精度格式化
        if precision == 0:
            # 只接受整数（如 ASTER）
            formatted = quantity.quantize(Decimal('1'))
        else:
            # 支持小数（如 BTC, ETH）
            formatted = quantity.quantize(Decimal('0.1') ** precision)
        
        # 手动去除末尾的零，避免科学计数法
        result = str(formatted)
        if '.' in result:
            result = result.rstrip('0').rstrip('.')  # 去掉末尾的0和小数点
        
        return result

    def format_price(self, symbol: str, price: Decimal, symbol_info: Optional[BackpackSymbolInfo] = None) -> str:
        """格式化价格精度，返回字符串"""
        if symbol_info:
            precision = symbol_info.price_precision
        else:
            precision = 8  # 默认精度
        
        # 格式化精度
        formatted = price.quantize(Decimal('0.1') ** precision)
        
        # 手动去除末尾的零，避免科学计数法
        result = str(formatted)
        if '.' in result:
            result = result.rstrip('0').rstrip('.')  # 去掉末尾的0和小数点
        
        return result
    
    def _normalize_symbol(self, symbol: str) -> str:
        """标准化符号格式 - 向后兼容"""
        return self._normalize_backpack_symbol(symbol)

    async def _use_default_symbols(self) -> None:
        """使用默认的交易对配置 - 🔥 修改：只包含永续合约"""
        self._supported_symbols = [
            # 永续合约
            "BTC_USDC_PERP", "ETH_USDC_PERP", "SOL_USDC_PERP", 
            "AVAX_USDC_PERP", "DOGE_USDC_PERP", "XRP_USDC_PERP",
            "SUI_USDC_PERP", "JUP_USDC_PERP", "WIF_USDC_PERP",
            "LTC_USDC_PERP", "ADA_USDC_PERP", "LINK_USDC_PERP",
            "BNB_USDC_PERP", "BONK_USDC_PERP", "PYTH_USDC_PERP",
            "JTO_USDC_PERP", "RNDR_USDC_PERP", "W_USDC_PERP",
            "POPCAT_USDC_PERP", "DRIFT_USDC_PERP", "PENDLE_USDC_PERP",
            "NEAR_USDC_PERP", "ARB_USDC_PERP", "OP_USDC_PERP",
            "PEPE_USDC_PERP", "FLOKI_USDC_PERP", "SHIB_USDC_PERP",
            "MEME_USDC_PERP", "GIGA_USDC_PERP", "PNUT_USDC_PERP"
        ]
        if self.logger:
            self.logger.info(f"✅ 使用默认永续合约交易对列表: {len(self._supported_symbols)} 个")
            self.logger.info(f"📊 交易对类型: 100% 永续合约")

    async def get_supported_symbols(self) -> List[str]:
        """获取交易所实际支持的交易对列表"""
        if not self._supported_symbols:
            await self._use_default_symbols()
        return self._supported_symbols.copy()

    async def get_market_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取市场信息"""
        if not self._market_info:
            # 如果市场信息为空，尝试使用默认符号
            await self._use_default_symbols()
        return self._market_info.get(symbol)

    def get_symbol_info(self, symbol: str) -> Optional[BackpackSymbolInfo]:
        """获取交易对信息"""
        # 解析symbol获取base和quote
        if '_' in symbol:
            parts = symbol.split('_')
            if len(parts) >= 2:
                base = parts[0]
                quote = '_'.join(parts[1:])
            else:
                base = symbol
                quote = 'USDC'
        else:
            base = symbol
            quote = 'USDC'
        
        return BackpackSymbolInfo(symbol, base, quote)

    def is_valid_symbol(self, symbol: str) -> bool:
        """检查符号是否有效"""
        return symbol in self._supported_symbols

    def get_default_symbols(self) -> List[str]:
        """获取默认支持的交易对 - 🔥 修改：只返回永续合约"""
        return [
            # 永续合约
            "BTC_USDC_PERP", "ETH_USDC_PERP", "SOL_USDC_PERP", 
            "AVAX_USDC_PERP", "DOGE_USDC_PERP", "XRP_USDC_PERP",
            "SUI_USDC_PERP", "JUP_USDC_PERP", "WIF_USDC_PERP",
            "LTC_USDC_PERP", "ADA_USDC_PERP", "LINK_USDC_PERP",
            "BNB_USDC_PERP", "BONK_USDC_PERP", "PYTH_USDC_PERP",
            "JTO_USDC_PERP", "RNDR_USDC_PERP", "W_USDC_PERP",
            "POPCAT_USDC_PERP", "DRIFT_USDC_PERP", "PENDLE_USDC_PERP",
            "NEAR_USDC_PERP", "ARB_USDC_PERP", "OP_USDC_PERP",
            "PEPE_USDC_PERP", "FLOKI_USDC_PERP", "SHIB_USDC_PERP",
            "MEME_USDC_PERP", "GIGA_USDC_PERP", "PNUT_USDC_PERP"
        ]

    def get_base_url(self) -> str:
        """获取基础URL"""
        return self.base_url

    def get_websocket_url(self) -> str:
        """获取WebSocket URL"""
        return self.ws_url

    def get_price_precision(self, symbol: str) -> int:
        """获取价格精度"""
        symbol_info = self.get_symbol_info(symbol)
        return symbol_info.price_precision if symbol_info else 8

    def get_qty_precision(self, symbol: str) -> int:
        """获取数量精度"""
        symbol_info = self.get_symbol_info(symbol)
        return symbol_info.qty_precision if symbol_info else 8

    def calculate_order_cost(self, amount: Decimal, price: Decimal) -> Decimal:
        """计算订单成本"""
        return amount * price

    def is_perpetual_contract(self, symbol: str) -> bool:
        """判断是否为永续合约"""
        return symbol.endswith('_PERP')

    def extract_base_quote(self, symbol: str) -> tuple:
        """提取base和quote货币"""
        if '_' in symbol:
            parts = symbol.split('_')
            if len(parts) >= 3 and parts[-1] == 'PERP':
                # 永续合约格式: BTC_USDC_PERP
                base = parts[0]
                quote = '_'.join(parts[1:-1])
            elif len(parts) >= 2:
                # 现货格式: BTC_USDC
                base = parts[0]
                quote = '_'.join(parts[1:])
            else:
                base = symbol
                quote = 'USDC'
        else:
            base = symbol
            quote = 'USDC'
        
        return base, quote

    def build_symbol(self, base: str, quote: str, contract_type: str = 'PERP') -> str:
        """构建符号"""
        if contract_type == 'PERP':
            return f"{base}_{quote}_PERP"
        else:
            return f"{base}_{quote}"

    def get_contract_type(self, symbol: str) -> str:
        """获取合约类型"""
        if symbol.endswith('_PERP'):
            return 'PERPETUAL'
        else:
            return 'SPOT'

    def set_logger(self, logger):
        """设置日志器"""
        self.logger = logger

    def get_logger(self):
        """获取日志器"""
        return self.logger 