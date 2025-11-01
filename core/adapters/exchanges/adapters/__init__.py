"""
   交易所适配器子模块

   包含各个具体交易所的适配器实现，每个适配器都基于统一的ExchangeInterface
   接口，提供标准化的交易所功能。

   支持的交易所:
   - Hyperliquid: 永续合约交易所
   - Backpack: 永续合约交易所  
   - Binance: 期货交易所
   - OKX: 现货、永续合约、期货、期权交易所
   - EdgeX: 永续合约交易所
   - Lighter: 永续合约交易所

   每个适配器都包含:
   - 完整的交易功能实现
   - WebSocket实时数据流支持
   - 自动重连和错误处理
   - 符合MESA事件驱动架构
   """

from .hyperliquid import HyperliquidAdapter
from .backpack import BackpackAdapter
from .binance import BinanceAdapter
from .okx import OKXAdapter
from .edgex import EdgeXAdapter
from .lighter import LighterAdapter

__all__ = [
    'HyperliquidAdapter',
    'BackpackAdapter',
    'BinanceAdapter',
    'OKXAdapter',
    'EdgeXAdapter',
    'LighterAdapter'
]
