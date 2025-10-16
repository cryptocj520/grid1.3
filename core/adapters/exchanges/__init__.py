"""
MESA交易所适配层模块

本模块提供统一的交易所接口和适配器实现，支持多种交易所的统一管理。
采用事件驱动架构，与MESA引擎核心组件无缝集成。

核心组件:
- ExchangeInterface: 标准化交易所接口
- ExchangeAdapter: 交易所适配器基类
- ExchangeFactory: 交易所工厂，统一管理和创建交易所实例
- ExchangeManager: 交易所管理器，处理生命周期管理
- 具体交易所实现: HyperliquidAdapter, BackpackAdapter, BinanceAdapter

特性:
- 异步优先的API设计
- 事件驱动的数据流
- 统一的错误处理机制
- 自动重连和健康检查
- 配置驱动的适配器创建

版本: 1.0.0
作者: MESA开发团队
"""

from .interface import ExchangeInterface, ExchangeConfig, ExchangeStatus
from .models import (
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
    TradeData
)
from .adapter import ExchangeAdapter
from .factory import ExchangeFactory, get_exchange_factory
from .manager import ExchangeManager

# 具体交易所适配器
from .adapters.hyperliquid import HyperliquidAdapter
from .adapters.backpack import BackpackAdapter
from .adapters.binance import BinanceAdapter

__all__ = [
    # 核心接口和基类
    'ExchangeInterface',
    'ExchangeConfig',
    'ExchangeStatus',
    'ExchangeAdapter',

    # 数据模型
    'ExchangeType',
    'OrderSide',
    'OrderType',
    'OrderStatus',
    'PositionSide',
    'MarginMode',
    'OrderData',
    'PositionData',
    'BalanceData',
    'TickerData',
    'OHLCVData',
    'OrderBookData',
    'TradeData',

    # 管理组件
    'ExchangeFactory',
    'get_exchange_factory',
    'ExchangeManager',

    # 具体适配器
    'HyperliquidAdapter',
    'BackpackAdapter',
    'BinanceAdapter',
]

# 版本信息
__version__ = '1.0.0'
