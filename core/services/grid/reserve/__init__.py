"""
现货预留BTC管理模块

此模块仅对现货网格生效，永续合约不使用。

功能：
1. 追踪预留BTC消耗
2. 自动补充预留BTC
3. 启动时检查和购买
4. 运行时监控和补充

使用场景：
- Hyperliquid现货买入时，手续费从BTC扣除
- 需要预留一定BTC来弥补手续费消耗
- 当预留不足50%时自动补充
"""

from .spot_reserve_manager import SpotReserveManager
from .reserve_monitor import ReserveMonitor
from .reserve_checker import (
    check_spot_reserve_on_startup,
    auto_buy_reserve_if_needed
)

__all__ = [
    'SpotReserveManager',
    'ReserveMonitor',
    'check_spot_reserve_on_startup',
    'auto_buy_reserve_if_needed',
]

__version__ = '1.0.0'

