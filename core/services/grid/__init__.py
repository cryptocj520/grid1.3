"""
网格交易系统模块

完整的网格交易系统实现，支持：
- 做多网格（Long Grid）
- 做空网格（Short Grid）
- 等差网格间隔
- 自动反向挂单
- 批量成交处理
- 实时终端监控

使用示例：
    from core.services.grid import GridCoordinator, GridConfig, GridType
    
    # 创建配置
    config = GridConfig(
        exchange="backpack",
        symbol="BTC_USDC_PERP",
        grid_type=GridType.LONG,
        lower_price=Decimal("80000"),
        upper_price=Decimal("100000"),
        grid_interval=Decimal("100"),
        order_amount=Decimal("0.025")
    )
    
    # 创建协调器并启动
    coordinator = GridCoordinator(config, strategy, engine, tracker)
    await coordinator.start()
"""

# 数据模型
from .models import (
    GridConfig,
    GridType,
    GridDirection,
    GridState,
    GridLevel,
    GridStatus,
    GridOrder,
    GridOrderStatus,
    GridOrderSide,
    GridMetrics,
    GridStatistics
)

# 接口
from .interfaces import (
    IGridStrategy,
    IGridEngine,
    IPositionTracker
)

# 实现
from .implementations import (
    GridStrategyImpl,
    GridEngineImpl,
    PositionTrackerImpl
)

# 协调器
from .coordinator import GridCoordinator

# 终端界面
from .terminal_ui import GridTerminalUI

__all__ = [
    # 模型
    'GridConfig',
    'GridType',
    'GridDirection',
    'GridState',
    'GridLevel',
    'GridStatus',
    'GridOrder',
    'GridOrderStatus',
    'GridOrderSide',
    'GridMetrics',
    'GridStatistics',

    # 接口
    'IGridStrategy',
    'IGridEngine',
    'IPositionTracker',

    # 实现
    'GridStrategyImpl',
    'GridEngineImpl',
    'PositionTrackerImpl',

    # 协调器
    'GridCoordinator',

    # 终端界面
    'GridTerminalUI',
]

__version__ = '1.3.0'
