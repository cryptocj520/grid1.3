"""
重构后的套利系统模块

基于分层架构设计的套利系统，包含精度管理、决策引擎、交易执行和协调器等核心组件。

模块结构：
- initialization/: 初始化模块
  - precision_manager.py: 精度管理器
  - arbitrage_initializer.py: 套利系统初始化器
- execution/: 执行模块
  - trade_execution_manager.py: 统一交易执行管理器
  - exchange_registry.py: 交易所注册器
- decision/: 决策模块
  - arbitrage_decision_engine.py: 套利决策引擎
  - opportunity_processor.py: 机会处理器（与现有监视器模块集成）
- coordinator/: 协调模块
  - arbitrage_coordinator.py: 套利协调器
- risk_manager/: 风险管理模块
  - risk_manager.py: 风险管理器
  - risk_models.py: 风险管理数据模型
- position_manager/: 持仓管理模块
  - position_manager.py: 持仓管理器
  - position_models.py: 持仓管理数据模型
- shared/: 共享模块
  - models.py: 数据模型
  - precision_cache.py: 精度缓存
  - config.py: 配置管理
"""

# 核心组件导出
from .initialization.precision_manager import PrecisionManager
from .initialization.arbitrage_initializer import ArbitrageInitializer

from .execution.trade_execution_manager import TradeExecutionManager
from .execution.exchange_registry import ExchangeRegistry

from .decision.arbitrage_decision_engine import ArbitrageDecisionEngine
from .decision.opportunity_processor import OpportunityProcessor

from .coordinator.arbitrage_coordinator import ArbitrageCoordinator

from .risk_manager.risk_manager import RiskManager
from .risk_manager.risk_models import (
    RiskLevel, RiskAssessmentResult, RiskMetrics, RiskAlert, RiskLimit, RiskConfiguration
)

from .position_manager.position_manager import PositionManager
from .position_manager.position_models import (
    PositionStatus, PositionType, PositionSummary, PositionMetrics, PositionEvent, PositionConfiguration
)

# 共享模块导出
from .shared.models import (
    # 枚举类
    ArbitrageDirection,
    ArbitrageStatus,
    OrderType,
    ExecutionStrategy,
    
    # 数据模型
    PrecisionInfo,
    MarketSnapshot,
    TradePlan,
    OrderInfo,
    ExecutionResult,
    ArbitragePosition,
    RiskAssessment,
    ArbitrageOpportunity,
    
    # 工具函数
    adjust_precision,
    calculate_spread_percentage,
    determine_direction
)

from .shared.precision_cache import PrecisionCache, PrecisionCacheManager

from .shared.config import (
    PrecisionConfig,
    DecisionConfig,
    ExecutionConfig,
    RiskConfig,
    MonitoringConfig,
    IntegrationConfig,
    ArbitrageSystemConfig,
    ArbitrageConfigManager
)

# 便捷导出
__all__ = [
    # 核心组件
    'PrecisionManager',
    'ArbitrageInitializer',
    'TradeExecutionManager',
    'ExchangeRegistry',
    'ArbitrageDecisionEngine',
    'OpportunityProcessor',
    'ArbitrageCoordinator',
    
    # 风险管理
    'RiskManager',
    'RiskLevel',
    'RiskAssessmentResult',
    'RiskMetrics',
    'RiskAlert',
    'RiskLimit',
    'RiskConfiguration',
    
    # 持仓管理
    'PositionManager',
    'PositionStatus',
    'PositionType',
    'PositionSummary',
    'PositionMetrics',
    'PositionEvent',
    'PositionConfiguration',
    
    # 枚举类
    'ArbitrageDirection',
    'ArbitrageStatus',
    'OrderType',
    'ExecutionStrategy',
    
    # 数据模型
    'PrecisionInfo',
    'MarketSnapshot',
    'TradePlan',
    'OrderInfo',
    'ExecutionResult',
    'ArbitragePosition',
    'RiskAssessment',
    'ArbitrageOpportunity',
    
    # 缓存管理
    'PrecisionCache',
    'PrecisionCacheManager',
    
    # 配置管理
    'PrecisionConfig',
    'DecisionConfig',
    'ExecutionConfig',
    'RiskConfig',
    'MonitoringConfig',
    'IntegrationConfig',
    'ArbitrageSystemConfig',
    'ArbitrageConfigManager',
    
    # 工具函数
    'adjust_precision',
    'calculate_spread_percentage',
    'determine_direction'
]


# 版本信息
__version__ = '2.1.0'
__author__ = 'Arbitrage System Team'
__description__ = '重构后的套利系统模块 - 包含独立风险管理和持仓管理'


# 快速使用示例
def create_arbitrage_system(exchange_adapters, config_path=None):
    """
    创建套利系统的便捷函数
    
    Args:
        exchange_adapters: 交易所适配器字典
        config_path: 配置文件路径
        
    Returns:
        ArbitrageInitializer: 套利系统初始化器
    """
    initializer = ArbitrageInitializer(exchange_adapters)
    return initializer


# 使用说明
__doc__ += """

使用示例：

1. 基本使用：
```python
from core.services.arbitrage import create_arbitrage_system

# 创建套利系统
arbitrage_system = create_arbitrage_system(exchange_adapters)

# 初始化
await arbitrage_system.initialize(
    config_path="config/arbitrage/default.yaml",
    overlapping_symbols=["BTC/USDT", "ETH/USDT"]
)

# 启动
await arbitrage_system.start()

# 处理市场数据
await arbitrage_system.handle_spread_analysis_result(spread_data)
```

2. 高级使用：
```python
from core.services.arbitrage import (
    ArbitrageInitializer, 
    ArbitrageCoordinator,
    OpportunityProcessor,
    RiskManager,
    PositionManager
)

# 创建初始化器
initializer = ArbitrageInitializer(exchange_adapters)

# 注册回调
initializer.register_integration_callback(
    'market_data_callback',
    my_market_data_handler
)

# 获取组件
coordinator = initializer.get_arbitrage_coordinator()
processor = initializer.get_opportunity_processor()

# 独立使用风险管理器
risk_manager = RiskManager()
await risk_manager.start_monitoring()

# 独立使用持仓管理器
position_manager = PositionManager()
await position_manager.start_monitoring()
```

3. 与现有监视器模块集成：
```python
# 在现有监视器模块中
from core.services.arbitrage import ArbitrageInitializer

# 创建套利系统
arbitrage_system = ArbitrageInitializer(exchange_adapters)
await arbitrage_system.initialize(overlapping_symbols=symbols)
await arbitrage_system.start()

# 在价差分析完成后
await arbitrage_system.handle_spread_analysis_result(spread_analysis_result)

# 在ticker更新时
await arbitrage_system.handle_ticker_update(ticker_data)
```

核心特性：
- 🎯 精度管理：自动获取和缓存交易所精度信息
- 🧠 智能决策：基于价差分析的套利决策引擎
- ⚡ 统一执行：标准化的交易执行管理器
- 🔄 无缝集成：与现有监视器模块完美集成
- 📊 实时监控：完整的统计信息和性能监控
- 🛡️ 风险控制：独立的风险管理器，多层风险评估和控制机制
- 📈 持仓管理：专业的持仓管理器，完整的持仓生命周期管理
- 🔧 配置管理：灵活的配置系统和热更新支持
- 🚨 告警系统：智能风险告警和事件通知
- 📋 事件追踪：完整的操作事件记录和分析

架构优势：
- 模块化设计，易于维护和扩展
- 依赖注入，降低耦合度
- 异步处理，高性能执行
- 错误处理，健壮性保证
- 占位符设计，渐进式完善
"""
