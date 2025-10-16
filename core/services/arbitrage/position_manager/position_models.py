"""
持仓管理相关数据模型

定义持仓管理、持仓监控、持仓统计等功能的数据结构
"""

from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from decimal import Decimal


class PositionStatus(Enum):
    """持仓状态"""
    OPENING = "opening"         # 开仓中
    ACTIVE = "active"           # 活跃
    PARTIAL_FILLED = "partial_filled"  # 部分成交
    CLOSING = "closing"         # 平仓中
    CLOSED = "closed"           # 已平仓
    EXPIRED = "expired"         # 已过期
    FAILED = "failed"           # 失败
    CANCELLED = "cancelled"     # 已取消


class PositionType(Enum):
    """持仓类型"""
    LONG = "long"               # 多头
    SHORT = "short"             # 空头
    HEDGE = "hedge"             # 对冲
    ARBITRAGE = "arbitrage"     # 套利


class PositionCloseReason(Enum):
    """平仓原因"""
    MANUAL = "manual"           # 手动平仓
    STOP_LOSS = "stop_loss"     # 止损
    TAKE_PROFIT = "take_profit" # 止盈
    TIMEOUT = "timeout"         # 超时
    RISK_LIMIT = "risk_limit"   # 风险限制
    EMERGENCY = "emergency"     # 紧急平仓
    SYSTEM = "system"           # 系统平仓


class PositionEventType(Enum):
    """持仓事件类型"""
    CREATED = "created"         # 创建
    UPDATED = "updated"         # 更新
    FILLED = "filled"           # 成交
    PARTIAL_FILLED = "partial_filled"  # 部分成交
    CLOSED = "closed"           # 关闭
    EXPIRED = "expired"         # 过期
    ERROR = "error"             # 错误


@dataclass
class PositionMetrics:
    """持仓指标"""
    symbol: str
    timestamp: datetime = field(default_factory=datetime.now)
    
    # 持仓基本信息
    position_count: int = 0
    active_positions: int = 0
    total_quantity: Decimal = Decimal('0')
    avg_position_size: Decimal = Decimal('0')
    
    # 盈亏信息
    total_unrealized_pnl: Decimal = Decimal('0')
    total_realized_pnl: Decimal = Decimal('0')
    avg_pnl_per_position: Decimal = Decimal('0')
    win_rate: float = 0.0
    
    # 时间信息
    avg_holding_time: timedelta = timedelta(minutes=0)
    max_holding_time: timedelta = timedelta(minutes=0)
    min_holding_time: timedelta = timedelta(minutes=0)
    
    # 风险指标
    max_drawdown: Decimal = Decimal('0')
    sharpe_ratio: float = 0.0
    var_95: Decimal = Decimal('0')  # 95% Value at Risk
    
    # 执行效率
    avg_execution_time: float = 0.0
    success_rate: float = 0.0
    slippage: Decimal = Decimal('0')
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp.isoformat(),
            'position_count': self.position_count,
            'active_positions': self.active_positions,
            'total_quantity': float(self.total_quantity),
            'avg_position_size': float(self.avg_position_size),
            'total_unrealized_pnl': float(self.total_unrealized_pnl),
            'total_realized_pnl': float(self.total_realized_pnl),
            'avg_pnl_per_position': float(self.avg_pnl_per_position),
            'win_rate': self.win_rate,
            'avg_holding_time': self.avg_holding_time.total_seconds(),
            'max_holding_time': self.max_holding_time.total_seconds(),
            'min_holding_time': self.min_holding_time.total_seconds(),
            'max_drawdown': float(self.max_drawdown),
            'sharpe_ratio': self.sharpe_ratio,
            'var_95': float(self.var_95),
            'avg_execution_time': self.avg_execution_time,
            'success_rate': self.success_rate,
            'slippage': float(self.slippage)
        }


@dataclass
class PositionSummary:
    """持仓汇总"""
    symbol: str
    summary_time: datetime = field(default_factory=datetime.now)
    
    # 持仓数量统计
    total_positions: int = 0
    active_positions: int = 0
    closed_positions: int = 0
    failed_positions: int = 0
    
    # 资金统计
    total_base_amount: Decimal = Decimal('0')    # 基础资产总量
    total_quote_amount: Decimal = Decimal('0')   # 计价资产总量
    net_position: Decimal = Decimal('0')         # 净持仓
    
    # 盈亏统计
    total_unrealized_pnl: Decimal = Decimal('0')
    total_realized_pnl: Decimal = Decimal('0')
    total_pnl: Decimal = Decimal('0')
    
    # 风险统计
    max_single_position: Decimal = Decimal('0')
    total_exposure: Decimal = Decimal('0')
    leverage_ratio: float = 0.0
    
    # 时间统计
    oldest_position_time: Optional[datetime] = None
    newest_position_time: Optional[datetime] = None
    
    @property
    def is_balanced(self) -> bool:
        """是否平衡（净持仓接近0）"""
        return abs(self.net_position) < Decimal('0.01')
    
    @property
    def pnl_percentage(self) -> float:
        """盈亏百分比"""
        if self.total_base_amount == 0:
            return 0.0
        return float(self.total_pnl / self.total_base_amount * 100)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'symbol': self.symbol,
            'summary_time': self.summary_time.isoformat(),
            'total_positions': self.total_positions,
            'active_positions': self.active_positions,
            'closed_positions': self.closed_positions,
            'failed_positions': self.failed_positions,
            'total_base_amount': float(self.total_base_amount),
            'total_quote_amount': float(self.total_quote_amount),
            'net_position': float(self.net_position),
            'total_unrealized_pnl': float(self.total_unrealized_pnl),
            'total_realized_pnl': float(self.total_realized_pnl),
            'total_pnl': float(self.total_pnl),
            'max_single_position': float(self.max_single_position),
            'total_exposure': float(self.total_exposure),
            'leverage_ratio': self.leverage_ratio,
            'is_balanced': self.is_balanced,
            'pnl_percentage': self.pnl_percentage,
            'oldest_position_time': self.oldest_position_time.isoformat() if self.oldest_position_time else None,
            'newest_position_time': self.newest_position_time.isoformat() if self.newest_position_time else None
        }


@dataclass
class PositionEvent:
    """持仓事件"""
    event_id: str
    position_id: str
    event_type: PositionEventType
    symbol: str
    description: str
    details: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'event_id': self.event_id,
            'position_id': self.position_id,
            'event_type': self.event_type.value,
            'symbol': self.symbol,
            'description': self.description,
            'details': self.details,
            'created_at': self.created_at.isoformat()
        }


@dataclass
class PositionConfiguration:
    """持仓配置"""
    # 持仓限制
    max_position_count: int = 10                  # 最大持仓数量
    max_position_size: Decimal = Decimal('10000') # 最大单个持仓规模
    max_total_exposure: Decimal = Decimal('50000') # 最大总敞口
    
    # 时间限制
    max_holding_time: timedelta = timedelta(hours=1)  # 最大持仓时间
    position_timeout: timedelta = timedelta(minutes=30)  # 持仓超时时间
    
    # 盈亏控制
    default_stop_loss: Decimal = Decimal('-100')   # 默认止损
    default_take_profit: Decimal = Decimal('200')  # 默认止盈
    auto_close_on_profit: bool = True              # 盈利自动平仓
    auto_close_on_loss: bool = True                # 损失自动平仓
    
    # 风险控制
    enable_risk_checks: bool = True                # 启用风险检查
    max_drawdown_threshold: Decimal = Decimal('500')  # 最大回撤阈值
    correlation_threshold: float = 0.8             # 相关性阈值
    
    # 监控设置
    update_interval: int = 5                       # 更新间隔（秒）
    pnl_update_interval: int = 10                  # 盈亏更新间隔（秒）
    metrics_update_interval: int = 60              # 指标更新间隔（秒）
    
    # 通知设置
    enable_notifications: bool = True              # 启用通知
    notify_on_open: bool = True                    # 开仓通知
    notify_on_close: bool = True                   # 平仓通知
    notify_on_profit: bool = True                  # 盈利通知
    notify_on_loss: bool = True                    # 损失通知
    
    # 数据保留
    history_retention_days: int = 30               # 历史数据保留天数
    event_retention_count: int = 10000             # 事件保留数量
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'max_position_count': self.max_position_count,
            'max_position_size': float(self.max_position_size),
            'max_total_exposure': float(self.max_total_exposure),
            'max_holding_time': self.max_holding_time.total_seconds(),
            'position_timeout': self.position_timeout.total_seconds(),
            'default_stop_loss': float(self.default_stop_loss),
            'default_take_profit': float(self.default_take_profit),
            'auto_close_on_profit': self.auto_close_on_profit,
            'auto_close_on_loss': self.auto_close_on_loss,
            'enable_risk_checks': self.enable_risk_checks,
            'max_drawdown_threshold': float(self.max_drawdown_threshold),
            'correlation_threshold': self.correlation_threshold,
            'update_interval': self.update_interval,
            'pnl_update_interval': self.pnl_update_interval,
            'metrics_update_interval': self.metrics_update_interval,
            'enable_notifications': self.enable_notifications,
            'notify_on_open': self.notify_on_open,
            'notify_on_close': self.notify_on_close,
            'notify_on_profit': self.notify_on_profit,
            'notify_on_loss': self.notify_on_loss,
            'history_retention_days': self.history_retention_days,
            'event_retention_count': self.event_retention_count
        }


@dataclass
class PositionAnalysis:
    """持仓分析结果"""
    symbol: str
    analysis_time: datetime = field(default_factory=datetime.now)
    
    # 性能分析
    total_return: Decimal = Decimal('0')           # 总收益
    annualized_return: float = 0.0                 # 年化收益率
    volatility: float = 0.0                        # 波动率
    sharpe_ratio: float = 0.0                      # 夏普比率
    max_drawdown: Decimal = Decimal('0')           # 最大回撤
    
    # 持仓分析
    win_rate: float = 0.0                          # 胜率
    avg_win: Decimal = Decimal('0')                # 平均盈利
    avg_loss: Decimal = Decimal('0')               # 平均亏损
    profit_factor: float = 0.0                     # 盈利因子
    
    # 时间分析
    avg_holding_time: timedelta = timedelta(minutes=0)
    holding_time_distribution: Dict[str, int] = field(default_factory=dict)
    
    # 风险分析
    var_95: Decimal = Decimal('0')                 # 95% VaR
    expected_shortfall: Decimal = Decimal('0')     # 期望损失
    beta: float = 0.0                              # 贝塔系数
    
    # 建议
    recommendations: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'symbol': self.symbol,
            'analysis_time': self.analysis_time.isoformat(),
            'total_return': float(self.total_return),
            'annualized_return': self.annualized_return,
            'volatility': self.volatility,
            'sharpe_ratio': self.sharpe_ratio,
            'max_drawdown': float(self.max_drawdown),
            'win_rate': self.win_rate,
            'avg_win': float(self.avg_win),
            'avg_loss': float(self.avg_loss),
            'profit_factor': self.profit_factor,
            'avg_holding_time': self.avg_holding_time.total_seconds(),
            'holding_time_distribution': self.holding_time_distribution,
            'var_95': float(self.var_95),
            'expected_shortfall': float(self.expected_shortfall),
            'beta': self.beta,
            'recommendations': self.recommendations
        } 