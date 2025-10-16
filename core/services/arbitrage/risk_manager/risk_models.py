"""
风险管理相关数据模型

定义风险评估、风险监控、风险控制等功能的数据结构
"""

from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from decimal import Decimal


class RiskLevel(Enum):
    """风险等级"""
    LOW = "low"              # 低风险
    MEDIUM = "medium"        # 中风险
    HIGH = "high"            # 高风险
    CRITICAL = "critical"    # 临界风险
    EMERGENCY = "emergency"  # 紧急风险


class RiskType(Enum):
    """风险类型"""
    MARKET = "market"           # 市场风险
    LIQUIDITY = "liquidity"     # 流动性风险
    COUNTERPARTY = "counterparty"  # 交易对手风险
    OPERATIONAL = "operational"  # 操作风险
    TECHNICAL = "technical"     # 技术风险
    REGULATORY = "regulatory"   # 监管风险


class RiskAlertType(Enum):
    """风险告警类型"""
    POSITION_LIMIT = "position_limit"    # 持仓限制
    LOSS_LIMIT = "loss_limit"           # 损失限制
    VOLUME_LIMIT = "volume_limit"       # 成交量限制
    SPREAD_ANOMALY = "spread_anomaly"   # 价差异常
    EXPOSURE_LIMIT = "exposure_limit"   # 敞口限制
    EMERGENCY_STOP = "emergency_stop"   # 紧急停止


@dataclass
class RiskLimit:
    """风险限制配置"""
    limit_type: str
    max_value: Decimal
    warning_threshold: Decimal  # 警告阈值（通常为最大值的80%）
    current_value: Decimal = Decimal('0')
    
    @property
    def utilization_ratio(self) -> float:
        """使用率"""
        if self.max_value == 0:
            return 0.0
        return float(self.current_value / self.max_value)
    
    @property
    def is_warning(self) -> bool:
        """是否达到警告阈值"""
        return self.current_value >= self.warning_threshold
    
    @property
    def is_exceeded(self) -> bool:
        """是否超过限制"""
        return self.current_value >= self.max_value


@dataclass
class RiskMetrics:
    """风险指标"""
    symbol: str
    timestamp: datetime = field(default_factory=datetime.now)
    
    # 持仓风险
    position_count: int = 0
    total_exposure: Decimal = Decimal('0')
    max_single_position: Decimal = Decimal('0')
    
    # 盈亏风险
    unrealized_pnl: Decimal = Decimal('0')
    realized_pnl: Decimal = Decimal('0')
    daily_pnl: Decimal = Decimal('0')
    max_drawdown: Decimal = Decimal('0')
    
    # 成交量风险
    daily_volume: Decimal = Decimal('0')
    avg_volume: Decimal = Decimal('0')
    volume_ratio: float = 0.0
    
    # 价差风险
    current_spread: Decimal = Decimal('0')
    avg_spread: Decimal = Decimal('0')
    spread_volatility: float = 0.0
    
    # 时间风险
    max_holding_time: timedelta = timedelta(minutes=5)
    avg_holding_time: timedelta = timedelta(minutes=0)
    
    # 流动性风险
    bid_ask_spread: Decimal = Decimal('0')
    market_depth: Decimal = Decimal('0')
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp.isoformat(),
            'position_count': self.position_count,
            'total_exposure': float(self.total_exposure),
            'max_single_position': float(self.max_single_position),
            'unrealized_pnl': float(self.unrealized_pnl),
            'realized_pnl': float(self.realized_pnl),
            'daily_pnl': float(self.daily_pnl),
            'max_drawdown': float(self.max_drawdown),
            'daily_volume': float(self.daily_volume),
            'avg_volume': float(self.avg_volume),
            'volume_ratio': self.volume_ratio,
            'current_spread': float(self.current_spread),
            'avg_spread': float(self.avg_spread),
            'spread_volatility': self.spread_volatility,
            'max_holding_time': self.max_holding_time.total_seconds(),
            'avg_holding_time': self.avg_holding_time.total_seconds(),
            'bid_ask_spread': float(self.bid_ask_spread),
            'market_depth': float(self.market_depth)
        }


@dataclass
class RiskAssessmentResult:
    """风险评估结果"""
    symbol: str
    assessment_time: datetime = field(default_factory=datetime.now)
    
    # 风险评分
    overall_risk_score: float = 0.0        # 总体风险评分 (0-1)
    risk_level: RiskLevel = RiskLevel.LOW  # 风险等级
    
    # 分项风险评分
    market_risk_score: float = 0.0         # 市场风险
    liquidity_risk_score: float = 0.0      # 流动性风险
    operational_risk_score: float = 0.0    # 操作风险
    
    # 风险因子
    risk_factors: Dict[str, float] = field(default_factory=dict)
    
    # 风险警告
    warnings: List[str] = field(default_factory=list)
    
    # 建议
    recommendations: List[str] = field(default_factory=list)
    
    # 限制建议
    max_position_size: Decimal = Decimal('0')
    recommended_size: Decimal = Decimal('0')
    
    # 执行建议
    can_execute: bool = True
    should_reduce_position: bool = False
    should_close_all: bool = False
    
    @property
    def is_acceptable(self) -> bool:
        """风险是否可接受"""
        return self.overall_risk_score < 0.8 and self.risk_level != RiskLevel.CRITICAL


@dataclass
class RiskAlert:
    """风险告警"""
    alert_id: str
    alert_type: RiskAlertType
    risk_level: RiskLevel
    symbol: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    acknowledged: bool = False
    resolved: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'alert_id': self.alert_id,
            'alert_type': self.alert_type.value,
            'risk_level': self.risk_level.value,
            'symbol': self.symbol,
            'message': self.message,
            'details': self.details,
            'created_at': self.created_at.isoformat(),
            'acknowledged': self.acknowledged,
            'resolved': self.resolved
        }


@dataclass
class RiskEvent:
    """风险事件"""
    event_id: str
    event_type: str
    symbol: str
    description: str
    risk_level: RiskLevel
    impact: str                           # 影响描述
    action_taken: str                     # 采取的行动
    created_at: datetime = field(default_factory=datetime.now)
    resolved_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'event_id': self.event_id,
            'event_type': self.event_type,
            'symbol': self.symbol,
            'description': self.description,
            'risk_level': self.risk_level.value,
            'impact': self.impact,
            'action_taken': self.action_taken,
            'created_at': self.created_at.isoformat(),
            'resolved_at': self.resolved_at.isoformat() if self.resolved_at else None
        }


@dataclass
class RiskConfiguration:
    """风险配置"""
    # 基础风险限制
    max_daily_loss: Decimal = Decimal('1000')        # 最大日损失
    max_position_count: int = 5                      # 最大持仓数量
    max_exposure_per_symbol: Decimal = Decimal('5000')  # 每个符号的最大敞口
    max_total_exposure: Decimal = Decimal('20000')   # 最大总敞口
    
    # 阈值设置
    stop_loss_threshold: Decimal = Decimal('-100')   # 止损阈值
    take_profit_threshold: Decimal = Decimal('200')  # 止盈阈值
    warning_threshold_ratio: float = 0.8             # 警告阈值比例
    
    # 价差风险
    max_spread_threshold: Decimal = Decimal('5.0')   # 最大价差阈值
    min_spread_threshold: Decimal = Decimal('0.05')  # 最小价差阈值
    spread_volatility_threshold: float = 0.3         # 价差波动率阈值
    
    # 流动性风险
    min_volume_threshold: Decimal = Decimal('1000')  # 最小成交量阈值
    max_bid_ask_spread: Decimal = Decimal('0.01')    # 最大买卖价差
    
    # 时间风险
    max_holding_time: timedelta = timedelta(minutes=30)  # 最大持仓时间
    position_timeout: timedelta = timedelta(minutes=5)   # 持仓超时时间
    
    # 检查间隔
    risk_check_interval: int = 10                    # 风险检查间隔（秒）
    metrics_update_interval: int = 60                # 指标更新间隔（秒）
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'max_daily_loss': float(self.max_daily_loss),
            'max_position_count': self.max_position_count,
            'max_exposure_per_symbol': float(self.max_exposure_per_symbol),
            'max_total_exposure': float(self.max_total_exposure),
            'stop_loss_threshold': float(self.stop_loss_threshold),
            'take_profit_threshold': float(self.take_profit_threshold),
            'warning_threshold_ratio': self.warning_threshold_ratio,
            'max_spread_threshold': float(self.max_spread_threshold),
            'min_spread_threshold': float(self.min_spread_threshold),
            'spread_volatility_threshold': self.spread_volatility_threshold,
            'min_volume_threshold': float(self.min_volume_threshold),
            'max_bid_ask_spread': float(self.max_bid_ask_spread),
            'max_holding_time': self.max_holding_time.total_seconds(),
            'position_timeout': self.position_timeout.total_seconds(),
            'risk_check_interval': self.risk_check_interval,
            'metrics_update_interval': self.metrics_update_interval
        } 