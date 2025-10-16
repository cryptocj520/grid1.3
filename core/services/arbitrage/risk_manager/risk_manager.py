"""
风险管理器

独立的风险管理功能实现，负责：
- 风险评估和监控
- 风险限制管理
- 风险告警和事件处理
- 风险指标计算
"""

import asyncio
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal
from collections import defaultdict, deque

from core.logging import get_logger
from ..shared.models import MarketSnapshot, ArbitragePosition, ArbitrageDirection
from .risk_models import (
    RiskLevel, RiskType, RiskAlertType, RiskAssessmentResult, 
    RiskMetrics, RiskAlert, RiskEvent, RiskLimit, RiskConfiguration
)


class RiskManager:
    """风险管理器"""
    
    def __init__(self, config: Optional[RiskConfiguration] = None):
        """
        初始化风险管理器
        
        Args:
            config: 风险配置
        """
        self.config = config or RiskConfiguration()
        self.logger = get_logger(__name__)
        
        # 风险限制管理
        self.risk_limits: Dict[str, RiskLimit] = {}
        self._initialize_risk_limits()
        
        # 风险指标
        self.risk_metrics: Dict[str, RiskMetrics] = {}
        
        # 风险告警
        self.active_alerts: Dict[str, RiskAlert] = {}
        self.alert_history: deque = deque(maxlen=1000)
        
        # 风险事件
        self.risk_events: deque = deque(maxlen=1000)
        
        # 实时数据
        self.current_positions: Dict[str, ArbitragePosition] = {}
        self.spread_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # 统计信息
        self.daily_stats = {
            'total_trades': 0,
            'successful_trades': 0,
            'failed_trades': 0,
            'total_profit': Decimal('0'),
            'total_loss': Decimal('0'),
            'max_drawdown': Decimal('0'),
            'start_time': datetime.now()
        }
        
        # 紧急停止标志
        self.emergency_stop = False
        
        # 风险监控任务
        self.monitoring_task: Optional[asyncio.Task] = None
        self.is_monitoring = False
        
        # 回调函数
        self.alert_callback: Optional[Callable] = None
        self.event_callback: Optional[Callable] = None
    
    def _initialize_risk_limits(self):
        """初始化风险限制"""
        # 损失限制
        self.risk_limits['daily_loss'] = RiskLimit(
            limit_type='daily_loss',
            max_value=self.config.max_daily_loss,
            warning_threshold=self.config.max_daily_loss * Decimal(str(self.config.warning_threshold_ratio))
        )
        
        # 持仓数量限制
        self.risk_limits['position_count'] = RiskLimit(
            limit_type='position_count',
            max_value=Decimal(str(self.config.max_position_count)),
            warning_threshold=Decimal(str(self.config.max_position_count * self.config.warning_threshold_ratio))
        )
        
        # 总敞口限制
        self.risk_limits['total_exposure'] = RiskLimit(
            limit_type='total_exposure',
            max_value=self.config.max_total_exposure,
            warning_threshold=self.config.max_total_exposure * Decimal(str(self.config.warning_threshold_ratio))
        )
    
    async def start_monitoring(self):
        """启动风险监控"""
        if self.is_monitoring:
            return
        
        self.is_monitoring = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        self.logger.info("风险监控已启动")
    
    async def stop_monitoring(self):
        """停止风险监控"""
        self.is_monitoring = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        self.logger.info("风险监控已停止")
    
    async def _monitoring_loop(self):
        """风险监控循环"""
        while self.is_monitoring:
            try:
                # 更新风险指标
                await self._update_risk_metrics()
                
                # 检查风险限制
                await self._check_risk_limits()
                
                # 检查持仓风险
                await self._check_position_risks()
                
                # 清理过期数据
                await self._cleanup_expired_data()
                
                # 等待下一次检查
                await asyncio.sleep(self.config.risk_check_interval)
                
            except Exception as e:
                self.logger.error(f"风险监控循环错误: {e}")
                await asyncio.sleep(5)
    
    async def assess_market_risk(self, market_snapshot: MarketSnapshot) -> RiskAssessmentResult:
        """
        评估市场风险
        
        Args:
            market_snapshot: 市场快照
            
        Returns:
            风险评估结果
        """
        try:
            symbol = market_snapshot.symbol
            
            # 初始化风险评估结果
            assessment = RiskAssessmentResult(symbol=symbol)
            
            # 价差风险评估
            spread_risk = await self._assess_spread_risk(market_snapshot)
            assessment.market_risk_score += spread_risk
            
            # 流动性风险评估
            liquidity_risk = await self._assess_liquidity_risk(market_snapshot)
            assessment.liquidity_risk_score += liquidity_risk
            
            # 操作风险评估
            operational_risk = await self._assess_operational_risk(market_snapshot)
            assessment.operational_risk_score += operational_risk
            
            # 计算总体风险评分
            assessment.overall_risk_score = (
                assessment.market_risk_score * 0.4 +
                assessment.liquidity_risk_score * 0.3 +
                assessment.operational_risk_score * 0.3
            )
            
            # 确定风险等级
            assessment.risk_level = self._determine_risk_level(assessment.overall_risk_score)
            
            # 生成风险建议
            await self._generate_risk_recommendations(assessment, market_snapshot)
            
            return assessment
            
        except Exception as e:
            self.logger.error(f"评估市场风险失败: {e}")
            return RiskAssessmentResult(
                symbol=market_snapshot.symbol,
                overall_risk_score=1.0,
                risk_level=RiskLevel.CRITICAL,
                can_execute=False,
                warnings=["风险评估失败"]
            )
    
    async def _assess_spread_risk(self, market_snapshot: MarketSnapshot) -> float:
        """评估价差风险"""
        risk_score = 0.0
        
        # 检查价差是否在合理范围内
        if market_snapshot.spread_percentage > self.config.max_spread_threshold:
            risk_score += 0.5
        elif market_snapshot.spread_percentage < self.config.min_spread_threshold:
            risk_score += 0.3
        
        # 检查价差波动率
        symbol = market_snapshot.symbol
        self.spread_history[symbol].append(market_snapshot.spread_percentage)
        
        if len(self.spread_history[symbol]) > 10:
            spreads = list(self.spread_history[symbol])
            avg_spread = sum(spreads) / len(spreads)
            spread_std = (sum((s - avg_spread) ** 2 for s in spreads) / len(spreads)) ** 0.5
            
            if spread_std > self.config.spread_volatility_threshold:
                risk_score += 0.3
        
        return min(risk_score, 1.0)
    
    async def _assess_liquidity_risk(self, market_snapshot: MarketSnapshot) -> float:
        """评估流动性风险"""
        risk_score = 0.0
        
        # 检查成交量
        total_volume = sum(market_snapshot.volume_info.values())
        if total_volume < self.config.min_volume_threshold:
            risk_score += 0.4
        
        # 检查买卖价差
        bid_ask_spread = market_snapshot.best_ask - market_snapshot.best_bid
        if bid_ask_spread > self.config.max_bid_ask_spread:
            risk_score += 0.3
        
        return min(risk_score, 1.0)
    
    async def _assess_operational_risk(self, market_snapshot: MarketSnapshot) -> float:
        """评估操作风险"""
        risk_score = 0.0
        
        # 检查紧急停止状态
        if self.emergency_stop:
            risk_score += 1.0
        
        # 检查当前持仓数量
        current_positions = len(self.current_positions)
        if current_positions >= self.config.max_position_count:
            risk_score += 0.5
        
        # 检查日损失
        daily_loss = abs(min(self.daily_stats['total_profit'], Decimal('0')))
        if daily_loss >= self.config.max_daily_loss:
            risk_score += 0.8
        
        return min(risk_score, 1.0)
    
    def _determine_risk_level(self, risk_score: float) -> RiskLevel:
        """确定风险等级"""
        if risk_score >= 0.9:
            return RiskLevel.CRITICAL
        elif risk_score >= 0.7:
            return RiskLevel.HIGH
        elif risk_score >= 0.5:
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW
    
    async def _generate_risk_recommendations(self, assessment: RiskAssessmentResult, market_snapshot: MarketSnapshot):
        """生成风险建议"""
        # 基于风险等级生成建议
        if assessment.risk_level == RiskLevel.CRITICAL:
            assessment.can_execute = False
            assessment.should_close_all = True
            assessment.recommendations.append("立即关闭所有持仓")
            assessment.warnings.append("风险过高，禁止新建仓位")
        
        elif assessment.risk_level == RiskLevel.HIGH:
            assessment.can_execute = False
            assessment.should_reduce_position = True
            assessment.recommendations.append("减少持仓规模")
            assessment.warnings.append("风险较高，暂停新建仓位")
        
        elif assessment.risk_level == RiskLevel.MEDIUM:
            assessment.can_execute = True
            assessment.max_position_size = self.config.max_exposure_per_symbol * Decimal('0.5')
            assessment.recommended_size = self.config.max_exposure_per_symbol * Decimal('0.3')
            assessment.recommendations.append("谨慎交易，限制仓位规模")
        
        else:
            assessment.can_execute = True
            assessment.max_position_size = self.config.max_exposure_per_symbol
            assessment.recommended_size = self.config.max_exposure_per_symbol * Decimal('0.8')
    
    async def check_position_risk(self, position: ArbitragePosition) -> bool:
        """
        检查持仓风险
        
        Args:
            position: 套利持仓
            
        Returns:
            是否应该关闭持仓
        """
        try:
            # 检查持仓时间
            holding_time = datetime.now() - position.created_at
            if holding_time > self.config.max_holding_time:
                await self._create_risk_alert(
                    RiskAlertType.POSITION_LIMIT,
                    RiskLevel.HIGH,
                    position.symbol,
                    f"持仓时间过长: {holding_time}"
                )
                return True
            
            # 检查止损
            if position.unrealized_pnl <= self.config.stop_loss_threshold:
                await self._create_risk_alert(
                    RiskAlertType.LOSS_LIMIT,
                    RiskLevel.HIGH,
                    position.symbol,
                    f"触发止损: {position.unrealized_pnl}"
                )
                return True
            
            # 检查止盈
            if position.unrealized_pnl >= self.config.take_profit_threshold:
                await self._create_risk_alert(
                    RiskAlertType.POSITION_LIMIT,
                    RiskLevel.MEDIUM,
                    position.symbol,
                    f"触发止盈: {position.unrealized_pnl}"
                )
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"检查持仓风险失败: {e}")
            return False
    
    async def _update_risk_metrics(self):
        """更新风险指标"""
        try:
            for symbol, position in self.current_positions.items():
                if symbol not in self.risk_metrics:
                    self.risk_metrics[symbol] = RiskMetrics(symbol=symbol)
                
                metrics = self.risk_metrics[symbol]
                
                # 更新持仓指标
                metrics.position_count = len([p for p in self.current_positions.values() 
                                            if p.symbol == symbol])
                metrics.total_exposure = sum(p.quantity for p in self.current_positions.values() 
                                           if p.symbol == symbol)
                metrics.unrealized_pnl = sum(p.unrealized_pnl for p in self.current_positions.values() 
                                           if p.symbol == symbol)
                
                # 更新时间戳
                metrics.timestamp = datetime.now()
                
        except Exception as e:
            self.logger.error(f"更新风险指标失败: {e}")
    
    async def _check_risk_limits(self):
        """检查风险限制"""
        try:
            # 检查日损失限制
            daily_loss = abs(min(self.daily_stats['total_profit'], Decimal('0')))
            self.risk_limits['daily_loss'].current_value = daily_loss
            
            if self.risk_limits['daily_loss'].is_exceeded:
                await self._create_risk_alert(
                    RiskAlertType.LOSS_LIMIT,
                    RiskLevel.CRITICAL,
                    "SYSTEM",
                    f"日损失超限: {daily_loss}"
                )
                self.set_emergency_stop(True)
            
            # 检查持仓数量限制
            position_count = len(self.current_positions)
            self.risk_limits['position_count'].current_value = Decimal(str(position_count))
            
            if self.risk_limits['position_count'].is_exceeded:
                await self._create_risk_alert(
                    RiskAlertType.POSITION_LIMIT,
                    RiskLevel.HIGH,
                    "SYSTEM",
                    f"持仓数量超限: {position_count}"
                )
            
            # 检查总敞口限制
            total_exposure = sum(p.quantity for p in self.current_positions.values())
            self.risk_limits['total_exposure'].current_value = total_exposure
            
            if self.risk_limits['total_exposure'].is_exceeded:
                await self._create_risk_alert(
                    RiskAlertType.EXPOSURE_LIMIT,
                    RiskLevel.HIGH,
                    "SYSTEM",
                    f"总敞口超限: {total_exposure}"
                )
                
        except Exception as e:
            self.logger.error(f"检查风险限制失败: {e}")
    
    async def _check_position_risks(self):
        """检查持仓风险"""
        try:
            for position in self.current_positions.values():
                should_close = await self.check_position_risk(position)
                if should_close:
                    # 这里可以触发平仓操作
                    pass
                    
        except Exception as e:
            self.logger.error(f"检查持仓风险失败: {e}")
    
    async def _create_risk_alert(self, alert_type: RiskAlertType, risk_level: RiskLevel, 
                                symbol: str, message: str, details: Dict[str, Any] = None):
        """创建风险告警"""
        try:
            alert = RiskAlert(
                alert_id=str(uuid.uuid4()),
                alert_type=alert_type,
                risk_level=risk_level,
                symbol=symbol,
                message=message,
                details=details or {}
            )
            
            self.active_alerts[alert.alert_id] = alert
            self.alert_history.append(alert)
            
            # 触发告警回调
            if self.alert_callback:
                await self.alert_callback(alert)
            
            self.logger.warning(f"风险告警: {message}")
            
        except Exception as e:
            self.logger.error(f"创建风险告警失败: {e}")
    
    async def _cleanup_expired_data(self):
        """清理过期数据"""
        try:
            # 清理已解决的告警
            cutoff_time = datetime.now() - timedelta(hours=24)
            expired_alerts = [
                alert_id for alert_id, alert in self.active_alerts.items()
                if alert.resolved and alert.created_at < cutoff_time
            ]
            
            for alert_id in expired_alerts:
                del self.active_alerts[alert_id]
                
        except Exception as e:
            self.logger.error(f"清理过期数据失败: {e}")
    
    # 公共接口方法
    def update_positions(self, positions: Dict[str, ArbitragePosition]):
        """更新持仓信息"""
        self.current_positions = positions
    
    def update_daily_stats(self, stats: Dict[str, Any]):
        """更新日统计信息"""
        self.daily_stats.update(stats)
    
    def set_emergency_stop(self, stop: bool):
        """设置紧急停止"""
        self.emergency_stop = stop
        if stop:
            self.logger.critical("紧急停止已激活")
        else:
            self.logger.info("紧急停止已解除")
    
    def is_trading_allowed(self) -> bool:
        """检查是否允许交易"""
        return not self.emergency_stop and not self.risk_limits['daily_loss'].is_exceeded
    
    def set_alert_callback(self, callback: Callable):
        """设置告警回调"""
        self.alert_callback = callback
    
    def set_event_callback(self, callback: Callable):
        """设置事件回调"""
        self.event_callback = callback
    
    def get_risk_metrics(self, symbol: str = None) -> Dict[str, Any]:
        """获取风险指标"""
        if symbol:
            return self.risk_metrics.get(symbol, RiskMetrics(symbol=symbol)).to_dict()
        else:
            return {s: m.to_dict() for s, m in self.risk_metrics.items()}
    
    def get_active_alerts(self) -> List[RiskAlert]:
        """获取活跃告警"""
        return list(self.active_alerts.values())
    
    def get_risk_limits(self) -> Dict[str, Dict[str, Any]]:
        """获取风险限制状态"""
        return {
            limit_type: {
                'max_value': float(limit.max_value),
                'current_value': float(limit.current_value),
                'utilization_ratio': limit.utilization_ratio,
                'is_warning': limit.is_warning,
                'is_exceeded': limit.is_exceeded
            }
            for limit_type, limit in self.risk_limits.items()
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """获取风险管理统计信息"""
        return {
            'emergency_stop': self.emergency_stop,
            'is_monitoring': self.is_monitoring,
            'active_alerts': len(self.active_alerts),
            'total_alerts': len(self.alert_history),
            'risk_metrics_count': len(self.risk_metrics),
            'current_positions': len(self.current_positions),
            'daily_stats': {
                key: float(value) if isinstance(value, Decimal) else value
                for key, value in self.daily_stats.items()
                if key != 'start_time'
            }
        } 