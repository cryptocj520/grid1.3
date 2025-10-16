"""
持仓管理器

独立的持仓管理功能实现，负责：
- 持仓创建和生命周期管理
- 持仓监控和更新
- 盈亏计算和统计
- 持仓分析和报告
"""

import asyncio
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal
from collections import defaultdict, deque
from statistics import mean, stdev

from core.logging import get_logger
from ..shared.models import ArbitragePosition, ArbitrageDirection, ArbitrageStatus, OrderInfo
from .position_models import (
    PositionStatus, PositionType, PositionCloseReason, PositionEventType,
    PositionMetrics, PositionSummary, PositionEvent, PositionConfiguration, PositionAnalysis
)


class PositionManager:
    """持仓管理器"""
    
    def __init__(self, config: Optional[PositionConfiguration] = None):
        """
        初始化持仓管理器
        
        Args:
            config: 持仓配置
        """
        self.config = config or PositionConfiguration()
        self.logger = get_logger(__name__)
        
        # 持仓存储
        self.active_positions: Dict[str, ArbitragePosition] = {}
        self.closed_positions: Dict[str, ArbitragePosition] = {}
        self.position_history: deque = deque(maxlen=10000)
        
        # 持仓指标
        self.position_metrics: Dict[str, PositionMetrics] = {}
        
        # 持仓事件
        self.position_events: deque = deque(maxlen=self.config.event_retention_count)
        
        # 实时数据
        self.pnl_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.position_snapshots: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # 统计信息
        self.global_stats = {
            'total_positions': 0,
            'active_positions': 0,
            'closed_positions': 0,
            'successful_positions': 0,
            'failed_positions': 0,
            'total_pnl': Decimal('0'),
            'total_volume': Decimal('0'),
            'start_time': datetime.now()
        }
        
        # 监控任务
        self.monitoring_task: Optional[asyncio.Task] = None
        self.is_monitoring = False
        
        # 回调函数
        self.position_callback: Optional[Callable] = None
        self.pnl_callback: Optional[Callable] = None
        self.event_callback: Optional[Callable] = None
    
    async def start_monitoring(self):
        """启动持仓监控"""
        if self.is_monitoring:
            return
        
        self.is_monitoring = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        self.logger.info("持仓监控已启动")
    
    async def stop_monitoring(self):
        """停止持仓监控"""
        self.is_monitoring = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        self.logger.info("持仓监控已停止")
    
    async def _monitoring_loop(self):
        """持仓监控循环"""
        while self.is_monitoring:
            try:
                # 更新持仓盈亏
                await self._update_position_pnl()
                
                # 检查持仓状态
                await self._check_position_status()
                
                # 更新指标
                await self._update_metrics()
                
                # 清理过期数据
                await self._cleanup_expired_data()
                
                # 等待下一次更新
                await asyncio.sleep(self.config.update_interval)
                
            except Exception as e:
                self.logger.error(f"持仓监控循环错误: {e}")
                await asyncio.sleep(5)
    
    async def create_position(
        self,
        position_id: str,
        symbol: str,
        direction: ArbitrageDirection,
        quantity: Decimal,
        long_exchange: str,
        short_exchange: str,
        long_order: Optional[OrderInfo] = None,
        short_order: Optional[OrderInfo] = None,
        entry_spread: Optional[Decimal] = None
    ) -> ArbitragePosition:
        """
        创建新持仓
        
        Args:
            position_id: 持仓ID
            symbol: 交易对符号
            direction: 套利方向
            quantity: 持仓数量
            long_exchange: 多头交易所
            short_exchange: 空头交易所
            long_order: 多头订单
            short_order: 空头订单
            entry_spread: 入场价差
            
        Returns:
            创建的持仓对象
        """
        try:
            # 检查持仓限制
            if not await self._check_position_limits(symbol, quantity):
                raise ValueError("持仓限制检查失败")
            
            # 创建持仓对象
            position = ArbitragePosition(
                position_id=position_id,
                symbol=symbol,
                direction=direction,
                status=ArbitrageStatus.ACTIVE,
                long_exchange=long_exchange,
                short_exchange=short_exchange,
                quantity=quantity,
                entry_price_diff=entry_spread or Decimal('0'),
                long_order=long_order,
                short_order=short_order,
                unrealized_pnl=Decimal('0'),
                created_at=datetime.now()
            )
            
            # 保存持仓
            self.active_positions[position_id] = position
            
            # 更新统计信息
            self.global_stats['total_positions'] += 1
            self.global_stats['active_positions'] += 1
            
            # 记录事件
            await self._create_position_event(
                position_id, PositionEventType.CREATED, symbol,
                f"创建持仓: {direction.value}, 数量: {quantity}"
            )
            
            # 通知回调
            if self.position_callback:
                await self.position_callback(position, 'created')
            
            self.logger.info(f"创建持仓成功: {position_id}")
            return position
            
        except Exception as e:
            self.logger.error(f"创建持仓失败: {position_id} - {e}")
            raise
    
    async def update_position(
        self,
        position_id: str,
        **kwargs
    ) -> Optional[ArbitragePosition]:
        """
        更新持仓信息
        
        Args:
            position_id: 持仓ID
            **kwargs: 更新的字段
            
        Returns:
            更新后的持仓对象
        """
        try:
            position = self.active_positions.get(position_id)
            if not position:
                self.logger.warning(f"持仓不存在: {position_id}")
                return None
            
            # 更新字段
            for field, value in kwargs.items():
                if hasattr(position, field):
                    setattr(position, field, value)
            
            # 更新时间戳
            position.updated_at = datetime.now()
            
            # 记录事件
            await self._create_position_event(
                position_id, PositionEventType.UPDATED, position.symbol,
                f"更新持仓: {list(kwargs.keys())}"
            )
            
            # 通知回调
            if self.position_callback:
                await self.position_callback(position, 'updated')
            
            return position
            
        except Exception as e:
            self.logger.error(f"更新持仓失败: {position_id} - {e}")
            return None
    
    async def close_position(
        self,
        position_id: str,
        close_reason: str = "manual",
        realized_pnl: Optional[Decimal] = None
    ) -> Optional[ArbitragePosition]:
        """
        关闭持仓
        
        Args:
            position_id: 持仓ID
            close_reason: 关闭原因
            realized_pnl: 实现盈亏
            
        Returns:
            关闭的持仓对象
        """
        try:
            position = self.active_positions.get(position_id)
            if not position:
                self.logger.warning(f"持仓不存在: {position_id}")
                return None
            
            # 更新持仓状态
            position.status = ArbitrageStatus.CLOSED
            position.closed_at = datetime.now()
            
            # 设置实现盈亏
            if realized_pnl is not None:
                position.realized_pnl = realized_pnl
            else:
                position.realized_pnl = position.unrealized_pnl
            
            # 从活跃持仓移除
            del self.active_positions[position_id]
            
            # 添加到已关闭持仓
            self.closed_positions[position_id] = position
            self.position_history.append(position)
            
            # 更新统计信息
            self.global_stats['active_positions'] -= 1
            self.global_stats['closed_positions'] += 1
            
            if position.realized_pnl > 0:
                self.global_stats['successful_positions'] += 1
            else:
                self.global_stats['failed_positions'] += 1
            
            self.global_stats['total_pnl'] += position.realized_pnl
            
            # 记录事件
            await self._create_position_event(
                position_id, PositionEventType.CLOSED, position.symbol,
                f"关闭持仓: {close_reason}, 盈亏: {position.realized_pnl}"
            )
            
            # 通知回调
            if self.position_callback:
                await self.position_callback(position, 'closed')
            
            self.logger.info(f"关闭持仓成功: {position_id}")
            return position
            
        except Exception as e:
            self.logger.error(f"关闭持仓失败: {position_id} - {e}")
            return None
    
    async def calculate_position_pnl(
        self,
        position: ArbitragePosition,
        current_spread: Decimal
    ) -> Decimal:
        """
        计算持仓盈亏
        
        Args:
            position: 持仓对象
            current_spread: 当前价差
            
        Returns:
            盈亏金额
        """
        try:
            # 简化的盈亏计算
            # 实际应该根据具体的交易所价格和费用计算
            spread_diff = current_spread - position.entry_price_diff
            
            # 根据套利方向调整盈亏
            if position.direction == ArbitrageDirection.LONG_A_SHORT_B:
                pnl = spread_diff * position.quantity
            else:
                pnl = -spread_diff * position.quantity
            
            return pnl
            
        except Exception as e:
            self.logger.error(f"计算盈亏失败: {position.position_id} - {e}")
            return Decimal('0')
    
    async def update_position_pnl(
        self,
        position_id: str,
        current_spread: Decimal
    ) -> Optional[Decimal]:
        """
        更新持仓盈亏
        
        Args:
            position_id: 持仓ID
            current_spread: 当前价差
            
        Returns:
            更新后的盈亏
        """
        try:
            position = self.active_positions.get(position_id)
            if not position:
                return None
            
            # 计算盈亏
            pnl = await self.calculate_position_pnl(position, current_spread)
            
            # 更新持仓盈亏
            position.unrealized_pnl = pnl
            position.current_price_diff = current_spread
            position.updated_at = datetime.now()
            
            # 记录盈亏历史
            self.pnl_history[position_id].append({
                'timestamp': datetime.now(),
                'pnl': pnl,
                'spread': current_spread
            })
            
            # 检查止损止盈
            await self._check_stop_loss_take_profit(position)
            
            # 通知回调
            if self.pnl_callback:
                await self.pnl_callback(position, pnl)
            
            return pnl
            
        except Exception as e:
            self.logger.error(f"更新持仓盈亏失败: {position_id} - {e}")
            return None
    
    async def _check_position_limits(self, symbol: str, quantity: Decimal) -> bool:
        """检查持仓限制"""
        try:
            # 检查持仓数量限制
            if len(self.active_positions) >= self.config.max_position_count:
                self.logger.warning("持仓数量超过限制")
                return False
            
            # 检查单个持仓规模限制
            if quantity > self.config.max_position_size:
                self.logger.warning(f"持仓规模超过限制: {quantity}")
                return False
            
            # 检查总敞口限制
            total_exposure = sum(p.quantity for p in self.active_positions.values())
            if total_exposure + quantity > self.config.max_total_exposure:
                self.logger.warning(f"总敞口超过限制: {total_exposure + quantity}")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"检查持仓限制失败: {e}")
            return False
    
    async def _check_stop_loss_take_profit(self, position: ArbitragePosition):
        """检查止损止盈"""
        try:
            if not self.config.auto_close_on_loss and not self.config.auto_close_on_profit:
                return
            
            # 检查止损
            if (self.config.auto_close_on_loss and 
                position.unrealized_pnl <= self.config.default_stop_loss):
                await self.close_position(
                    position.position_id,
                    PositionCloseReason.STOP_LOSS.value,
                    position.unrealized_pnl
                )
                return
            
            # 检查止盈
            if (self.config.auto_close_on_profit and 
                position.unrealized_pnl >= self.config.default_take_profit):
                await self.close_position(
                    position.position_id,
                    PositionCloseReason.TAKE_PROFIT.value,
                    position.unrealized_pnl
                )
                return
            
        except Exception as e:
            self.logger.error(f"检查止损止盈失败: {position.position_id} - {e}")
    
    async def _update_position_pnl(self):
        """更新所有持仓盈亏"""
        try:
            # 这里应该从市场数据获取当前价差
            # 为简化，使用随机值模拟
            for position in self.active_positions.values():
                # TODO: 从市场数据获取实际价差
                current_spread = position.entry_price_diff * Decimal('1.01')
                await self.update_position_pnl(position.position_id, current_spread)
                
        except Exception as e:
            self.logger.error(f"更新持仓盈亏失败: {e}")
    
    async def _check_position_status(self):
        """检查持仓状态"""
        try:
            current_time = datetime.now()
            
            for position in list(self.active_positions.values()):
                # 检查持仓超时
                holding_time = current_time - position.created_at
                if holding_time > self.config.max_holding_time:
                    await self.close_position(
                        position.position_id,
                        PositionCloseReason.TIMEOUT.value
                    )
                    
        except Exception as e:
            self.logger.error(f"检查持仓状态失败: {e}")
    
    async def _update_metrics(self):
        """更新持仓指标"""
        try:
            # 按符号分组计算指标
            symbol_groups = defaultdict(list)
            for position in self.active_positions.values():
                symbol_groups[position.symbol].append(position)
            
            for symbol, positions in symbol_groups.items():
                metrics = PositionMetrics(symbol=symbol)
                
                # 基本指标
                metrics.position_count = len(positions)
                metrics.active_positions = len(positions)
                metrics.total_quantity = sum(p.quantity for p in positions)
                metrics.avg_position_size = metrics.total_quantity / len(positions) if positions else Decimal('0')
                
                # 盈亏指标
                metrics.total_unrealized_pnl = sum(p.unrealized_pnl for p in positions)
                
                # 时间指标
                holding_times = [(datetime.now() - p.created_at).total_seconds() 
                               for p in positions]
                if holding_times:
                    metrics.avg_holding_time = timedelta(seconds=mean(holding_times))
                    metrics.max_holding_time = timedelta(seconds=max(holding_times))
                    metrics.min_holding_time = timedelta(seconds=min(holding_times))
                
                self.position_metrics[symbol] = metrics
                
        except Exception as e:
            self.logger.error(f"更新指标失败: {e}")
    
    async def _cleanup_expired_data(self):
        """清理过期数据"""
        try:
            cutoff_time = datetime.now() - timedelta(days=self.config.history_retention_days)
            
            # 清理已关闭持仓
            expired_positions = [
                pos_id for pos_id, pos in self.closed_positions.items()
                if pos.closed_at and pos.closed_at < cutoff_time
            ]
            
            for pos_id in expired_positions:
                del self.closed_positions[pos_id]
            
            # 清理盈亏历史
            for pos_id, history in self.pnl_history.items():
                while history and history[0]['timestamp'] < cutoff_time:
                    history.popleft()
                    
        except Exception as e:
            self.logger.error(f"清理过期数据失败: {e}")
    
    async def _create_position_event(
        self,
        position_id: str,
        event_type: PositionEventType,
        symbol: str,
        description: str,
        details: Dict[str, Any] = None
    ):
        """创建持仓事件"""
        try:
            event = PositionEvent(
                event_id=str(uuid.uuid4()),
                position_id=position_id,
                event_type=event_type,
                symbol=symbol,
                description=description,
                details=details or {}
            )
            
            self.position_events.append(event)
            
            # 通知回调
            if self.event_callback:
                await self.event_callback(event)
                
        except Exception as e:
            self.logger.error(f"创建持仓事件失败: {e}")
    
    # 公共接口方法
    def get_position(self, position_id: str) -> Optional[ArbitragePosition]:
        """获取持仓信息"""
        return (self.active_positions.get(position_id) or 
                self.closed_positions.get(position_id))
    
    def get_active_positions(self, symbol: str = None) -> List[ArbitragePosition]:
        """获取活跃持仓"""
        positions = list(self.active_positions.values())
        if symbol:
            positions = [p for p in positions if p.symbol == symbol]
        return positions
    
    def get_closed_positions(self, symbol: str = None, limit: int = 100) -> List[ArbitragePosition]:
        """获取已关闭持仓"""
        positions = list(self.position_history)[-limit:]
        if symbol:
            positions = [p for p in positions if p.symbol == symbol]
        return positions
    
    def get_position_summary(self, symbol: str = None) -> PositionSummary:
        """获取持仓汇总"""
        try:
            if symbol:
                active_positions = [p for p in self.active_positions.values() if p.symbol == symbol]
                closed_positions = [p for p in self.closed_positions.values() if p.symbol == symbol]
                summary_symbol = symbol
            else:
                active_positions = list(self.active_positions.values())
                closed_positions = list(self.closed_positions.values())
                summary_symbol = "ALL"
            
            summary = PositionSummary(symbol=summary_symbol)
            
            # 持仓数量统计
            summary.active_positions = len(active_positions)
            summary.closed_positions = len(closed_positions)
            summary.total_positions = summary.active_positions + summary.closed_positions
            
            # 资金统计
            summary.total_base_amount = sum(p.quantity for p in active_positions)
            summary.net_position = summary.total_base_amount
            
            # 盈亏统计
            summary.total_unrealized_pnl = sum(p.unrealized_pnl for p in active_positions)
            summary.total_realized_pnl = sum(p.realized_pnl for p in closed_positions 
                                           if p.realized_pnl is not None)
            summary.total_pnl = summary.total_unrealized_pnl + summary.total_realized_pnl
            
            # 风险统计
            if active_positions:
                summary.max_single_position = max(p.quantity for p in active_positions)
                summary.total_exposure = sum(p.quantity for p in active_positions)
                
                # 时间统计
                position_times = [p.created_at for p in active_positions]
                summary.oldest_position_time = min(position_times)
                summary.newest_position_time = max(position_times)
            
            return summary
            
        except Exception as e:
            self.logger.error(f"获取持仓汇总失败: {e}")
            return PositionSummary(symbol=symbol or "ALL")
    
    def get_position_metrics(self, symbol: str = None) -> Dict[str, Any]:
        """获取持仓指标"""
        if symbol:
            return self.position_metrics.get(symbol, PositionMetrics(symbol=symbol)).to_dict()
        else:
            return {s: m.to_dict() for s, m in self.position_metrics.items()}
    
    def get_position_events(self, position_id: str = None, limit: int = 100) -> List[PositionEvent]:
        """获取持仓事件"""
        events = list(self.position_events)[-limit:]
        if position_id:
            events = [e for e in events if e.position_id == position_id]
        return events
    
    def get_stats(self) -> Dict[str, Any]:
        """获取持仓管理统计信息"""
        return {
            'is_monitoring': self.is_monitoring,
            'active_positions': len(self.active_positions),
            'closed_positions': len(self.closed_positions),
            'total_events': len(self.position_events),
            'global_stats': {
                key: float(value) if isinstance(value, Decimal) else value
                for key, value in self.global_stats.items()
                if key != 'start_time'
            }
        }
    
    def set_position_callback(self, callback: Callable):
        """设置持仓回调"""
        self.position_callback = callback
    
    def set_pnl_callback(self, callback: Callable):
        """设置盈亏回调"""
        self.pnl_callback = callback
    
    def set_event_callback(self, callback: Callable):
        """设置事件回调"""
        self.event_callback = callback 