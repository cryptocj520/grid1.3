"""
套利协调器

作为整个系统的核心协调组件，负责协调各个模块之间的工作流程
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal

from core.logging import get_logger
from core.adapters.exchanges.interface import ExchangeInterface
from ..initialization.precision_manager import PrecisionManager
from ..execution.trade_execution_manager import TradeExecutionManager
from ..decision.arbitrage_decision_engine import ArbitrageDecisionEngine
from ..risk_manager.risk_manager import RiskManager
from ..position_manager.position_manager import PositionManager
from ..shared.models import (
    TradePlan, ExecutionResult, ArbitrageOpportunity, ArbitragePosition, ArbitrageStatus
)


class ArbitrageCoordinator:
    """套利协调器"""
    
    def __init__(
        self,
        exchange_adapters: Dict[str, ExchangeInterface],
        config: Dict[str, Any] = None
    ):
        """
        初始化套利协调器
        
        Args:
            exchange_adapters: 交易所适配器字典
            config: 配置参数
        """
        self.exchange_adapters = exchange_adapters
        self.config = config or {}
        
        # 初始化核心组件
        self.precision_manager = PrecisionManager(exchange_adapters)
        self.risk_manager = RiskManager(self.config.get('risk', {}))
        self.position_manager = PositionManager(self.config.get('position', {}))
        self.decision_engine = ArbitrageDecisionEngine(
            self.precision_manager, 
            self.config.get('decision', {})
        )
        self.execution_manager = TradeExecutionManager(
            exchange_adapters,
            self.precision_manager,
            self.config.get('execution', {})
        )
        
        # 状态管理
        self.execution_history: List[ExecutionResult] = []
        self.is_running = False
        
        # 统计信息
        self.stats = {
            'total_opportunities': 0,
            'executed_trades': 0,
            'successful_trades': 0,
            'failed_trades': 0,
            'total_profit': Decimal('0'),
            'start_time': None
        }
        
        # 外部回调
        self.market_data_callback: Optional[Callable] = None
        self.execution_callback: Optional[Callable] = None
        
        self.logger = get_logger(__name__)
    
    async def initialize(self, overlapping_symbols: List[str]) -> bool:
        """
        初始化套利系统
        
        Args:
            overlapping_symbols: 重叠交易对列表
            
        Returns:
            是否成功初始化
        """
        try:
            self.logger.info("开始初始化套利系统...")
            
            # 初始化精度管理器
            precision_success = await self.precision_manager.initialize_precision_cache(
                overlapping_symbols
            )
            
            if not precision_success:
                self.logger.error("精度管理器初始化失败")
                return False
            
            # 启动风险管理器
            await self.risk_manager.start_monitoring()
            
            # 启动持仓管理器
            await self.position_manager.start_monitoring()
            
            # 设置决策引擎回调
            self.decision_engine.set_opportunity_callback(self._handle_opportunity)
            
            # 设置模块间的回调
            self.position_manager.set_position_callback(self._on_position_change)
            self.risk_manager.set_alert_callback(self._on_risk_alert)
            
            # 重置统计信息
            self.stats['start_time'] = datetime.now()
            
            self.logger.info("套利系统初始化完成")
            return True
            
        except Exception as e:
            self.logger.error(f"初始化套利系统失败: {e}")
            return False
    
    async def start(self):
        """启动套利系统"""
        try:
            if self.is_running:
                self.logger.warning("套利系统已经在运行中")
                return
            
            self.is_running = True
            self.logger.info("套利系统已启动")
            
            # 启动监控任务
            asyncio.create_task(self._monitoring_loop())
            
        except Exception as e:
            self.logger.error(f"启动套利系统失败: {e}")
            self.is_running = False
    
    async def stop(self):
        """停止套利系统"""
        try:
            self.is_running = False
            
            # 关闭所有活跃位置
            await self._close_all_positions()
            
            # 关闭组件
            await self.precision_manager.shutdown()
            await self.risk_manager.stop_monitoring()
            await self.position_manager.stop_monitoring()
            
            self.logger.info("套利系统已停止")
            
        except Exception as e:
            self.logger.error(f"停止套利系统失败: {e}")
    
    async def handle_market_data(self, market_data: Dict[str, Any]):
        """
        处理市场数据（与现有监视器模块集成的主要接口）
        
        Args:
            market_data: 市场数据
        """
        try:
            if not self.is_running:
                return
            
            # 更新统计信息
            self.stats['total_opportunities'] += 1
            
            # 传递给决策引擎分析
            trade_plan = await self.decision_engine.analyze_market_data(market_data)
            
            if trade_plan:
                # 风险评估
                risk_assessment = await self.risk_manager.assess_market_risk(trade_plan.market_snapshot)
                
                if risk_assessment.can_execute:
                    # 执行交易计划
                    await self._execute_trade_plan(trade_plan)
                else:
                    self.logger.warning(f"风险评估不通过，跳过交易: {risk_assessment.warnings}")
            
            # 触发外部回调
            if self.market_data_callback:
                await self.market_data_callback(market_data, trade_plan)
                
        except Exception as e:
            self.logger.error(f"处理市场数据失败: {e}")
    
    async def _handle_opportunity(self, opportunity: ArbitrageOpportunity, trade_plan: TradePlan):
        """
        处理套利机会
        
        Args:
            opportunity: 套利机会
            trade_plan: 交易计划
        """
        try:
            self.logger.info(f"发现套利机会: {opportunity.symbol} - {opportunity.spread_percentage}%")
            
            # 验证机会是否仍然有效
            if not opportunity.is_valid:
                self.logger.warning(f"套利机会已过期: {opportunity.opportunity_id}")
                return
            
            # 执行交易计划
            await self._execute_trade_plan(trade_plan)
            
        except Exception as e:
            self.logger.error(f"处理套利机会失败: {e}")
    
    async def _execute_trade_plan(self, trade_plan: TradePlan):
        """
        执行交易计划
        
        Args:
            trade_plan: 交易计划
        """
        try:
            self.logger.info(f"开始执行交易计划: {trade_plan.plan_id}")
            
            # 更新统计信息
            self.stats['executed_trades'] += 1
            
            # 执行交易
            result = await self.execution_manager.execute_trade_plan(trade_plan)
            
            # 记录执行结果
            self.execution_history.append(result)
            
            if result.success:
                # 创建活跃位置
                position = await self._create_position(trade_plan, result)
                if position:
                    # 通过持仓管理器管理持仓
                    await self.position_manager.create_position(
                        position_id=position.position_id,
                        symbol=position.symbol,
                        direction=position.direction,
                        quantity=position.quantity,
                        long_exchange=position.long_exchange,
                        short_exchange=position.short_exchange,
                        long_order=position.long_order,
                        short_order=position.short_order,
                        entry_spread=position.entry_price_diff
                    )
                
                # 更新统计信息
                self.stats['successful_trades'] += 1
                if result.actual_profit:
                    self.stats['total_profit'] += result.actual_profit
                
                self.logger.info(f"交易执行成功: {trade_plan.plan_id}")
                
            else:
                # 更新失败统计
                self.stats['failed_trades'] += 1
                self.logger.error(f"交易执行失败: {trade_plan.plan_id} - {result.error_message}")
            
            # 触发执行回调
            if self.execution_callback:
                await self.execution_callback(trade_plan, result)
                
        except Exception as e:
            self.logger.error(f"执行交易计划失败: {e}")
    
    async def _create_position(
        self, 
        trade_plan: TradePlan, 
        result: ExecutionResult
    ) -> Optional[ArbitragePosition]:
        """
        创建套利位置
        
        Args:
            trade_plan: 交易计划
            result: 执行结果
            
        Returns:
            套利位置
        """
        try:
            if not result.success or not result.long_order or not result.short_order:
                return None
            
            position = ArbitragePosition(
                position_id=f"pos_{trade_plan.plan_id}",
                symbol=trade_plan.symbol,
                direction=trade_plan.direction,
                status=ArbitrageStatus.ACTIVE,
                long_exchange=trade_plan.long_exchange,
                short_exchange=trade_plan.short_exchange,
                quantity=trade_plan.quantity,
                entry_price_diff=trade_plan.expected_profit / trade_plan.quantity,
                long_order=result.long_order,
                short_order=result.short_order,
                unrealized_pnl=result.actual_profit or Decimal('0'),
                created_at=datetime.now()
            )
            
            return position
            
        except Exception as e:
            self.logger.error(f"创建套利位置失败: {e}")
            return None
    
    async def _monitoring_loop(self):
        """监控循环"""
        while self.is_running:
            try:
                # 监控活跃位置
                await self._monitor_active_positions()
                
                # 清理过期数据
                await self._cleanup_expired_data()
                
                # 等待下一个周期
                await asyncio.sleep(10)  # 10秒监控周期
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"监控循环出错: {e}")
                await asyncio.sleep(5)
    
    async def _monitor_active_positions(self):
        """监控活跃位置"""
        try:
            for position_id, position in list(self.active_positions.items()):
                # 检查位置是否需要关闭
                if await self._should_close_position(position):
                    await self._close_position(position_id)
                    
        except Exception as e:
            self.logger.error(f"监控活跃位置失败: {e}")
    
    async def _should_close_position(self, position: ArbitragePosition) -> bool:
        """
        检查是否应该关闭位置
        
        Args:
            position: 套利位置
            
        Returns:
            是否应该关闭
        """
        try:
            # 检查持仓时间
            holding_time = (datetime.now() - position.created_at).total_seconds()
            max_holding_time = self.config.get('max_holding_time', 300)  # 5分钟
            
            if holding_time > max_holding_time:
                return True
            
            # TODO: 实现更复杂的关闭逻辑
            # - 价差回归检查
            # - 盈亏阈值检查
            # - 市场条件检查
            
            return False
            
        except Exception as e:
            self.logger.error(f"检查位置关闭条件失败: {e}")
            return False
    
    async def _close_position(self, position_id: str):
        """
        关闭位置
        
        Args:
            position_id: 位置ID
        """
        try:
            position = self.active_positions.get(position_id)
            if not position:
                return
            
            self.logger.info(f"关闭套利位置: {position_id}")
            
            # 更新位置状态
            position.status = ArbitrageStatus.CLOSING
            position.updated_at = datetime.now()
            
            # TODO: 实现位置关闭逻辑
            # - 平仓订单
            # - 盈亏计算
            # - 状态更新
            
            # 暂时标记为已关闭
            position.status = ArbitrageStatus.CLOSED
            position.closed_at = datetime.now()
            
            # 从活跃位置中移除
            del self.active_positions[position_id]
            
            self.logger.info(f"套利位置已关闭: {position_id}")
            
        except Exception as e:
            self.logger.error(f"关闭位置失败: {position_id} - {e}")
    
    async def _close_all_positions(self):
        """关闭所有活跃位置"""
        try:
            active_positions = self.position_manager.get_active_positions()
            for position in active_positions:
                await self.position_manager.close_position(
                    position.position_id,
                    "system_shutdown"
                )
                
        except Exception as e:
            self.logger.error(f"关闭所有位置失败: {e}")
    
    async def _cleanup_expired_data(self):
        """清理过期数据"""
        try:
            # 清理执行历史
            cutoff_time = datetime.now() - timedelta(hours=24)
            self.execution_history = [
                result for result in self.execution_history
                if result.timestamp > cutoff_time
            ]
            
        except Exception as e:
            self.logger.error(f"清理过期数据失败: {e}")
    
    def set_market_data_callback(self, callback: Callable):
        """设置市场数据回调"""
        self.market_data_callback = callback
    
    def set_execution_callback(self, callback: Callable):
        """设置执行回调"""
        self.execution_callback = callback
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.stats.copy()
        stats['active_positions'] = len(self.position_manager.get_active_positions())
        stats['running_time'] = (
            datetime.now() - self.stats['start_time']
        ).total_seconds() if self.stats['start_time'] else 0
        
        # 添加子模块统计
        stats['risk_manager'] = self.risk_manager.get_stats()
        stats['position_manager'] = self.position_manager.get_stats()
        
        return stats
    
    def get_active_positions(self) -> List[ArbitragePosition]:
        """获取活跃位置列表"""
        return self.position_manager.get_active_positions()
    
    def get_execution_history(self, limit: int = 100) -> List[ExecutionResult]:
        """获取执行历史"""
        return self.execution_history[-limit:]
    
    # TODO: 高级功能占位符
    async def optimize_execution_timing(self, trade_plan: TradePlan) -> TradePlan:
        """
        优化执行时机
        
        Args:
            trade_plan: 原始交易计划
            
        Returns:
            优化后的交易计划
        """
        # TODO: 实现执行时机优化
        return trade_plan
    
    async def manage_risk_exposure(self):
        """管理风险敞口"""
        # TODO: 实现风险敞口管理
        pass
    
    async def rebalance_positions(self):
        """重新平衡位置"""
        # TODO: 实现位置重新平衡
        pass
    
    async def generate_performance_report(self) -> Dict[str, Any]:
        """生成性能报告"""
        # TODO: 实现性能报告生成
        return {
            'status': 'not_implemented',
            'stats': self.get_stats()
        }
    
    async def _on_position_change(self, position: ArbitragePosition, action: str):
        """
        持仓变化回调
        
        Args:
            position: 持仓对象
            action: 动作类型 ('created', 'updated', 'closed')
        """
        try:
            # 更新风险管理器的持仓信息
            active_positions = {pos.position_id: pos for pos in self.position_manager.get_active_positions()}
            self.risk_manager.update_positions(active_positions)
            
            # 更新统计信息
            if action == 'created':
                self.logger.info(f"持仓创建: {position.position_id}")
            elif action == 'closed':
                self.logger.info(f"持仓关闭: {position.position_id}, 盈亏: {position.realized_pnl}")
                
        except Exception as e:
            self.logger.error(f"处理持仓变化失败: {e}")
    
    async def _on_risk_alert(self, alert):
        """
        风险告警回调
        
        Args:
            alert: 风险告警对象
        """
        try:
            self.logger.warning(f"风险告警: {alert.message}")
            
            # 根据告警类型采取相应行动
            if alert.risk_level.value == 'critical':
                # 严重风险，暂停所有交易
                self.logger.critical("检测到严重风险，启动紧急停止")
                # TODO: 实现紧急停止逻辑
                
        except Exception as e:
            self.logger.error(f"处理风险告警失败: {e}")
    
    async def emergency_shutdown(self):
        """紧急关闭"""
        # TODO: 实现紧急关闭逻辑
        self.logger.warning("执行紧急关闭")
        await self.stop() 