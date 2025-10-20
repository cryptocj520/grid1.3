"""
持仓监控模块

🔥 重大修改：完全使用REST API进行持仓同步（不再依赖WebSocket）
原因：Backpack WebSocket持仓流不推送订单成交导致的变化，导致缓存过期
"""

import asyncio
import time
from typing import Dict, Any, Optional
from decimal import Decimal
from datetime import datetime

from ....logging import get_logger


class PositionMonitor:
    """
    持仓监控管理器（纯REST API）

    职责：
    1. 定时REST查询（30秒间隔）
    2. 事件触发REST查询（5秒去重）
    3. REST失败保护（暂停订单）
    4. 持仓异常检测（紧急停止）

    设计原则：
    - 持仓数据：REST API（准确但较慢）
    - 订单数据：WebSocket（快速且可靠）- 由其他模块处理
    """

    def __init__(self, engine, tracker, config, coordinator):
        """
        初始化持仓监控器

        Args:
            engine: 执行引擎
            tracker: 持仓跟踪器
            config: 网格配置
            coordinator: 协调器引用（用于访问剥头皮管理器等）
        """
        self.logger = get_logger(__name__)
        self.engine = engine
        self.tracker = tracker
        self.config = config
        self.coordinator = coordinator

        # 🆕 REST查询配置
        self._rest_query_interval: int = 1   # REST查询间隔（秒）- 适应剧烈波动
        self._rest_query_debounce: int = 5   # 事件触发去重时间（秒）
        self._rest_timeout: int = 5          # REST查询超时（秒）

        # 🆕 REST失败保护配置
        self._rest_max_failures: int = 3     # 最大连续失败次数
        self._rest_failure_count: int = 0    # 当前连续失败次数
        self._rest_last_success_time: float = 0  # 最后成功时间
        self._rest_last_query_time: float = 0    # 最后查询时间
        self._rest_is_available: bool = True     # REST API可用性

        # 🆕 持仓异常保护配置
        self._position_change_alert_threshold: float = 100  # 持仓变化告警阈值（%）
        self._position_max_multiplier: int = 10             # 最大持仓倍数

        # 持仓缓存（用于变化检测）
        self._last_position_size = Decimal('0')
        self._last_position_price = Decimal('0')

        # 事件触发查询去重
        self._last_event_query_time: float = 0

        # 监控任务
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None

    async def start_monitoring(self):
        """启动持仓监控（纯REST API）"""
        if self._running:
            self.logger.warning("持仓监控已经在运行")
            return

        self._running = True

        # 🆕 用REST API同步初始持仓
        try:
            self.logger.info("📊 正在同步初始持仓数据（REST API）...")
            await self._query_and_update_position(is_initial=True)
            self.logger.info("✅ 初始持仓同步完成（REST）")
        except Exception as rest_error:
            self.logger.error(f"❌ REST API初始持仓同步失败: {rest_error}")
            self._rest_failure_count += 1
            # 初始同步失败也记录，但不阻止启动

        # 启动REST定时查询循环
        self._monitor_task = asyncio.create_task(
            self._rest_position_query_loop())
        self.logger.info("✅ 持仓监控已启动（纯REST API，1秒高频查询，适应剧烈波动）")

    async def stop_monitoring(self):
        """停止持仓监控"""
        self._running = False

        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self.logger.info("✅ 持仓监控已停止")

    async def _query_and_update_position(self, is_initial: bool = False, is_event_triggered: bool = False) -> bool:
        """
        查询并更新持仓数据（核心方法）

        Args:
            is_initial: 是否是初始同步
            is_event_triggered: 是否是事件触发（而非定时查询）

        Returns:
            bool: 查询是否成功
        """
        try:
            current_time = time.time()
            self._rest_last_query_time = current_time

            # 查询持仓
            positions = await asyncio.wait_for(
                self.engine.exchange.get_positions([self.config.symbol]),
                timeout=self._rest_timeout
            )

            if not positions:
                # 无持仓
                if is_initial or self._last_position_size != Decimal('0'):
                    self.logger.info("📊 REST查询: 当前无持仓")
                    self.tracker.sync_initial_position(
                        position=Decimal('0'),
                        entry_price=Decimal('0')
                    )
                    self._last_position_size = Decimal('0')
                    self._last_position_price = Decimal('0')

                    # 更新剥头皮管理器
                    if self.coordinator.scalping_manager and self.coordinator.scalping_manager.is_active():
                        initial_capital = self.coordinator.scalping_manager.get_initial_capital()
                        self.coordinator.scalping_manager.update_position(
                            Decimal('0'), Decimal('0'),
                            initial_capital, self.coordinator.balance_monitor.collateral_balance
                        )

                # 🆕 REST查询成功
                self._rest_failure_count = 0
                self._rest_last_success_time = current_time
                self._rest_is_available = True

                # 🆕 恢复订单操作（如果之前被暂停）
                if hasattr(self.coordinator, 'is_paused') and self.coordinator.is_paused:
                    self.logger.info("✅ REST API恢复正常，解除订单暂停")
                    self.coordinator.is_paused = False

                return True

            # 有持仓
            position = positions[0]
            position_qty = position.size if position.side.value.lower() == 'long' else - \
                position.size

            # 🆕 持仓异常检测
            if not is_initial:
                await self._check_position_anomaly(position_qty)

            # 更新持仓追踪器
            self.tracker.sync_initial_position(
                position=position_qty,
                entry_price=position.entry_price
            )

            # 检测持仓变化
            position_changed = (position_qty != self._last_position_size)

            # 更新剥头皮管理器（如果持仓变化）
            if position_changed and self.coordinator.scalping_manager and self.coordinator.scalping_manager.is_active():
                initial_capital = self.coordinator.scalping_manager.get_initial_capital()
                self.coordinator.scalping_manager.update_position(
                    position_qty, position.entry_price,
                    initial_capital, self.coordinator.balance_monitor.collateral_balance
                )

            # 记录日志
            if is_initial:
                self.logger.info(
                    f"✅ 初始持仓: {position.side.value} {abs(position_qty)} @ ${position.entry_price}"
                )
            elif position_changed:
                self.logger.info(
                    f"📡 REST同步: 持仓变化 {self._last_position_size} → {position_qty}, "
                    f"成本=${position.entry_price:.2f}"
                )

            # 更新缓存
            self._last_position_size = position_qty
            self._last_position_price = position.entry_price

            # 🆕 REST查询成功
            self._rest_failure_count = 0
            self._rest_last_success_time = current_time
            self._rest_is_available = True

            # 🆕 恢复订单操作（如果之前被暂停）
            if hasattr(self.coordinator, 'is_paused') and self.coordinator.is_paused:
                self.logger.info("✅ REST API恢复正常，解除订单暂停")
                self.coordinator.is_paused = False

            return True

        except asyncio.TimeoutError:
            self._rest_failure_count += 1
            self.logger.error(
                f"❌ REST查询超时（>{self._rest_timeout}秒）"
                f"[失败次数: {self._rest_failure_count}/{self._rest_max_failures}]"
            )
            await self._handle_rest_failure()
            return False

        except Exception as e:
            self._rest_failure_count += 1
            self.logger.error(
                f"❌ REST查询失败: {e} "
                f"[失败次数: {self._rest_failure_count}/{self._rest_max_failures}]"
            )
            await self._handle_rest_failure()
            return False

    async def _check_position_anomaly(self, new_position: Decimal):
        """
        检测持仓异常（防止持仓失控）

        Args:
            new_position: 新的持仓数量
        """
        if self._last_position_size == Decimal('0'):
            return  # 首次有持仓，不检测

        # 计算持仓变化率
        position_change = abs(new_position - self._last_position_size)
        if self._last_position_size != Decimal('0'):
            change_percentage = (
                position_change / abs(self._last_position_size)) * 100
        else:
            change_percentage = Decimal('0')

        # 告警阈值检测
        if change_percentage > self._position_change_alert_threshold:
            self.logger.warning(
                f"⚠️ 持仓变化异常告警: {self._last_position_size} → {new_position}, "
                f"变化率={change_percentage:.1f}% (阈值={self._position_change_alert_threshold}%)"
            )

        # 紧急停止检测
        expected_max_position = abs(
            self._last_position_size) * self._position_max_multiplier
        if abs(new_position) > expected_max_position and expected_max_position > 0:
            self.logger.critical(
                f"🚨 持仓异常！紧急停止交易！\n"
                f"   上次持仓: {self._last_position_size}\n"
                f"   当前持仓: {new_position}\n"
                f"   超出预期: {self._position_max_multiplier}倍\n"
                f"   需要人工确认后才能恢复！"
            )
            # 🆕 触发紧急停止
            self.coordinator.is_emergency_stopped = True
            self.coordinator.is_paused = True

    async def _handle_rest_failure(self):
        """处理REST查询失败"""
        self._rest_is_available = False

        # 连续失败达到阈值：暂停订单操作
        if self._rest_failure_count >= self._rest_max_failures:
            if not hasattr(self.coordinator, 'is_paused') or not self.coordinator.is_paused:
                self.logger.error(
                    f"🚫 REST连续失败{self._rest_failure_count}次，暂停所有订单操作！\n"
                    f"   将持续尝试重连，成功后自动恢复..."
                )
                self.coordinator.is_paused = True

    async def _rest_position_query_loop(self):
        """REST定时查询循环（核心监控循环）"""
        self.logger.info(
            f"🔄 REST持仓查询循环已启动: 间隔={self._rest_query_interval}秒"
        )

        while self._running:
            try:
                await asyncio.sleep(self._rest_query_interval)

                # 定时查询
                success = await self._query_and_update_position(is_initial=False, is_event_triggered=False)

                if success:
                    self.logger.debug(f"✅ 定时REST查询成功")
                else:
                    self.logger.warning(f"⚠️ 定时REST查询失败")

            except asyncio.CancelledError:
                self.logger.info("🔄 REST查询循环已取消")
                break
            except Exception as e:
                self.logger.error(f"❌ REST查询循环错误: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(10)

        self.logger.info("🔄 REST查询循环已退出")

    async def trigger_event_query(self, event_name: str = "unknown"):
        """
        事件触发的持仓查询（带去重）

        Args:
            event_name: 事件名称（用于日志）
        """
        current_time = time.time()

        # 去重：5秒内只查询一次
        if current_time - self._last_event_query_time < self._rest_query_debounce:
            self.logger.debug(
                f"⏭️ 跳过事件查询（{event_name}）：去重时间未到"
            )
            return

        self._last_event_query_time = current_time
        self.logger.info(f"🔔 事件触发持仓查询: {event_name}")

        await self._query_and_update_position(is_initial=False, is_event_triggered=True)

    def is_rest_available(self) -> bool:
        """REST API是否可用"""
        return self._rest_is_available

    def get_position_data_source(self) -> str:
        """获取当前持仓数据来源"""
        return "REST API"
