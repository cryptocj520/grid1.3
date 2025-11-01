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

        # 🔥 初始化阶段配置（避免首次启动和重置时的误报）
        self._initial_phase: bool = True                    # 是否处于初始化阶段
        self._initial_phase_duration: int = 60              # 初始化阶段持续时间（秒）
        self._initial_phase_start_time: float = 0          # 初始化阶段开始时间

        # 持仓缓存（用于变化检测）
        self._last_position_size = Decimal('0')
        self._last_position_price = Decimal('0')

        # 🔥 现货模式：初始余额基准（用于计算持仓）
        self._initial_base_balance: Optional[Decimal] = None  # 启动时的基础货币总余额

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

        # 🔥 进入初始化阶段（避免首次启动时的持仓变化误报）
        self._initial_phase = True
        self._initial_phase_start_time = time.time()
        self.logger.info(
            f"🔄 进入初始化阶段: 持续{self._initial_phase_duration}秒, "
            f"期间不检测持仓变化异常"
        )

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

            # 🔥 区分现货和合约的持仓查询方式
            is_spot_mode = self._is_spot_mode()

            if is_spot_mode:
                # 现货模式：通过余额查询
                position_qty, entry_price = await self._query_spot_position()
            else:
                # 合约模式：通过持仓查询（保持原逻辑）
                positions = await asyncio.wait_for(
                    self.engine.exchange.get_positions([self.config.symbol]),
                    timeout=self._rest_timeout
                )

                if not positions:
                    position_qty = Decimal('0')
                    entry_price = Decimal('0')
                else:
                    position = positions[0]
                    # 🔥 统一使用传统规则（系统内部表示）
                    # LONG(多头) = 正数, SHORT(空头) = 负数
                    # Lighter适配器已经在get_positions中完成了方向转换
                    position_qty = position.size if position.side.value.lower() == 'long' else - \
                        position.size
                    entry_price = position.entry_price

            # 统一处理持仓数据
            if position_qty == 0:
                # 无持仓
                if is_initial or self._last_position_size != Decimal('0'):
                    mode_str = "现货" if is_spot_mode else "合约"
                    self.logger.info(f"📊 REST查询({mode_str}): 当前无持仓")
                    # 🔥 持仓数据的唯一来源：REST API查询结果
                    # tracker不再通过WebSocket订单成交事件更新持仓
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
            # 🆕 持仓异常检测
            if not is_initial:
                await self._check_position_anomaly(position_qty)

            # 🔥 更新持仓追踪器（持仓数据的唯一来源）
            # 所有持仓数据都来自REST API，不再使用WebSocket成交事件更新
            self.tracker.sync_initial_position(
                position=position_qty,
                entry_price=entry_price
            )

            # 检测持仓变化
            position_changed = (position_qty != self._last_position_size)

            # 更新剥头皮管理器（如果持仓变化）
            if position_changed and self.coordinator.scalping_manager and self.coordinator.scalping_manager.is_active():
                initial_capital = self.coordinator.scalping_manager.get_initial_capital()
                self.coordinator.scalping_manager.update_position(
                    position_qty, entry_price,
                    initial_capital, self.coordinator.balance_monitor.collateral_balance
                )

            # 记录日志
            side_str = "Long" if position_qty > 0 else "Short" if position_qty < 0 else "None"
            mode_str = "现货" if is_spot_mode else "合约"

            if is_initial:
                self.logger.info(
                    f"✅ 初始持仓({mode_str}): {side_str} {abs(position_qty)} @ ${entry_price}"
                )
            elif position_changed:
                self.logger.info(
                    f"📡 REST同步({mode_str}): 持仓变化 {self._last_position_size} → {position_qty}, "
                    f"成本=${entry_price:.2f}"
                )

            # 更新缓存
            self._last_position_size = position_qty
            self._last_position_price = entry_price

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
        # 🔥 初始化阶段：跳过持仓变化异常检测
        if self._initial_phase:
            current_time = time.time()
            elapsed_time = current_time - self._initial_phase_start_time

            # 检查初始化阶段是否结束
            if elapsed_time >= self._initial_phase_duration:
                self._initial_phase = False
                self.logger.info(
                    f"✅ 初始化阶段结束（{elapsed_time:.1f}秒）, 持仓变化异常检测已启用"
                )
            else:
                # 仍在初始化阶段，跳过检测
                self.logger.debug(
                    f"🔄 初始化阶段: 持仓变化 {self._last_position_size} → {new_position}, "
                    f"剩余{self._initial_phase_duration - elapsed_time:.0f}秒 (不检测异常)"
                )
                return

        # 🔥 持仓归零逻辑：小于1个基础网格数量的持仓视为0
        # 这样可以避免微小的精度误差导致的异常告警
        order_amount = self.config.order_amount

        # 将小于 order_amount 的持仓归零
        normalized_last_position = self._last_position_size
        normalized_new_position = new_position

        if abs(self._last_position_size) < order_amount:
            normalized_last_position = Decimal('0')
            self.logger.debug(
                f"🔍 持仓归零: 上次持仓 {self._last_position_size} < {order_amount}, 视为0"
            )

        if abs(new_position) < order_amount:
            normalized_new_position = Decimal('0')
            self.logger.debug(
                f"🔍 持仓归零: 当前持仓 {new_position} < {order_amount}, 视为0"
            )

        if normalized_last_position == Decimal('0'):
            return  # 上次持仓为0（或被归零），不检测

        # 计算持仓变化率（使用归零后的持仓）
        position_change = abs(normalized_new_position -
                              normalized_last_position)
        if normalized_last_position != Decimal('0'):
            change_percentage = (
                position_change / abs(normalized_last_position)) * 100
        else:
            change_percentage = Decimal('0')

        # 告警阈值检测
        if change_percentage > self._position_change_alert_threshold:
            # 显示归零后的持仓，以及原始持仓
            if normalized_last_position != self._last_position_size or normalized_new_position != new_position:
                self.logger.warning(
                    f"⚠️ 持仓变化异常告警: {normalized_last_position} → {normalized_new_position} "
                    f"(原始: {self._last_position_size} → {new_position}), "
                    f"变化率={change_percentage:.1f}% (阈值={self._position_change_alert_threshold}%)"
                )
            else:
                self.logger.warning(
                    f"⚠️ 持仓变化异常告警: {self._last_position_size} → {new_position}, "
                    f"变化率={change_percentage:.1f}% (阈值={self._position_change_alert_threshold}%)"
                )

        # 紧急停止检测（使用归零后的持仓）
        expected_max_position = abs(
            normalized_last_position) * self._position_max_multiplier
        if abs(normalized_new_position) > expected_max_position and expected_max_position > 0:
            self.logger.critical(
                f"🚨 持仓异常！紧急停止交易！\n"
                f"   上次持仓: {normalized_last_position} (原始: {self._last_position_size})\n"
                f"   当前持仓: {normalized_new_position} (原始: {new_position})\n"
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

    def _is_spot_mode(self) -> bool:
        """判断是否是现货模式"""
        try:
            # 🔥 修复导入路径：4个点，不是5个点
            from ....adapters.exchanges.interface import ExchangeType
            if hasattr(self.engine, 'exchange') and hasattr(self.engine.exchange, 'config'):
                is_spot = self.engine.exchange.config.exchange_type == ExchangeType.SPOT
                self.logger.debug(
                    f"🔍 现货模式判断: {is_spot} (exchange_type={self.engine.exchange.config.exchange_type})")
                return is_spot
        except Exception as e:
            self.logger.error(f"❌ 判断现货模式失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        return False

    async def _query_spot_position(self) -> tuple:
        """
        查询现货持仓（通过余额）

        Returns:
            (position_qty, entry_price): 持仓数量和成本价
        """
        try:
            # 解析交易对，获取基础货币
            symbol_parts = self.config.symbol.split('/')
            base_currency = symbol_parts[0]  # UBTC

            # 查询余额
            balances = await self.engine.exchange.get_balances()

            # 获取基础货币余额
            total_balance = Decimal('0')
            if balances:
                for balance in balances:
                    if balance.currency == base_currency:
                        total_balance = balance.total  # 总余额
                        break

            # 🔥 现货持仓计算逻辑：
            # 1. 首次查询：如果总余额 >= 预留，说明预留充足，将总余额作为基准，持仓从0开始
            # 2. 后续查询：持仓 = 当前总余额 - 初始余额基准
            if self._initial_base_balance is None:
                # 首次查询，记录初始余额基准
                self._initial_base_balance = total_balance

                # 检查是否有预留管理器
                if self.coordinator.reserve_manager:
                    # 🔥 使用预留基数（reserve_amount），而不是当前剩余（get_current_reserve）
                    # 启动时判断账户余额是否 >= 预留基数
                    reserve_amount = self.coordinator.reserve_manager.reserve_amount
                    if total_balance >= reserve_amount:
                        # 余额充足，说明账户已有预留，持仓从0开始
                        self.logger.info(
                            f"✅ 现货启动检查: 账户余额={total_balance} >= 预留={reserve_amount}, "
                            f"持仓从0开始计算"
                        )
                        trading_balance = Decimal('0')
                    else:
                        # 余额不足（不应该发生，因为启动检查会拦截）
                        trading_balance = total_balance - reserve_amount
                        self.logger.warning(
                            f"⚠️ 现货启动检查: 账户余额={total_balance} < 预留={reserve_amount}, "
                            f"持仓={trading_balance}（异常状态）"
                        )
                else:
                    # 没有预留管理器，全部余额视为持仓
                    trading_balance = total_balance
            else:
                # 后续查询：持仓 = 当前余额 - 初始余额基准
                trading_balance = total_balance - self._initial_base_balance
                self.logger.debug(
                    f"📊 现货持仓: 当前余额={total_balance}, "
                    f"初始基准={self._initial_base_balance}, 持仓={trading_balance}"
                )

            # 如果没有持仓，返回0
            if trading_balance <= 0:
                return Decimal('0'), Decimal('0')

            # 🔥 计算平均成本：使用tracker记录的成交均价
            # 如果tracker有数据，使用其平均成本；否则使用当前价格
            current_tracker_cost = self.tracker.get_average_cost()
            if current_tracker_cost > 0:
                entry_price = current_tracker_cost
            else:
                # 首次查询，使用当前市场价格作为初始成本
                try:
                    entry_price = await self.engine.get_current_price()
                except:
                    entry_price = Decimal('0')

            self.logger.debug(
                f"📊 现货持仓: {trading_balance} {base_currency} @ ${entry_price}"
            )

            return trading_balance, entry_price

        except Exception as e:
            self.logger.error(f"❌ 查询现货持仓失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return Decimal('0'), Decimal('0')

    def end_initial_phase(self):
        """
        手动结束初始化阶段

        用途：
        - 当批量挂单完成后，可以调用此方法立即结束初始化阶段
        - 启用持仓变化异常检测
        """
        if self._initial_phase:
            elapsed_time = time.time() - self._initial_phase_start_time
            self._initial_phase = False
            self.logger.info(
                f"✅ 手动结束初始化阶段（已运行{elapsed_time:.1f}秒）, "
                f"持仓变化异常检测已启用"
            )
        else:
            self.logger.debug("初始化阶段已经结束，无需重复操作")

    def restart_initial_phase(self, duration: Optional[int] = None):
        """
        重新开始初始化阶段

        用途：
        - 重置网格时，调用此方法重新进入初始化阶段
        - 避免重置后的持仓变化误报

        Args:
            duration: 初始化阶段持续时间（秒），如果不提供则使用默认值
        """
        if duration is not None:
            self._initial_phase_duration = duration

        self._initial_phase = True
        self._initial_phase_start_time = time.time()

        # 🔥 重置现货初始余额基准，使持仓从0重新计算
        self._initial_base_balance = None

        self.logger.info(
            f"🔄 重新进入初始化阶段: 持续{self._initial_phase_duration}秒, "
            f"期间不检测持仓变化异常"
        )
