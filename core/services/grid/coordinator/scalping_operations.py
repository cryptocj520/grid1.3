"""
剥头皮操作模块

提供剥头皮模式的激活、退出、止盈订单管理等操作
"""

import asyncio
from typing import Optional
from decimal import Decimal
from datetime import datetime

from ....logging import get_logger
from ..models import GridOrder, GridOrderSide
from ..models.grid_config import GridType
from .order_operations import OrderOperations


class ScalpingOperations:
    """
    剥头皮操作管理器

    职责：
    1. 激活剥头皮模式（取消卖单、挂止盈订单）
    2. 退出剥头皮模式（恢复正常网格）
    3. 处理止盈订单成交
    4. 更新止盈订单（持仓变化时）
    """

    def __init__(
        self,
        coordinator,
        scalping_manager,
        engine,
        state,
        tracker,
        strategy,
        config
    ):
        """
        初始化剥头皮操作管理器

        Args:
            coordinator: 协调器引用
            scalping_manager: 剥头皮管理器
            engine: 执行引擎
            state: 网格状态
            tracker: 持仓跟踪器
            strategy: 网格策略
            config: 网格配置
        """
        self.logger = get_logger(__name__)
        self.coordinator = coordinator
        self.scalping_manager = scalping_manager
        self.engine = engine
        self.state = state
        self.tracker = tracker
        self.strategy = strategy
        self.config = config

        # 创建订单操作实例
        self.order_ops = OrderOperations(engine, state, config, coordinator)

        # 🛡️ 操作频率限制（基于方向性订单成交，防止错误循环）
        self._last_directional_order_id: str = ""  # 最后一次方向性订单的ID
        self._same_directional_order_count = 0     # 同一方向性订单下的调整次数
        self._max_same_directional_updates = 1    # 同一方向性订单只操作1次

    def _is_spot_mode(self) -> bool:
        """判断是否是现货模式"""
        try:
            from ....adapters.exchanges.interface import ExchangeType
            if hasattr(self.engine, 'exchange') and hasattr(self.engine.exchange, 'config'):
                return self.engine.exchange.config.exchange_type == ExchangeType.SPOT
        except Exception as e:
            self.logger.debug(f"判断现货模式失败: {e}")
        return False

    def _get_reserve_amount(self) -> Decimal:
        """
        获取预留数量（仅现货模式）

        Returns:
            预留BTC数量，如果不是现货模式或没有预留管理器则返回0
        """
        if not self._is_spot_mode():
            return Decimal('0')

        try:
            if hasattr(self.coordinator, 'reserve_manager') and self.coordinator.reserve_manager:
                return self.coordinator.reserve_manager.reserve_amount
        except Exception as e:
            self.logger.debug(f"获取预留数量失败: {e}")

        return Decimal('0')

    def update_last_directional_order(self, order_id: str, order_side: str):
        """
        更新最后一次方向性订单ID（由coordinator在订单成交时调用）

        Args:
            order_id: 订单ID
            order_side: 订单方向 ('Buy' 或 'Sell')
        """
        # 判断是否是方向性订单
        is_directional = False

        if self.config.grid_type == GridType.FOLLOW_LONG:
            # 做多网格：只记录买单
            is_directional = (order_side == 'Buy')
        elif self.config.grid_type == GridType.FOLLOW_SHORT:
            # 做空网格：只记录卖单
            is_directional = (order_side == 'Sell')

        if is_directional:
            self._last_directional_order_id = order_id
            self.logger.debug(
                f"📌 更新方向性订单ID: {order_id} "
                f"({'买单' if order_side == 'Buy' else '卖单'})"
            )

    async def activate(self):
        """激活剥头皮模式（完整流程）"""
        self.logger.warning("🔴 正在激活剥头皮模式...")

        # 🔥 0.0 如果当前不是剥头皮模式，说明是新的触发，清空频率限制计数器（双重保险）
        if not self.scalping_manager.is_active():
            self._last_directional_order_id = ""
            self._same_directional_order_count = 0
            if hasattr(self, '_last_checked_directional_order_id'):
                self._last_checked_directional_order_id = ""
            self.logger.debug("🔄 新的剥头皮触发，已清空操作频率限制计数器（激活时）")

        # 🛡️ 0. 检查全局状态（REST失败或持仓异常时拒绝激活）
        if hasattr(self.coordinator, 'is_emergency_stopped') and self.coordinator.is_emergency_stopped:
            self.logger.error(
                "🚨 系统紧急停止中（持仓异常），拒绝激活剥头皮模式！"
            )
            return

        if hasattr(self.coordinator, 'is_paused') and self.coordinator.is_paused:
            self.logger.error(
                "⏸️ REST API不可用（连续失败），拒绝激活剥头皮模式！\n"
                "   原因：无法获取准确持仓数据，操作存在风险\n"
                "   等待REST API恢复后才能激活"
            )
            return

        # 1. 激活剥头皮管理器
        self.scalping_manager.activate()

        # 2. 取消所有卖单（带验证）- 做多网格
        if not await self.order_ops.cancel_sell_orders_with_verification(max_attempts=3):
            self.logger.error("❌ 取消卖单失败，剥头皮激活中止")
            self.scalping_manager.deactivate()
            return

        # 🔥 3. 直接从tracker获取持仓（来自position_monitor的REST数据，每秒更新）
        self.logger.info(
            "📊 正在获取持仓信息（使用tracker数据，来自position_monitor的REST监控）...")

        current_position = self.tracker.get_current_position()
        average_cost = self.tracker.get_average_cost()

        # 🛡️ 3.1 检查持仓是否为0（剥头皮需要有持仓才能激活）
        if current_position == 0:
            self.logger.error(
                "❌ 当前无持仓，无法激活剥头皮模式！\n"
                "   剥头皮模式需要先有持仓才能激活"
            )
            self.scalping_manager.deactivate()
            return

        self.logger.info(
            f"📊 持仓（来源: position_monitor的REST数据）: "
            f"{current_position} {self.config.symbol.split('_')[0]}, "
            f"平均成本: ${average_cost:,.2f}"
        )

        # 🔥 强制更新余额（确保当前权益计算准确）
        # 原因：激活剥头皮时可能刚有订单成交，余额监控器的缓存数据可能过时
        # 必须在计算止盈价格之前获取最新的USDC和BTC余额
        self.logger.info("💰 激活剥头皮前强制更新余额...")
        await self.coordinator.balance_monitor.update_balance()

        initial_capital = self.scalping_manager.get_initial_capital()
        self.scalping_manager.update_position(
            current_position, average_cost, initial_capital,
            self.coordinator.balance_monitor.collateral_balance
        )

        # 4. 挂止盈订单（带验证）
        if not await self.place_take_profit_order_with_verification(max_attempts=3):
            self.logger.error("❌ 挂止盈订单失败，但剥头皮模式已激活")
            # 不中止流程，继续运行

        # 5. 注册WebSocket持仓更新回调（事件驱动）
        if not hasattr(self.engine.exchange, '_position_callbacks'):
            self.engine.exchange._position_callbacks = []
        if self.coordinator._on_position_update_from_ws not in self.engine.exchange._position_callbacks:
            self.engine.exchange._position_callbacks.append(
                self.coordinator._on_position_update_from_ws)
            self.logger.info("✅ 已注册WebSocket持仓更新回调（事件驱动）")

        # 🆕 增加剥头皮触发次数（仅标记）
        self.coordinator._scalping_trigger_count += 1
        self.logger.info(
            f"📊 剥头皮触发次数: {self.coordinator._scalping_trigger_count}")

        self.logger.warning("✅ 剥头皮模式已激活")

    async def deactivate(self):
        """退出剥头皮模式，恢复正常网格"""
        self.logger.info("🟢 正在退出剥头皮模式...")

        # 🛡️ 0. 检查全局状态（紧急停止时只停用管理器，不执行订单操作）
        if hasattr(self.coordinator, 'is_emergency_stopped') and self.coordinator.is_emergency_stopped:
            self.logger.error(
                "🚨 系统紧急停止中，只停用剥头皮管理器，不执行订单操作！\n"
                "   需人工检查后手动恢复订单"
            )
            self.scalping_manager.deactivate()
            return

        if hasattr(self.coordinator, 'is_paused') and self.coordinator.is_paused:
            self.logger.warning(
                "⏸️ REST API不可用，只停用剥头皮管理器，不执行订单操作\n"
                "   等待REST API恢复后手动恢复订单"
            )
            self.scalping_manager.deactivate()
            return

        # 1. 移除WebSocket持仓更新回调
        if hasattr(self.engine.exchange, '_position_callbacks'):
            if self.coordinator._on_position_update_from_ws in self.engine.exchange._position_callbacks:
                self.engine.exchange._position_callbacks.remove(
                    self.coordinator._on_position_update_from_ws)
                self.logger.info("✅ 已移除WebSocket持仓更新回调")

        # 2. 停用剥头皮管理器（先停用，避免干扰）
        self.scalping_manager.deactivate()

        # 3. 取消所有订单（包括止盈订单和反向订单）
        self.logger.info("📋 步骤 1/3: 取消所有订单...")
        cancel_verified = await self.order_ops.cancel_all_orders_with_verification(
            max_retries=3,
            retry_delay=1.5,
            first_delay=0.8
        )

        # 4. 仅在验证成功后才恢复正常网格
        if cancel_verified:
            self.logger.info("📋 步骤 2/3: 恢复正常网格模式，重新挂单...")

            try:
                # 重新生成所有网格订单
                initial_orders = self.strategy.initialize(self.config)

                # 批量挂单
                placed_orders = await self.engine.place_batch_orders(initial_orders)

                # 更新状态
                for order in placed_orders:
                    if order.order_id not in self.state.active_orders:
                        self.state.add_order(order)

                self.logger.info(f"✅ 已恢复正常网格，挂出 {len(placed_orders)} 个订单")

            except Exception as e:
                self.logger.error(f"❌ 恢复正常网格失败: {e}")
        else:
            self.logger.error("❌ 由于订单取消验证失败，跳过恢复正常网格步骤")
            self.logger.error("💡 剥头皮模式已停用，但网格未恢复，系统处于暂停状态")

    async def handle_take_profit_filled(self):
        """处理剥头皮止盈订单成交（持仓已平仓，需要重置网格并重新初始化本金）"""
        try:
            # 关键：设置重置标志，防止并发操作
            self.coordinator._resetting = True
            self.logger.warning("🎯 剥头皮止盈订单已成交！（锁定系统）")

            # 🛡️ 0. 检查全局状态（紧急停止时只停止系统，不执行重置）
            if hasattr(self.coordinator, 'is_emergency_stopped') and self.coordinator.is_emergency_stopped:
                self.logger.error(
                    "🚨 系统紧急停止中，止盈成交但不执行重置！\n"
                    "   需人工检查后决定下一步操作"
                )
                await self.coordinator.stop()
                return

            if hasattr(self.coordinator, 'is_paused') and self.coordinator.is_paused:
                self.logger.warning(
                    "⏸️ REST API不可用，止盈成交但暂不执行重置\n"
                    "   等待REST API恢复后再决定下一步操作"
                )
                # 先不停止系统，等待REST恢复
                return

            # 等待一小段时间，让平仓完成并余额更新
            await asyncio.sleep(2)

            # 根据网格类型决定后续行为
            if self.config.is_follow_mode():
                # 跟随移动网格：重置并重启（重新初始化本金）
                self.logger.info("🔄 跟随移动网格模式：准备重置并重启...")

                # 使用reset_manager的通用重置工作流
                from .grid_reset_manager import GridResetManager
                reset_manager = GridResetManager(
                    self.coordinator, self.config, self.state,
                    self.engine, self.tracker, self.strategy
                )

                # 重置（不需要再平仓，因为止盈订单已平仓）
                await reset_manager._generic_reset_workflow(
                    reset_type="剥头皮止盈",
                    should_close_position=False,  # 已平仓
                    should_reinit_capital=True,  # 需要重新初始化本金
                    update_price_range=True  # 更新价格区间
                )

                # 重置完成后，获取最新余额作为新本金
                try:
                    await self.coordinator.balance_monitor.update_balance()
                    new_capital = self.coordinator.balance_monitor.collateral_balance
                    self.logger.info(f"📊 重置后最新本金: ${new_capital:,.3f}")

                    # 重新初始化所有管理器的本金
                    if self.coordinator.capital_protection_manager:
                        self.coordinator.capital_protection_manager.initialize_capital(
                            new_capital, is_reinit=True)
                    if self.coordinator.take_profit_manager:
                        self.coordinator.take_profit_manager.initialize_capital(
                            new_capital, is_reinit=True)
                    if self.scalping_manager:
                        self.scalping_manager.initialize_capital(
                            new_capital, is_reinit=True)

                    self.logger.info(f"💰 所有管理器本金已更新为最新余额: ${new_capital:,.3f}")
                except Exception as e:
                    self.logger.error(f"⚠️ 获取最新余额失败: {e}")

                self.logger.info("✅ 剥头皮重置完成，价格移动网格已重启")

                # 🔥 清空频率限制计数器，允许下次剥头皮触发时重新挂止盈订单
                self._last_directional_order_id = ""
                self._same_directional_order_count = 0
                if hasattr(self, '_last_checked_directional_order_id'):
                    self._last_checked_directional_order_id = ""
                self.logger.debug("🔄 已清空剥头皮操作频率限制计数器（重置后）")
            else:
                # 普通/马丁网格：停止系统
                self.logger.info("⏸️  普通/马丁网格模式：停止系统")
                await self.coordinator.stop()
        finally:
            # 关键：无论成功或失败，都要释放重置锁
            self.coordinator._resetting = False
            self.logger.info("🔓 系统锁定已释放")

    async def place_take_profit_order_with_verification(
        self,
        max_attempts: int = 3,
        skip_frequency_check: bool = False  # 🆕 是否跳过频率限制检查
    ) -> bool:
        """
        挂止盈订单，并验证成功

        Args:
            max_attempts: 最大尝试次数
            skip_frequency_check: 是否跳过频率限制检查
                - False（默认）：检查频率限制，防止错误循环
                - True：跳过频率限制，用于更新操作（已取消旧订单）

        Returns:
            True: 止盈订单已挂出
            False: 挂单失败
        """
        if not self.scalping_manager or not self.scalping_manager.is_active():
            return False

        # 🛡️ 0. 检查全局状态（REST失败或持仓异常时跳过挂单）
        if hasattr(self.coordinator, 'is_emergency_stopped') and self.coordinator.is_emergency_stopped:
            self.logger.error("🚨 系统紧急停止中，跳过挂止盈订单")
            return False

        if hasattr(self.coordinator, 'is_paused') and self.coordinator.is_paused:
            self.logger.warning("⏸️ REST API不可用，跳过挂止盈订单（等待恢复）")
            return False

        # 🛡️ 1. 操作频率限制（基于方向性订单成交，防止错误循环）
        # 🆕 如果是更新操作（已取消旧订单），跳过频率限制检查
        if not skip_frequency_check:
            current_directional_order_id = self._last_directional_order_id

            if not hasattr(self, '_last_checked_directional_order_id'):
                self._last_checked_directional_order_id = ""

            if current_directional_order_id == self._last_checked_directional_order_id:
                # 方向性订单ID未变化（无新的买单/卖单成交）
                if self._same_directional_order_count >= self._max_same_directional_updates:
                    direction_name = "买单" if self.config.grid_type == GridType.FOLLOW_LONG else "卖单"
                    self.logger.warning(
                        f"⏸️ 操作频率限制: 无新的{direction_name}成交，"
                        f"已挂单{self._same_directional_order_count}次，达到上限（{self._max_same_directional_updates}次）\n"
                        f"   防止错误循环，等待新的{direction_name}成交后才能继续操作\n"
                        f"   最后方向性订单ID: {current_directional_order_id or '(无)'}"
                    )
                    return False
                else:
                    self._same_directional_order_count += 1
                    direction_name = "买单" if self.config.grid_type == GridType.FOLLOW_LONG else "卖单"
                    self.logger.info(
                        f"📊 无新{direction_name}成交: "
                        f"第{self._same_directional_order_count}/{self._max_same_directional_updates}次挂单操作"
                    )
            else:
                # 方向性订单ID变化了（有新的买单/卖单成交）
                direction_name = "买单" if self.config.grid_type == GridType.FOLLOW_LONG else "卖单"
                if self._last_checked_directional_order_id:
                    self.logger.info(
                        f"🔄 检测到新{direction_name}成交: "
                        f"{self._last_checked_directional_order_id[:8]}... → {current_directional_order_id[:8] if current_directional_order_id else '(无)'}..., "
                        f"重置挂单计数器（上次挂单{self._same_directional_order_count}次）"
                    )
                self._last_checked_directional_order_id = current_directional_order_id
                self._same_directional_order_count = 1
        else:
            self.logger.info("🔄 更新止盈订单: 跳过频率限制检查（已取消旧订单）")

        for attempt in range(max_attempts):
            self.logger.info(
                f"🔄 挂止盈订单尝试 {attempt+1}/{max_attempts}..."
            )

            # 🔥 1. 从tracker获取最新持仓（来自position_monitor的REST数据）
            tracker_position = self.tracker.get_current_position()
            tracker_cost = self.tracker.get_average_cost()

            if tracker_position == 0:
                self.logger.info("📋 tracker显示无持仓，无需挂止盈订单")
                return True

            self.logger.info(
                f"📊 持仓验证（tracker）: {tracker_position} @ ${tracker_cost:,.2f}"
            )

            # 🔥 2. 更新ScalpingManager的持仓（确保一致）
            initial_capital = self.scalping_manager.get_initial_capital()
            self.scalping_manager.update_position(
                tracker_position,
                tracker_cost,
                initial_capital,
                self.coordinator.balance_monitor.collateral_balance
            )

            # 3. 获取当前价格
            try:
                current_price = await self.engine.get_current_price()
            except Exception as e:
                self.logger.error(f"获取当前价格失败: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(0.5)
                continue

            # 4. 计算止盈订单
            # 🔥 现货模式：传入预留BTC数量，用于对称计算回本价格
            reserve_amount = self._get_reserve_amount() if self._is_spot_mode() else None
            tp_order = self.scalping_manager.calculate_take_profit_order(
                current_price, reserve_amount=reserve_amount)

            if not tp_order:
                self.logger.warning("⚠️ 无法计算止盈订单")
                if attempt < max_attempts - 1:
                    continue
                return False

            # 🔥 5. 验证止盈订单数量 = tracker持仓
            if tp_order.amount != abs(tracker_position):
                self.logger.error(
                    f"❌ 安全拒绝: 止盈订单数量{tp_order.amount} "
                    f"!= tracker持仓{abs(tracker_position)}，重新尝试..."
                )
                if attempt < max_attempts - 1:
                    await asyncio.sleep(0.5)
                    continue
                return False

            self.logger.info(
                f"✅ 止盈订单验证通过: 数量{tp_order.amount} = tracker持仓{abs(tracker_position)}"
            )

            # 6. 挂止盈订单（使用order_ops的验证挂单方法）
            placed_order = await self.order_ops.place_order_with_verification(
                tp_order, max_attempts=1  # 这里只尝试1次，外层循环会重试
            )

            if placed_order:
                self.logger.info(
                    f"✅ 止盈订单已挂出: {placed_order.order_id} "
                    f"{placed_order.side.value} {placed_order.amount} @ ${placed_order.price}"
                )

                # 🔥 更新 ScalpingManager 的缓存，使用交易所返回的真实订单ID
                self.scalping_manager.update_take_profit_order_with_real_id(
                    placed_order)

                # 🛡️ 最终验证：检查止盈订单是否符合预期
                await asyncio.sleep(1.0)  # 等待1秒让订单状态稳定

                self.logger.info("🔍 执行最终验证：检查止盈订单是否符合预期...")

                # 1. 验证持仓是否还与挂单时一致
                final_position = self.tracker.get_current_position()
                # 🔥 使用基础网格数量作为容差（允许精度截断和手续费误差）
                tolerance = self.config.order_amount
                position_diff = abs(abs(final_position) - placed_order.amount)

                if position_diff > tolerance:
                    self.logger.error(
                        f"🚨 最终验证失败: 持仓已变化！\n"
                        f"   挂单时持仓: {placed_order.amount}\n"
                        f"   当前持仓: {abs(final_position)}\n"
                        f"   差异: {position_diff} (超过容差{tolerance})\n"
                        f"   ⚠️ 止盈订单可能不匹配，请人工检查！\n"
                        f"   系统不会执行额外操作，等待下次方向性订单成交"
                    )
                    return True  # 返回True表示挂单成功，但有警告
                elif position_diff > 0:
                    self.logger.debug(
                        f"✅ 持仓差异在容差范围内: "
                        f"差异{position_diff} <= 容差{tolerance}（手续费/精度误差）"
                    )

                # 2. 验证止盈订单是否还存在且数量正确
                try:
                    open_orders = await self.engine.exchange.get_open_orders(
                        symbol=self.config.symbol
                    )
                    tp_order_found = False
                    tp_order_correct = False

                    for order in open_orders:
                        # placed_order 是 GridOrder，使用 order_id；order 是 OrderData，使用 id
                        if order.id == placed_order.order_id:
                            tp_order_found = True
                            # 🔥 使用基础网格数量作为容差（允许精度截断和手续费误差）
                            order_diff = abs(
                                order.amount - abs(final_position))

                            if order_diff <= tolerance:
                                tp_order_correct = True
                                if order_diff == 0:
                                    self.logger.info(
                                        f"✅ 最终验证通过: 止盈订单存在且数量完全一致 "
                                        f"({order.amount} = {abs(final_position)})"
                                    )
                                else:
                                    self.logger.info(
                                        f"✅ 最终验证通过: 止盈订单存在且数量在容差范围内\n"
                                        f"   止盈订单: {order.amount}\n"
                                        f"   当前持仓: {abs(final_position)}\n"
                                        f"   差异: {order_diff} <= 容差{tolerance}（手续费/精度误差）"
                                    )
                            else:
                                self.logger.error(
                                    f"🚨 最终验证失败: 止盈订单数量不匹配！\n"
                                    f"   止盈订单数量: {order.amount}\n"
                                    f"   当前持仓: {abs(final_position)}\n"
                                    f"   差异: {order_diff} (超过容差{tolerance})\n"
                                    f"   ⚠️ 可能存在问题，请人工检查！\n"
                                    f"   系统不会执行额外操作，等待下次方向性订单成交"
                                )
                            break

                    if not tp_order_found:
                        self.logger.error(
                            f"🚨 最终验证失败: 止盈订单不存在！\n"
                            f"   订单ID: {placed_order.order_id}\n"
                            f"   ⚠️ 订单可能已被取消或成交，请人工检查！\n"
                            f"   系统不会执行额外操作，等待下次方向性订单成交"
                        )

                except Exception as e:
                    self.logger.error(
                        f"🚨 最终验证出错: 无法获取开放订单\n"
                        f"   错误: {e}\n"
                        f"   ⚠️ 无法确认止盈订单状态，请人工检查！\n"
                        f"   系统不会执行额外操作，等待下次方向性订单成交"
                    )

                return True
            else:
                self.logger.warning(
                    f"⚠️ 止盈订单挂出失败，准备第{attempt+2}次尝试..."
                )

        # 达到最大尝试次数，挂单仍失败
        self.logger.error(
            f"❌ 挂止盈订单失败: 已尝试{max_attempts}次"
        )
        return False

    async def update_take_profit_order_if_needed(self):
        """如果持仓变化，更新止盈订单（带验证）"""
        if not self.scalping_manager or not self.scalping_manager.is_active():
            self.logger.debug("⏭️ 跳过更新止盈订单: 剥头皮未激活")
            return

        # 🛡️ 0. 检查全局状态（REST失败或持仓异常时跳过更新）
        if hasattr(self.coordinator, 'is_emergency_stopped') and self.coordinator.is_emergency_stopped:
            self.logger.warning("🚨 系统紧急停止中，跳过更新止盈订单")
            return

        if hasattr(self.coordinator, 'is_paused') and self.coordinator.is_paused:
            self.logger.debug("⏸️ REST API不可用，跳过更新止盈订单（等待恢复）")
            return

        current_position = self.tracker.get_current_position()
        tp_order = self.scalping_manager.get_current_take_profit_order()

        self.logger.debug(
            f"🔍 检查止盈订单是否需要更新: "
            f"当前持仓={current_position}, "
            f"止盈订单数量={tp_order.amount if tp_order else None}"
        )

        # 检查止盈订单是否需要更新
        if not self.scalping_manager.is_take_profit_order_outdated(current_position):
            self.logger.debug("✅ 止盈订单无需更新")
            return

        self.logger.info("📋 持仓变化，需要更新止盈订单...")

        # 1. 取消旧止盈订单（带验证）
        old_tp_order = self.scalping_manager.get_current_take_profit_order()
        if old_tp_order:
            max_cancel_attempts = 3
            cancel_success = False

            for attempt in range(max_cancel_attempts):
                try:
                    await self.engine.cancel_order(old_tp_order.order_id)
                    self.state.remove_order(old_tp_order.order_id)
                    self.logger.info(f"✅ 已取消旧止盈订单: {old_tp_order.order_id}")

                    # 等待取消完成
                    await asyncio.sleep(0.3)

                    # 验证订单已取消（从交易所查询）
                    try:
                        exchange_orders = await self.engine.exchange.get_open_orders(
                            symbol=self.config.symbol
                        )
                        found = any(
                            order.id == old_tp_order.order_id
                            for order in exchange_orders
                        )

                        if not found:
                            self.logger.info("✅ 验证通过: 旧止盈订单已取消")
                            cancel_success = True
                            break
                        else:
                            self.logger.warning(
                                f"⚠️ 验证失败 (尝试{attempt+1}/{max_cancel_attempts}): "
                                f"订单仍存在，重新取消..."
                            )
                    except Exception as e:
                        self.logger.error(f"验证取消失败: {e}")

                except Exception as e:
                    error_msg = str(e).lower()
                    if "not found" in error_msg or "does not exist" in error_msg:
                        self.logger.info("订单已不存在，视为取消成功")
                        cancel_success = True
                        break
                    else:
                        self.logger.error(f"取消旧止盈订单失败: {e}")

            if not cancel_success:
                self.logger.error("❌ 取消旧止盈订单失败，中止更新")
                return

        # 2. 挂新止盈订单（带验证）
        # 🆕 跳过频率限制检查，因为"取消旧订单+挂新订单"是一个完整的更新操作
        if not await self.place_take_profit_order_with_verification(
            max_attempts=3,
            skip_frequency_check=True  # 跳过频率限制
        ):
            self.logger.error("❌ 挂新止盈订单失败")
        else:
            self.logger.info("✅ 止盈订单已更新")
