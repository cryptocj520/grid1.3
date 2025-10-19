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
        self.order_ops = OrderOperations(engine, state, config)

    async def activate(self):
        """激活剥头皮模式（完整流程）"""
        self.logger.warning("🔴 正在激活剥头皮模式...")

        # 1. 激活剥头皮管理器
        self.scalping_manager.activate()

        # 2. 取消所有卖单（带验证）- 做多网格
        if not await self.order_ops.cancel_sell_orders_with_verification(max_attempts=3):
            self.logger.error("❌ 取消卖单失败，剥头皮激活中止")
            self.scalping_manager.deactivate()
            return

        # 3. 混合策略获取实时持仓：WebSocket优先，REST API备用
        self.logger.info("📊 正在获取实时持仓信息（WebSocket优先，REST API备用）...")

        # 第一步：尝试从WebSocket缓存获取
        position_data = await self.engine.get_real_time_position(self.config.symbol)
        current_position = position_data['size']
        average_cost = position_data['entry_price']
        data_source = "WebSocket"

        # 第二步：如果WebSocket缓存为空，使用REST API作为备用
        if current_position == 0 and average_cost == 0:
            self.logger.warning(
                "⚠️ WebSocket持仓缓存为空（交易所未推送初始持仓），"
                "使用REST API获取准确数据..."
            )

            try:
                positions = await self.engine.exchange.get_positions(
                    symbols=[self.config.symbol]
                )

                if positions and len(positions) > 0:
                    position = positions[0]
                    current_position = position.size or Decimal('0')
                    average_cost = position.entry_price or Decimal('0')

                    # 根据方向确定持仓符号
                    if hasattr(position, 'side'):
                        from ....adapters.exchanges import PositionSide
                        if position.side == PositionSide.SHORT and current_position != 0:
                            current_position = -current_position

                    data_source = "REST API"

                    # 同步到WebSocket缓存（供后续使用）
                    if not hasattr(self.engine.exchange, '_position_cache'):
                        self.engine.exchange._position_cache = {}
                    self.engine.exchange._position_cache[self.config.symbol] = {
                        'size': current_position,
                        'entry_price': average_cost,
                        'unrealized_pnl': position.unrealized_pnl or Decimal('0'),
                        'side': 'Long' if current_position > 0 else 'Short',
                        'timestamp': datetime.now()
                    }

                    self.logger.info(
                        f"✅ REST API获取成功: {current_position} {self.config.symbol.split('_')[0]}, "
                        f"成本=${average_cost:,.2f}，已同步到WebSocket缓存"
                    )
                else:
                    self.logger.warning("⚠️ REST API返回空持仓")

            except Exception as e:
                self.logger.error(f"❌ REST API获取持仓失败: {e}")
                import traceback
                self.logger.error(traceback.format_exc())

        self.logger.info(
            f"📊 最终持仓（来源: {data_source}）: "
            f"{current_position} {self.config.symbol.split('_')[0]}, "
            f"平均成本: ${average_cost:,.2f}"
        )

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
        max_attempts: int = 3
    ) -> bool:
        """
        挂止盈订单，并验证成功

        Args:
            max_attempts: 最大尝试次数

        Returns:
            True: 止盈订单已挂出
            False: 挂单失败
        """
        if not self.scalping_manager or not self.scalping_manager.is_active():
            return False

        for attempt in range(max_attempts):
            self.logger.info(
                f"🔄 挂止盈订单尝试 {attempt+1}/{max_attempts}..."
            )

            # 1. 获取当前价格
            try:
                current_price = await self.engine.get_current_price()
            except Exception as e:
                self.logger.error(f"获取当前价格失败: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(0.5)
                continue

            # 2. 计算止盈订单
            tp_order = self.scalping_manager.calculate_take_profit_order(
                current_price)

            if not tp_order:
                self.logger.info("📋 当前无持仓，无需挂止盈订单")
                return True  # 无持仓视为成功

            # 3. 挂止盈订单（使用order_ops的验证挂单方法）
            placed_order = await self.order_ops.place_order_with_verification(
                tp_order, max_attempts=1  # 这里只尝试1次，外层循环会重试
            )

            if placed_order:
                self.logger.info(f"✅ 止盈订单挂出成功（尝试{attempt+1}次）")
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
            return

        current_position = self.tracker.get_current_position()

        # 检查止盈订单是否需要更新
        if not self.scalping_manager.is_take_profit_order_outdated(current_position):
            return

        self.logger.info("📋 持仓变化，更新止盈订单...")

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
        if not await self.place_take_profit_order_with_verification(max_attempts=3):
            self.logger.error("❌ 挂新止盈订单失败")
        else:
            self.logger.info("✅ 止盈订单已更新")
