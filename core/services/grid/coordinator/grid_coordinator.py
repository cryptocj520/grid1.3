"""
网格交易系统协调器

核心协调逻辑：
1. 初始化网格系统
2. 处理订单成交事件
3. 自动挂反向订单
4. 异常处理和暂停恢复
"""

import asyncio
import time
from typing import Any, Dict, List, Optional
from decimal import Decimal
from datetime import datetime

from ....logging import get_logger
from ..interfaces import IGridStrategy, IGridEngine, IPositionTracker
from ..models import (
    GridConfig, GridState, GridOrder, GridOrderSide,
    GridOrderStatus, GridStatus, GridStatistics
)
from ..scalping import ScalpingManager
from ..capital_protection import CapitalProtectionManager
from ..take_profit import TakeProfitManager
from ..price_lock import PriceLockManager

# 🔥 导入新模块
from .grid_reset_manager import GridResetManager
from .position_monitor import PositionMonitor
from .balance_monitor import BalanceMonitor
from .scalping_operations import ScalpingOperations


class GridCoordinator:
    """
    网格交易系统协调器

    职责：
    1. 整合策略、引擎、跟踪器
    2. 订单成交后的反向挂单逻辑
    3. 批量成交处理
    4. 系统状态管理
    5. 异常处理
    """

    def __init__(
        self,
        config: GridConfig,
        strategy: IGridStrategy,
        engine: IGridEngine,
        tracker: IPositionTracker,
        grid_state: GridState
    ):
        """
        初始化协调器

        Args:
            config: 网格配置
            strategy: 网格策略
            engine: 执行引擎
            tracker: 持仓跟踪器
            grid_state: 网格状态（共享实例）
        """
        self.logger = get_logger(__name__)
        self.config = config
        self.strategy = strategy
        self.engine = engine
        self.tracker = tracker

        # 🔥 设置 engine 的 coordinator 引用（用于 health_checker 访问剥头皮管理器等）
        if hasattr(engine, 'coordinator'):
            engine.coordinator = self

        # 网格状态（使用传入的共享实例）
        self.state = grid_state

        # 运行控制
        self._running = False
        self._paused = False
        self._resetting = False  # 🔥 重置进行中标志（本金保护、剥头皮重置等）

        # 异常计数
        self._error_count = 0
        self._max_errors = 5  # 最大错误次数，超过则暂停

        # 🆕 触发次数统计（仅标记次数，无实质性功能）
        self._scalping_trigger_count = 0  # 剥头皮模式触发次数
        self._price_escape_trigger_count = 0  # 价格朝有利方向脱离触发次数
        self._take_profit_trigger_count = 0  # 止盈模式触发次数
        self._capital_protection_trigger_count = 0  # 本金保护模式触发次数

        # 🔥 价格移动网格专用
        self._price_escape_start_time: Optional[float] = None  # 价格脱离开始时间
        self._last_escape_check_time: float = 0  # 上次检查时间
        self._escape_check_interval: int = 10  # 检查间隔（秒）
        self._is_resetting: bool = False  # 是否正在重置网格

        # 🔥 剥头皮管理器
        self.scalping_manager: Optional[ScalpingManager] = None
        self._scalping_position_monitor_task: Optional[asyncio.Task] = None
        self._scalping_position_check_interval: int = 1  # 剥头皮模式持仓检查间隔（秒，REST轮询）
        self._last_ws_position_size = Decimal('0')  # 用于WebSocket事件驱动
        self._last_ws_position_price = Decimal('0')
        # 🔥 持仓监控状态（类似订单统计的混合模式）
        self._position_ws_enabled: bool = False  # WebSocket持仓监控是否启用
        self._last_position_ws_time: float = 0  # 最后一次收到WebSocket持仓更新的时间
        self._last_order_filled_time: float = 0  # 最后一次订单成交的时间（用于判断WS是否失效）
        self._position_ws_response_timeout: int = 5  # 订单成交后WebSocket响应超时（秒）
        self._position_ws_check_interval: int = 5  # 尝试恢复WebSocket的间隔（秒）
        self._last_position_ws_check_time: float = 0  # 上次检查WebSocket的时间
        # 🔥 定期REST校验（心跳检测）
        self._position_rest_verify_interval: int = 60  # 每分钟用REST校验WebSocket持仓（秒）
        self._last_position_rest_verify_time: float = 0  # 上次REST校验的时间
        if config.is_scalping_enabled():
            self.scalping_manager = ScalpingManager(config)
            self.logger.info("✅ 剥头皮管理器已启用")

        # 🛡️ 本金保护管理器
        self.capital_protection_manager: Optional[CapitalProtectionManager] = None
        if config.is_capital_protection_enabled():
            self.capital_protection_manager = CapitalProtectionManager(config)
            self.logger.info("✅ 本金保护管理器已启用")

        # 💰 止盈管理器
        self.take_profit_manager: Optional[TakeProfitManager] = None
        if config.take_profit_enabled:
            self.take_profit_manager = TakeProfitManager(config)
            self.logger.info("✅ 止盈管理器已启用")

        # 🔒 价格锁定管理器
        self.price_lock_manager: Optional[PriceLockManager] = None
        if config.price_lock_enabled:
            self.price_lock_manager = PriceLockManager(config)
            self.logger.info("✅ 价格锁定管理器已启用")

        # 💰 账户余额（由BalanceMonitor管理）
        self._spot_balance: Decimal = Decimal('0')  # 现货余额（未用作保证金）
        self._collateral_balance: Decimal = Decimal('0')  # 抵押品余额（用作保证金）
        self._order_locked_balance: Decimal = Decimal('0')  # 订单冻结余额

        # 🔥 新增：模块化组件初始化
        self.reset_manager = GridResetManager(
            self, config, grid_state, engine, tracker, strategy
        )
        self.position_monitor = PositionMonitor(
            engine, tracker, config, self
        )
        self.balance_monitor = BalanceMonitor(
            engine, config, self, update_interval=10
        )

        # 剥头皮操作模块（可选）
        self.scalping_ops: Optional[ScalpingOperations] = None
        if config.is_scalping_enabled() and self.scalping_manager:
            self.scalping_ops = ScalpingOperations(
                self, self.scalping_manager, engine, grid_state,
                tracker, strategy, config
            )

        self.logger.info(f"✅ 网格协调器初始化完成（模块化版本）: {config}")

    async def initialize(self):
        """初始化网格系统"""
        try:
            self.logger.info("开始初始化网格系统...")

            # 1. 先初始化执行引擎（设置 engine.config）
            await self.engine.initialize(self.config)
            self.logger.info("执行引擎初始化完成")

            # 🔥 价格移动网格：获取当前价格并设置价格区间
            if self.config.is_follow_mode():
                current_price = await self.engine.get_current_price()
                self.config.update_price_range_for_follow_mode(current_price)
                self.logger.info(
                    f"价格移动网格：根据当前价格 ${current_price:,.2f} 设置价格区间 "
                    f"[${self.config.lower_price:,.2f}, ${self.config.upper_price:,.2f}]"
                )

            # 2. 初始化网格状态
            self.state.initialize_grid_levels(
                self.config.grid_count,
                self.config.get_grid_price
            )
            self.logger.info(f"网格状态初始化完成，共{self.config.grid_count}个网格层级")

            # 3. 初始化策略，生成所有初始订单
            initial_orders = self.strategy.initialize(self.config)

            # 🔥 价格移动网格：价格区间在初始化后才设置
            if self.config.is_follow_mode():
                self.logger.info(
                    f"策略初始化完成，生成{len(initial_orders)}个初始订单，"
                    f"覆盖价格区间 [${self.config.lower_price:,.2f}, ${self.config.upper_price:,.2f}]"
                )
            else:
                self.logger.info(
                    f"策略初始化完成，生成{len(initial_orders)}个初始订单，"
                    f"覆盖价格区间 ${self.config.lower_price:,.2f} - ${self.config.upper_price:,.2f}"
                )

            # 4. 订阅订单更新
            self.engine.subscribe_order_updates(self._on_order_filled)
            self.logger.info("订单更新订阅完成")

            # 🔥 提前设置_running标志，确保监控任务能正常运行
            self._running = True

            # 🔄 4.5. 启动持仓监控（使用新模块 PositionMonitor）
            await self.position_monitor.start_monitoring()

            # 5. 批量下所有初始订单（关键修改）
            self.logger.info(f"开始批量挂单，共{len(initial_orders)}个订单...")
            placed_orders = await self.engine.place_batch_orders(initial_orders)

            # 6. 批量添加到状态追踪（只添加未成交的订单）
            self.logger.info(f"开始添加{len(placed_orders)}个订单到状态追踪...")
            added_count = 0
            skipped_count = 0
            for order in placed_orders:
                # 🔥 检查订单是否已经在状态中（可能已经通过WebSocket成交回调处理）
                if order.order_id in self.state.active_orders:
                    skipped_count += 1
                    self.logger.debug(
                        f"⏭️ 跳过已存在订单: {order.order_id} (Grid {order.grid_id}, {order.side.value})"
                    )
                    continue

                # 🔥 检查订单是否已经成交（状态为FILLED）
                if order.status == GridOrderStatus.FILLED:
                    skipped_count += 1
                    self.logger.debug(
                        f"⏭️ 跳过已成交订单: {order.order_id} (Grid {order.grid_id}, {order.side.value})"
                    )
                    continue

                self.state.add_order(order)
                added_count += 1
                self.logger.debug(
                    f"✅ 已添加订单到状态: {order.order_id} (Grid {order.grid_id}, {order.side.value})")

            self.logger.info(
                f"✅ 成功挂出{len(placed_orders)}/{len(initial_orders)}个订单，"
                f"覆盖整个价格区间"
            )
            self.logger.info(
                f"📊 订单添加统计: 新增={added_count}, 跳过={skipped_count} "
                f"(已存在或已成交)"
            )
            self.logger.info(
                f"📊 状态统计: "
                f"买单={self.state.pending_buy_orders}, "
                f"卖单={self.state.pending_sell_orders}, "
                f"活跃订单={len(self.state.active_orders)}"
            )

            # 7. 启动系统
            self.state.start()
            # self._running = True  # 已在启动监控任务前设置

            self.logger.info("✅ 网格系统初始化完成，所有订单已就位，等待成交")

        except Exception as e:
            self.logger.error(f"❌ 网格系统初始化失败: {e}")
            self.state.set_error()
            raise

    async def _on_order_filled(self, filled_order: GridOrder):
        """
        订单成交回调 - 核心逻辑

        当订单成交时：
        1. 记录成交信息
        2. 检查剥头皮模式
        3. 计算反向订单参数
        4. 立即挂反向订单

        Args:
            filled_order: 已成交订单
        """
        try:
            # 🔥 关键检查：防止在重置期间处理订单
            if self._paused:
                self.logger.warning("系统已暂停，跳过订单处理")
                return

            if self._resetting:
                self.logger.warning("⚠️ 系统正在重置中，跳过订单处理")
                return

            self.logger.info(
                f"📢 订单成交: {filled_order.side.value} "
                f"{filled_order.filled_amount}@{filled_order.filled_price} "
                f"(Grid {filled_order.grid_id})"
            )

            # 🔥 记录订单成交时间（用于持仓监控健康检查）
            self.position_monitor.record_order_filled()

            # 1. 更新状态
            self.state.mark_order_filled(
                filled_order.order_id,
                filled_order.filled_price,
                filled_order.filled_amount or filled_order.amount
            )

            # 2. 记录到持仓跟踪器
            self.tracker.record_filled_order(filled_order)

            # 🔥 3. 检查剥头皮模式（使用新模块）
            if self.scalping_manager and self.scalping_ops:
                # 检查是否是止盈订单成交
                if self._is_take_profit_order_filled(filled_order):
                    await self.scalping_ops.handle_take_profit_filled()
                    return  # 止盈成交后不再挂反向订单

                # 更新持仓信息到剥头皮管理器
                current_position = self.tracker.get_current_position()
                average_cost = self.tracker.get_average_cost()
                initial_capital = self.scalping_manager.get_initial_capital()
                self.scalping_manager.update_position(
                    current_position, average_cost, initial_capital,
                    self.balance_monitor.collateral_balance
                )

                # 检查是否需要更新止盈订单
                await self.scalping_ops.update_take_profit_order_if_needed()

            # 🛡️ 3.5. 检查本金保护模式
            if self.capital_protection_manager:
                current_price = filled_order.filled_price
                current_grid_index = self.config.find_nearest_grid_index(
                    current_price)
                await self._check_capital_protection_mode(current_price, current_grid_index)

            # 4. 计算反向订单参数
            # 🔥 剥头皮模式下可能不挂反向订单
            if self.scalping_manager and self.scalping_manager.is_active():
                # 剥头皮模式：只挂建仓单，不挂平仓单
                if not self._should_place_reverse_order_in_scalping(filled_order):
                    self.logger.info(f"🔴 剥头皮模式: 不挂反向订单")
                    return

            new_side, new_price, new_grid_id = self.strategy.calculate_reverse_order(
                filled_order,
                self.config.grid_interval,
                self.config.reverse_order_grid_distance
            )

            # 5. 创建反向订单
            reverse_order = GridOrder(
                order_id="",  # 等待执行引擎填充
                grid_id=new_grid_id,
                side=new_side,
                price=new_price,
                amount=filled_order.filled_amount or filled_order.amount,  # 数量完全一致
                status=GridOrderStatus.PENDING,
                created_at=datetime.now(),
                parent_order_id=filled_order.order_id
            )

            # 6. 下反向订单
            placed_order = await self.engine.place_order(reverse_order)
            self.state.add_order(placed_order)

            # 7. 记录关联关系
            filled_order.reverse_order_id = placed_order.order_id

            self.logger.info(
                f"✅ 反向订单已挂: {new_side.value} "
                f"{reverse_order.amount}@{new_price} "
                f"(Grid {new_grid_id})"
            )

            # 8. 更新当前价格
            current_price = await self.engine.get_current_price()
            current_grid_id = self.config.get_grid_index_by_price(
                current_price)
            self.state.update_current_price(current_price, current_grid_id)

            # 🔥 9. 检查是否触发或退出剥头皮模式
            await self._check_scalping_mode(current_price, current_grid_id)

            # 重置错误计数
            self._error_count = 0

        except Exception as e:
            self.logger.error(f"❌ 处理订单成交失败: {e}")
            self._handle_error(e)

    async def _on_batch_orders_filled(self, filled_orders: List[GridOrder]):
        """
        批量订单成交处理

        处理价格剧烈波动导致的多订单同时成交

        Args:
            filled_orders: 已成交订单列表
        """
        try:
            # 🔥 关键检查：防止在重置期间处理订单
            if self._paused:
                self.logger.warning("系统已暂停，跳过批量订单处理")
                return

            if self._resetting:
                self.logger.warning("⚠️ 系统正在重置中，跳过批量订单处理")
                return

            self.logger.info(
                f"⚡ 批量成交: {len(filled_orders)}个订单"
            )

            # 1. 批量更新状态和记录
            for order in filled_orders:
                self.state.mark_order_filled(
                    order.order_id,
                    order.filled_price,
                    order.filled_amount or order.amount
                )
                self.tracker.record_filled_order(order)

            # 2. 批量计算反向订单
            reverse_params = self.strategy.calculate_batch_reverse_orders(
                filled_orders,
                self.config.grid_interval,
                self.config.reverse_order_grid_distance
            )

            # 3. 创建反向订单列表
            reverse_orders = []
            for side, price, grid_id, amount in reverse_params:
                order = GridOrder(
                    order_id="",
                    grid_id=grid_id,
                    side=side,
                    price=price,
                    amount=amount,
                    status=GridOrderStatus.PENDING,
                    created_at=datetime.now()
                )
                reverse_orders.append(order)

            # 4. 批量下单
            placed_orders = await self.engine.place_batch_orders(reverse_orders)

            # 5. 批量更新状态
            for order in placed_orders:
                self.state.add_order(order)

            self.logger.info(
                f"✅ 批量反向订单已挂: {len(placed_orders)}个"
            )

            # 6. 更新当前价格
            current_price = await self.engine.get_current_price()
            current_grid_id = self.config.get_grid_index_by_price(
                current_price)
            self.state.update_current_price(current_price, current_grid_id)

            # 重置错误计数
            self._error_count = 0

        except Exception as e:
            self.logger.error(f"❌ 批量处理订单成交失败: {e}")
            self._handle_error(e)

    def _handle_error(self, error: Exception):
        """
        处理异常

        策略：
        1. 记录错误
        2. 增加错误计数
        3. 超过阈值则暂停系统

        Args:
            error: 异常对象
        """
        self._error_count += 1

        self.logger.error(
            f"异常发生 ({self._error_count}/{self._max_errors}): {error}"
        )

        # 如果错误次数过多，暂停系统
        if self._error_count >= self._max_errors:
            self.logger.error(
                f"❌ 错误次数达到上限({self._max_errors})，暂停系统"
            )
            asyncio.create_task(self.pause())

    async def start(self):
        """启动网格系统"""
        if self._running:
            self.logger.warning("网格系统已经在运行")
            return

        await self.initialize()
        await self.engine.start()

        # 🔥 主动同步初始持仓到WebSocket缓存
        # Backpack的WebSocket只在持仓变化时推送，不会推送初始状态
        # 所以我们需要在启动时主动获取一次
        position_data = {'size': Decimal('0'), 'entry_price': Decimal(
            '0'), 'unrealized_pnl': Decimal('0')}
        try:
            self.logger.info("📊 正在同步初始持仓数据...")
            position_data = await self.engine.get_real_time_position(self.config.symbol)

            # 如果WebSocket缓存为空，使用REST API获取并同步
            if position_data['size'] == 0 and position_data['entry_price'] == 0:
                positions = await self.engine.exchange.get_positions(symbols=[self.config.symbol])
                if positions and len(positions) > 0:
                    position = positions[0]
                    real_size = position.size or Decimal('0')
                    real_entry_price = position.entry_price or Decimal('0')

                    # 同步到WebSocket缓存
                    if hasattr(self.engine.exchange, '_position_cache'):
                        self.engine.exchange._position_cache[self.config.symbol] = {
                            'size': real_size,
                            'entry_price': real_entry_price,
                            'unrealized_pnl': position.unrealized_pnl or Decimal('0'),
                            'side': 'Long' if real_size > 0 else 'Short',
                            'timestamp': datetime.now()
                        }
                        self.logger.info(
                            f"✅ 初始持仓已同步到WebSocket缓存: "
                            f"{real_size} {self.config.symbol.split('_')[0]}, "
                            f"成本=${real_entry_price:,.2f}"
                        )
                        # 更新position_data供后续使用
                        position_data = {
                            'size': real_size,
                            'entry_price': real_entry_price,
                            'unrealized_pnl': position.unrealized_pnl or Decimal('0')
                        }
            else:
                # WebSocket缓存已有数据
                self.logger.info(
                    f"✅ WebSocket缓存已有持仓数据: "
                    f"{position_data['size']} {self.config.symbol.split('_')[0]}, "
                    f"成本=${position_data['entry_price']:,.2f}"
                )
        except Exception as e:
            self.logger.warning(f"同步初始持仓失败（不影响运行）: {e}")

        # 🔥 检查是否应该立即激活剥头皮模式
        # 如果启动时已有持仓，且价格已在触发阈值以下，立即激活
        if self.config.is_scalping_enabled():
            try:
                current_price = await self.engine.get_current_price()
                current_grid_id = self.config.get_grid_index_by_price(
                    current_price)

                # 更新scalping_manager的持仓信息
                if position_data['size'] != 0:
                    initial_capital = self.scalping_manager.get_initial_capital()
                    self.scalping_manager.update_position(
                        position_data['size'],
                        position_data['entry_price'],
                        initial_capital,
                        self.balance_monitor.collateral_balance  # 🔥 使用 BalanceMonitor 的余额
                    )

                # 检查是否应该触发剥头皮模式（需要传递current_price和current_grid_id）
                if self.scalping_manager.should_trigger(current_price, current_grid_id):
                    self.logger.info(
                        f"🎯 检测到启动时已在触发区域 (Grid {current_grid_id} <= "
                        f"Grid {self.config.get_scalping_trigger_grid()})，立即激活剥头皮模式"
                    )
                    # 🔥 使用新模块
                    if self.scalping_ops:
                        await self.scalping_ops.activate()
                else:
                    self.logger.info(
                        f"📊 剥头皮模式待触发 (当前: Grid {current_grid_id}, "
                        f"触发点: Grid {self.config.get_scalping_trigger_grid()})"
                    )
            except Exception as e:
                self.logger.warning(f"检查剥头皮模式失败: {e}")
                import traceback
                self.logger.error(traceback.format_exc())

        # 🔥 价格移动网格：启动价格脱离监控
        if self.config.is_follow_mode():
            asyncio.create_task(self._price_escape_monitor())
            self.logger.info("✅ 价格脱离监控已启动")

        # 💰 启动余额轮询监控（使用新模块 BalanceMonitor）
        await self.balance_monitor.start_monitoring()

        self.logger.info("🚀 网格系统已启动")

    async def pause(self):
        """暂停网格系统（保留挂单）"""
        self._paused = True
        self.state.pause()

        self.logger.info("⏸️ 网格系统已暂停")

    async def resume(self):
        """恢复网格系统"""
        self._paused = False
        self._error_count = 0  # 重置错误计数
        self.state.resume()

        self.logger.info("▶️ 网格系统已恢复")

    async def stop(self):
        """停止网格系统（取消所有挂单）"""
        self._running = False
        self._paused = False

        # 💰 停止余额监控（使用新模块）
        await self.balance_monitor.stop_monitoring()

        # 🔄 停止持仓同步监控（使用新模块）
        await self.position_monitor.stop_monitoring()

        # 取消所有挂单
        cancelled_count = await self.engine.cancel_all_orders()
        self.logger.info(f"取消了{cancelled_count}个挂单")

        # 停止引擎
        await self.engine.stop()

        # 更新状态
        self.state.stop()

        self.logger.info("⏹️ 网格系统已停止")

    async def get_statistics(self) -> GridStatistics:
        """
        获取统计数据（优先使用WebSocket真实持仓）

        Returns:
            网格统计数据
        """
        # 更新当前价格
        try:
            current_price = await self.engine.get_current_price()
            current_grid_id = self.config.get_grid_index_by_price(
                current_price)
            self.state.update_current_price(current_price, current_grid_id)
        except Exception as e:
            self.logger.warning(f"获取当前价格失败: {e}")

        # 🔥 同步engine的最新订单统计到state
        self._sync_orders_from_engine()

        # 获取统计数据（本地追踪器）
        stats = self.tracker.get_statistics()

        # 🔥 优先使用WebSocket缓存的真实持仓数据（但需要检查WebSocket是否可用）
        # 注意：只有在WebSocket缓存有效且WebSocket监控正常时才使用缓存
        try:
            position_data = await self.engine.get_real_time_position(self.config.symbol)
            ws_position = position_data['size']
            ws_entry_price = position_data['entry_price']
            has_cache = position_data.get('has_cache', False)

            # 🔥 关键修复：只有在WebSocket启用且缓存有效时才使用WebSocket缓存
            # 如果WebSocket已失效（切换到REST备用模式），则使用PositionTracker数据
            if has_cache and self._position_ws_enabled:
                stats.current_position = ws_position
                stats.average_cost = ws_entry_price
                stats.position_data_source = "WebSocket缓存"  # 🔥 标记数据来源

                # 重新计算未实现盈亏（使用WebSocket的真实持仓）
                if ws_position != 0 and current_price > 0:
                    stats.unrealized_profit = ws_position * \
                        (current_price - ws_entry_price)

                self.logger.debug(
                    f"📊 使用WebSocket缓存持仓: {ws_position}, 成本=${ws_entry_price}"
                )
            else:
                # WebSocket失效或缓存无效，使用PositionTracker的数据
                # 判断PositionTracker的数据来源
                if self._position_ws_enabled:
                    stats.position_data_source = "WebSocket回调"  # 🔥 通过WebSocket回调同步到Tracker
                else:
                    stats.position_data_source = "REST API备用"  # 🔥 通过REST API备用模式同步到Tracker

                self.logger.debug(
                    f"📊 使用PositionTracker: {stats.current_position}, "
                    f"成本=${stats.average_cost}, 来源={stats.position_data_source} "
                    f"(WS启用={self._position_ws_enabled}, 缓存={has_cache})"
                )
        except Exception as e:
            # 如果获取WebSocket数据失败，使用本地追踪器的数据
            stats.position_data_source = "PositionTracker"  # 🔥 降级到Tracker
            self.logger.debug(f"获取WebSocket持仓失败，使用本地追踪器数据: {e}")

        # 🔥 添加监控方式信息
        stats.monitoring_mode = self.engine.get_monitoring_mode()

        # 💰 使用真实的账户余额（从 BalanceMonitor 获取）
        balances = self.balance_monitor.get_balances()
        stats.spot_balance = balances['spot_balance']
        stats.collateral_balance = balances['collateral_balance']
        stats.order_locked_balance = balances['order_locked_balance']
        stats.total_balance = balances['total_balance']

        # 🛡️ 本金保护模式状态
        if self.capital_protection_manager:
            stats.capital_protection_enabled = True
            stats.capital_protection_active = self.capital_protection_manager.is_active()
            stats.initial_capital = self.capital_protection_manager.get_initial_capital()
            stats.capital_profit_loss = self.capital_protection_manager.get_profit_loss(
                self.balance_monitor.collateral_balance)  # 🔥 使用 BalanceMonitor 的余额

        # 🔄 价格脱离监控状态（价格移动网格专用）
        if self.config.is_follow_mode() and self._price_escape_start_time is not None:
            import time
            escape_duration = int(time.time() - self._price_escape_start_time)
            stats.price_escape_active = True
            stats.price_escape_duration = escape_duration
            stats.price_escape_timeout = self.config.follow_timeout
            stats.price_escape_remaining = max(
                0, self.config.follow_timeout - escape_duration)

            # 判断脱离方向
            if current_price < self.config.lower_price:
                stats.price_escape_direction = "down"
            elif current_price > self.config.upper_price:
                stats.price_escape_direction = "up"

        # 💰 止盈模式状态
        if self.take_profit_manager:
            stats.take_profit_enabled = True
            stats.take_profit_active = self.take_profit_manager.is_active()
            stats.take_profit_initial_capital = self.take_profit_manager.get_initial_capital()
            stats.take_profit_current_profit = self.take_profit_manager.get_profit_amount(
                self.balance_monitor.collateral_balance)  # 🔥 使用 BalanceMonitor 的余额
            stats.take_profit_profit_rate = self.take_profit_manager.get_profit_percentage(
                self.balance_monitor.collateral_balance)  # 🔥 使用 BalanceMonitor 的余额
            stats.take_profit_threshold = self.config.take_profit_percentage * 100  # 转为百分比

        # 🔒 价格锁定模式状态
        if self.price_lock_manager:
            stats.price_lock_enabled = True
            stats.price_lock_active = self.price_lock_manager.is_locked()
            stats.price_lock_threshold = self.config.price_lock_threshold

        # 🆕 触发次数统计（仅标记）
        stats.scalping_trigger_count = self._scalping_trigger_count
        stats.price_escape_trigger_count = self._price_escape_trigger_count
        stats.take_profit_trigger_count = self._take_profit_trigger_count
        stats.capital_protection_trigger_count = self._capital_protection_trigger_count

        return stats

    def get_state(self) -> GridState:
        """获取网格状态"""
        return self.state

    def is_running(self) -> bool:
        """是否运行中"""
        return self._running and not self._paused

    def is_paused(self) -> bool:
        """是否暂停"""
        return self._paused

    def is_stopped(self) -> bool:
        """是否已停止"""
        return not self._running

    def get_status_text(self) -> str:
        """获取状态文本"""
        if self._paused:
            return "⏸️ 已暂停"
        elif self._running:
            return "🟢 运行中"
        else:
            return "⏹️ 已停止"

    async def _scalping_position_monitor_loop(self):
        """
        [已弃用] 剥头皮模式持仓监控循环（REST API轮询方式）

        ⚠️ 此方法已被WebSocket事件驱动方式取代，保留仅作备份
        现在使用 _on_position_update_from_ws() 实时处理持仓更新
        """
        self.logger.warning("⚠️ 使用了已弃用的REST API轮询监控（应该使用WebSocket事件驱动）")
        self.logger.info("📊 剥头皮持仓监控循环已启动")

        last_position = Decimal('0')
        last_entry_price = Decimal('0')

        try:
            while self.scalping_manager and self.scalping_manager.is_active():
                try:
                    # 从API获取实时持仓
                    position_data = await self.engine.get_real_time_position(self.config.symbol)
                    current_position = position_data['size']
                    current_entry_price = position_data['entry_price']

                    # 检查是否有变化
                    position_changed = (
                        current_position != last_position or
                        current_entry_price != last_entry_price
                    )

                    if position_changed:
                        self.logger.info(
                            f"📊 持仓变化检测: "
                            f"数量 {last_position} → {current_position}, "
                            f"成本 ${last_entry_price:,.2f} → ${current_entry_price:,.2f}"
                        )

                        # 更新剥头皮管理器的持仓信息
                        initial_capital = self.scalping_manager.get_initial_capital()
                        self.scalping_manager.update_position(
                            current_position, current_entry_price, initial_capital,
                            self.balance_monitor.collateral_balance)  # 🔥 使用 BalanceMonitor 的余额

                        # 更新止盈订单
                        await self._update_take_profit_order_after_position_change(
                            current_position,
                            current_entry_price
                        )

                        # 更新记录
                        last_position = current_position
                        last_entry_price = current_entry_price

                    # 等待下次检查
                    await asyncio.sleep(self._scalping_position_check_interval)

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger.error(f"持仓监控出错: {e}")
                    await asyncio.sleep(self._scalping_position_check_interval)

        except asyncio.CancelledError:
            self.logger.info("📊 剥头皮持仓监控循环已取消")
        except Exception as e:
            self.logger.error(f"持仓监控循环异常: {e}")
        finally:
            self.logger.info("📊 剥头皮持仓监控循环已结束")

    async def _update_take_profit_order_after_position_change(
        self,
        new_position: Decimal,
        new_entry_price: Decimal
    ):
        """
        持仓变化后更新止盈订单

        Args:
            new_position: 新的持仓数量
            new_entry_price: 新的平均成本价
        """
        if new_position == 0:
            # 持仓归零，取消止盈订单
            if self.scalping_manager.get_current_take_profit_order():
                tp_order = self.scalping_manager.get_current_take_profit_order()
                try:
                    await self.engine.cancel_order(tp_order.order_id)
                    self.state.remove_order(tp_order.order_id)
                    self.logger.info("✅ 持仓归零，已取消止盈订单")
                except Exception as e:
                    self.logger.error(f"取消止盈订单失败: {e}")
            return

        # 取消旧止盈订单
        old_tp_order = self.scalping_manager.get_current_take_profit_order()
        if old_tp_order:
            try:
                await self.engine.cancel_order(old_tp_order.order_id)
                self.state.remove_order(old_tp_order.order_id)
                self.logger.info(f"🔄 已取消旧止盈订单: {old_tp_order.order_id}")
            except Exception as e:
                self.logger.error(f"取消旧止盈订单失败: {e}")

        # 挂新止盈订单
        await self._place_take_profit_order()
        self.logger.info("✅ 止盈订单已更新")

    async def _on_position_update_from_ws(self, position_info: Dict[str, Any]) -> None:
        """
        WebSocket持仓更新回调（事件驱动，实时响应）

        当WebSocket收到持仓更新推送时自动调用
        """
        try:
            # 只在剥头皮模式激活时处理
            if not self.scalping_manager or not self.scalping_manager.is_active():
                return

            # 只处理当前交易对的持仓
            if position_info.get('symbol') != self.config.symbol:
                return

            current_position = position_info.get('size', Decimal('0'))
            entry_price = position_info.get('entry_price', Decimal('0'))

            # 检查是否有变化
            position_changed = (
                current_position != self._last_ws_position_size or
                entry_price != self._last_ws_position_price
            )

            if position_changed:
                self.logger.info(
                    f"📊 WebSocket持仓变化: "
                    f"数量 {self._last_ws_position_size} → {current_position}, "
                    f"成本 ${self._last_ws_position_price:,.2f} → ${entry_price:,.2f}"
                )

                # 更新剥头皮管理器
                initial_capital = self.scalping_manager.get_initial_capital()
                self.scalping_manager.update_position(
                    current_position, entry_price, initial_capital,
                    self.balance_monitor.collateral_balance)  # 🔥 使用 BalanceMonitor 的余额

                # 更新止盈订单
                await self._update_take_profit_order_after_position_change(
                    current_position,
                    entry_price
                )

                # 更新记录
                self._last_ws_position_size = current_position
                self._last_ws_position_price = entry_price

        except Exception as e:
            self.logger.error(f"处理WebSocket持仓更新失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    def __repr__(self) -> str:
        return (
            f"GridCoordinator("
            f"status={self.get_status_text()}, "
            f"position={self.tracker.get_current_position()}, "
            f"errors={self._error_count})"
        )

    # ==================== 价格移动网格专用方法 ====================

    async def _price_escape_monitor(self):
        """
        价格脱离监控（价格移动网格专用）

        定期检查价格是否脱离网格范围，如果脱离时间超过阈值则重置网格
        """
        import time

        self.logger.info("🔍 价格脱离监控循环已启动")

        while self._running and not self._paused:
            try:
                current_time = time.time()

                # 检查间隔
                if current_time - self._last_escape_check_time < self._escape_check_interval:
                    await asyncio.sleep(1)
                    continue

                self._last_escape_check_time = current_time

                # 获取当前价格
                current_price = await self.engine.get_current_price()

                # 检查是否脱离
                should_reset, direction = self.config.check_price_escape(
                    current_price)

                if should_reset:
                    # 记录脱离开始时间
                    if self._price_escape_start_time is None:
                        self._price_escape_start_time = current_time
                        self.logger.warning(
                            f"⚠️ 价格脱离网格范围（{direction}方向）: "
                            f"当前价格=${current_price:,.2f}, "
                            f"网格区间=[${self.config.lower_price:,.2f}, ${self.config.upper_price:,.2f}]"
                        )

                    # 检查脱离时间是否超过阈值
                    escape_duration = current_time - self._price_escape_start_time

                    if escape_duration >= self.config.follow_timeout:
                        self.logger.warning(
                            f"🔄 价格脱离超时（{escape_duration:.0f}秒 >= {self.config.follow_timeout}秒），"
                            f"准备重置网格..."
                        )
                        # 🔥 使用新模块
                        await self.reset_manager.execute_price_follow_reset(current_price, direction)
                        self._price_escape_start_time = None
                    else:
                        self.logger.info(
                            f"⏳ 价格脱离中（{direction}方向），"
                            f"已持续 {escape_duration:.0f}/{self.config.follow_timeout}秒"
                        )
                else:
                    # 价格回到范围内，重置脱离计时
                    if self._price_escape_start_time is not None:
                        self.logger.info(
                            f"✅ 价格已回到网格范围内: ${current_price:,.2f}"
                        )
                        self._price_escape_start_time = None

                    # 🔒 检查是否需要解除价格锁定
                    if self.price_lock_manager and self.price_lock_manager.is_locked():
                        if self.price_lock_manager.check_unlock_condition(
                            current_price,
                            self.config.lower_price,
                            self.config.upper_price
                        ):
                            self.price_lock_manager.deactivate_lock()
                            self.logger.info("🔓 价格锁定已解除，恢复正常网格交易")

                await asyncio.sleep(1)

            except asyncio.CancelledError:
                self.logger.info("价格脱离监控已停止")
                break
            except Exception as e:
                self.logger.error(f"价格脱离监控出错: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(10)  # 出错后等待10秒再继续

    async def _check_scalping_mode(self, current_price: Decimal, current_grid_index: int):
        """
        检查是否触发或退出剥头皮模式

        Args:
            current_price: 当前价格
            current_grid_index: 当前网格索引
        """
        if not self.scalping_manager or not self.scalping_ops:
            return

        # 检查是否应该触发剥头皮（使用新模块）
        if self.scalping_manager.should_trigger(current_price, current_grid_index):
            await self.scalping_ops.activate()

        # 检查是否应该退出剥头皮（使用新模块）
        elif self.scalping_manager.should_exit(current_price, current_grid_index):
            await self.scalping_ops.deactivate()

    async def _check_capital_protection_mode(self, current_price: Decimal, current_grid_index: int):
        """
        检查是否触发本金保护模式

        Args:
            current_price: 当前价格
            current_grid_index: 当前网格索引
        """
        if not self.capital_protection_manager:
            return

        # 如果已经触发，检查是否回本
        if self.capital_protection_manager.is_active():
            # 检查抵押品是否回本
            if self.capital_protection_manager.check_capital_recovery(
                self.balance_monitor.collateral_balance
            ):
                self.logger.warning(
                    f"🛡️ 本金保护：抵押品已回本，准备重置网格！"
                )
                # 🔥 使用新模块
                await self.reset_manager.execute_capital_protection_reset()
        else:
            # 检查是否应该触发
            if self.capital_protection_manager.should_trigger(current_price, current_grid_index):
                self.capital_protection_manager.activate()
                self.logger.warning(
                    f"🛡️ 本金保护已激活！等待抵押品回本... "
                    f"初始本金: ${self.capital_protection_manager.get_initial_capital():,.2f}"
                )

    async def _reset_fixed_range_grid(self, new_capital: Optional[Decimal] = None):
        """重置固定范围网格（保持原有范围）

        Args:
            new_capital: 新的初始本金（止盈后使用）
        """
        try:
            self.logger.info("🔄 重置固定范围网格（保持价格区间）...")

            # 重置所有管理器状态
            if self.scalping_manager:
                self.scalping_manager.reset()
            if self.capital_protection_manager:
                self.capital_protection_manager.reset()
            if self.take_profit_manager:
                self.take_profit_manager.reset()

            # 重置追踪器和状态
            self.tracker.reset()
            self.state.active_orders.clear()  # 清空所有活跃订单
            self.state.pending_buy_orders = 0
            self.state.pending_sell_orders = 0

            # 重新初始化网格层级（保持原有价格区间）
            self.state.initialize_grid_levels(
                self.config.grid_count,
                self.config.get_grid_price
            )

            # 生成并挂出新订单（使用原有价格范围）
            self.logger.info(
                f"🚀 重新初始化固定范围网格并挂单: "
                f"${self.config.lower_price:,.2f} - ${self.config.upper_price:,.2f}"
            )
            initial_orders = self.strategy.initialize(self.config)
            self.logger.info(f"📋 生成 {len(initial_orders)} 个初始订单")

            placed_orders = await self.engine.place_batch_orders(initial_orders)
            self.logger.info(f"✅ 成功挂出 {len(placed_orders)} 个订单")

            # 🔥 关键修复：等待WebSocket处理立即成交的订单
            await asyncio.sleep(2)

            # 添加到状态追踪（只添加未成交的订单）
            added_count = 0
            skipped_filled = 0
            skipped_exists = 0

            try:
                # 获取当前实际挂单（从引擎）
                engine_pending_orders = self.engine.get_pending_orders()
                engine_pending_ids = {
                    order.order_id for order in engine_pending_orders}

                for order in placed_orders:
                    if order.order_id in self.state.active_orders:
                        skipped_exists += 1
                        continue
                    # 🔥 关键：检查订单是否真的还在挂单中
                    if order.order_id not in engine_pending_ids:
                        self.logger.debug(f"订单 {order.order_id} 已成交或取消，跳过添加")
                        skipped_filled += 1
                        continue
                    self.state.add_order(order)
                    added_count += 1
            except Exception as e:
                self.logger.warning(f"⚠️ 无法从引擎获取挂单列表，使用订单状态判断: {e}")
                # Fallback：使用订单自身的状态
                for order in placed_orders:
                    if order.order_id in self.state.active_orders:
                        skipped_exists += 1
                        continue
                    if order.status == GridOrderStatus.FILLED:
                        self.logger.debug(f"订单 {order.order_id} 立即成交，跳过添加")
                        skipped_filled += 1
                        continue
                    self.state.add_order(order)
                    added_count += 1

            buy_count = len(
                [o for o in self.state.active_orders.values() if o.side == GridOrderSide.BUY])
            sell_count = len(
                [o for o in self.state.active_orders.values() if o.side == GridOrderSide.SELL])
            self.logger.info(
                f"📊 订单添加详情: "
                f"新增={added_count}, "
                f"跳过(已成交)={skipped_filled}, "
                f"跳过(已存在)={skipped_exists}"
            )
            self.logger.info(
                f"📊 状态统计: "
                f"买单={buy_count}, "
                f"卖单={sell_count}, "
                f"活跃订单={len(self.state.active_orders)}"
            )

            # 🔥 重新初始化本金（止盈后）
            if new_capital is not None:
                if self.capital_protection_manager:
                    self.capital_protection_manager.initialize_capital(
                        new_capital, is_reinit=True)
                if self.take_profit_manager:
                    self.take_profit_manager.initialize_capital(
                        new_capital, is_reinit=True)
                if self.scalping_manager:
                    self.scalping_manager.initialize_capital(
                        new_capital, is_reinit=True)
                self.logger.info(f"💰 本金已重新初始化: ${new_capital:,.3f}")

            self.logger.info("✅ 固定范围网格重置完成，继续运行")

        except Exception as e:
            self.logger.error(f"❌ 固定范围网格重置失败: {e}")
            raise

    async def _place_take_profit_order(self):
        """挂止盈订单"""
        if not self.scalping_manager or not self.scalping_manager.is_active():
            return

        # 获取当前价格
        current_price = await self.engine.get_current_price()

        # 计算止盈订单
        tp_order = self.scalping_manager.calculate_take_profit_order(
            current_price)

        if not tp_order:
            self.logger.info("📋 当前无持仓，不挂止盈订单")
            return

        try:
            # 下止盈订单
            placed_order = await self.engine.place_order(tp_order)
            self.state.add_order(placed_order)

            self.logger.info(
                f"💰 止盈订单已挂: {placed_order.side.value} "
                f"{placed_order.amount}@{placed_order.price} "
                f"(Grid {placed_order.grid_id})"
            )
        except Exception as e:
            self.logger.error(f"❌ 挂止盈订单失败: {e}")

    def _is_take_profit_order_filled(self, filled_order: GridOrder) -> bool:
        """判断是否是止盈订单成交"""
        if not self.scalping_manager or not self.scalping_manager.is_active():
            return False

        tp_order = self.scalping_manager.get_current_take_profit_order()
        if not tp_order:
            return False

        return filled_order.order_id == tp_order.order_id

    def _should_place_reverse_order_in_scalping(self, filled_order: GridOrder) -> bool:
        """
        判断在剥头皮模式下是否应该挂反向订单

        Args:
            filled_order: 已成交订单

        Returns:
            是否应该挂反向订单
        """
        from ..models import GridType

        # 做多网格：只挂买单（建仓），不挂卖单（平仓）
        if self.config.grid_type in [GridType.LONG, GridType.FOLLOW_LONG, GridType.MARTINGALE_LONG]:
            # 如果成交的是买单，应该挂卖单，但剥头皮模式不挂
            return filled_order.side == GridOrderSide.SELL

        # 做空网格：只挂卖单（建仓），不挂买单（平仓）
        else:
            # 如果成交的是卖单，应该挂买单，但剥头皮模式不挂
            return filled_order.side == GridOrderSide.BUY

    def _sync_orders_from_engine(self):
        """
        从engine同步最新的订单统计到state

        健康检查后，engine的_pending_orders可能已更新，需要同步到state
        这样UI才能显示正确的订单数量

        🔥 修复：同时同步state.active_orders，确保订单成交时能正确更新统计
        """
        try:
            # 从engine获取当前挂单
            engine_orders = self.engine.get_pending_orders()

            # 统计买单和卖单数量
            buy_count = sum(
                1 for order in engine_orders if order.side == GridOrderSide.BUY)
            sell_count = sum(
                1 for order in engine_orders if order.side == GridOrderSide.SELL)

            # 更新state的统计数据
            self.state.pending_buy_orders = buy_count
            self.state.pending_sell_orders = sell_count

            # 🔥 新增：同步state.active_orders
            # 确保state.active_orders包含所有engine中的订单
            engine_order_ids = {order.order_id for order in engine_orders}
            state_order_ids = set(self.state.active_orders.keys())

            # 1. 移除state中已不存在于engine的订单
            removed_orders = state_order_ids - engine_order_ids
            for order_id in removed_orders:
                if order_id in self.state.active_orders:
                    del self.state.active_orders[order_id]

            # 2. 添加engine中存在但state中没有的订单（健康检查新增的）
            added_orders = engine_order_ids - state_order_ids
            for order in engine_orders:
                if order.order_id in added_orders:
                    # 添加到state.active_orders，这样成交时能正确更新统计
                    self.state.active_orders[order.order_id] = order

            # 记录同步信息
            if removed_orders or added_orders:
                self.logger.debug(
                    f"📊 订单同步: State增加{len(added_orders)}个, 移除{len(removed_orders)}个, "
                    f"当前={len(self.state.active_orders)}个"
                )

            # 如果engine和state的订单数量差异较大，记录日志
            state_total = len(self.state.active_orders)
            engine_total = len(engine_orders)

            if abs(state_total - engine_total) > 5:
                self.logger.warning(
                    f"⚠️ 订单同步后仍有差异: State={state_total}个, Engine={engine_total}个, "
                    f"差异={abs(state_total - engine_total)}个"
                )

        except Exception as e:
            self.logger.debug(f"同步订单统计失败: {e}")

    def _safe_decimal(self, value, default='0') -> Decimal:
        """安全转换为Decimal"""
        try:
            if value is None:
                return Decimal(default)
            return Decimal(str(value))
        except:
            return Decimal(default)
