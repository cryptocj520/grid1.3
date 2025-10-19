"""
持仓监控模块

提供WebSocket + REST混合持仓监控策略
"""

import asyncio
import time
from typing import Dict, Any, Optional
from decimal import Decimal
from datetime import datetime

from ....logging import get_logger


class PositionMonitor:
    """
    持仓监控管理器（WebSocket优先，REST备用，自动重连）

    职责：
    1. WebSocket持仓监控（实时）
    2. REST API备用监控（WebSocket失败时）
    3. 定期REST校验（心跳检测）
    4. WebSocket自动重连
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

        # WebSocket监控状态
        self._position_ws_enabled: bool = False
        self._last_position_ws_time: float = 0
        self._last_order_filled_time: float = 0

        # REST备用监控状态
        self._last_position_rest_sync: float = 0

        # REST定期校验状态
        self._last_position_rest_verify_time: float = 0

        # 持仓缓存
        self._last_ws_position_size = Decimal('0')
        self._last_ws_position_price = Decimal('0')

        # 配置参数
        self._position_ws_response_timeout: int = 5  # WebSocket响应超时（秒）
        self._position_rest_verify_interval: int = 60  # REST校验间隔（秒）
        self._scalping_position_check_interval: int = 1  # REST备用轮询间隔（秒）

        # 监控任务
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None

    async def start_monitoring(self):
        """启动持仓监控"""
        if self._running:
            self.logger.warning("持仓监控已经在运行")
            return

        self._running = True

        # 订阅WebSocket持仓更新
        try:
            self.logger.info("🔄 订阅WebSocket持仓更新流...")

            if hasattr(self.engine.exchange, 'subscribe_position_updates'):
                await self.engine.exchange.subscribe_position_updates(
                    self.config.symbol,
                    self._on_position_update
                )
                self._position_ws_enabled = True
                # 🔥 同步更新 GridCoordinator 的标志
                self.coordinator._position_ws_enabled = True
                self.logger.info("✅ WebSocket持仓更新流订阅成功")
            else:
                self.logger.warning("⚠️ 交易所不支持WebSocket持仓订阅")
        except Exception as e:
            self.logger.warning(f"⚠️ WebSocket持仓订阅失败: {e}")
            self._position_ws_enabled = False
            # 🔥 同步更新 GridCoordinator 的标志
            self.coordinator._position_ws_enabled = False

        # 用REST API同步初始持仓
        try:
            self.logger.info("📊 正在同步初始持仓数据（REST API）...")
            positions = await self.engine.exchange.get_positions([self.config.symbol])
            if positions:
                position = positions[0]
                position_qty = position.size if position.side.value.lower() == 'long' else - \
                    position.size
                self.tracker.sync_initial_position(
                    position=position_qty,
                    entry_price=position.entry_price
                )
                self.logger.info(
                    f"✅ 初始持仓同步完成（REST）: {position.side.value} {position.size} @ ${position.entry_price}"
                )
            else:
                self.logger.info("📊 REST API显示无持仓")
        except Exception as rest_error:
            self.logger.warning(f"⚠️ REST API初始持仓同步失败: {rest_error}")

        # 启动监控循环
        self._monitor_task = asyncio.create_task(self._position_sync_loop())
        self.logger.info("✅ 持仓监控已启动（WebSocket优先，REST备用，自动重连）")

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

    async def _on_position_update(self, position_info: Dict[str, Any]):
        """
        WebSocket持仓更新回调

        Args:
            position_info: 持仓信息字典
        """
        try:
            symbol = position_info.get('symbol')
            if symbol != self.config.symbol:
                return

            position_size = position_info.get('size', 0)
            entry_price = position_info.get('entry_price', 0)
            side = position_info.get('side', 'Unknown')

            # 同步持仓到追踪器
            self.tracker.sync_initial_position(
                position=position_size,
                entry_price=entry_price
            )

            # 更新WebSocket最后接收时间
            self._last_position_ws_time = time.time()

            # 更新WebSocket持仓记录
            self._last_ws_position_size = position_size
            self._last_ws_position_price = entry_price

            # 标记WebSocket持仓监控为启用状态
            if not self._position_ws_enabled:
                self._position_ws_enabled = True
                # 🔥 同步更新 GridCoordinator 的标志
                self.coordinator._position_ws_enabled = True
                self.logger.info("✅ WebSocket持仓监控已启用（收到首次持仓更新）")

            self.logger.info(
                f"📊 WebSocket持仓同步: {symbol} {side} "
                f"数量={position_size}, 成本=${entry_price}"
            )

        except Exception as e:
            self.logger.error(f"❌ 处理WebSocket持仓更新失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    async def _position_sync_loop(self):
        """持仓同步监控循环"""
        self._last_position_ws_time = time.time()
        self._last_position_rest_sync = 0
        last_rest_log_time = 0

        # 配置参数
        rest_sync_interval = self._scalping_position_check_interval
        ws_reconnect_interval = 5
        monitor_check_interval = 1
        rest_log_interval = 60

        self.logger.info(
            f"🔄 持仓同步监控已启动: "
            f"WS响应超时={self._position_ws_response_timeout}秒, "
            f"REST校验间隔={self._position_rest_verify_interval}秒"
        )

        last_ws_reconnect_attempt = 0

        while self._running:
            try:
                await asyncio.sleep(monitor_check_interval)

                current_time = time.time()

                # 检查WebSocket健康状态
                if self._position_ws_enabled:
                    ws_should_fail = False

                    # 条件1：订单成交了，但WebSocket没有响应
                    if self._last_order_filled_time > 0:
                        order_ws_delay = current_time - self._last_order_filled_time
                        ws_response_delay = self._last_order_filled_time - self._last_position_ws_time

                        if order_ws_delay > self._position_ws_response_timeout and ws_response_delay > 0:
                            self.logger.warning(
                                f"⚠️ WebSocket失效: 订单成交{order_ws_delay:.1f}秒后仍无持仓更新，"
                                f"切换到REST备用模式"
                            )
                            ws_should_fail = True

                    # 条件2：剥头皮模式下持仓为0（异常情况）
                    if self.coordinator.scalping_manager and self.coordinator.scalping_manager.is_active():
                        current_position = self.tracker.get_current_position()
                        if abs(current_position) == 0:
                            self.logger.warning(
                                f"⚠️ WebSocket异常: 剥头皮模式下持仓为0（不应该发生），"
                                f"切换到REST备用模式"
                            )
                            ws_should_fail = True

                    if ws_should_fail:
                        self._position_ws_enabled = False

                # 定期REST校验（心跳检测）
                if self._position_ws_enabled:
                    time_since_last_verify = current_time - self._last_position_rest_verify_time

                    if time_since_last_verify >= self._position_rest_verify_interval:
                        await self._verify_position_with_rest()
                        self._last_position_rest_verify_time = current_time

                # REST备用同步
                if not self._position_ws_enabled:
                    if current_time - self._last_position_rest_sync > rest_sync_interval:
                        await self._sync_position_with_rest(current_time, last_rest_log_time, rest_log_interval)
                        self._last_position_rest_sync = current_time

                # 尝试重连WebSocket
                if not self._position_ws_enabled and (current_time - last_ws_reconnect_attempt > ws_reconnect_interval):
                    await self._reconnect_websocket()
                    last_ws_reconnect_attempt = current_time

            except asyncio.CancelledError:
                self.logger.info("🔄 持仓同步监控任务已取消")
                break
            except Exception as e:
                self.logger.error(f"❌ 持仓同步监控错误: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(10)

        self.logger.info("🔄 持仓同步监控任务已退出")

    async def _verify_position_with_rest(self):
        """使用REST API验证WebSocket持仓（心跳检测）"""
        try:
            positions = await self.engine.exchange.get_positions([self.config.symbol])

            if positions and len(positions) > 0:
                position = positions[0]
                rest_position = position.size or Decimal('0')

                # 根据方向确定持仓符号
                if hasattr(position, 'side'):
                    from ....adapters.exchanges import PositionSide
                    if position.side == PositionSide.SHORT and rest_position != 0:
                        rest_position = -rest_position

                ws_position = self._last_ws_position_size
                position_diff = abs(rest_position - ws_position)

                if position_diff > Decimal('0.01'):
                    self.logger.warning(
                        f"⚠️ WebSocket持仓校验失败: "
                        f"WS={ws_position}, REST={rest_position}, "
                        f"差异={position_diff}, 切换到REST备用模式"
                    )
                    self._position_ws_enabled = False

                    # 立即用REST数据更新持仓
                    if self.coordinator.scalping_manager and self.coordinator.scalping_manager.is_active():
                        initial_capital = self.coordinator.scalping_manager.get_initial_capital()
                        self.coordinator.scalping_manager.update_position(
                            rest_position, position.entry_price,
                            initial_capital, self.coordinator.balance_monitor.collateral_balance
                        )
                        self._last_ws_position_size = rest_position
                        self._last_ws_position_price = position.entry_price
                else:
                    self.logger.info(
                        f"✅ WebSocket持仓校验通过: WS={ws_position}, REST={rest_position}"
                    )
        except Exception as e:
            self.logger.warning(f"⚠️ REST持仓校验失败: {e}")

    async def _sync_position_with_rest(self, current_time, last_rest_log_time, rest_log_interval):
        """使用REST API同步持仓（WebSocket失败时）"""
        try:
            positions = await self.engine.exchange.get_positions([self.config.symbol])
            if positions:
                position = positions[0]
                position_qty = position.size if position.side.value.lower() == 'long' else - \
                    position.size

                self.tracker.sync_initial_position(
                    position=position_qty,
                    entry_price=position.entry_price
                )

                # 剥头皮模式：检查持仓变化并更新止盈订单
                if self.coordinator.scalping_manager and self.coordinator.scalping_manager.is_active():
                    old_position = self._last_ws_position_size

                    if position_qty != old_position:
                        initial_capital = self.coordinator.scalping_manager.get_initial_capital()
                        self.coordinator.scalping_manager.update_position(
                            position_qty, position.entry_price,
                            initial_capital, self.coordinator.balance_monitor.collateral_balance
                        )

                        self._last_ws_position_size = position_qty
                        self._last_ws_position_price = position.entry_price

                        self.logger.info(
                            f"📡 REST备用同步: 数量 {old_position} → {position_qty}, "
                            f"成本=${position.entry_price:.2f}"
                        )
        except Exception as e:
            self.logger.warning(f"⚠️ REST持仓同步失败: {e}")

    async def _reconnect_websocket(self):
        """尝试重连WebSocket"""
        try:
            self.logger.info("🔄 尝试重新订阅WebSocket持仓更新...")

            if hasattr(self.engine.exchange, 'subscribe_position_updates'):
                await self.engine.exchange.subscribe_position_updates(
                    self.config.symbol,
                    self._on_position_update
                )
                self._position_ws_enabled = True
                # 🔥 同步更新 GridCoordinator 的标志
                self.coordinator._position_ws_enabled = True
                self._last_position_ws_time = time.time()
                self.logger.info("✅ WebSocket持仓订阅重连成功！")
        except Exception as e:
            self.logger.warning(f"⚠️ WebSocket重连失败: {e}")

    def record_order_filled(self):
        """记录订单成交时间（用于WebSocket响应检测）"""
        self._last_order_filled_time = time.time()

    def get_position_data_source(self) -> str:
        """获取当前持仓数据来源"""
        if self._position_ws_enabled:
            return "WebSocket实时"
        else:
            return "REST API备用"
