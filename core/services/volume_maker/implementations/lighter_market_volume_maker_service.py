"""
Lighter市价刷量交易服务实现（基于Backpack信号）

架构：
- 信号源：Backpack（监控价格稳定、订单簿条件）
- 执行端：Lighter（执行市价交易）
- 模式：仅市价模式（无需监控Lighter订单成交）

核心流程：
1. 监控Backpack订单簿 → 等待价格稳定
2. 在Lighter执行市价开仓
3. 在Lighter执行市价平仓
4. 循环执行
"""

import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, Tuple, Dict, Any
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from ....adapters.exchanges.interface import ExchangeInterface
from ....adapters.exchanges.models import OrderSide, OrderType, OrderData, OrderBookData, PositionSide, OrderStatus

from ..interfaces.volume_maker_service import IVolumeMakerService
from ..models.volume_maker_config import VolumeMakerConfig
from ..models.volume_maker_statistics import (
    VolumeMakerStatistics,
    CycleResult,
    CycleStatus
)
from ..hourly_statistics import HourlyStatisticsTracker


class LighterMarketVolumeMakerService(IVolumeMakerService):
    """
    Lighter市价刷量服务（基于Backpack信号）

    核心特性：
    - 双适配器架构：Backpack监控 + Lighter执行
    - 仅市价模式：简单高效，无需监控订单
    - 完全复用原脚本的判断逻辑
    """

    def __init__(
        self,
        signal_adapter: ExchangeInterface,
        execution_adapter: ExchangeInterface
    ):
        """
        初始化Lighter刷量服务

        Args:
            signal_adapter: 信号交易所适配器（Backpack）
            execution_adapter: 执行交易所适配器（Lighter）
        """
        self.signal_adapter = signal_adapter  # Backpack（只读）
        self.execution_adapter = execution_adapter  # Lighter（读写）

        self.config: Optional[VolumeMakerConfig] = None
        self.statistics = VolumeMakerStatistics()

        # 运行状态
        self._running = False
        self._paused = False
        self._should_stop = False
        self._stop_called = False  # 防止重复调用stop()

        # 当前持仓（Lighter上的）
        self._current_position = Decimal("0")

        # 日志
        self.logger: Optional[logging.Logger] = None

        # 任务
        self._main_task: Optional[asyncio.Task] = None

        # 📊 小时级统计跟踪器
        self._hourly_tracker: Optional[HourlyStatisticsTracker] = None

        # 🔥 交易方向策略（用于交替或随机）
        self._last_direction: Optional[str] = None  # "long" or "short"

        # 🔥 最新订单簿数据（用于UI显示）
        self._latest_orderbook: Optional['OrderBookData'] = None

        # 🔥 最新余额数据（用于UI显示）
        self._latest_balance: Optional[Decimal] = None
        self._balance_currency: str = "USDC"  # 余额币种

        # 🔥 WebSocket订单成交监控（基于状态机，不依赖order_id）
        # 状态机：IDLE -> WAITING_OPEN -> POSITION_OPEN -> WAITING_CLOSE -> IDLE
        self._fill_state = "IDLE"  # IDLE, WAITING_OPEN, WAITING_CLOSE
        self._expected_side: Optional[str] = None  # "buy" or "sell"
        self._expected_amount: Optional[Decimal] = None
        self._accumulated_amount: Decimal = Decimal("0")
        self._accumulated_cost: Decimal = Decimal("0")  # 用于计算平均价格
        self._fill_event: Optional[asyncio.Event] = None
        self._fill_lock = asyncio.Lock()  # 保护状态变更

    async def initialize(self, config: VolumeMakerConfig) -> bool:
        """初始化刷量服务"""
        try:
            self.config = config

            # 验证配置（必须是市价模式）
            if self.config.order_mode != 'market':
                self.logger.error("❌ Lighter刷量服务仅支持市价模式")
                return False

            # 初始化日志
            self._setup_logging()

            self.logger.info("=" * 70)
            self.logger.info("Lighter市价刷量服务（基于Backpack信号）")
            self.logger.info("=" * 70)
            self.logger.info(f"信号交易所: Backpack")
            self.logger.info(f"执行交易所: Lighter")
            self.logger.info(f"交易模式: 市价模式")
            self.logger.info(
                f"信号符号: {self.config.signal_symbol or self.config.symbol}")
            self.logger.info(
                f"执行符号: {self.config.execution_symbol or self.config.symbol}")
            self.logger.info(f"订单大小: {self.config.order_size}")

            # 🔥 反向交易模式提示
            if self.config.reverse_trading:
                self.logger.info("🔄 反向交易模式: 已启用（所有开仓和平仓方向反转）")
            else:
                self.logger.info("📈 反向交易模式: 未启用（正常模式）")

            self.logger.info("=" * 70)

            # 初始化小时级统计跟踪器
            self._hourly_tracker = HourlyStatisticsTracker()

            # 连接信号交易所（Backpack）
            if not self.signal_adapter.is_connected():
                self.logger.info("🔗 连接信号交易所（Backpack）...")
                await self.signal_adapter.connect()
                self.logger.info("✅ Backpack连接成功")

            # Lighter适配器已在启动脚本中初始化
            if self.execution_adapter.is_connected():
                self.logger.info("✅ Lighter适配器已连接")

            # 🔥 启动WebSocket订阅订单成交
            await self._setup_websocket_subscription()

            # 检查Lighter余额
            if not await self._check_execution_balance():
                return False

            self.logger.info("✅ 初始化完成")
            return True

        except Exception as e:
            self.logger.error(f"❌ 初始化失败: {e}", exc_info=True)
            return False

    async def _setup_websocket_subscription(self):
        """设置WebSocket订阅以监控订单成交"""
        try:
            # 检查执行适配器是否有WebSocket模块
            if not hasattr(self.execution_adapter, '_websocket'):
                self.logger.warning("⚠️ Lighter适配器没有 _websocket 属性")
                return

            if not self.execution_adapter._websocket:
                self.logger.warning("⚠️ Lighter适配器的 _websocket 为 None")
                return

            # 订阅订单成交
            ws = self.execution_adapter._websocket
            await ws.subscribe_order_fills(self._on_order_fill)
            self.logger.info("✅ 已启动Lighter订单成交WebSocket订阅")

        except Exception as e:
            self.logger.error(f"❌ 启动WebSocket订阅失败: {e}", exc_info=True)
            self.logger.warning("⚠️ 将使用fallback方案获取成交价")

    async def _on_order_fill(self, order: OrderData):
        """
        订单成交回调（由WebSocket触发）

        🔥 新逻辑：基于状态机匹配，不依赖order_id
        - 只关注方向和数量是否匹配当前状态
        - 累加成交直到满足期望数量
        - 计算平均成交价格

        Args:
            order: 成交的订单数据
        """
        try:
            async with self._fill_lock:
                # 如果不在等待状态，忽略
                if self._fill_state not in ["WAITING_OPEN", "WAITING_CLOSE"]:
                    return

                # 检查方向是否匹配
                order_side = order.side.value.lower()  # "buy" or "sell"
                if order_side != self._expected_side:
                    return

                # 累加成交数量和成本
                fill_amount = order.filled if order.filled else order.amount
                fill_price = order.average if order.average else order.price

                self._accumulated_amount += fill_amount
                self._accumulated_cost += fill_amount * fill_price

                self.logger.info(
                    f"📨 WebSocket收到成交 - "
                    f"方向: {order_side}, "
                    f"数量: {fill_amount}, "
                    f"价格: {fill_price}, "
                    f"累计: {self._accumulated_amount}/{self._expected_amount}"
                )

                # 检查是否已满足期望数量
                if self._accumulated_amount >= self._expected_amount:
                    # 计算平均价格
                    avg_price = self._accumulated_cost / self._accumulated_amount
                    self.logger.info(
                        f"✅ 成交完成 - "
                        f"总数量: {self._accumulated_amount}, "
                        f"平均价格: {avg_price:.2f}"
                    )

                    # 触发等待事件
                    if self._fill_event:
                        self._fill_event.set()

        except Exception as e:
            self.logger.error(f"❌ 处理订单成交回调失败: {e}", exc_info=True)

    def _prepare_fill_tracking(self, side: str, amount: Decimal, state: str):
        """
        准备成交追踪（设置状态机）

        Args:
            side: 订单方向 "buy" or "sell"
            amount: 期望成交数量
            state: 目标状态 "WAITING_OPEN" or "WAITING_CLOSE"
        """
        self._fill_state = state
        self._expected_side = side.lower()
        self._expected_amount = amount
        self._accumulated_amount = Decimal("0")
        self._accumulated_cost = Decimal("0")
        self._fill_event = asyncio.Event()

        self.logger.debug(
            f"🎯 准备追踪成交 - 状态: {state}, 方向: {side}, 数量: {amount}"
        )

    async def _wait_for_order_fill(self, side: str, amount: Decimal, timeout: float = 10.0) -> Optional[Dict[str, Any]]:
        """
        等待订单成交（通过WebSocket推送）

        🔥 新逻辑：基于状态机等待，不依赖order_id
        - 通过方向和数量匹配成交
        - 支持部分成交累加
        - 返回平均成交价格

        Args:
            side: 订单方向 "buy" or "sell"
            amount: 期望成交数量
            timeout: 超时时间（秒）

        Returns:
            Dict: {"average_price": Decimal, "filled_amount": Decimal} 或 None（超时）
        """
        try:
            if not self._fill_event:
                self.logger.error("❌ 未准备成交追踪，请先调用 _prepare_fill_tracking")
                return None

            try:
                await asyncio.wait_for(self._fill_event.wait(), timeout=timeout)

                # 成功收到成交通知
                avg_price = self._accumulated_cost / self._accumulated_amount

                return {
                    "average_price": avg_price,
                    "filled_amount": self._accumulated_amount
                }

            except asyncio.TimeoutError:
                self.logger.warning(
                    f"⏰ 订单成交超时 - "
                    f"方向: {side}, "
                    f"期望: {amount}, "
                    f"已收到: {self._accumulated_amount}"
                )
                return None

        except Exception as e:
            self.logger.error(f"❌ 等待订单成交失败: {e}", exc_info=True)
            return None
        finally:
            # 重置状态
            self._fill_state = "IDLE"
            self._fill_event = None

    def _setup_logging(self):
        """设置日志"""
        # 创建日志目录
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        # 创建logger（使用标准logging）
        logger_name = f"lighter_market_volume_maker_{self.config.symbol}"
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.INFO)

        # 清除已有的处理器
        self.logger.handlers.clear()

        # 文件处理器
        log_file = log_dir / f"lighter_volume_maker_{self.config.symbol}.log"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)

        # 🔥 移除控制台处理器，只输出到文件
        # 配置根logger，让所有模块（包括lighter_websocket）的日志都输出到同一个文件
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)

        # 清除根logger已有的处理器
        root_logger.handlers.clear()

        # 只添加文件处理器到根logger（不添加控制台处理器）
        root_logger.addHandler(file_handler)

        # 让当前logger也使用文件处理器，但禁止传播以避免重复
        self.logger.propagate = False
        self.logger.addHandler(file_handler)

    async def start(self) -> None:
        """启动刷量服务"""
        if self._running:
            self.logger.warning("服务已在运行")
            return

        self._running = True
        self._should_stop = False
        self.statistics.is_running = True
        self.statistics.start_time = datetime.now()

        self.logger.info("🚀 启动Lighter市价刷量服务...")

        # 启动主循环
        self._main_task = asyncio.create_task(self._main_loop())

    async def stop(self) -> None:
        """停止刷量服务"""
        # 防止重复执行 stop
        if self._stop_called:
            return
        self._stop_called = True

        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("⏸️  正在停止Lighter刷量服务...")
        self.logger.info("=" * 70)

        self._should_stop = True
        self._running = False

        # 取消主任务
        if self._main_task and not self._main_task.done():
            self._main_task.cancel()
            try:
                await asyncio.wait_for(self._main_task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

        # 清理持仓（添加超时保护）
        try:
            await asyncio.wait_for(self._cleanup_if_needed(), timeout=3.0)
        except asyncio.TimeoutError:
            self.logger.warning("⏰ 清理持仓超时，跳过")

        # 更新统计信息
        self.statistics.is_running = False
        self.statistics.end_time = datetime.now()

        # 输出最终统计
        self.logger.info("")
        self.logger.info("📊 最终统计:")
        self.logger.info(f"   总轮次: {self.statistics.total_cycles}")
        self.logger.info(f"   成功: {self.statistics.successful_cycles}")
        self.logger.info(f"   失败: {self.statistics.failed_cycles}")
        if self.statistics.total_cycles > 0:
            success_rate = (self.statistics.successful_cycles /
                            self.statistics.total_cycles) * 100
            self.logger.info(f"   成功率: {success_rate:.1f}%")

        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("✅ Lighter刷量服务已停止")
        self.logger.info("=" * 70)

    def pause(self) -> None:
        """暂停交易"""
        self._paused = True
        self.logger.info("⏸️  交易已暂停")

    def resume(self) -> None:
        """恢复交易"""
        self._paused = False
        self.logger.info("▶️  交易已恢复")

    def is_running(self) -> bool:
        """是否正在运行"""
        return self._running

    def is_paused(self) -> bool:
        """是否已暂停"""
        return self._paused

    async def _interruptible_sleep(self, duration: float) -> bool:
        """
        可中断的睡眠

        Args:
            duration: 睡眠时长（秒）

        Returns:
            True如果正常完成，False如果被中断
        """
        elapsed = 0.0
        step = 0.1  # 每0.1秒检查一次

        while elapsed < duration:
            if self._should_stop:
                return False  # 被中断

            sleep_time = min(step, duration - elapsed)
            await asyncio.sleep(sleep_time)
            elapsed += sleep_time

        return True  # 正常完成

    def get_statistics(self) -> VolumeMakerStatistics:
        """获取统计信息"""
        return self.statistics

    def get_status_text(self) -> str:
        """获取状态文本"""
        if not self._running:
            return "已停止"
        elif self._paused:
            return "已暂停"
        else:
            return "运行中"

    async def emergency_stop(self) -> None:
        """紧急停止（简化版：只平Lighter持仓）"""
        self.logger.warning("🚨 执行紧急停止！")

        try:
            # 平掉Lighter所有持仓
            positions = await self.execution_adapter.get_positions()
            for pos in positions:
                if abs(pos.size) > 0:
                    self.logger.warning(
                        f"⚠️ 紧急平仓: {pos.size} {self.config.symbol}")
                    side = OrderSide.SELL if pos.size > 0 else OrderSide.BUY
                    await self.execution_adapter.place_market_order(
                        symbol=self.config.symbol,
                        side=side,
                        quantity=abs(pos.size),
                        reduce_only=True,  # 🔥 只减仓模式：紧急平仓时避免误操作
                        skip_order_index_query=True  # 🔥 跳过 order_index 查询
                    )
                    self.logger.info("✅ 紧急平仓完成")
        except Exception as e:
            self.logger.error(f"❌ 紧急平仓失败: {e}")

        # 停止服务
        await self.stop()

    async def _cleanup_if_needed(self) -> None:
        """检查并清理Lighter残留持仓"""
        try:
            # 🔥 添加超时保护，避免卡住
            positions = await asyncio.wait_for(
                self.execution_adapter.get_positions(),
                timeout=5.0  # 5秒超时
            )

            if not positions:
                self.logger.info("✅ Lighter无残留持仓")
                return

            for pos in positions:
                if abs(pos.size) > 0:
                    self.logger.warning(f"⚠️ 检测到Lighter残留持仓: {pos.size}，执行清理")

                    # 🔥 确定平仓方向（与持仓方向相反）
                    # 必须使用 side 字段，因为 size 是绝对值
                    side = OrderSide.SELL if pos.side == PositionSide.LONG else OrderSide.BUY
                    close_direction = "sell" if side == OrderSide.SELL else "buy"
                    close_quantity = abs(pos.size)

                    position_side_str = "多头" if pos.side == PositionSide.LONG else "空头"
                    close_side_str = "卖出" if side == OrderSide.SELL else "买入"
                    self.logger.info(
                        f"📊 清理持仓 - 持仓方向: {position_side_str}, 平仓方向: {close_side_str}, 数量: {close_quantity}")

                    # 🔥 准备成交追踪（在下单前设置状态机）
                    self._prepare_fill_tracking(
                        side=close_direction,
                        amount=close_quantity,
                        state="WAITING_CLOSE"
                    )

                    # 🔥 清理操作也添加超时
                    order = await asyncio.wait_for(
                        self.execution_adapter.place_market_order(
                            symbol=self.config.symbol,
                            side=side,
                            quantity=close_quantity,
                            reduce_only=True,  # 🔥 只减仓模式：避免误开新仓
                            skip_order_index_query=True  # 🔥 跳过 order_index 查询
                        ),
                        timeout=10.0  # 10秒超时
                    )

                    if order:
                        # 等待 WebSocket 成交通知
                        fill_result = await self._wait_for_order_fill(
                            side=close_direction,
                            amount=close_quantity,
                            timeout=10.0
                        )
                        if fill_result:
                            self.logger.info(
                                f"✅ 持仓清理完成 - "
                                f"平均价格: {fill_result['average_price']:.2f}, "
                                f"成交数量: {fill_result['filled_amount']}")
                        else:
                            self.logger.info("✅ 持仓清理完成（未收到成交确认）")

        except asyncio.TimeoutError:
            self.logger.warning("⏰ 检查/清理持仓超时，跳过")
        except Exception as e:
            self.logger.error(f"❌ 清理持仓失败: {e}")

    async def _main_loop(self) -> None:
        """主循环"""
        try:
            while not self._should_stop:
                # 检查是否暂停
                if self._paused:
                    await asyncio.sleep(0.5)  # 🔥 缩短sleep时间，快速响应
                    continue

                # 检查是否达到最大轮次
                if self.config.max_cycles > 0 and self.statistics.total_cycles >= self.config.max_cycles:
                    self.logger.info(f"✅ 达到最大轮次 {self.config.max_cycles}，停止交易")
                    break

                # 检查连续失败次数
                if self.statistics.consecutive_fails >= self.config.max_consecutive_fails:
                    self.logger.error(
                        f"❌ 连续失败 {self.config.max_consecutive_fails} 次，停止交易")
                    break

                # 执行一轮交易
                try:
                    await self._execute_market_cycle()
                except asyncio.CancelledError:
                    self.logger.info("⚠️ 交易轮次被取消")
                    raise  # 🔥 重新抛出，让外层处理
                except Exception as e:
                    self.logger.error(f"❌ 执行轮次出错: {e}", exc_info=True)

                    # 🔥 清理持仓（添加超时和停止检查）
                    if not self._should_stop:
                        try:
                            await asyncio.wait_for(self._cleanup_if_needed(), timeout=3.0)
                        except asyncio.TimeoutError:
                            self.logger.warning("⏰ 清理持仓超时")

                    # 🔥 分段sleep，快速响应停止信号
                    for _ in range(10):  # 10次 * 0.5秒 = 5秒
                        if self._should_stop:
                            break
                        await asyncio.sleep(0.5)

                # 🔥 轮次间隔（分段sleep，快速响应停止）
                if self.config.cycle_interval > 0 and not self._should_stop:
                    sleep_segments = int(self.config.cycle_interval / 0.5)
                    for _ in range(sleep_segments):
                        if self._should_stop:
                            break
                        await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            self.logger.info("✅ 主循环被取消")
            raise  # 🔥 继续传播取消信号
        except Exception as e:
            self.logger.error(f"❌ 主循环异常: {e}", exc_info=True)
        finally:
            self._running = False
            self.statistics.is_running = False

    async def _execute_market_cycle(self) -> None:
        """执行一轮市价交易（主流程编排）"""
        cycle_id = self.statistics.total_cycles + 1
        result = self._create_cycle_result(cycle_id)

        self.logger.info(f"━━━━━━ 开始第 {cycle_id} 轮（Lighter市价模式）━━━━━━")

        try:
            # 预检查
            if not await self._pre_cycle_checks():
                return

            # 等待稳定市场并获取数据
            market_data = await self._wait_for_stable_market(result)
            if not market_data:
                return

            # 执行开仓
            direction = self._decide_direction()
            if not await self._execute_open_position(direction, market_data, result):
                return

            # 等待平仓信号
            await self._wait_for_close_signal(market_data, result)

            # 执行平仓并验证
            if not await self._execute_close_and_verify(direction, result):
                return

            # 标记成功
            result.status = CycleStatus.SUCCESS
            self.logger.info("✅ Lighter持仓已清空，本轮完成")

        except Exception as e:
            await self._handle_cycle_error(result, e)

        finally:
            self._finalize_cycle_result(result, cycle_id)

    def _create_cycle_result(self, cycle_id: int) -> CycleResult:
        """创建交易轮次结果对象"""
        start_time = datetime.now()
        return CycleResult(
            cycle_id=cycle_id,
            status=CycleStatus.FAILED,
            start_time=start_time,
            end_time=start_time,
            duration=timedelta(seconds=0),
            bid_price=Decimal("0"),
            ask_price=Decimal("0"),
            spread=Decimal("0")
        )

    async def _pre_cycle_checks(self) -> bool:
        """执行轮次开始前的检查（清理持仓+余额检查）"""
        # 检查停止信号
        if self._should_stop:
            self.logger.info("⚠️ 检测到停止信号，跳过本轮")
            return False

        # 清理残留持仓
        self.logger.info("🔍 检查Lighter是否有残留持仓...")
        try:
            await asyncio.wait_for(self._cleanup_if_needed(), timeout=5.0)
        except asyncio.TimeoutError:
            self.logger.warning("⏰ 检查残留持仓超时，继续")

        # 再次检查停止信号
        if self._should_stop:
            return False

        # 检查余额
        if self.config.min_balance is not None:
            if not await self._check_execution_balance():
                self.logger.error("❌ Lighter余额不足，停止交易")
                self._running = False
                return False

        return True

    async def _wait_for_stable_market(self, result: CycleResult) -> Optional[Tuple]:
        """等待Backpack价格稳定并返回市场数据"""
        self.logger.info("📊 监控Backpack价格稳定...")
        stable_data = await self._wait_for_backpack_stable_price()

        if not stable_data:
            result.status = CycleStatus.TIMEOUT
            result.error_message = "Backpack价格稳定检测超时"
            return None

        bid_price, ask_price, bid_amount, ask_amount, quantity_ratio = stable_data
        result.bid_price = bid_price
        result.ask_price = ask_price
        result.spread = ask_price - bid_price
        result.quantity_ratio = quantity_ratio

        self.logger.info(
            f"✅ Backpack价格稳定 - 买1: {bid_price}, 卖1: {ask_price}, 价差: {result.spread}")

        return stable_data

    async def _execute_open_position(self, direction: str, market_data: Tuple, result: CycleResult) -> bool:
        """执行开仓操作"""
        bid_price, ask_price, bid_amount, ask_amount, quantity_ratio = market_data

        result.filled_side = direction
        self.logger.info(f"🎯 交易方向: {direction.upper()}")

        # 在Lighter执行开仓
        self.logger.info(f"📝 在Lighter执行市价{direction}单...")
        open_order = await self._execute_lighter_market_open(direction)

        if not open_order:
            result.status = CycleStatus.FAILED
            result.error_message = "Lighter开仓失败"
            return False

        # 设置成交价格（优先使用WebSocket真实成交价）
        if open_order.average:
            result.filled_price = open_order.average
        else:
            result.filled_price = bid_price if direction == 'buy' else ask_price

        result.filled_amount = open_order.filled or self.config.order_size

        if direction == 'buy':
            result.buy_order_id = open_order.id
        else:
            result.sell_order_id = open_order.id

        self.logger.info(
            f"✅ Lighter开仓成功 - 价格: {result.filled_price}, 数量: {result.filled_amount}")

        return True

    async def _wait_for_close_signal(self, market_data: Tuple, result: CycleResult) -> None:
        """等待平仓信号"""
        bid_price, ask_price, bid_amount, ask_amount, _ = market_data

        if self.config.market_wait_price_change:
            # 监控Backpack价格变化或数量反转
            self.logger.info("📊 监控Backpack价格变化...")
            wait_result = await self._wait_for_backpack_price_change_or_reversal(
                bid_price, ask_price, bid_amount, ask_amount)

            if wait_result:
                elapsed, reason = wait_result
                # 🔥 保存等待时间和平仓原因
                result.wait_time = elapsed
                result.close_reason = reason
                self.logger.info(
                    f"✅ 触发平仓信号 - 原因: {reason}, 耗时: {elapsed:.2f}秒")
            else:
                self.logger.warning("⚠️ 等待平仓信号超时，强制平仓")
                result.close_reason = "timeout"
        else:
            # 使用固定延迟
            if self.config.post_trade_delay > 0:
                self.logger.info(
                    f"⏱️ 等待{self.config.post_trade_delay}秒后平仓...")
                await self._interruptible_sleep(self.config.post_trade_delay)
                result.wait_time = float(self.config.post_trade_delay)
                result.close_reason = "interval"
            else:
                result.close_reason = "immediate"

    async def _execute_close_and_verify(self, direction: str, result: CycleResult) -> bool:
        """执行平仓并验证持仓清空"""
        # 执行平仓
        self.logger.info("💰 在Lighter市价平仓...")
        close_result = await self._execute_lighter_market_close(direction)

        if close_result:
            result.close_price, result.close_amount = close_result

            # 计算盈亏
            if direction == 'buy':
                result.pnl = (result.close_price -
                              result.filled_price) * result.filled_amount
            else:
                result.pnl = (result.filled_price -
                              result.close_price) * result.filled_amount

            self.logger.info(
                f"✅ Lighter平仓完成 - 价格: {result.close_price}, 盈亏: {result.pnl}")

        # 🔥 等待链上确认，避免频繁查询触发API限流
        # 使用基础延迟，不使用指数退避（这是正常流程）
        # 等待时间由配置文件指定，默认30秒
        wait_time = self.config.chain_confirmation_wait
        self.logger.info(f"⏰ 等待{wait_time}秒让链上确认平仓交易...")
        await asyncio.sleep(wait_time)

        # 验证持仓清空
        self.logger.info("🔍 验证Lighter持仓...")
        position_cleared = await self._verify_lighter_position_cleared(max_retries=5)

        if not position_cleared:
            self.logger.error("❌ Lighter仍有持仓，本轮标记为失败")
            result.status = CycleStatus.FAILED
            result.error_message = "平仓后仍有持仓"
            return False

        return True

    async def _handle_cycle_error(self, result: CycleResult, error: Exception) -> None:
        """处理轮次执行错误"""
        result.status = CycleStatus.FAILED
        result.error_message = str(error)
        self.logger.error(f"❌ 轮次执行失败: {error}", exc_info=True)

        # 异常情况下也要检查并清理持仓
        try:
            self.logger.warning("⚠️ 异常发生，检查Lighter是否有残留持仓...")
            await self._cleanup_if_needed()
        except Exception as cleanup_error:
            self.logger.error(f"❌ 清理持仓失败: {cleanup_error}")

    def _finalize_cycle_result(self, result: CycleResult, cycle_id: int) -> None:
        """完成轮次结果（更新统计信息）"""
        # 更新结果
        result.end_time = datetime.now()
        result.duration = result.end_time - result.start_time

        # 更新统计
        self.statistics.update_from_cycle(result)

        # 更新小时级统计
        if self._hourly_tracker:
            self._hourly_tracker.add_cycle(result)

        self.logger.info(
            f"━━━━━━ 第 {cycle_id} 轮结束 - {result.status.value} ━━━━━━\n")

    async def _check_execution_balance(self) -> bool:
        """检查Lighter余额"""
        try:
            balance = await self.execution_adapter.get_account_balance()

            # 查找USDC或USD余额
            usdc_balance = None
            for bal in balance:
                if bal.currency.upper() in ['USDC', 'USD', 'USDT']:
                    usdc_balance = bal.free
                    self._balance_currency = bal.currency.upper()  # 🔥 更新币种
                    break

            if usdc_balance is None:
                self.logger.warning("⚠️ 未找到USDC余额")
                return True  # 继续运行

            # 🔥 更新最新余额（用于UI显示）
            self._latest_balance = usdc_balance

            if self.config.min_balance is not None and usdc_balance < Decimal(str(self.config.min_balance)):
                self.logger.error(
                    f"❌ Lighter余额不足 - 当前: {usdc_balance}, 要求: {self.config.min_balance}")
                return False

            self.logger.info(
                f"✅ Lighter余额检查通过 - {self._balance_currency}: {usdc_balance}")
            return True

        except Exception as e:
            self.logger.error(f"❌ 检查Lighter余额失败: {e}")
            return False

    async def _wait_for_backpack_stable_price(self) -> Optional[Tuple[Decimal, Decimal, Decimal, Decimal, Optional[float]]]:
        """
        等待Backpack价格稳定（复用原脚本的所有判断逻辑）

        判断条件：
        1. 价格稳定性
        2. 买卖单数量对比反转检测（可选）
        3. 买卖单数量比例检查（可选）
        4. 最小数量检查（可选）

        Returns:
            (bid_price, ask_price, bid_amount, ask_amount, quantity_ratio) 或 None
        """
        duration = self.config.stability_check_duration
        tolerance = self.config.price_tolerance
        interval = self.config.check_interval
        check_reversal = self.config.check_orderbook_reversal

        last_bid: Optional[Decimal] = None
        last_ask: Optional[Decimal] = None
        stable_start: Optional[datetime] = None

        # 🔥 买卖单数量对比反转检测
        initial_orderbook_side: Optional[str] = None
        reversal_count = 0
        final_ratio: Optional[float] = None

        timeout = 300  # 最多等待5分钟
        start_time = datetime.now()

        while (datetime.now() - start_time).total_seconds() < timeout:
            try:
                # 🔥 从Backpack获取订单簿（使用signal_symbol）
                signal_symbol = self.config.signal_symbol or self.config.symbol
                orderbook = await self.signal_adapter.get_orderbook(signal_symbol)

                # 更新最新订单簿（用于UI显示）
                self._latest_orderbook = orderbook

                if not orderbook.bids or not orderbook.asks:
                    if not await self._interruptible_sleep(interval):
                        self.logger.info("⏸️ 价格稳定检查被中断")
                        return None
                    continue

                current_bid = orderbook.bids[0].price
                current_ask = orderbook.asks[0].price

                # 获取买卖单数量
                bid_amount = orderbook.bids[0].size
                ask_amount = orderbook.asks[0].size

                # 检查价格是否稳定
                if last_bid is not None and last_ask is not None:
                    bid_changed = abs(current_bid - last_bid) > tolerance
                    ask_changed = abs(current_ask - last_ask) > tolerance

                    # 检查买卖单数量对比是否反转（严格遵循原始Backpack逻辑）
                    orderbook_reversed = False
                    if check_reversal:
                        current_side = "ask_more" if ask_amount > bid_amount else "bid_more"

                        if initial_orderbook_side is None:
                            initial_orderbook_side = current_side
                        elif current_side != initial_orderbook_side:
                            orderbook_reversed = True
                            reversal_count += 1
                            # 🔥 与原始Backpack一致：每次反转都输出日志
                            self.logger.info(
                                f"📊 Backpack买卖单数量对比发生反转 (第{reversal_count}次) - "
                                f"初始: {initial_orderbook_side}, 当前: {current_side}, "
                                f"买1数量: {bid_amount}, 卖1数量: {ask_amount}")

                    # 判断是否需要重置（严格遵循原始Backpack逻辑）
                    if bid_changed or ask_changed or orderbook_reversed:
                        # 🔥 与原始Backpack一致：反转立即重置倒计时
                        if orderbook_reversed:
                            current_side = "ask_more" if ask_amount > bid_amount else "bid_more"
                            initial_orderbook_side = current_side
                        stable_start = None
                    elif stable_start is None:
                        stable_start = datetime.now()
                    else:
                        stable_duration = (
                            datetime.now() - stable_start).total_seconds()
                        if stable_duration >= duration:
                            # 🔥 买卖单数量比例检查
                            if self.config.orderbook_quantity_ratio > 0:
                                max_amount = max(bid_amount, ask_amount)
                                min_amount = min(bid_amount, ask_amount)

                                if min_amount > 0:
                                    ratio = float(
                                        max_amount / min_amount) * 100
                                    final_ratio = ratio

                                    if ratio < self.config.orderbook_quantity_ratio:
                                        self.logger.info(
                                            f"⚠️ Backpack买卖单比例不足，重新计时 - "
                                            f"当前: {ratio:.1f}%, 要求: {self.config.orderbook_quantity_ratio:.1f}%")
                                        stable_start = None
                                        continue

                            # 🔥 最小数量检查（市价模式）
                            if self.config.orderbook_min_quantity > 0:
                                larger_amount = max(bid_amount, ask_amount)
                                if larger_amount < Decimal(str(self.config.orderbook_min_quantity)):
                                    self.logger.info(
                                        f"⏳ Backpack订单簿数量不足，继续等待 - "
                                        f"当前: {larger_amount}, 要求: {self.config.orderbook_min_quantity}")
                                    if not await self._interruptible_sleep(interval):
                                        self.logger.info("⏸️ 价格稳定检查被中断")
                                        return None
                                    continue

                            # 所有条件满足，返回价格和数量
                            return (current_bid, current_ask, bid_amount, ask_amount, final_ratio)

                last_bid = current_bid
                last_ask = current_ask

                if not await self._interruptible_sleep(interval):
                    self.logger.info("⏸️ 价格稳定检查被中断")
                    return None

            except Exception as e:
                self.logger.error(f"检查Backpack价格稳定失败: {e}")
                if not await self._interruptible_sleep(interval):
                    self.logger.info("⏸️ 价格稳定检查被中断")
                    return None

        self.logger.warning("⚠️ 等待Backpack价格稳定超时")
        return None

    def _decide_direction(self) -> str:
        """
        决定交易方向

        策略：
        - 如果配置了direction_strategy="alternate"，交替买卖
        - 否则使用伪随机选择（基于时间戳）
        - 如果启用reverse_trading，最终方向会反转

        Returns:
            "buy" 或 "sell"
        """
        if hasattr(self.config, 'direction_strategy') and self.config.direction_strategy == "alternate":
            # 交替模式
            if self._last_direction is None or self._last_direction == "sell":
                direction = "buy"
            else:
                direction = "sell"
        else:
            # 伪随机模式（基于时间戳纳秒的奇偶性）
            import time
            direction = "buy" if int(
                time.time() * 1000000) % 2 == 0 else "sell"

        # 🔥 反向交易模式：如果启用，反转方向
        if self.config.reverse_trading:
            original_direction = direction
            direction = "sell" if direction == "buy" else "buy"
            self.logger.debug(
                f"🔄 反向交易模式: {original_direction} → {direction}")

        self._last_direction = direction
        return direction

    async def _wait_for_backpack_price_change_or_reversal(
            self,
            initial_bid: Decimal,
            initial_ask: Decimal,
            initial_bid_amount: Decimal,
            initial_ask_amount: Decimal) -> Optional[Tuple[float, str]]:
        """
        等待Backpack价格变化或买卖单数量反转（复用原脚本逻辑）

        监控条件：
        1. 价格变化达到要求次数
        2. 买卖单数量反转（可选）

        Args:
            initial_bid: 开仓时的买1价格
            initial_ask: 开仓时的卖1价格
            initial_bid_amount: 开仓时的买1数量
            initial_ask_amount: 开仓时的卖1数量

        Returns:
            (耗时秒数, 触发原因) 或 None（超时）
        """
        timeout = self.config.market_wait_timeout
        check_interval = self.config.check_interval
        required_count = self.config.market_price_change_count
        check_reversal = self.config.market_close_on_quantity_reversal

        # 记录初始数量关系
        initial_side = "bid_more" if initial_bid_amount > initial_ask_amount else "ask_more"

        self.logger.info(
            f"📊 监控Backpack订单簿 - "
            f"初始买1: {initial_bid}, 初始卖1: {initial_ask}, "
            f"初始数量关系: {'买单多' if initial_side == 'bid_more' else '卖单多'}, "
            f"超时: {timeout}秒, 价格变化要求: {required_count}次")

        # 价格变化次数统计
        price_change_count = 0
        last_bid = initial_bid
        last_ask = initial_ask
        start_time = datetime.now()

        try:
            while (datetime.now() - start_time).total_seconds() < timeout:
                # 🔥 从Backpack获取订单簿（使用signal_symbol）
                signal_symbol = self.config.signal_symbol or self.config.symbol
                orderbook = await self.signal_adapter.get_orderbook(signal_symbol)

                # 更新最新订单簿（用于UI显示）
                self._latest_orderbook = orderbook

                if not orderbook.bids or not orderbook.asks:
                    if not await self._interruptible_sleep(check_interval):
                        self.logger.info("⏸️ 价格变化监控被中断")
                        return None
                    continue

                current_bid = orderbook.bids[0].price
                current_ask = orderbook.asks[0].price
                current_bid_amount = orderbook.bids[0].size
                current_ask_amount = orderbook.asks[0].size

                # 🔥 检查买卖单数量反转（如果启用）
                if check_reversal:
                    current_side = "bid_more" if current_bid_amount > current_ask_amount else "ask_more"

                    if current_side != initial_side:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        self.logger.info(
                            f"✅ Backpack买卖单数量反转 - "
                            f"初始: {'买单多' if initial_side == 'bid_more' else '卖单多'}, "
                            f"当前: {'买单多' if current_side == 'bid_more' else '卖单多'}, "
                            f"买1数量: {initial_bid_amount} → {current_bid_amount}, "
                            f"卖1数量: {initial_ask_amount} → {current_ask_amount}, "
                            f"耗时: {elapsed:.2f}秒")
                        return (elapsed, "quantity_reversal")

                # 🔥 检查价格是否相对于上一次变化
                if current_bid != last_bid or current_ask != last_ask:
                    price_change_count += 1
                    self.logger.info(
                        f"📈 Backpack价格变化 #{price_change_count}/{required_count} - "
                        f"买1: {last_bid} → {current_bid}, "
                        f"卖1: {last_ask} → {current_ask}")

                    # 更新上一次的价格
                    last_bid = current_bid
                    last_ask = current_ask

                    # 🔥 达到要求的变化次数，触发平仓
                    if price_change_count >= required_count:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        self.logger.info(
                            f"✅ Backpack价格变化达到要求 - "
                            f"变化{price_change_count}次 >= 要求{required_count}次, "
                            f"耗时: {elapsed:.2f}秒")
                        return (elapsed, "price_change")

                # 🔥 可中断的睡眠
                if not await self._interruptible_sleep(check_interval):
                    self.logger.info("⏸️ 价格变化监控被中断")
                    return None

        except Exception as e:
            self.logger.error(f"❌ 监控Backpack价格变化失败: {e}", exc_info=True)
            return None

        # 超时
        elapsed = (datetime.now() - start_time).total_seconds()
        self.logger.warning(
            f"⚠️ 等待Backpack价格变化超时 - "
            f"耗时: {elapsed:.2f}秒, 价格变化次数: {price_change_count}/{required_count}")
        return None

    async def _execute_lighter_market_open(self, direction: str) -> Optional[OrderData]:
        """
        在Lighter执行市价开仓

        Args:
            direction: "buy" 或 "sell"

        Returns:
            OrderData 或 None
        """
        try:
            side = OrderSide.BUY if direction == "buy" else OrderSide.SELL

            # 🔥 准备成交追踪（在下单前设置状态机）
            self._prepare_fill_tracking(
                side=direction,  # "buy" or "sell"
                amount=self.config.order_size,
                state="WAITING_OPEN"
            )

            # 🔥 使用execution_symbol
            execution_symbol = self.config.execution_symbol or self.config.symbol
            order = await self.execution_adapter.place_market_order(
                symbol=execution_symbol,
                side=side,
                quantity=self.config.order_size,
                reduce_only=False,  # 🔥 开仓模式：允许建仓和加仓（与网格交易程序一致）
                skip_order_index_query=True  # 🔥 跳过 order_index 查询（使用状态机匹配）
            )

            # 🔥 市价单特性：立即提交但返回时状态是PENDING
            # 只要有order且有id(tx_hash)，就认为成功
            if order and order.id:
                self.logger.info(f"✅ Lighter市价开仓提交成功: tx={order.id[:16]}...")

                # 🔥 等待WebSocket推送订单成交信息（获取真实成交价）
                # 超时时间由配置文件指定（默认15秒）：Lighter是链上交易所，确认时间较长
                fill_result = await self._wait_for_order_fill(
                    side=direction,
                    amount=self.config.order_size,
                    timeout=self.config.websocket_fill_timeout
                )
                if fill_result:
                    # 使用WebSocket获取的真实成交价
                    order.average = fill_result["average_price"]
                    order.filled = fill_result["filled_amount"]
                    self.logger.info(
                        f"✅ 从WebSocket获取开仓价: {order.average}, 成交量: {order.filled}")
                else:
                    self.logger.warning(
                        "⚠️ 未收到WebSocket成交通知，使用Backpack市场价作为估算")
                    # Fallback：使用Backpack市场价作为估算
                    signal_symbol = self.config.signal_symbol or self.config.symbol
                    orderbook = await self.signal_adapter.get_orderbook(signal_symbol)
                    if orderbook and orderbook.bids and orderbook.asks:
                        if side == OrderSide.BUY:
                            order.average = orderbook.asks[0].price
                        else:
                            order.average = orderbook.bids[0].price
                        order.filled = self.config.order_size
                        self.logger.info(f"   使用估算开仓价: {order.average}")
                    else:
                        self.logger.error("   无法获取市场价，开仓价设为0")
                        order.average = Decimal("0")
                        order.filled = self.config.order_size

                return order
            else:
                self.logger.error(f"❌ Lighter市价单提交失败: {order}")
                return None

        except Exception as e:
            self.logger.error(f"❌ Lighter市价开仓失败: {e}", exc_info=True)
            return None

    async def _execute_lighter_market_close(self, direction: str) -> Optional[Tuple[Decimal, Decimal]]:
        """
        在Lighter市价平仓

        Args:
            direction: 开仓方向（"buy" 或 "sell"）

        Returns:
            (平仓价格, 平仓数量) 或 None
        """
        try:
            # 平仓方向与开仓相反
            close_side = OrderSide.SELL if direction == "buy" else OrderSide.BUY
            close_direction = "sell" if direction == "buy" else "buy"

            # 🔥 准备成交追踪（在下单前设置状态机）
            self._prepare_fill_tracking(
                side=close_direction,  # 平仓方向（与开仓相反）
                amount=self.config.order_size,
                state="WAITING_CLOSE"
            )

            # 🔥 使用execution_symbol
            execution_symbol = self.config.execution_symbol or self.config.symbol
            order = await self.execution_adapter.place_market_order(
                symbol=execution_symbol,
                side=close_side,
                quantity=self.config.order_size,
                reduce_only=True,  # 🔥 只减仓模式：不会开新仓或加仓
                skip_order_index_query=True  # 🔥 跳过 order_index 查询（使用状态机匹配）
            )

            # 🔥 市价单特性：立即提交但返回时状态是PENDING
            # 只要有order且有id(tx_hash)，就认为成功
            if order and order.id:
                self.logger.info(f"✅ Lighter市价平仓提交成功: tx={order.id[:16]}...")

                # 🔥 等待WebSocket推送订单成交信息（获取真实成交价）
                # 超时时间由配置文件指定（默认15秒）：Lighter是链上交易所，确认时间较长
                fill_result = await self._wait_for_order_fill(
                    side=close_direction,
                    amount=self.config.order_size,
                    timeout=self.config.websocket_fill_timeout
                )
                if fill_result:
                    # 使用WebSocket获取的真实成交价
                    close_price = fill_result["average_price"]
                    close_amount = fill_result["filled_amount"]
                    self.logger.info(
                        f"✅ 从WebSocket获取平仓价: {close_price}, 成交量: {close_amount}")
                else:
                    self.logger.warning(
                        "⚠️ 未收到WebSocket成交通知，使用Backpack市场价作为估算")
                    # Fallback：使用Backpack市场价作为估算
                    signal_symbol = self.config.signal_symbol or self.config.symbol
                    try:
                        orderbook = await self.signal_adapter.get_orderbook(signal_symbol)
                        if orderbook and orderbook.bids and orderbook.asks:
                            # 根据平仓方向选择合适的价格
                            if close_side == OrderSide.BUY:
                                close_price = orderbook.asks[0].price
                            else:
                                close_price = orderbook.bids[0].price
                            close_amount = self.config.order_size
                            self.logger.info(f"   使用估算平仓价: {close_price}")
                        else:
                            close_price = Decimal("0")
                            close_amount = self.config.order_size
                            self.logger.warning("   无法获取市场价，平仓价设为0")
                    except Exception as e:
                        close_price = Decimal("0")
                        close_amount = self.config.order_size
                        self.logger.warning(f"   获取市场价失败: {e}，平仓价设为0")

                return (close_price, close_amount)
            else:
                self.logger.error(f"❌ Lighter平仓单提交失败: {order}")
                return None

        except Exception as e:
            self.logger.error(f"❌ Lighter市价平仓失败: {e}", exc_info=True)
            return None

    async def _verify_lighter_position_cleared(self, max_retries: int = 5, auto_close: bool = True) -> bool:
        """
        验证Lighter持仓已清空，发现持仓时自动平仓

        由于链上确认需要时间，会重试多次
        如果发现持仓残留，会自动尝试平仓

        Args:
            max_retries: 最大重试次数
            auto_close: 是否自动平仓残留持仓

        Returns:
            True: 持仓已清空
            False: 仍有持仓（需要人工介入）
        """
        close_attempts = 0  # 记录平仓尝试次数
        max_close_attempts = 5  # 🔥 最多尝试5次自动平仓，超过则暂停等待人工干预

        for retry in range(max_retries):
            try:
                # 🔥 检查停止信号，快速退出
                if self._should_stop:
                    self.logger.warning("⚠️ 检测到停止信号，跳过持仓验证")
                    return True

                # 🔥 等待链上确认（可中断），使用指数退避策略
                # 公式: min(30 * 2^(retry-1), 120)
                # 第1次检查: 不等待（retry=0）
                # 第2次检查: min(30 * 2^0, 120) = 30秒
                # 第3次检查: min(30 * 2^1, 120) = 60秒
                # 第4次检查: min(30 * 2^2, 120) = 120秒（达到上限）
                # 第5次及以后: 120秒
                if retry > 0:
                    base_delay = 30
                    max_delay = 120
                    retry_interval = min(
                        base_delay * (2 ** (retry - 1)), max_delay)
                    self.logger.info(
                        f"⏰ 等待 {retry_interval} 秒后第{retry+1}次检查持仓（指数退避）..."
                    )
                    if not await self._interruptible_sleep(retry_interval):
                        self.logger.info("⏸️ 持仓验证被中断")
                        return True  # 假设持仓已清空，允许退出

                # 查询持仓（添加超时保护）
                positions = await asyncio.wait_for(
                    self.execution_adapter.get_positions(),
                    timeout=5.0  # 5秒超时
                )

                # 检查是否有持仓
                has_position = False
                remaining_position = None
                for pos in positions:
                    if abs(pos.size) > 0:
                        has_position = True
                        remaining_position = pos
                        self.logger.warning(
                            f"⚠️ 第{retry+1}次检查: Lighter仍有持仓 {pos.symbol}: {pos.size}")
                        break

                if not has_position:
                    if retry > 0:
                        self.logger.info(f"✅ 第{retry+1}次检查: Lighter持仓已清空")
                    return True

                # 🔥 发现残留持仓，尝试自动平仓
                if auto_close and remaining_position and close_attempts < max_close_attempts:
                    close_attempts += 1
                    self.logger.warning(
                        f"🔄 第{close_attempts}次尝试自动平仓残留持仓: "
                        f"{remaining_position.symbol} {remaining_position.size}")

                    try:
                        # 🔥 重新查询持仓，获取最新的方向和数量
                        self.logger.info("🔍 重新查询持仓，确认最新方向和数量...")
                        fresh_positions = await asyncio.wait_for(
                            self.execution_adapter.get_positions(),
                            timeout=5.0
                        )

                        # 找到当前symbol的持仓
                        current_position = None
                        for pos in fresh_positions:
                            if pos.symbol == remaining_position.symbol and abs(pos.size) > 0:
                                current_position = pos
                                break

                        if not current_position:
                            self.logger.info("✅ 重新查询后，持仓已清空")
                            return True

                        # 🔥 确定平仓方向（与持仓方向相反）
                        # 必须使用最新查询的 side 字段
                        close_side = OrderSide.SELL if current_position.side == PositionSide.LONG else OrderSide.BUY
                        close_quantity = abs(current_position.size)

                        # 记录持仓方向和平仓方向
                        position_side_str = "多头" if current_position.side == PositionSide.LONG else "空头"
                        close_side_str = "卖出" if close_side == OrderSide.SELL else "买入"
                        close_direction = "sell" if close_side == OrderSide.SELL else "buy"
                        self.logger.info(
                            f"📊 最新持仓方向: {position_side_str}, 数量: {close_quantity}, 平仓方向: {close_side_str}")

                        # 🔥 准备成交追踪（在下单前设置状态机）
                        self._prepare_fill_tracking(
                            side=close_direction,
                            amount=Decimal(str(close_quantity)),
                            state="WAITING_CLOSE"
                        )

                        # 执行平仓
                        execution_symbol = self.config.execution_symbol or self.config.symbol

                        # 🔥 详细记录下单前的参数
                        self.logger.info(
                            f"📝 准备下单 - symbol: {execution_symbol}, "
                            f"side: {close_side}, "
                            f"quantity: {close_quantity}, "
                            f"quantity类型: {type(close_quantity)}")

                        order = await self.execution_adapter.place_market_order(
                            symbol=execution_symbol,
                            side=close_side,
                            quantity=Decimal(str(close_quantity)),
                            reduce_only=True,  # 🔥 只减仓模式：避免越平越多
                            skip_order_index_query=True  # 🔥 跳过 order_index 查询
                        )

                        if order and order.id:
                            self.logger.info(
                                f"✅ 自动平仓订单已提交: tx={order.id[:16]}..., "
                                f"方向={close_side}, "
                                f"请求数量={close_quantity}, "
                                f"成交数量={order.filled if order.filled else 'N/A'}")

                            # 🔥 等待 WebSocket 成交通知（获取真实成交价）
                            fill_result = await self._wait_for_order_fill(
                                side=close_direction,
                                amount=Decimal(str(close_quantity)),
                                timeout=10.0  # 自动平仓使用较短超时
                            )
                            if fill_result:
                                self.logger.info(
                                    f"✅ 自动平仓成交确认 - "
                                    f"平均价格: {fill_result['average_price']:.2f}, "
                                    f"成交数量: {fill_result['filled_amount']}")
                            else:
                                self.logger.warning(
                                    "⚠️ 未收到自动平仓成交通知（将通过持仓验证确认）")

                            # 🔥 指数退避延迟：避免API限流
                            # 公式: min(base * 2^(attempt - 1), max_delay)
                            # 第1次: min(30 * 2^0, 120) = 30秒
                            # 第2次: min(30 * 2^1, 120) = 60秒
                            # 第3次: min(30 * 2^2, 120) = 120秒（达到上限）
                            # 第4次及以后: 120秒
                            base_delay = 30
                            max_delay = 120
                            wait_time = min(
                                base_delay * (2 ** (close_attempts - 1)), max_delay)
                            self.logger.info(
                                f"⏰ 等待 {wait_time} 秒（指数退避，第{close_attempts}次尝试），避免API限流..."
                            )
                            await asyncio.sleep(wait_time)
                        else:
                            self.logger.error("❌ 自动平仓订单提交失败")
                            # 即使失败也要等待，使用基础延迟
                            base_delay = 30
                            self.logger.info(f"⏰ 等待 {base_delay} 秒后重试...")
                            await asyncio.sleep(base_delay)

                    except Exception as e:
                        self.logger.error(f"❌ 自动平仓失败: {e}")
                        # 即使异常也要等待，使用基础延迟避免限流
                        base_delay = 30
                        self.logger.info(f"⏰ 等待 {base_delay} 秒后重试...")
                        await asyncio.sleep(base_delay)

                    # 🔥 在重新检查前等待，避免频繁查询触发API限流
                    # 使用指数退避策略：min(30 * 2^(retry), 120)
                    # 第1轮: min(30 * 2^0, 120) = 30秒
                    # 第2轮: min(30 * 2^1, 120) = 60秒
                    # 第3轮及以后: min(30 * 2^2+, 120) = 120秒
                    if retry < max_retries - 1:
                        base_delay = 30
                        max_delay = 120
                        wait_time = min(base_delay * (2 ** retry), max_delay)
                        self.logger.info(
                            f"⏰ 等待 {wait_time} 秒后重新检查持仓（指数退避，第{retry+1}次检查）..."
                        )
                        await asyncio.sleep(wait_time)

                    # 重新开始检查（不增加retry计数）
                    continue

                # 🔥 如果是最后一次重试，或者已经尝试过5次平仓
                if retry == max_retries - 1 or close_attempts >= max_close_attempts:
                    self.logger.error("")
                    self.logger.error("=" * 70)
                    self.logger.error("❌ 检测到残留持仓，已尝试5次自动平仓仍失败")
                    self.logger.error(
                        f"   持仓: {remaining_position.symbol if remaining_position else 'Unknown'}")
                    self.logger.error(
                        f"   数量: {remaining_position.size if remaining_position else 'Unknown'}")
                    self.logger.error(f"   检查次数: {retry+1}/{max_retries}")
                    self.logger.error(
                        f"   平仓尝试次数: {close_attempts}/{max_close_attempts}")
                    self.logger.error("")
                    self.logger.error("⚠️  系统已暂停交易，等待人工介入")
                    self.logger.error("   请手动平仓后重启系统")
                    self.logger.error("   或检查Lighter API是否正常")
                    self.logger.error("=" * 70)
                    self.logger.error("")

                    # 🔥 暂停系统，不再进入下一轮
                    self._running = False
                    self._should_stop = True
                    return False

            except asyncio.TimeoutError:
                self.logger.warning(f"⏰ 第{retry+1}次检查持仓超时")
                if retry == max_retries - 1:
                    self.logger.error("❌ 多次检查超时，系统将暂停")
                    self._running = False
                    self._should_stop = True
                    return False
            except Exception as e:
                self.logger.error(f"❌ 查询Lighter持仓失败: {e}")
                if retry == max_retries - 1:
                    return False

        return False
