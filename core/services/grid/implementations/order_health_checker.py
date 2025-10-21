"""
订单健康检查器

职责：
1. 检测并清理重复订单
2. 检测并清理超出范围的订单
3. 评估网格覆盖情况
4. 根据安全规则决定是否补单
"""

import asyncio
from typing import List, Tuple, Set, Dict, Optional
from decimal import Decimal
from datetime import datetime
from collections import defaultdict

from ....logging import get_logger
from ....adapters.exchanges import OrderSide as ExchangeOrderSide, PositionSide, OrderType
from ....adapters.exchanges.models import PositionData
from ..models import GridConfig, GridOrder, GridOrderSide, GridOrderStatus, GridType


class OrderHealthChecker:
    """订单健康检查器"""

    def __init__(self, config: GridConfig, engine):
        """
        初始化健康检查器

        Args:
            config: 网格配置
            engine: 网格引擎实例（用于访问交易所和下单功能）
        """
        self.config = config
        self.engine = engine
        self.logger = get_logger(__name__)

        # 🔥 配置健康检查日志：只输出到文件，不显示在终端UI
        import logging
        from logging.handlers import RotatingFileHandler

        # 设置 Logger 级别为 DEBUG，以便记录所有健康检查日志
        self.logger.logger.setLevel(logging.DEBUG)

        # 移除所有处理器，只保留文件处理器
        self.logger.logger.handlers.clear()

        # 重新添加文件处理器（只写入文件，不显示在终端）
        log_file = f"logs/{__name__}.log"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=50 * 1024 * 1024,  # 50MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        self.logger.logger.addHandler(file_handler)

        self.logger.debug(
            f"订单健康检查器初始化: 网格数={config.grid_count}, "
            f"反手距离={config.reverse_order_grid_distance}格"
        )

    async def _fetch_orders_and_positions(self) -> Tuple[List, List[PositionData]]:
        """
        获取订单和持仓数据（纯REST API）

        🔥 重大修改：持仓数据也使用REST API，不再依赖WebSocket缓存
        原因：Backpack WebSocket持仓流不推送订单成交导致的变化

        修改内容：
        - 订单：从交易所REST API获取（实时准确）✅
        - 持仓：从交易所REST API获取（不再使用WebSocket缓存）✅

        Returns:
            (订单列表, 持仓列表)
        """
        try:
            # 获取订单（使用REST API）
            orders = await self.engine.exchange.get_open_orders(self.config.symbol)

            # 🆕 获取持仓（使用REST API）
            try:
                positions = await self.engine.exchange.get_positions([self.config.symbol])

                if positions:
                    self.logger.debug(
                        f"📊 健康检查使用REST API持仓数据: "
                        f"方向={positions[0].side.value}, 数量={positions[0].size}, "
                        f"成本={positions[0].entry_price}"
                    )
                else:
                    self.logger.debug("📊 健康检查: REST API显示无持仓")

            except Exception as rest_error:
                self.logger.error(f"❌ REST API获取持仓失败: {rest_error}")
                import traceback
                self.logger.error(traceback.format_exc())
                positions = []

            return orders, positions

        except Exception as e:
            self.logger.error(f"获取订单和持仓失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return [], []

    def _calculate_expected_position(self, total_grids: int, current_buy_orders: int, current_sell_orders: int) -> Decimal:
        """
        根据订单状态计算预期持仓数量

        逻辑：
        - 普通网格：预期持仓 = 成交数量 × 单格金额
        - 马丁网格：逐个格式化累加（模拟交易所的精度处理）

        🔥 关键改进：马丁网格不再使用等差数列公式
        原因：交易所会对每个订单金额进行精度格式化（四舍五入），
        破坏了等差性质，必须逐个格式化后累加才能得到准确的预期持仓。

        Args:
            total_grids: 总网格数量
            current_buy_orders: 当前买单数量
            current_sell_orders: 当前卖单数量

        Returns:
            预期持仓数量（正数=多头，负数=空头）
        """
        from decimal import ROUND_HALF_UP

        # 计算已成交的订单数量
        if self.config.grid_type in [GridType.LONG, GridType.FOLLOW_LONG, GridType.MARTINGALE_LONG]:
            # 做多网格：原本应该有total_grids个买单，现在有current_buy_orders个
            # 说明成交了 (total_grids - current_buy_orders) 个买单
            filled_buy_count = total_grids - current_buy_orders

            # 判断是否为马丁网格
            if self.config.martingale_increment and self.config.martingale_increment > 0:
                # 马丁网格：逐个格式化后累加（模拟交易所处理）
                # 假设从高价往低价连续成交（Grid N → Grid N-M+1）
                expected_position = Decimal('0')
                precision_quantizer = Decimal(
                    '0.1') ** self.config.quantity_precision

                # 计算哪些Grid已成交（从高价格ID开始）
                start_grid_id = self.config.grid_count - filled_buy_count + 1

                for grid_id in range(start_grid_id, self.config.grid_count + 1):
                    # 获取该网格的理论金额
                    raw_amount = self.config.get_grid_order_amount(grid_id)
                    # 🔥 关键：应用交易所的精度格式化（四舍五入）
                    formatted_amount = raw_amount.quantize(
                        precision_quantizer, rounding=ROUND_HALF_UP)
                    expected_position += formatted_amount

                self.logger.debug(
                    f"📊 马丁做多网格预期持仓计算: "
                    f"成交{filled_buy_count}个 (Grid {start_grid_id}-{self.config.grid_count}), "
                    f"精度={self.config.quantity_precision}位小数, "
                    f"总计={expected_position}"
                )
            else:
                # 普通网格：固定金额
                expected_position = Decimal(
                    str(filled_buy_count)) * self.config.order_amount

        elif self.config.grid_type in [GridType.SHORT, GridType.FOLLOW_SHORT, GridType.MARTINGALE_SHORT]:
            # 做空网格：原本应该有total_grids个卖单，现在有current_sell_orders个
            # 说明成交了 (total_grids - current_sell_orders) 个卖单
            filled_sell_count = total_grids - current_sell_orders

            # 判断是否为马丁网格
            if self.config.martingale_increment and self.config.martingale_increment > 0:
                # 马丁网格：逐个格式化后累加（模拟交易所处理）
                # 假设从低价往高价连续成交（Grid 1 → Grid M）
                expected_position = Decimal('0')
                precision_quantizer = Decimal(
                    '0.1') ** self.config.quantity_precision

                for grid_id in range(1, filled_sell_count + 1):
                    # 获取该网格的理论金额
                    raw_amount = self.config.get_grid_order_amount(grid_id)
                    # 🔥 关键：应用交易所的精度格式化（四舍五入）
                    formatted_amount = raw_amount.quantize(
                        precision_quantizer, rounding=ROUND_HALF_UP)
                    expected_position += formatted_amount

                # 做空网格持仓为负数
                expected_position = -expected_position

                self.logger.debug(
                    f"📊 马丁做空网格预期持仓计算: "
                    f"成交{filled_sell_count}个 (Grid 1-{filled_sell_count}), "
                    f"精度={self.config.quantity_precision}位小数, "
                    f"总计={expected_position}"
                )
            else:
                # 普通网格：固定金额（负数）
                expected_position = - \
                    Decimal(str(filled_sell_count)) * self.config.order_amount

        else:
            self.logger.warning(f"未知的网格类型: {self.config.grid_type}")
            expected_position = Decimal('0')

        return expected_position

    def _check_position_health(
        self,
        expected_position: Decimal,
        actual_positions: List[PositionData]
    ) -> Dict[str, any]:
        """
        检查持仓健康状态

        Args:
            expected_position: 预期持仓数量（正数=多头，负数=空头）
            actual_positions: 实际持仓列表

        Returns:
            健康检查结果字典
        """
        result = {
            'is_healthy': True,
            'issues': [],
            'expected_position': expected_position,
            'actual_position': Decimal('0'),
            'position_side': None,
            'expected_side': None,
            'needs_adjustment': False,
            'adjustment_amount': Decimal('0'),
            # 'open_long', 'open_short', 'close_long', 'close_short', 'reverse'
            'adjustment_action': None
        }

        # 确定预期方向
        if expected_position > 0:
            result['expected_side'] = PositionSide.LONG
        elif expected_position < 0:
            result['expected_side'] = PositionSide.SHORT
        else:
            result['expected_side'] = None  # 无持仓

        # 查找当前交易对的持仓
        position = None
        for pos in actual_positions:
            if pos.symbol == self.config.symbol:
                position = pos
                break

        # 获取实际持仓
        if position:
            # 检查持仓数量是否有效
            if position.size is None or position.size == 0:
                # 🔥 持仓数量为None或0时，都视为无持仓
                # 防止"幽灵持仓"（size=0但有side的情况）
                if position.size == 0:
                    self.logger.warning(
                        f"⚠️ 检测到0持仓但有方向({position.side})，视为无持仓")
                else:
                    self.logger.warning(f"⚠️ 持仓数量为None，视为无持仓")
                result['actual_position'] = Decimal('0')
                result['position_side'] = None
            else:
                # 根据方向确定持仓数量的正负号
                if position.side == PositionSide.LONG:
                    result['actual_position'] = position.size
                    result['position_side'] = PositionSide.LONG
                elif position.side == PositionSide.SHORT:
                    result['actual_position'] = -position.size  # 空头用负数表示
                    result['position_side'] = PositionSide.SHORT
        else:
            result['actual_position'] = Decimal('0')
            result['position_side'] = None

        # 检查持仓方向
        if result['expected_side'] != result['position_side']:
            result['is_healthy'] = False
            result['needs_adjustment'] = True

            if result['expected_side'] is None:
                # 预期无持仓，但实际有持仓
                result['issues'].append('存在多余持仓需要平仓')
                if result['position_side'] == PositionSide.LONG:
                    result['adjustment_action'] = 'close_long'
                    result['adjustment_amount'] = result['actual_position']
                else:
                    result['adjustment_action'] = 'close_short'
                    result['adjustment_amount'] = abs(
                        result['actual_position'])

            elif result['position_side'] is None:
                # 预期有持仓，但实际无持仓
                result['issues'].append('缺少持仓需要开仓')
                if result['expected_side'] == PositionSide.LONG:
                    result['adjustment_action'] = 'open_long'
                    result['adjustment_amount'] = expected_position
                else:
                    result['adjustment_action'] = 'open_short'
                    result['adjustment_amount'] = abs(expected_position)

            else:
                # 持仓方向相反
                result['issues'].append('持仓方向错误需要反向')
                result['adjustment_action'] = 'reverse'
                if result['expected_side'] == PositionSide.LONG:
                    # 当前是空头，需要先平空，再开多
                    result['adjustment_amount'] = expected_position
                else:
                    # 当前是多头，需要先平多，再开空
                    result['adjustment_amount'] = abs(expected_position)

        # 检查持仓数量（只有方向正确时才检查数量）
        elif result['expected_side'] is not None:
            # 允许的误差范围（单格数量的0.01）
            tolerance = self.config.order_amount * Decimal('0.01')
            position_diff = abs(result['actual_position'] - expected_position)

            if position_diff > tolerance:
                result['is_healthy'] = False
                result['needs_adjustment'] = True
                result['issues'].append(f'持仓数量不匹配（差异: {position_diff}）')

                if result['actual_position'] > expected_position:
                    # 持仓过多，需要平仓
                    result['adjustment_amount'] = position_diff
                    if result['expected_side'] == PositionSide.LONG:
                        result['adjustment_action'] = 'close_long'
                    else:
                        result['adjustment_action'] = 'close_short'
                else:
                    # 持仓不足，需要开仓
                    result['adjustment_amount'] = position_diff
                    if result['expected_side'] == PositionSide.LONG:
                        result['adjustment_action'] = 'open_long'
                    else:
                        result['adjustment_action'] = 'open_short'

        return result

    async def _adjust_position(self, adjustment_info: Dict) -> bool:
        """
        调整持仓到预期状态

        Args:
            adjustment_info: 调整信息字典

        Returns:
            是否调整成功
        """
        try:
            action = adjustment_info['adjustment_action']
            amount = adjustment_info['adjustment_amount']

            if action is None or amount == 0:
                self.logger.debug("无需调整持仓")
                return True

            self.logger.debug(f"开始持仓调整: 动作={action}, 数量={amount}")

            # 获取当前价格（用于市价单）
            current_price = await self.engine.get_current_price()

            if action == 'reverse':
                # 反向持仓：先平仓，再开仓
                self.logger.warning("⚠️ 检测到持仓方向错误，需要反向调整")

                # 🔥 检查实际持仓是否为0（幽灵持仓）
                actual_position_abs = abs(adjustment_info['actual_position'])
                if actual_position_abs == 0:
                    self.logger.warning(
                        "⚠️ 实际持仓为0（可能是幽灵持仓），跳过平仓步骤，直接建仓"
                    )
                else:
                    # 第一步：平掉反向仓位（只有真实持仓才需要平仓）
                    if adjustment_info['position_side'] == PositionSide.LONG:
                        self.logger.debug(
                            f"第1步：平多仓 {adjustment_info['actual_position']}")
                        await self._close_position(
                            PositionSide.LONG,
                            actual_position_abs,
                            current_price
                        )
                    else:
                        self.logger.debug(
                            f"第1步：平空仓 {actual_position_abs}")
                        await self._close_position(
                            PositionSide.SHORT,
                            actual_position_abs,
                            current_price
                        )

                    # 等待平仓生效
                    await asyncio.sleep(2)

                # 第二步：开正确方向的仓位
                if adjustment_info['expected_side'] == PositionSide.LONG:
                    self.logger.debug(f"第2步：开多仓 {amount}")
                    await self._open_position(PositionSide.LONG, amount, current_price)
                else:
                    self.logger.debug(f"第2步：开空仓 {amount}")
                    await self._open_position(PositionSide.SHORT, amount, current_price)

            elif action == 'close_long':
                # 平多仓
                self.logger.debug(f"平多仓: {amount}")
                await self._close_position(PositionSide.LONG, amount, current_price)

            elif action == 'close_short':
                # 平空仓
                self.logger.debug(f"平空仓: {amount}")
                await self._close_position(PositionSide.SHORT, amount, current_price)

            elif action == 'open_long':
                # 开多仓
                self.logger.debug(f"开多仓: {amount}")
                await self._open_position(PositionSide.LONG, amount, current_price)

            elif action == 'open_short':
                # 开空仓
                self.logger.debug(f"开空仓: {amount}")
                await self._open_position(PositionSide.SHORT, amount, current_price)

            self.logger.debug("✅ 持仓调整完成")
            return True

        except Exception as e:
            self.logger.error(f"❌ 持仓调整失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False

    async def _close_position(self, side: PositionSide, amount: Decimal, current_price: Decimal):
        """
        平仓（使用市价单）

        Args:
            side: 持仓方向（要平的仓位）
            amount: 平仓数量
            current_price: 当前价格（仅用于日志记录）
        """
        try:
            # 平多仓 = 卖出，平空仓 = 买入
            if side == PositionSide.LONG:
                order_side = ExchangeOrderSide.SELL
            else:
                order_side = ExchangeOrderSide.BUY

            # 使用市价单平仓，确保成交
            self.logger.debug(
                f"使用市价单平仓: {order_side.value} {amount} (参考价格: {current_price})")

            # 调用交易所接口平仓（市价单）
            # 注意：Backpack API 不支持 reduceOnly 参数，直接使用市价单即可平仓
            order = await self.engine.exchange.create_order(
                symbol=self.config.symbol,
                side=order_side,
                order_type=OrderType.MARKET,  # 使用市价单
                amount=amount,
                price=None  # 市价单不需要价格
                # 不传递 params，避免 Backpack API 签名错误
            )

            self.logger.debug(
                f"✅ 平仓市价单已提交: {order_side.value} {amount}, OrderID={order.id}")

        except Exception as e:
            self.logger.error(f"❌ 平仓失败: {e}")
            raise

    async def _open_position(self, side: PositionSide, amount: Decimal, current_price: Decimal):
        """
        开仓（使用市价单）

        Args:
            side: 持仓方向（要开的仓位）
            amount: 开仓数量
            current_price: 当前价格（仅用于日志记录）
        """
        try:
            # 开多仓 = 买入，开空仓 = 卖出
            if side == PositionSide.LONG:
                order_side = ExchangeOrderSide.BUY
            else:
                order_side = ExchangeOrderSide.SELL

            # 使用市价单开仓，确保成交
            self.logger.debug(
                f"使用市价单开仓: {order_side.value} {amount} (参考价格: {current_price})")

            # 调用交易所接口开仓（市价单）
            order = await self.engine.exchange.create_order(
                symbol=self.config.symbol,
                side=order_side,
                order_type=OrderType.MARKET,  # 使用市价单
                amount=amount,
                price=None,  # 市价单不需要价格
                params={}
            )

            self.logger.debug(
                f"✅ 开仓市价单已提交: {order_side.value} {amount}, OrderID={order.id}")

        except Exception as e:
            self.logger.error(f"❌ 开仓失败: {e}")
            raise

    async def perform_health_check(self):
        """
        执行订单健康检查（含持仓检查）

        流程：
        1. 并发获取订单和持仓数据
        2. 检查订单数量和持仓状态
        3. 如有异常，进行二次检查（订单+持仓）
        4. 确认问题后执行修复（持仓优先，然后订单）
        5. 安全检查和补单决策
        """
        try:
            self.logger.debug("=" * 80)
            self.logger.debug("🔍 开始执行订单和持仓健康检查")
            self.logger.debug("=" * 80)

            # ==================== 阶段0: 剥头皮模式检查 ====================
            # 🔥 如果剥头皮模式已激活，只进行诊断报告，不做任何修改操作
            is_scalping_active = False
            if hasattr(self.engine, 'coordinator') and self.engine.coordinator:
                coordinator = self.engine.coordinator
                if coordinator.scalping_manager and coordinator.scalping_manager.is_active():
                    is_scalping_active = True
                    self.logger.warning(
                        "🔴 剥头皮模式已激活，健康检查仅执行诊断报告，不执行补单/取消/持仓调整操作")
                    self.logger.debug("💡 原因: 剥头皮模式会自行管理订单和持仓，健康检查不应干预")

            # ==================== 阶段1: 并发获取订单和持仓数据 ====================
            self.logger.debug("📊 阶段1: 并发获取订单和持仓数据（防止竞态）")

            # 第一次并发获取订单和持仓
            orders, positions = await self._fetch_orders_and_positions()
            first_order_count = len(orders)

            self.logger.debug(
                f"📡 第一次获取: 订单={first_order_count}个, 持仓={len(positions)}个")

            if not orders:
                self.logger.warning("⚠️ 未获取到任何挂单，跳过健康检查")
                return

            # 统计订单类型
            buy_count = sum(1 for o in orders if o.side ==
                            ExchangeOrderSide.BUY)
            sell_count = sum(1 for o in orders if o.side ==
                             ExchangeOrderSide.SELL)

            self.logger.debug(
                f"📡 第一次统计: 总订单={len(orders)}个, "
                f"买单={buy_count}个, 卖单={sell_count}个"
            )

            # ==================== 阶段2: 计算预期持仓并检查 ====================
            self.logger.debug("📊 阶段2: 计算预期持仓并检查持仓健康状态")

            # 计算预期持仓
            expected_position = self._calculate_expected_position(
                self.config.grid_count,
                buy_count,
                sell_count
            )

            self.logger.debug(f"📍 预期持仓: {expected_position}")

            # 检查持仓健康状态
            position_health = self._check_position_health(
                expected_position, positions)

            self.logger.debug(
                f"📍 实际持仓: {position_health['actual_position']} "
                f"(方向: {position_health['position_side']})"
            )

            # 判断是否需要二次检查
            order_count_abnormal = first_order_count != self.config.grid_count
            position_abnormal = not position_health['is_healthy']

            needs_recheck = order_count_abnormal or position_abnormal

            if needs_recheck:
                self.logger.warning("⚠️ 检测到异常，准备进行二次检查")

                if order_count_abnormal:
                    self.logger.warning(
                        f"  - 订单数量异常: 期望{self.config.grid_count}个，实际{first_order_count}个"
                    )

                if position_abnormal:
                    self.logger.warning(
                        f"  - 持仓异常: {', '.join(position_health['issues'])}")

                # 🔥 二次验证机制：避免竞态条件
                # 因为持仓依赖订单状态，所以必须同时重新获取订单和持仓
                self.logger.debug("⏰ 等待3秒后进行二次验证（同时检查订单和持仓）...")
                await asyncio.sleep(3)

                # 第二次并发获取订单和持仓
                orders, positions = await self._fetch_orders_and_positions()
                second_order_count = len(orders)

                # 重新统计
                buy_count = sum(1 for o in orders if o.side ==
                                ExchangeOrderSide.BUY)
                sell_count = sum(1 for o in orders if o.side ==
                                 ExchangeOrderSide.SELL)

                self.logger.debug(
                    f"📡 第二次获取: 订单={second_order_count}个, "
                    f"买单={buy_count}个, 卖单={sell_count}个, "
                    f"持仓={len(positions)}个"
                )

                # 重新计算预期持仓
                expected_position = self._calculate_expected_position(
                    self.config.grid_count,
                    buy_count,
                    sell_count
                )

                # 重新检查持仓
                position_health = self._check_position_health(
                    expected_position, positions)

                self.logger.debug(f"📍 二次检查 - 预期持仓: {expected_position}")
                self.logger.debug(
                    f"📍 二次检查 - 实际持仓: {position_health['actual_position']} "
                    f"(方向: {position_health['position_side']})"
                )

                # 判断问题是否依然存在
                order_still_abnormal = second_order_count != self.config.grid_count
                position_still_abnormal = not position_health['is_healthy']

                if not order_still_abnormal and not position_still_abnormal:
                    # 第二次检查恢复正常
                    self.logger.debug(
                        f"✅ 二次验证通过: 订单和持仓均已恢复正常，"
                        f"判定为成交过程中的瞬时异常"
                    )
                else:
                    # 第二次检查仍然异常
                    if order_still_abnormal:
                        self.logger.warning(
                            f"⚠️ 二次验证: 订单数仍异常({second_order_count}个)"
                        )
                    if position_still_abnormal:
                        self.logger.warning(
                            f"⚠️ 二次验证: 持仓仍异常 - {', '.join(position_health['issues'])}"
                        )
                    self.logger.debug("继续执行修复流程...")

            # ==================== 阶段2.5: 剥头皮模式持仓偏差检测 ====================
            # 🆕 如果是剥头皮模式，检查持仓偏差是否严重
            if is_scalping_active:
                self.logger.debug("🔍 阶段2.5: 剥头皮模式持仓偏差检测")

                # 计算偏差
                expected_pos = position_health['expected_position']
                actual_pos = position_health['actual_position']

                if expected_pos != 0:
                    position_diff = abs(actual_pos - expected_pos)
                    deviation_percent = float(
                        position_diff / abs(expected_pos) * 100)

                    self.logger.debug(
                        f"📊 持仓偏差分析:\n"
                        f"   预期持仓: {expected_pos}\n"
                        f"   实际持仓: {actual_pos}\n"
                        f"   偏差: {deviation_percent:.1f}%"
                    )

                    # 判断偏差等级（两级：警告10% + 紧急停止50%）
                    warning_threshold = 10   # 警告阈值：10%
                    emergency_threshold = 50  # 紧急停止阈值：50%

                    if deviation_percent >= emergency_threshold:
                        # 紧急级别：触发紧急停止
                        self.logger.critical(
                            f"🚨 剥头皮模式持仓偏差达到紧急阈值！\n"
                            f"   预期持仓: {expected_pos}\n"
                            f"   实际持仓: {actual_pos}\n"
                            f"   偏差: {deviation_percent:.1f}% (紧急阈值: {emergency_threshold}%)\n"
                            f"   ⚠️ 触发紧急停止，停止所有订单操作！\n"
                            f"   需要人工检查和干预！"
                        )

                        # 触发紧急停止
                        coordinator.is_emergency_stopped = True

                        # 不再继续执行后续的修复流程
                        self.logger.critical("🚨 终止健康检查，等待人工干预")
                        return

                    elif deviation_percent >= warning_threshold:
                        # 警告级别：输出警告
                        self.logger.warning(
                            f"⚠️ 剥头皮模式持仓偏差超过警告阈值\n"
                            f"   预期持仓: {expected_pos}\n"
                            f"   实际持仓: {actual_pos}\n"
                            f"   偏差: {deviation_percent:.1f}% (警告阈值: {warning_threshold}%)\n"
                            f"   请关注持仓变化"
                        )

                    else:
                        # 正常级别
                        self.logger.debug(
                            f"✅ 剥头皮模式持仓检查通过，偏差: {deviation_percent:.1f}%"
                        )

                elif actual_pos != 0:
                    # 预期持仓为0，但实际有持仓
                    self.logger.critical(
                        f"🚨 剥头皮模式异常：预期持仓为0，但实际持仓为{actual_pos}！\n"
                        f"   触发紧急停止，需要人工检查！"
                    )
                    coordinator.is_emergency_stopped = True
                    self.logger.critical("🚨 终止健康检查，等待人工干预")
                    return
                else:
                    # 预期和实际都为0
                    self.logger.debug("✅ 剥头皮模式持仓检查通过，预期和实际均为0")

            # ==================== 阶段3: 记录持仓检查结果（暂不调整）====================
            # 注意：此时只记录持仓状态，不立即调整
            # 原因：需要先调整订单，订单稳定后再根据最终订单状态调整持仓
            if position_health['needs_adjustment']:
                self.logger.warning(
                    f"⚠️ 阶段3: 检测到持仓异常，暂不调整（将在订单调整后处理）"
                )
                self.logger.warning(
                    f"   持仓问题: {', '.join(position_health['issues'])}")
            else:
                self.logger.debug("✅ 阶段3: 持仓健康（基于当前订单状态）")

            # ==================== 阶段4: 分析订单实际分布（反向计算）====================
            self.logger.debug("📊 阶段4: 分析订单实际分布")

            actual_range = self._calculate_actual_range_from_orders(orders)

            # 安全检查
            allow_filling = len(orders) < self.config.grid_count

            if allow_filling:
                self.logger.debug(
                    f"✅ 安全检查: 订单数({len(orders)}) < 网格数({self.config.grid_count}), "
                    f"允许补单"
                )
            else:
                self.logger.warning(
                    f"🔴 安全检查: 订单数({len(orders)}) >= 网格数({self.config.grid_count}), "
                    f"禁止补单，仅清理问题订单"
                )

            # ==================== 阶段5: 确定理论扩展范围 ====================
            self.logger.debug("📏 阶段5: 确定理论扩展范围")

            theoretical_range = self._determine_extended_range(orders)

            # ==================== 阶段6: 对比差异并诊断问题 ====================
            self.logger.debug("🔍 阶段6: 对比差异并诊断问题")

            # 对比实际范围和理论范围
            self._compare_ranges(actual_range, theoretical_range)

            # 诊断问题订单
            problem_orders = self._diagnose_problem_orders(
                orders, actual_range, theoretical_range)

            # ==================== 阶段7: 清理问题订单 ====================
            if problem_orders['duplicates'] or problem_orders['out_of_range']:
                if is_scalping_active:
                    # 剥头皮模式激活，只报告问题，不清理
                    self.logger.warning(
                        f"🔴 检测到问题订单: 重复={len(problem_orders['duplicates'])}个, "
                        f"超范围={len(problem_orders['out_of_range'])}个"
                    )
                    self.logger.debug("💡 剥头皮模式激活中，跳过清理操作，由剥头皮管理器处理")
                else:
                    # 正常模式，执行清理
                    self.logger.debug("🧹 阶段7: 清理问题订单")
                    cleaned_count = await self._clean_problem_orders(problem_orders)

                    if cleaned_count > 0:
                        self.logger.debug(
                            f"✅ 已清理 {cleaned_count} 个问题订单，等待生效...")
                        await asyncio.sleep(2)  # 等待订单取消生效

                        # ==================== 阶段8: 重新获取订单 ====================
                        self.logger.debug("🔄 阶段8: 重新获取订单")
                        orders = await self.engine.exchange.get_open_orders(self.config.symbol)
                        self.logger.debug(f"📡 清理后剩余: {len(orders)}个订单")
            else:
                self.logger.debug("✅ 阶段7: 未发现问题订单，跳过清理")

            # ==================== 阶段9: 评估网格覆盖 ====================
            self.logger.debug("📊 阶段9: 评估网格覆盖")

            covered_grids, missing_grids, profit_gap_grids = self._evaluate_grid_coverage(
                orders, theoretical_range
            )

            # ==================== 阶段10: 补单决策 ====================
            self.logger.debug("💡 阶段10: 补单决策")

            if not missing_grids:
                if theoretical_range['extended']:
                    self.logger.debug(
                        f"✅ 网格健康检查完成: 网格完整"
                    )
                    self.logger.debug(
                        f"   物理范围: Grid 1-{theoretical_range['max_grid']}"
                    )
                    self.logger.debug(
                        f"   预期订单: {theoretical_range['expected_count']}个"
                    )
                    self.logger.debug(
                        f"   实际订单: {len(orders)}个"
                    )
                    # 使用动态计算的获利空格
                    if profit_gap_grids:
                        gap_list = sorted(profit_gap_grids)
                        gap_range = f"Grid {min(gap_list)}-{max(gap_list)}" if len(
                            gap_list) > 1 else f"Grid {gap_list[0]}"
                        self.logger.debug(
                            f"   获利空格: {len(profit_gap_grids)}个 ({gap_range})"
                        )
                    else:
                        self.logger.debug(
                            f"   获利空格: 0个"
                        )
                else:
                    self.logger.debug(
                        f"✅ 网格健康检查完成: 网格完整，"
                        f"预期{theoretical_range['expected_count']}个订单，"
                        f"实际{len(orders)}个订单"
                    )
            elif is_scalping_active:
                # 剥头皮模式激活，只报告缺失，不补单
                self.logger.warning(
                    f"🔴 检测到{len(missing_grids)}个缺失网格"
                )
                if len(missing_grids) <= 10:
                    self.logger.warning(f"   缺失网格: {missing_grids}")
                else:
                    self.logger.warning(
                        f"   缺失网格: {missing_grids[:5]}...{missing_grids[-5:]} (共{len(missing_grids)}个)"
                    )
                self.logger.debug("💡 剥头皮模式激活中，跳过补单操作，由剥头皮管理器处理")
            elif not allow_filling:
                self.logger.warning(
                    f"⚠️ 检测到{len(missing_grids)}个真正缺失网格，但订单数已达上限，禁止补单"
                )
                self.logger.warning(
                    f"💡 建议: 手动检查是否存在异常订单"
                )
            else:
                # 允许补单
                await self._fill_missing_grids(missing_grids, theoretical_range)

            # ==================== 阶段11: 订单调整完成后，重新检查并调整持仓 ====================
            self.logger.debug("=" * 80)
            self.logger.debug("📊 阶段11: 订单调整完成，现在检查并调整持仓")
            self.logger.debug("=" * 80)

            # 重新并发获取订单和持仓（订单已调整完成）
            final_orders, final_positions = await self._fetch_orders_and_positions()

            # 重新统计订单
            final_buy_count = sum(
                1 for o in final_orders if o.side == ExchangeOrderSide.BUY)
            final_sell_count = sum(
                1 for o in final_orders if o.side == ExchangeOrderSide.SELL)

            self.logger.debug(
                f"📡 最终订单状态: 总计={len(final_orders)}个, "
                f"买单={final_buy_count}个, 卖单={final_sell_count}个"
            )

            # 根据最终订单状态计算预期持仓
            final_expected_position = self._calculate_expected_position(
                self.config.grid_count,
                final_buy_count,
                final_sell_count
            )

            self.logger.debug(f"📍 最终预期持仓: {final_expected_position}")

            # 检查持仓健康状态
            final_position_health = self._check_position_health(
                final_expected_position, final_positions)

            self.logger.debug(
                f"📍 最终实际持仓: {final_position_health['actual_position']} "
                f"(方向: {final_position_health['position_side']})"
            )

            # 🔥 关键前提条件：执行持仓调整前，必须验证订单数量正确
            final_order_count_correct = len(
                final_orders) == self.config.grid_count

            if not final_order_count_correct:
                self.logger.warning(
                    f"⚠️ 阶段11: 订单数量不正确 "
                    f"(实际{len(final_orders)}个 ≠ 预期{self.config.grid_count}个)，"
                    f"跳过持仓调整"
                )
                self.logger.debug(
                    f"💡 原因: 持仓预期计算依赖订单数量正确，"
                    f"等待下次健康检查轮询时，订单调整完成后再调整持仓"
                )
                if final_position_health['needs_adjustment']:
                    self.logger.debug(
                        f"   检测到持仓异常: {', '.join(final_position_health['issues'])}, "
                        f"但暂不处理"
                    )

            # 执行持仓调整（如果需要且前提条件满足）
            elif final_position_health['needs_adjustment'] and not is_scalping_active:
                self.logger.debug("🔧 阶段11: 执行持仓调整")
                self.logger.debug(
                    f"   ✅ 前提条件: 订单数量正确({len(final_orders)}个 = {self.config.grid_count}个)")
                self.logger.debug(
                    f"   持仓问题: {', '.join(final_position_health['issues'])}")

                adjustment_success = await self._adjust_position(final_position_health)

                if adjustment_success:
                    self.logger.debug("✅ 持仓调整完成，等待生效...")
                    await asyncio.sleep(3)

                    # 最终验证持仓
                    _, verify_positions = await self._fetch_orders_and_positions()
                    verify_position_health = self._check_position_health(
                        final_expected_position, verify_positions)

                    if verify_position_health['is_healthy']:
                        self.logger.debug("✅ 持仓已恢复正常")
                    else:
                        self.logger.warning(
                            "⚠️ 持仓调整后仍存在问题，可能需要人工介入")
                        self.logger.warning(
                            f"   剩余问题: {', '.join(verify_position_health['issues'])}")
                else:
                    self.logger.error("❌ 持仓调整失败")
            elif final_position_health['needs_adjustment'] and is_scalping_active:
                self.logger.warning(
                    f"🔴 检测到持仓异常但剥头皮模式激活，跳过持仓调整: "
                    f"{', '.join(final_position_health['issues'])}"
                )
            else:
                self.logger.debug("✅ 阶段11: 持仓健康，无需调整")

            # ==================== 同步订单到本地缓存 ====================
            await self._sync_orders_to_engine(final_orders)

            self.logger.debug("=" * 80)
            self.logger.debug("✅ 订单和持仓健康检查完成")
            self.logger.debug("=" * 80)

        except Exception as e:
            self.logger.error(f"❌ 订单健康检查失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    def _calculate_actual_range_from_orders(self, orders: List) -> Dict:
        """
        从订单价格反向计算实际网格范围

        遍历所有订单，找出最低价和最高价，
        然后反向计算这些价格对应的网格ID

        Args:
            orders: 订单列表

        Returns:
            实际范围信息字典
        """
        if not orders:
            return {
                'min_price': None,
                'max_price': None,
                'min_grid': None,
                'max_grid': None,
                'price_span': None
            }

        # 找出最低价和最高价
        prices = [order.price for order in orders]
        min_price = min(prices)
        max_price = max(prices)

        # 反向计算网格ID
        if self.config.grid_type in [GridType.LONG, GridType.FOLLOW_LONG, GridType.MARTINGALE_LONG]:
            # 做多网格：Grid 1 = lower_price
            min_grid = round(
                (min_price - self.config.lower_price) / self.config.grid_interval
            ) + 1
            max_grid = round(
                (max_price - self.config.lower_price) / self.config.grid_interval
            ) + 1
        else:
            # 做空网格：Grid 1 = upper_price
            min_grid = round(
                (self.config.upper_price - max_price) / self.config.grid_interval
            ) + 1
            max_grid = round(
                (self.config.upper_price - min_price) / self.config.grid_interval
            ) + 1

        result = {
            'min_price': min_price,
            'max_price': max_price,
            'min_grid': min_grid,
            'max_grid': max_grid,
            'price_span': max_price - min_price
        }

        self.logger.debug(
            f"📊 实际订单分布: "
            f"价格范围 [{min_price}, {max_price}], "
            f"网格范围 [Grid {min_grid}, Grid {max_grid}]"
        )

        return result

    def _determine_extended_range(self, orders: List) -> Dict:
        """
        确定扩展范围

        根据订单类型判断是否需要扩展网格范围：
        - 做多网格 + 存在卖单 → 向上扩展
        - 做空网格 + 存在买单 → 向下扩展

        Args:
            orders: 订单列表

        Returns:
            扩展范围信息字典
        """
        # 检查订单类型
        has_buy = any(o.side == ExchangeOrderSide.BUY for o in orders)
        has_sell = any(o.side == ExchangeOrderSide.SELL for o in orders)

        # 基础范围
        result = {
            'lower_price': self.config.lower_price,
            'upper_price': self.config.upper_price,
            'min_grid': 1,
            'max_grid': self.config.grid_count,
            'expected_count': self.config.grid_count,
            'extended': False,
            'direction': None
        }

        # 判断是否需要扩展
        if self.config.grid_type in [GridType.LONG, GridType.FOLLOW_LONG, GridType.MARTINGALE_LONG]:
            if has_sell:
                # 做多网格有卖单，向上扩展
                result['extended'] = True
                result['direction'] = 'up'
                result['max_grid'] = self.config.grid_count + \
                    self.config.reverse_order_grid_distance
                result['upper_price'] = self.config.upper_price + (
                    self.config.grid_interval * self.config.reverse_order_grid_distance
                )
                # ⚠️ 重要：预期订单数保持不变（200），因为中间有获利空格
                # 物理范围扩展到202格，但预期订单数仍然是200
                result['expected_count'] = self.config.grid_count

                self.logger.debug(
                    f"🔼 做多网格检测到卖单，向上扩展:"
                )
                self.logger.debug(
                    f"   基础: [{result['lower_price']}, {self.config.upper_price}] "
                    f"(Grid 1-{self.config.grid_count})"
                )
                self.logger.debug(
                    f"   扩展: [{result['lower_price']}, {result['upper_price']}] "
                    f"(Grid 1-{result['max_grid']})"
                )
                self.logger.debug(
                    f"   预期订单数: {result['expected_count']}个 "
                    f"(中间{self.config.reverse_order_grid_distance}格为获利空格)"
                )

        elif self.config.grid_type in [GridType.SHORT, GridType.FOLLOW_SHORT, GridType.MARTINGALE_SHORT]:
            if has_buy:
                # 做空网格有买单，向下扩展
                result['extended'] = True
                result['direction'] = 'down'
                result['max_grid'] = self.config.grid_count + \
                    self.config.reverse_order_grid_distance
                result['lower_price'] = self.config.lower_price - (
                    self.config.grid_interval * self.config.reverse_order_grid_distance
                )
                # ⚠️ 重要：预期订单数保持不变（200），因为中间有获利空格
                # 物理范围扩展到202格，但预期订单数仍然是200
                result['expected_count'] = self.config.grid_count

                self.logger.debug(
                    f"🔽 做空网格检测到买单，向下扩展:"
                )
                self.logger.debug(
                    f"   基础: [{self.config.lower_price}, {result['upper_price']}] "
                    f"(Grid 1-{self.config.grid_count})"
                )
                self.logger.debug(
                    f"   扩展: [{result['lower_price']}, {result['upper_price']}] "
                    f"(Grid 1-{result['max_grid']})"
                )
                self.logger.debug(
                    f"   预期订单数: {result['expected_count']}个 "
                    f"(中间{self.config.reverse_order_grid_distance}格为获利空格)"
                )

        if not result['extended']:
            self.logger.debug(
                f"📏 使用基础范围: [{result['lower_price']}, {result['upper_price']}] "
                f"(Grid 1-{result['max_grid']})"
            )

        return result

    def _compare_ranges(self, actual_range: Dict, theoretical_range: Dict):
        """
        对比实际范围和理论范围，输出差异分析

        Args:
            actual_range: 实际订单覆盖的网格范围
            theoretical_range: 理论扩展范围
        """
        if not actual_range['min_grid'] or not actual_range['max_grid']:
            self.logger.warning("⚠️ 实际范围无效，跳过对比")
            return

        actual_min = actual_range['min_grid']
        actual_max = actual_range['max_grid']
        theory_min = theoretical_range['min_grid']
        theory_max = theoretical_range['max_grid']

        self.logger.debug("=" * 60)
        self.logger.debug("📊 范围对比分析:")
        self.logger.debug(
            f"   实际范围: Grid [{actual_min}, {actual_max}] "
            f"(价格: [{actual_range['min_price']}, {actual_range['max_price']}])"
        )
        self.logger.debug(
            f"   理论范围: Grid [{theory_min}, {theory_max}] "
            f"(价格: [{theoretical_range['lower_price']}, {theoretical_range['upper_price']}])"
        )

        # 分析差异
        issues = []

        if actual_min < theory_min:
            below_count = theory_min - actual_min
            issues.append(f"下限超出: {below_count}格低于理论下限")
            self.logger.warning(
                f"   ⚠️ 订单低于理论下限: Grid {actual_min} < Grid {theory_min} "
                f"(超出{below_count}格)"
            )

        if actual_max > theory_max:
            above_count = actual_max - theory_max
            issues.append(f"上限超出: {above_count}格高于理论上限")
            self.logger.warning(
                f"   ⚠️ 订单高于理论上限: Grid {actual_max} > Grid {theory_max} "
                f"(超出{above_count}格)"
            )

        if not issues:
            self.logger.debug("   ✅ 实际范围在理论范围内，正常")
        else:
            self.logger.warning(
                f"   ❌ 发现{len(issues)}个范围问题: {', '.join(issues)}")

        self.logger.debug("=" * 60)

    def _diagnose_problem_orders(
        self,
        orders: List,
        actual_range: Dict,
        theoretical_range: Dict
    ) -> Dict:
        """
        诊断问题订单（基于实际范围和理论范围对比）

        检测：
        1. 重复订单（相同价格的订单）
        2. 超出理论扩展范围的订单

        Args:
            orders: 订单列表
            actual_range: 实际订单范围
            theoretical_range: 理论扩展范围

        Returns:
            问题订单字典 {'duplicates': [], 'out_of_range': []}
        """
        problem_orders = {
            'duplicates': [],      # 重复订单
            'out_of_range': []     # 超出范围订单
        }

        # ========== 检查1: 重复订单 ==========
        self.logger.debug("📍 检查1: 诊断重复订单")

        price_to_orders = defaultdict(list)
        for order in orders:
            price_to_orders[order.price].append(order)

        duplicate_count = 0
        for price, order_list in price_to_orders.items():
            if len(order_list) > 1:
                # 保留第一个，其他标记为重复
                keep_order = order_list[0]
                duplicates = order_list[1:]
                problem_orders['duplicates'].extend(duplicates)
                duplicate_count += len(duplicates)

                self.logger.warning(
                    f"   ❌ 发现重复订单 @${price}: {len(order_list)}个订单, "
                    f"保留{keep_order.id[:10]}..., 标记{len(duplicates)}个为重复"
                )

        if duplicate_count > 0:
            self.logger.warning(
                f"   🔍 重复订单统计: 共{duplicate_count}个重复订单需要清理"
            )
        else:
            self.logger.debug("   ✅ 无重复订单")

        # ========== 检查2: 超出理论范围的订单 ==========
        self.logger.debug("📍 检查2: 诊断超出理论范围的订单")

        out_of_range_count = 0
        for order in orders:
            # 计算订单的真实网格ID
            if self.config.grid_type in [GridType.LONG, GridType.FOLLOW_LONG, GridType.MARTINGALE_LONG]:
                raw_index = round(
                    (order.price - self.config.lower_price) /
                    self.config.grid_interval
                ) + 1
            else:
                raw_index = round(
                    (self.config.upper_price - order.price) /
                    self.config.grid_interval
                ) + 1

            # 判断是否超出理论扩展范围
            if raw_index < theoretical_range['min_grid'] or raw_index > theoretical_range['max_grid']:
                problem_orders['out_of_range'].append((order, raw_index))
                out_of_range_count += 1
                self.logger.warning(
                    f"   ❌ 订单超出理论范围: {order.side.value} @{order.price} "
                    f"(Grid {raw_index}, 理论范围: {theoretical_range['min_grid']}-{theoretical_range['max_grid']})"
                )

        if out_of_range_count > 0:
            self.logger.warning(
                f"   🔍 超范围订单统计: 共{out_of_range_count}个订单超出理论范围"
            )
        else:
            self.logger.debug("   ✅ 所有订单都在理论范围内")

        return problem_orders

    async def _clean_problem_orders(self, problem_orders: Dict) -> int:
        """
        清理问题订单

        Args:
            problem_orders: 问题订单字典

        Returns:
            清理的订单数量
        """
        cleaned_count = 0

        # 清理重复订单
        for order in problem_orders['duplicates']:
            try:
                # 标记为预期取消（避免自动重新挂单）
                self.engine._expected_cancellations.add(order.id)
                await self.engine.exchange.cancel_order(order.id, self.config.symbol)
                cleaned_count += 1
                self.logger.debug(
                    f"🧹 已取消重复订单: {order.side.value} @{order.price} "
                    f"(ID: {order.id[:10]}...)"
                )
            except Exception as e:
                self.logger.error(f"❌ 取消重复订单失败: {e}")

        # 清理超出范围的订单
        for order, raw_index in problem_orders['out_of_range']:
            try:
                # 标记为预期取消
                self.engine._expected_cancellations.add(order.id)
                await self.engine.exchange.cancel_order(order.id, self.config.symbol)
                cleaned_count += 1
                self.logger.debug(
                    f"🧹 已取消超范围订单: {order.side.value} @{order.price} "
                    f"Grid={raw_index} (ID: {order.id[:10]}...)"
                )
            except Exception as e:
                self.logger.error(f"❌ 取消超范围订单失败: {e}")

        if cleaned_count > 0:
            self.logger.debug(f"✅ 问题订单清理完成: 共清理{cleaned_count}个订单")

        return cleaned_count

    def _evaluate_grid_coverage(
        self,
        orders: List,
        extended_range: Dict
    ) -> Tuple[Set[int], List[int], Set[int]]:
        """
        评估网格覆盖情况

        Args:
            orders: 订单列表
            extended_range: 扩展范围信息

        Returns:
            (已覆盖的网格集合, 缺失的网格列表, 获利空格集合)
        """
        covered_grids = set()
        anomaly_orders = []  # 记录异常订单（清理失败的）

        # 映射订单到网格
        for order in orders:
            try:
                if self.config.grid_type in [GridType.LONG, GridType.FOLLOW_LONG, GridType.MARTINGALE_LONG]:
                    raw_index = round(
                        (order.price - self.config.lower_price) /
                        self.config.grid_interval
                    ) + 1
                else:
                    raw_index = round(
                        (self.config.upper_price - order.price) /
                        self.config.grid_interval
                    ) + 1

                # 双重检查：如果订单超出范围，记录为异常（可能是清理失败）
                if raw_index < extended_range['min_grid'] or raw_index > extended_range['max_grid']:
                    anomaly_orders.append((order, raw_index))
                    self.logger.error(
                        f"❌ 发现异常订单（应该已被清理但仍存在）: "
                        f"{order.side.value} @{order.price} (Grid {raw_index}, "
                        f"范围: {extended_range['min_grid']}-{extended_range['max_grid']})"
                    )
                    # 不将异常订单加入覆盖统计
                    continue

                covered_grids.add(raw_index)

            except Exception as e:
                self.logger.warning(f"⚠️ 订单 @{order.price} 映射失败: {e}")

        # 如果有异常订单，输出警告
        if anomaly_orders:
            self.logger.error(
                f"🚨 警告: 发现 {len(anomaly_orders)} 个异常订单未被清理！"
            )
            self.logger.error(
                f"💡 建议: 这些订单可能需要手动取消"
            )

        # 找出缺失的网格
        all_grids = set(range(
            extended_range['min_grid'],
            extended_range['max_grid'] + 1
        ))
        missing_grids_raw = sorted(all_grids - covered_grids)

        # 动态计算获利空格：买单和卖单之间的空隙
        profit_gap_grids = set()
        if extended_range['extended'] and orders:
            # 分离买单和卖单
            buy_grids = []
            sell_grids = []

            for order in orders:
                # 计算订单的网格ID
                if self.config.grid_type in [GridType.LONG, GridType.FOLLOW_LONG, GridType.MARTINGALE_LONG]:
                    grid_id = round(
                        (order.price - self.config.lower_price) /
                        self.config.grid_interval
                    ) + 1
                else:
                    grid_id = round(
                        (self.config.upper_price - order.price) /
                        self.config.grid_interval
                    ) + 1

                # 分类
                if order.side == ExchangeOrderSide.BUY:
                    buy_grids.append(grid_id)
                elif order.side == ExchangeOrderSide.SELL:
                    sell_grids.append(grid_id)

            # 计算获利空格：买单最高网格 和 卖单最低网格 之间
            if buy_grids and sell_grids:
                max_buy_grid = max(buy_grids)
                min_sell_grid = min(sell_grids)

                # 获利空格 = (max_buy_grid, min_sell_grid) 之间的所有网格
                if min_sell_grid > max_buy_grid:
                    profit_gap_grids = set(
                        range(max_buy_grid + 1, min_sell_grid))

                    self.logger.debug(
                        f"📍 动态获利空格识别: 买单最高Grid {max_buy_grid}, "
                        f"卖单最低Grid {min_sell_grid}, "
                        f"获利空格 {sorted(profit_gap_grids)}"
                    )

        # 真正的缺失网格（排除获利空格）
        missing_grids = [
            g for g in missing_grids_raw if g not in profit_gap_grids]

        # 日志输出
        if profit_gap_grids:
            profit_gaps_in_missing = [
                g for g in missing_grids_raw if g in profit_gap_grids]
            if profit_gaps_in_missing:
                self.logger.debug(
                    f"📍 网格覆盖: 已覆盖={len(covered_grids)}格, "
                    f"获利空格={len(profit_gaps_in_missing)}格 (正常), "
                    f"真正缺失={len(missing_grids)}格"
                )
                self.logger.debug(
                    f"   获利空格: {sorted(profit_gaps_in_missing)} (用于获利，正常空着)"
                )
            else:
                self.logger.debug(
                    f"📍 网格覆盖: 已覆盖={len(covered_grids)}格, "
                    f"缺失={len(missing_grids)}格, "
                    f"预期={extended_range['expected_count']}格"
                )
        else:
            self.logger.debug(
                f"📍 网格覆盖: 已覆盖={len(covered_grids)}格, "
                f"缺失={len(missing_grids)}格, "
                f"预期={extended_range['expected_count']}格"
            )

        if missing_grids:
            if len(missing_grids) <= 10:
                self.logger.debug(f"   真正缺失网格: {missing_grids}")
            else:
                self.logger.debug(
                    f"   真正缺失网格: {missing_grids[:5]}...{missing_grids[-5:]} "
                    f"(共{len(missing_grids)}个)"
                )

        return covered_grids, missing_grids, profit_gap_grids

    async def _fill_missing_grids(self, missing_grids: List[int], extended_range: Dict):
        """
        补充缺失的网格

        Args:
            missing_grids: 缺失的网格ID列表
            extended_range: 扩展范围信息
        """
        try:
            if not missing_grids:
                return

            self.logger.debug(f"🔧 准备补充 {len(missing_grids)} 个缺失网格")

            # 获取当前价格
            current_price = await self.engine.get_current_price()
            self.logger.debug(f"📊 当前价格: ${current_price}")

            # 创建订单
            orders_to_place = []

            for grid_id in missing_grids:
                try:
                    grid_price = self.config.get_grid_price(grid_id)

                    # 判断订单方向
                    if grid_price < current_price:
                        side = GridOrderSide.BUY
                    elif grid_price > current_price:
                        side = GridOrderSide.SELL
                    else:
                        continue  # 价格相等，跳过

                    # 🔥 使用格式化后的订单数量（符合交易所精度）
                    amount = self.config.get_formatted_grid_order_amount(
                        grid_id)

                    # 创建订单
                    order = GridOrder(
                        order_id="",
                        grid_id=grid_id,
                        side=side,
                        price=grid_price,
                        amount=amount,  # 格式化后的金额
                        status=GridOrderStatus.PENDING,
                        created_at=datetime.now()
                    )
                    orders_to_place.append(order)

                except Exception as e:
                    self.logger.error(f"❌ 创建Grid {grid_id}订单失败: {e}")

            # 批量下单
            if orders_to_place:
                success_count = 0
                fail_count = 0

                for order in orders_to_place:
                    try:
                        await self.engine.place_order(order)
                        success_count += 1
                        self.logger.debug(
                            f"✅ 补充Grid {order.grid_id}: "
                            f"{order.side.value} {order.amount}@{order.price}"
                        )
                    except Exception as e:
                        fail_count += 1
                        self.logger.error(f"❌ 补充Grid {order.grid_id}失败: {e}")

                self.logger.debug(
                    f"✅ 补单完成: 成功={success_count}个, 失败={fail_count}个, "
                    f"总计={len(orders_to_place)}个"
                )

        except Exception as e:
            self.logger.error(f"❌ 补充缺失网格失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    async def _sync_orders_to_engine(self, exchange_orders: List):
        """
        同步订单到引擎的本地缓存

        Args:
            exchange_orders: 交易所订单列表
        """
        try:
            # 调用引擎的同步方法
            await self.engine._sync_orders_from_exchange(exchange_orders)

        except Exception as e:
            self.logger.error(f"❌ 同步订单到引擎失败: {e}")
