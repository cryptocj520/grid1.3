"""
订单操作模块

提供订单取消、挂单、验证等操作，并集成验证逻辑
"""

import asyncio
from typing import List, Optional, Callable, TYPE_CHECKING
from decimal import Decimal

from ....logging import get_logger
from ..models import GridOrder, GridOrderSide, GridOrderStatus
from .verification_utils import OrderVerificationUtils

if TYPE_CHECKING:
    from ....adapters.exchanges.models import OrderData


class OrderOperations:
    """
    订单操作管理器

    职责：
    1. 批量取消订单并验证
    2. 挂单并验证
    3. 取消特定类型订单并验证
    4. 统一错误处理和重试逻辑
    5. 🆕 订单操作暂停检查（REST失败保护）
    """

    def __init__(self, engine, state, config, coordinator=None):
        """
        初始化订单操作管理器

        Args:
            engine: 执行引擎
            state: 网格状态
            config: 网格配置
            coordinator: 协调器引用（用于检查暂停状态）
        """
        self.logger = get_logger(__name__)
        self.engine = engine
        self.state = state
        self.config = config
        self.coordinator = coordinator  # 🆕 添加coordinator引用

        # 创建验证工具实例
        self.verifier = OrderVerificationUtils(engine.exchange, config.symbol)

    def _check_if_paused(self, operation_name: str) -> bool:
        """
        检查系统是否暂停或紧急停止

        Args:
            operation_name: 操作名称（用于日志）

        Returns:
            True if paused/stopped, False if OK to proceed
        """
        if not self.coordinator:
            return False  # 没有coordinator引用，默认允许操作

        if self.coordinator.is_emergency_stopped:
            self.logger.error(
                f"🚨 系统紧急停止中，禁止{operation_name}操作！"
            )
            return True

        if self.coordinator.is_paused:
            self.logger.warning(
                f"⏸️ REST API暂时不可用，暂停{operation_name}操作（等待恢复）"
            )
            return True

        return False

    async def cancel_all_orders_with_verification(
        self,
        max_retries: int = 3,
        retry_delay: float = 1.5,
        first_delay: float = 0.8
    ) -> bool:
        """
        取消所有订单并验证（通用方法）

        流程：
        1. 批量取消所有订单
        2. 等待交易所处理
        3. 验证订单是否真的被取消
        4. 如果仍有订单，重试

        Args:
            max_retries: 最大重试次数
            retry_delay: 重试时的延迟（秒）
            first_delay: 首次验证的延迟（秒）

        Returns:
            True: 所有订单已取消
            False: 仍有订单无法取消
        """
        self.logger.info("📋 取消所有订单并验证...")

        # 1. 首次批量取消
        try:
            cancelled_count = await self.engine.cancel_all_orders()
            self.logger.info(f"✅ 批量取消API返回: {cancelled_count} 个订单")
        except Exception as e:
            self.logger.error(f"❌ 批量取消订单失败: {e}")

        # 2. 验证循环（带重试）
        cancel_verified = False

        for retry in range(max_retries):
            # 等待让交易所处理取消请求
            if retry == 0:
                await asyncio.sleep(first_delay)  # 首次验证等待时间短
            else:
                await asyncio.sleep(retry_delay)  # 重试时等待更长

            # 获取当前未成交订单数量
            open_count = await self.verifier.get_open_orders_count()

            if open_count == 0:
                # 验证成功
                self.logger.info(f"✅ 订单取消验证通过: 当前未成交订单 {open_count} 个")
                cancel_verified = True
                break
            elif open_count < 0:
                # 获取订单失败
                self.logger.error("❌ 无法获取未成交订单数量，跳过验证")
                break
            else:
                # 验证失败
                if retry < max_retries - 1:
                    # 还有重试机会，尝试再次取消
                    self.logger.warning(
                        f"⚠️ 第 {retry + 1} 次验证失败: 仍有 {open_count} 个未成交订单"
                    )
                    self.logger.info(f"🔄 尝试再次取消这些订单...")

                    # 再次调用取消订单
                    try:
                        retry_cancelled = await self.engine.cancel_all_orders()
                        self.logger.info(f"重试取消返回: {retry_cancelled} 个订单")
                    except Exception as e:
                        self.logger.error(f"重试取消失败: {e}")
                else:
                    # 已达到最大重试次数
                    self.logger.error(
                        f"❌ 订单取消验证最终失败！已重试 {max_retries} 次，仍有 {open_count} 个未成交订单"
                    )
                    self.logger.error(f"预期: 0 个订单, 实际: {open_count} 个订单")
                    self.logger.error("⚠️ 操作已暂停，不会继续后续步骤，避免超出订单限制")
                    self.logger.error("💡 建议: 请手动检查交易所订单")

        return cancel_verified

    async def cancel_orders_by_filter_with_verification(
        self,
        order_filter: Callable[[GridOrder], bool],
        filter_description: str,
        max_attempts: int = 3
    ) -> bool:
        """
        取消特定类型订单并验证

        循环逻辑：
        1. 收集需要取消的订单（根据过滤函数）
        2. 批量取消订单
        3. 从交易所验证
        4. 如果还有残留，再次批量取消
        5. 重复最多max_attempts次

        Args:
            order_filter: 订单过滤函数，返回True表示需要取消的订单
            filter_description: 过滤条件描述（用于日志）
            max_attempts: 最大尝试次数

        Returns:
            True: 所有满足条件的订单已取消
            False: 仍有满足条件的订单无法取消
        """
        for attempt in range(max_attempts):
            self.logger.info(
                f"🔄 取消{filter_description}尝试 {attempt+1}/{max_attempts}..."
            )

            # 1. 收集需要取消的订单（从本地状态）
            orders_to_cancel_list = []
            for order_id, order in list(self.state.active_orders.items()):
                if order_filter(order):
                    orders_to_cancel_list.append(order)

            if len(orders_to_cancel_list) == 0:
                self.logger.info(f"📋 本地状态显示无{filter_description}，验证交易所...")
                # 即使本地无订单，也要验证交易所
                if await self.verifier.verify_no_orders_by_filter(
                    order_filter, filter_description
                ):
                    return True
                else:
                    # 交易所还有订单，但本地状态没有，需要同步
                    self.logger.warning("⚠️ 本地状态与交易所不同步，从交易所获取...")
                    try:
                        exchange_orders = await self.engine.exchange.get_open_orders(
                            symbol=self.config.symbol
                        )
                        orders_to_cancel_list = [
                            order for order in exchange_orders
                            if order_filter(order)
                        ]
                    except Exception as e:
                        self.logger.error(f"从交易所获取订单失败: {e}")
                        continue

            self.logger.info(
                f"📋 准备取消 {len(orders_to_cancel_list)} 个{filter_description}")

            # 2. 批量取消订单（并发，提高速度）
            cancelled_count = 0
            failed_count = 0

            async def cancel_single_order(order):
                """取消单个订单"""
                try:
                    # 兼容 GridOrder（order_id）和 OrderData（id）
                    order_id = getattr(order, 'order_id', None) or getattr(
                        order, 'id', None)
                    if not order_id:
                        return False, "unknown"

                    await self.engine.cancel_order(order_id)
                    self.state.remove_order(order_id)
                    return True, order_id
                except Exception as e:
                    error_msg = str(e).lower()
                    order_id = getattr(order, 'order_id', None) or getattr(
                        order, 'id', None)
                    if "not found" in error_msg or "does not exist" in error_msg:
                        # 订单已不存在，从状态移除
                        if order_id:
                            self.state.remove_order(order_id)
                        return True, order_id or "unknown"
                    else:
                        return False, order_id or "unknown"

            # 并发取消（限制批次大小避免API限流）
            batch_size = 10
            for i in range(0, len(orders_to_cancel_list), batch_size):
                batch = orders_to_cancel_list[i:i+batch_size]
                tasks = [cancel_single_order(order) for order in batch]

                try:
                    results = await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True),
                        timeout=30.0
                    )

                    for result in results:
                        if isinstance(result, Exception):
                            failed_count += 1
                        elif result[0]:
                            cancelled_count += 1
                        else:
                            failed_count += 1

                except Exception as e:
                    self.logger.error(f"批量取消订单失败: {e}")
                    failed_count += len(batch)

                # 避免API限流
                if i + batch_size < len(orders_to_cancel_list):
                    await asyncio.sleep(0.1)

            self.logger.info(
                f"✅ 批量取消完成: 成功={cancelled_count}, 失败={failed_count}"
            )

            # 3. 等待一小段时间，让交易所处理取消请求
            await asyncio.sleep(0.3)

            # 4. 🔥 关键：从交易所验证是否还有满足条件的订单
            if await self.verifier.verify_no_orders_by_filter(
                order_filter, filter_description
            ):
                self.logger.info(
                    f"✅ 所有{filter_description}已成功取消（尝试{attempt+1}次）")
                return True
            else:
                self.logger.warning(
                    f"⚠️ 交易所仍有{filter_description}残留，准备第{attempt+2}次尝试..."
                )
                # 继续下一次循环

        # 达到最大尝试次数，仍有订单
        self.logger.error(
            f"❌ 取消{filter_description}失败: 已尝试{max_attempts}次，交易所仍有残留"
        )
        return False

    async def cancel_sell_orders_with_verification(self, max_attempts: int = 3) -> bool:
        """
        取消所有卖单并验证（做多网格剥头皮模式专用）

        Args:
            max_attempts: 最大尝试次数

        Returns:
            True: 所有卖单已取消
            False: 仍有卖单无法取消
        """
        return await self.cancel_orders_by_filter_with_verification(
            order_filter=lambda order: order.side == GridOrderSide.SELL,
            filter_description="卖单",
            max_attempts=max_attempts
        )

    async def cancel_buy_orders_with_verification(self, max_attempts: int = 3) -> bool:
        """
        取消所有买单并验证（做空网格剥头皮模式专用）

        Args:
            max_attempts: 最大尝试次数

        Returns:
            True: 所有买单已取消
            False: 仍有买单无法取消
        """
        return await self.cancel_orders_by_filter_with_verification(
            order_filter=lambda order: order.side == GridOrderSide.BUY,
            filter_description="买单",
            max_attempts=max_attempts
        )

    async def place_order_with_verification(
        self,
        order: GridOrder,
        max_attempts: int = 2  # 🔥 只重试1次（总共2次尝试）
    ) -> Optional[GridOrder]:
        """
        挂单并验证（新方案：提交→最终验证→重试）

        🔥 核心改进：
        1. 提交订单后，无论API成功还是失败，都执行最终验证
        2. 最终验证：从交易所查询订单是否真实存在
        3. 只有最终验证确认不存在，才重试
        4. 避免"API失败但订单实际成功"导致的重复挂单

        流程：
        1. 提交订单（捕获API成功/失败）
        2. 执行最终验证（两阶段：ID精确匹配 + 特征模糊匹配）
        3. 验证通过 → 返回订单
        4. 验证失败 → 等待5秒后重试（最多1次）

        Args:
            order: 待挂订单
            max_attempts: 最大尝试次数（默认2次）

        Returns:
            成功挂出的订单，失败返回None
        """
        from ....adapters.exchanges.models import OrderData

        # 🆕 检查系统是否暂停
        if self._check_if_paused("挂单"):
            return None

        for attempt in range(max_attempts):
            self.logger.info(
                f"🔄 挂单尝试 {attempt+1}/{max_attempts}..."
            )

            # ==================== 步骤1: 提交订单 ====================
            api_success = False
            returned_order = None
            api_error_msg = None

            try:
                placed_order = await self.engine.place_order(order)
                self.state.add_order(placed_order)
                api_success = True
                returned_order = placed_order

                self.logger.info(
                    f"📤 挂单API调用成功: {placed_order.order_id} "
                    f"{placed_order.side.value} {placed_order.amount} @ ${placed_order.price}"
                )

            except Exception as e:
                api_success = False
                api_error_msg = str(e)

                self.logger.warning(
                    f"❌ 挂单API调用失败: {e}\n"
                    f"   ⚠️ 注意：订单可能已提交但返回失败\n"
                    f"   将执行最终验证确认订单是否真实存在"
                )

            # ==================== 步骤2: 最终验证 ====================
            # 🔥 关键：无论API成功还是失败，都执行最终验证

            self.logger.info("🔍 执行最终验证：从交易所查询订单...")
            await asyncio.sleep(1.0)  # 等待订单状态稳定

            verified_order = await self._final_verification(
                returned_order.order_id if returned_order else None,
                order
            )

            # ==================== 步骤3: 根据验证结果决定下一步 ====================
            if verified_order:
                # 验证通过：订单确实存在
                if api_success:
                    self.logger.info(
                        f"✅ 挂单成功: API成功 + 最终验证通过\n"
                        f"   订单ID: {verified_order.id}"
                    )
                else:
                    self.logger.warning(
                        f"⚠️ 挂单成功（特殊情况）: API失败 + 最终验证通过\n"
                        f"   订单ID: {verified_order.id}\n"
                        f"   API错误: {api_error_msg}\n"
                        f"   说明：订单已提交但API返回失败"
                    )

                # 将验证通过的订单转换为GridOrder格式返回
                if returned_order:
                    return returned_order
                else:
                    # API失败但订单存在，需要添加到状态
                    grid_order = GridOrder(
                        order_id=verified_order.id,
                        grid_id=order.grid_id,
                        side=order.side,
                        price=verified_order.price,
                        amount=verified_order.amount,
                        status=GridOrderStatus.PENDING
                    )
                    self.state.add_order(grid_order)
                    return grid_order

            else:
                # 验证失败：订单确实不存在
                if api_success:
                    self.logger.error(
                        f"🚨 挂单失败（异常情况）: API成功 + 最终验证失败\n"
                        f"   API返回订单ID: {returned_order.order_id if returned_order else 'None'}\n"
                        f"   但交易所查询不到该订单\n"
                        f"   可能原因：临时ID、订单被立即取消、API数据错误"
                    )
                    # 从本地状态移除
                    if returned_order:
                        self.state.remove_order(returned_order.order_id)
                else:
                    self.logger.error(
                        f"❌ 挂单失败: API失败 + 最终验证失败\n"
                        f"   API错误: {api_error_msg}\n"
                        f"   交易所也没有该订单"
                    )

                # 准备重试
                if attempt < max_attempts - 1:
                    self.logger.info(
                        f"⏳ 等待5秒后重试 {attempt+2}/{max_attempts}...")
                    await asyncio.sleep(5.0)  # 🔥 重试间隔5秒
                else:
                    self.logger.error(f"❌ 挂单最终失败: 已尝试{max_attempts}次")
                    return None

        return None

    async def _final_verification(
        self,
        returned_order_id: Optional[str],
        expected_order: GridOrder
    ) -> Optional['OrderData']:
        """
        最终验证：从交易所查询确认订单是否存在

        验证策略（两阶段）：
        1. 阶段1：如果有返回的订单ID，精确匹配
        2. 阶段2：根据订单特征模糊匹配（价格+数量+方向）

        Args:
            returned_order_id: API返回的订单ID（可能为None）
            expected_order: 期望的订单（包含价格、数量、方向）

        Returns:
            OrderData: 已确认存在的订单
            None: 订单不存在
        """
        # 获取当前所有开放订单
        try:
            open_orders = await self.engine.exchange.get_open_orders(
                symbol=self.config.symbol
            )
        except Exception as e:
            self.logger.error(f"❌ 获取开放订单失败: {e}")
            return None

        # 阶段1: 如果有返回的订单ID，先精确匹配
        if returned_order_id:
            for order in open_orders:
                if order.id == returned_order_id:
                    self.logger.info(
                        f"✅ 阶段1验证通过: 找到订单ID {returned_order_id}"
                    )
                    return order

            self.logger.warning(
                f"⚠️ 阶段1验证失败: 订单ID {returned_order_id} 不存在"
            )

        # 阶段2: 根据订单特征模糊匹配（防止ID丢失或API未返回ID）
        for order in open_orders:
            if self._is_matching_order(order, expected_order):
                self.logger.info(
                    f"✅ 阶段2验证通过: 找到匹配订单 {order.id}\n"
                    f"   价格: ${order.price} (期望: ${expected_order.price})\n"
                    f"   数量: {order.amount} (期望: {expected_order.amount})\n"
                    f"   方向: {order.side.value} (期望: {expected_order.side.value})"
                )
                return order

        # 两个阶段都失败
        self.logger.error(
            f"❌ 最终验证失败: 未找到匹配订单\n"
            f"   期望订单: {expected_order.side.value} "
            f"{expected_order.amount} @ ${expected_order.price}"
        )
        return None

    def _is_matching_order(
        self,
        actual: 'OrderData',
        expected: GridOrder
    ) -> bool:
        """
        判断订单是否匹配

        验证维度：
        1. 方向匹配（必须完全一致）
        2. 价格匹配（允许0.1%误差）
        3. 数量匹配（允许0.01%误差）

        Args:
            actual: 交易所返回的实际订单
            expected: 期望的订单

        Returns:
            bool: 是否匹配
        """
        # 1. 方向必须完全一致
        if actual.side.value != expected.side.value:
            return False

        # 2. 价格匹配（允许0.1%误差）
        price_diff = abs(actual.price - expected.price)
        price_tolerance = expected.price * Decimal('0.001')  # 0.1%
        price_match = price_diff <= price_tolerance

        # 3. 数量匹配（允许0.01%误差）
        amount_diff = abs(actual.amount - expected.amount)
        amount_tolerance = expected.amount * Decimal('0.0001')  # 0.01%
        amount_match = amount_diff <= amount_tolerance

        return price_match and amount_match
