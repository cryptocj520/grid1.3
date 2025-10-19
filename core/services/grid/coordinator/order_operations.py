"""
订单操作模块

提供订单取消、挂单、验证等操作，并集成验证逻辑
"""

import asyncio
from typing import List, Optional, Callable
from decimal import Decimal

from ....logging import get_logger
from ..models import GridOrder, GridOrderSide, GridOrderStatus
from .verification_utils import OrderVerificationUtils


class OrderOperations:
    """
    订单操作管理器

    职责：
    1. 批量取消订单并验证
    2. 挂单并验证
    3. 取消特定类型订单并验证
    4. 统一错误处理和重试逻辑
    """

    def __init__(self, engine, state, config):
        """
        初始化订单操作管理器

        Args:
            engine: 执行引擎
            state: 网格状态
            config: 网格配置
        """
        self.logger = get_logger(__name__)
        self.engine = engine
        self.state = state
        self.config = config

        # 创建验证工具实例
        self.verifier = OrderVerificationUtils(engine.exchange, config.symbol)

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
        max_attempts: int = 3
    ) -> Optional[GridOrder]:
        """
        挂单并验证

        循环逻辑：
        1. 挂单
        2. 从交易所验证订单已挂出
        3. 如果未挂出，重新挂
        4. 重复最多max_attempts次

        Args:
            order: 待挂订单
            max_attempts: 最大尝试次数

        Returns:
            成功挂出的订单，失败返回None
        """
        for attempt in range(max_attempts):
            self.logger.info(
                f"🔄 挂单尝试 {attempt+1}/{max_attempts}..."
            )

            try:
                # 1. 挂单
                placed_order = await self.engine.place_order(order)
                self.state.add_order(placed_order)

                self.logger.info(
                    f"💰 订单已提交: {placed_order.side.value} "
                    f"{placed_order.amount}@${placed_order.price} "
                    f"(Grid {placed_order.grid_id})"
                )

                # 2. 等待让交易所处理挂单请求（增加等待时间，适应交易所延迟）
                await asyncio.sleep(1.0)

                # 3. 🔥 关键：从交易所验证订单已挂出（多次重试验证）
                verification_success = False
                max_verify_attempts = 3

                for verify_attempt in range(max_verify_attempts):
                    if await self.verifier.verify_order_exists(placed_order.order_id):
                        self.logger.info(
                            f"✅ 订单挂出成功（挂单尝试{attempt+1}次，验证尝试{verify_attempt+1}次）"
                        )
                        verification_success = True
                        break
                    else:
                        if verify_attempt < max_verify_attempts - 1:
                            self.logger.info(
                                f"⏳ 验证尝试{verify_attempt+1}/{max_verify_attempts}: "
                                f"订单未找到，等待1秒后重试验证..."
                            )
                            await asyncio.sleep(1.0)
                        else:
                            self.logger.warning(
                                f"⚠️ 验证失败（{max_verify_attempts}次尝试后订单仍未找到）"
                            )

                if verification_success:
                    return placed_order
                else:
                    self.logger.warning(
                        f"⚠️ 订单未在交易所找到，准备第{attempt+2}次挂单尝试..."
                    )
                    # 从本地状态移除，准备重试
                    self.state.remove_order(placed_order.order_id)

            except Exception as e:
                self.logger.error(f"挂单失败: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(0.5)

        # 达到最大尝试次数，挂单仍失败
        self.logger.error(
            f"❌ 挂单失败: 已尝试{max_attempts}次"
        )
        return None
