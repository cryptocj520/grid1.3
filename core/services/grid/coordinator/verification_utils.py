"""
订单验证工具模块

提供订单取消验证、订单存在验证等通用工具方法
"""

import asyncio
from typing import List, Optional, Callable
from decimal import Decimal

from ....logging import get_logger
from ..models import GridOrder, GridOrderSide, GridOrderStatus


class OrderVerificationUtils:
    """
    订单验证工具类

    职责：
    1. 验证订单是否被取消
    2. 验证订单是否已挂出
    3. 批量订单取消并验证
    4. 提供可复用的验证逻辑
    """

    def __init__(self, exchange, symbol: str):
        """
        初始化验证工具

        Args:
            exchange: 交易所适配器
            symbol: 交易对符号
        """
        self.logger = get_logger(__name__)
        self.exchange = exchange
        self.symbol = symbol

    async def verify_no_orders_by_filter(
        self,
        order_filter: Callable[[GridOrder], bool],
        filter_description: str,
        max_retries: int = 3
    ) -> bool:
        """
        验证满足条件的订单已全部不存在（通用方法）

        Args:
            order_filter: 订单过滤函数，返回True表示需要验证的订单
            filter_description: 过滤条件描述（用于日志）
            max_retries: 最大重试次数

        Returns:
            True: 满足条件的订单已全部不存在
            False: 仍有满足条件的订单存在
        """
        for retry in range(max_retries):
            try:
                # 从交易所获取当前挂单
                exchange_orders = await self.exchange.get_open_orders(
                    symbol=self.symbol
                )

                # 过滤出需要验证的订单
                filtered_orders = [
                    order for order in exchange_orders
                    if order_filter(order)
                ]

                if len(filtered_orders) == 0:
                    self.logger.info(f"✅ 验证通过: 交易所确认无{filter_description}")
                    return True
                else:
                    self.logger.warning(
                        f"⚠️ 验证失败 (尝试{retry+1}/{max_retries}): "
                        f"交易所仍有{len(filtered_orders)}个{filter_description}"
                    )
                    for order in filtered_orders:
                        self.logger.warning(
                            f"   残留订单: {order.id}, "
                            f"价格=${order.price}"
                        )

                    # 如果不是最后一次重试，等待后再验证
                    if retry < max_retries - 1:
                        await asyncio.sleep(0.5)

            except Exception as e:
                self.logger.error(f"验证{filter_description}失败: {e}")
                if retry < max_retries - 1:
                    await asyncio.sleep(0.5)

        return False

    async def verify_no_sell_orders(self, max_retries: int = 3) -> bool:
        """
        验证没有卖单（做多网格剥头皮模式）

        Args:
            max_retries: 最大重试次数

        Returns:
            True: 确认没有卖单
            False: 仍有卖单
        """
        return await self.verify_no_orders_by_filter(
            order_filter=lambda order: order.side == GridOrderSide.SELL,
            filter_description="卖单",
            max_retries=max_retries
        )

    async def verify_no_buy_orders(self, max_retries: int = 3) -> bool:
        """
        验证没有买单（做空网格剥头皮模式）

        Args:
            max_retries: 最大重试次数

        Returns:
            True: 确认没有买单
            False: 仍有买单
        """
        return await self.verify_no_orders_by_filter(
            order_filter=lambda order: order.side == GridOrderSide.BUY,
            filter_description="买单",
            max_retries=max_retries
        )

    async def verify_all_orders_cancelled(self, max_retries: int = 3) -> bool:
        """
        验证所有订单已取消

        Args:
            max_retries: 最大重试次数

        Returns:
            True: 所有订单已取消
            False: 仍有未取消的订单
        """
        for retry in range(max_retries):
            try:
                # 从交易所获取当前挂单
                exchange_orders = await self.exchange.get_open_orders(
                    symbol=self.symbol
                )

                if len(exchange_orders) == 0:
                    self.logger.info(f"✅ 验证通过: 交易所确认无挂单")
                    return True
                else:
                    self.logger.warning(
                        f"⚠️ 验证失败 (尝试{retry+1}/{max_retries}): "
                        f"交易所仍有{len(exchange_orders)}个挂单"
                    )

                    # 如果不是最后一次重试，等待后再验证
                    if retry < max_retries - 1:
                        await asyncio.sleep(0.5)

            except Exception as e:
                self.logger.error(f"验证订单取消失败: {e}")
                if retry < max_retries - 1:
                    await asyncio.sleep(0.5)

        return False

    async def verify_order_exists(
        self,
        expected_order_id: str,
        max_retries: int = 3
    ) -> bool:
        """
        验证订单已在交易所挂出

        Args:
            expected_order_id: 预期的订单ID
            max_retries: 最大重试次数

        Returns:
            True: 订单已挂出
            False: 订单未挂出
        """
        for retry in range(max_retries):
            try:
                # 从交易所获取当前挂单
                exchange_orders = await self.exchange.get_open_orders(
                    symbol=self.symbol
                )

                # 查找订单
                found = False
                for order in exchange_orders:
                    if order.id == expected_order_id:
                        found = True
                        self.logger.info(
                            f"✅ 验证通过: 订单已挂出 "
                            f"{order.side.value} {order.amount}@${order.price}"
                        )
                        break

                if found:
                    return True
                else:
                    self.logger.warning(
                        f"⚠️ 验证失败 (尝试{retry+1}/{max_retries}): "
                        f"交易所未找到订单 {expected_order_id}"
                    )

                    if retry < max_retries - 1:
                        await asyncio.sleep(0.5)

            except Exception as e:
                self.logger.error(f"验证订单存在失败: {e}")
                if retry < max_retries - 1:
                    await asyncio.sleep(0.5)

        return False

    async def get_open_orders_count(self) -> int:
        """
        获取当前未成交订单数量

        Returns:
            未成交订单数量（如果失败返回-1）
        """
        try:
            open_orders = await self.exchange.get_open_orders(self.symbol)
            return len(open_orders)
        except Exception as e:
            self.logger.error(f"获取未成交订单数量失败: {e}")
            return -1
