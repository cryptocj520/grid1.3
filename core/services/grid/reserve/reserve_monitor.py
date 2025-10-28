"""
预留监控任务

职责：
1. 定期检查预留健康度
2. 自动触发补充
3. 记录监控日志

在网格运行期间后台运行，仅对现货生效。
"""

import asyncio
from typing import Optional
from decimal import Decimal

from ....logging import get_logger


class ReserveMonitor:
    """预留监控器（后台任务）"""

    def __init__(
        self,
        reserve_manager,
        exchange_adapter,
        symbol: str,
        check_interval: int = 60
    ):
        """
        初始化预留监控器

        Args:
            reserve_manager: SpotReserveManager实例
            exchange_adapter: 交易所适配器
            symbol: 交易对符号
            check_interval: 检查间隔（秒），默认60秒
        """
        self.reserve_manager = reserve_manager
        self.exchange = exchange_adapter
        self.symbol = symbol
        self.check_interval = check_interval
        self.logger = get_logger(self.__class__.__name__)

        self._task: Optional[asyncio.Task] = None
        self._should_stop = False

    async def start(self):
        """启动监控任务"""
        if self._task is not None:
            self.logger.warning("监控任务已在运行")
            return

        self.logger.info(
            f"🔍 启动预留监控: "
            f"间隔={self.check_interval}秒, "
            f"币种={self.reserve_manager.base_currency}"
        )

        self._should_stop = False
        self._task = asyncio.create_task(self._monitor_loop())

    async def stop(self):
        """停止监控任务"""
        if self._task is None:
            return

        self.logger.info("⏹️ 停止预留监控...")
        self._should_stop = True

        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        self._task = None
        self.logger.info("✅ 预留监控已停止")

    async def _monitor_loop(self):
        """监控循环"""
        try:
            while not self._should_stop:
                try:
                    # 等待间隔
                    await asyncio.sleep(self.check_interval)

                    # 检查是否需要补充
                    if self.reserve_manager.need_replenish():
                        await self._trigger_replenish()
                    else:
                        # 定期输出健康状态
                        status = self.reserve_manager.get_status()
                        self.logger.debug(
                            f"{status['emoji']} 预留健康: {status['health_percent']:.1f}%, "
                            f"剩余: {status['current_reserve']:.8f} {self.reserve_manager.base_currency}"
                        )

                except Exception as e:
                    self.logger.error(f"监控循环错误: {e}", exc_info=True)
                    await asyncio.sleep(self.check_interval)

        except asyncio.CancelledError:
            self.logger.info("监控任务已取消")

    async def _trigger_replenish(self):
        """触发补充"""
        try:
            status_before = self.reserve_manager.get_status()

            self.logger.warning(
                f"⚠️ 预留不足 ({status_before['health_percent']:.1f}%), "
                f"触发自动补充..."
            )

            # 获取当前价格
            ticker = await self.exchange.get_ticker(self.symbol)
            current_price = Decimal(str(ticker.last))

            # 计算补充数量
            replenish_amount = self.reserve_manager.calculate_replenish_amount()

            if replenish_amount <= 0:
                self.logger.info("计算结果：无需补充")
                return

            # 执行补充
            success = await self.reserve_manager.execute_replenish(
                replenish_amount,
                current_price
            )

            if success:
                status_after = self.reserve_manager.get_status()
                self.logger.info(
                    f"✅ 自动补充成功: "
                    f"{replenish_amount} {self.reserve_manager.base_currency}, "
                    f"健康度: {status_before['health_percent']:.1f}% → "
                    f"{status_after['health_percent']:.1f}%"
                )
            else:
                self.logger.error("❌ 自动补充失败，将在下次检查时重试")

        except Exception as e:
            self.logger.error(f"补充触发失败: {e}", exc_info=True)
