"""
小时级盈亏统计模块

每小时独立统计交易数据，整点生成CSV报告
"""

import asyncio
import csv
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional
import logging

from .models.volume_maker_statistics import CycleResult, CycleStatus


class HourlyStatistics:
    """单个小时的统计数据"""

    def __init__(self, hour_start: datetime):
        self.hour_start = hour_start
        self.hour_end = hour_start + timedelta(hours=1)

        # 轮次统计
        self.total_cycles: int = 0
        self.successful_cycles: int = 0
        self.failed_cycles: int = 0
        self.timeout_cycles: int = 0

        # 盈亏统计
        self.total_pnl: Decimal = Decimal("0")
        self.total_fee: Decimal = Decimal("0")
        self.net_pnl: Decimal = Decimal("0")
        self.profit_cycles: int = 0
        self.loss_cycles: int = 0
        self.profit_rate: float = 0.0

        # 时间统计
        self.total_wait_time: float = 0.0
        self.wait_time_count: int = 0
        self.avg_wait_time: float = 0.0

        # 比例统计
        self.total_quantity_ratio: float = 0.0
        self.quantity_ratio_count: int = 0
        self.avg_quantity_ratio: float = 0.0

        # 详细记录
        self.cycles: List[CycleResult] = []

    def add_cycle(self, cycle: CycleResult) -> None:
        """添加一个交易轮次"""
        self.cycles.append(cycle)
        self.total_cycles += 1

        # 更新状态计数
        if cycle.status == CycleStatus.SUCCESS:
            self.successful_cycles += 1
        elif cycle.status == CycleStatus.FAILED:
            self.failed_cycles += 1
        elif cycle.status == CycleStatus.TIMEOUT:
            self.timeout_cycles += 1

        # 更新盈亏
        self.total_pnl += cycle.pnl
        self.total_fee += cycle.fee
        self.net_pnl = self.total_pnl - self.total_fee

        # 更新盈利/亏损订单统计
        if cycle.status == CycleStatus.SUCCESS and cycle.pnl != Decimal("0"):
            if cycle.pnl > Decimal("0"):
                self.profit_cycles += 1
            else:
                self.loss_cycles += 1

        # 计算盈利百分比
        completed_trades = self.profit_cycles + self.loss_cycles
        if completed_trades > 0:
            self.profit_rate = (self.profit_cycles / completed_trades) * 100

        # 更新等待时间统计
        if cycle.wait_time is not None:
            self.total_wait_time += cycle.wait_time
            self.wait_time_count += 1
            self.avg_wait_time = self.total_wait_time / self.wait_time_count

        # 更新数量比例统计
        if cycle.quantity_ratio is not None:
            self.total_quantity_ratio += cycle.quantity_ratio
            self.quantity_ratio_count += 1
            self.avg_quantity_ratio = self.total_quantity_ratio / self.quantity_ratio_count


class HourlyStatisticsTracker:
    """小时级统计跟踪器"""

    def __init__(self, output_dir: str = "logs/hourly_stats"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger(self.__class__.__name__)

        # 当前小时的统计
        self.current_hour_stats: Optional[HourlyStatistics] = None
        self.current_hour: Optional[datetime] = None

        # 历史小时统计（保留最近24小时）
        self.hourly_stats_history: Dict[datetime, HourlyStatistics] = {}

        # 后台任务
        self._check_task: Optional[asyncio.Task] = None
        self._should_stop = False

    def start(self) -> None:
        """启动小时统计跟踪"""
        self.logger.info("📊 启动小时级统计跟踪...")
        self._should_stop = False
        self._check_task = asyncio.create_task(self._hourly_check_loop())
        self.logger.info("✅ 小时级统计跟踪已启动")

    async def stop(self) -> None:
        """停止小时统计跟踪"""
        self.logger.info("⏹️ 停止小时级统计跟踪...")
        self._should_stop = True
        if self._check_task:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass

        # 导出当前小时的统计（如果有）
        if self.current_hour_stats and self.current_hour_stats.total_cycles > 0:
            await self._export_hour_statistics(self.current_hour_stats)

        self.logger.info("✅ 小时级统计跟踪已停止")

    def add_cycle(self, cycle: CycleResult) -> None:
        """添加一个交易轮次到当前小时统计"""
        cycle_hour = self._get_hour_start(cycle.end_time)

        # 如果是新的小时，创建新的统计对象
        if self.current_hour != cycle_hour:
            # 导出上一个小时的统计（如果有）
            if self.current_hour_stats and self.current_hour_stats.total_cycles > 0:
                asyncio.create_task(
                    self._export_hour_statistics(self.current_hour_stats))
                # 保存到历史记录
                self.hourly_stats_history[self.current_hour] = self.current_hour_stats

            # 创建新的小时统计
            self.current_hour = cycle_hour
            self.current_hour_stats = HourlyStatistics(cycle_hour)
            self.logger.info(f"📊 开始新的小时统计: {self._format_hour(cycle_hour)}")

        # 添加到当前小时统计
        self.current_hour_stats.add_cycle(cycle)

        # 清理旧的历史记录（只保留最近24小时）
        self._cleanup_old_history()

    async def _hourly_check_loop(self) -> None:
        """定期检查是否到达整点"""
        while not self._should_stop:
            try:
                now = datetime.now()

                # 检查是否到达整点（精确到分钟）
                if now.minute == 0 and now.second < 30:  # 整点后30秒内
                    # 获取上一个小时
                    last_hour = self._get_hour_start(now - timedelta(hours=1))

                    # 检查是否需要导出上一个小时的统计
                    if (self.current_hour == last_hour and
                        self.current_hour_stats and
                            self.current_hour_stats.total_cycles > 0):

                        self.logger.info(
                            f"⏰ 到达整点，导出小时统计: {self._format_hour(last_hour)}")
                        await self._export_hour_statistics(self.current_hour_stats)

                        # 保存到历史记录
                        self.hourly_stats_history[self.current_hour] = self.current_hour_stats

                        # 准备新的小时统计
                        self.current_hour = self._get_hour_start(now)
                        self.current_hour_stats = HourlyStatistics(
                            self.current_hour)

                        # 等待一段时间避免重复触发
                        await asyncio.sleep(60)

                # 每30秒检查一次
                await asyncio.sleep(30)

            except Exception as e:
                self.logger.error(f"❌ 小时检查循环出错: {e}", exc_info=True)
                await asyncio.sleep(30)

    async def _export_hour_statistics(self, stats: HourlyStatistics) -> None:
        """导出小时统计到CSV文件"""
        try:
            # 生成文件名
            hour_str = stats.hour_start.strftime("%Y%m%d_%H")
            summary_file = self.output_dir / f"hourly_summary_{hour_str}.csv"
            details_file = self.output_dir / f"hourly_details_{hour_str}.csv"

            # 导出统计摘要
            await self._export_summary(stats, summary_file)

            # 导出详细记录
            await self._export_details(stats, details_file)

            self.logger.info(
                f"✅ 已导出小时统计: {self._format_hour(stats.hour_start)} - "
                f"总轮次: {stats.total_cycles}, 净盈亏: ${stats.net_pnl:.6f}"
            )

        except Exception as e:
            self.logger.error(f"❌ 导出小时统计失败: {e}", exc_info=True)

    async def _export_summary(self, stats: HourlyStatistics, filepath: Path) -> None:
        """导出统计摘要到CSV"""
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # 写入标题
            writer.writerow(['小时统计摘要'])
            writer.writerow(
                ['时间段', f"{self._format_hour(stats.hour_start)} - {self._format_hour(stats.hour_end)}"])
            writer.writerow([])

            # 轮次统计
            writer.writerow(['轮次统计'])
            writer.writerow(['指标', '数值'])
            writer.writerow(['总轮次', stats.total_cycles])
            writer.writerow(['成功轮次', stats.successful_cycles])
            writer.writerow(['失败轮次', stats.failed_cycles])
            writer.writerow(['超时轮次', stats.timeout_cycles])
            success_rate = (stats.successful_cycles /
                            stats.total_cycles * 100) if stats.total_cycles > 0 else 0
            writer.writerow(['成功率', f'{success_rate:.2f}%'])
            writer.writerow([])

            # 盈亏统计
            writer.writerow(['盈亏统计'])
            writer.writerow(['指标', '数值'])
            writer.writerow(['总盈亏', f'${stats.total_pnl:.6f}'])
            writer.writerow(['总手续费', f'${stats.total_fee:.6f}'])
            writer.writerow(['净盈亏', f'${stats.net_pnl:.6f}'])
            writer.writerow(['盈利订单数', stats.profit_cycles])
            writer.writerow(['亏损订单数', stats.loss_cycles])
            writer.writerow(['盈利百分比', f'{stats.profit_rate:.2f}%'])
            writer.writerow([])

            # 时间统计
            writer.writerow(['时间统计'])
            writer.writerow(['指标', '数值'])
            writer.writerow(['平均等待时间', f'{stats.avg_wait_time:.2f}s'])
            writer.writerow(['总等待时间', f'{stats.total_wait_time:.2f}s'])
            writer.writerow([])

            # 比例统计
            writer.writerow(['比例统计'])
            writer.writerow(['指标', '数值'])
            writer.writerow(['平均数量比例', f'{stats.avg_quantity_ratio:.2f}%'])

    async def _export_details(self, stats: HourlyStatistics, filepath: Path) -> None:
        """导出详细记录到CSV"""
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # 写入表头
            writer.writerow([
                '轮次ID', '状态', '方向', '成交价', '平仓价',
                '盈亏', '手续费', '时长(秒)', '等待时间(秒)', '数量比例(%)',
                '平仓原因', '开始时间', '结束时间'
            ])

            # 写入每条记录
            for cycle in stats.cycles:
                status_map = {
                    CycleStatus.SUCCESS: '成功',
                    CycleStatus.FAILED: '失败',
                    CycleStatus.TIMEOUT: '超时',
                    CycleStatus.PARTIAL_FILL: '部分成交',
                    CycleStatus.CANCELLED: '取消'
                }

                # 平仓原因映射
                reason_map = {
                    'price_change': '价格变化',
                    'quantity_reversal': '数量反转',
                    'timeout': '超时',
                    'interval': '固定间隔',
                    'immediate': '立即平仓',
                    'error': '异常'
                }

                writer.writerow([
                    cycle.cycle_id,
                    status_map.get(cycle.status, cycle.status.value),
                    '买' if cycle.filled_side == 'buy' else '卖' if cycle.filled_side else '-',
                    f'{cycle.filled_price:.2f}' if cycle.filled_price else '-',
                    f'{cycle.close_price:.2f}' if cycle.close_price else '-',
                    f'{cycle.pnl:.6f}',
                    f'{cycle.fee:.6f}',
                    f'{cycle.duration.total_seconds():.0f}',
                    f'{cycle.wait_time:.1f}' if cycle.wait_time is not None else '-',
                    f'{cycle.quantity_ratio:.1f}' if cycle.quantity_ratio is not None else '-',
                    reason_map.get(
                        cycle.close_reason, cycle.close_reason) if cycle.close_reason else '-',
                    cycle.start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    cycle.end_time.strftime('%Y-%m-%d %H:%M:%S')
                ])

    def _get_hour_start(self, dt: datetime) -> datetime:
        """获取指定时间所在小时的开始时间"""
        return dt.replace(minute=0, second=0, microsecond=0)

    def _format_hour(self, dt: datetime) -> str:
        """格式化小时时间"""
        return dt.strftime('%Y-%m-%d %H:00')

    def _cleanup_old_history(self) -> None:
        """清理超过24小时的历史记录"""
        if not self.hourly_stats_history:
            return

        cutoff_time = datetime.now() - timedelta(hours=24)
        cutoff_hour = self._get_hour_start(cutoff_time)

        # 删除超过24小时的记录
        keys_to_delete = [
            hour for hour in self.hourly_stats_history.keys()
            if hour < cutoff_hour
        ]

        for key in keys_to_delete:
            del self.hourly_stats_history[key]

        if keys_to_delete:
            self.logger.debug(f"清理了 {len(keys_to_delete)} 个旧的小时统计记录")
