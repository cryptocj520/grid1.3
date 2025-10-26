"""
刷量交易统计模型
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import List, Optional


class CycleStatus(Enum):
    """交易轮次状态"""
    SUCCESS = "success"              # 成功完成
    FAILED = "failed"                # 失败
    TIMEOUT = "timeout"              # 超时
    PARTIAL_FILL = "partial_fill"    # 部分成交
    CANCELLED = "cancelled"          # 取消


@dataclass
class CycleResult:
    """单次交易轮次结果"""
    cycle_id: int                           # 轮次ID
    status: CycleStatus                     # 状态
    start_time: datetime                    # 开始时间
    end_time: datetime                      # 结束时间
    duration: timedelta                     # 持续时长

    # 价格信息
    bid_price: Decimal                      # 买1价格
    ask_price: Decimal                      # 卖1价格
    spread: Decimal                         # 价差

    # 订单信息
    buy_order_id: Optional[str] = None      # 买单ID
    sell_order_id: Optional[str] = None     # 卖单ID
    filled_side: Optional[str] = None       # 成交方向（'buy' or 'sell'）
    filled_price: Optional[Decimal] = None  # 成交价格
    filled_amount: Optional[Decimal] = None  # 成交数量

    # 平仓信息
    close_price: Optional[Decimal] = None   # 平仓价格
    close_amount: Optional[Decimal] = None  # 平仓数量

    # 盈亏
    pnl: Decimal = Decimal("0")             # 本轮盈亏
    fee: Decimal = Decimal("0")             # 手续费

    # 等待时间
    wait_time: Optional[float] = None       # 等待价格变化时间（秒）
    quantity_ratio: Optional[float] = None  # 买卖单数量比例（百分比）
    # 平仓原因（price_change/quantity_reversal/timeout/immediate）
    close_reason: Optional[str] = None

    # 错误信息
    error_message: Optional[str] = None     # 错误信息


@dataclass
class VolumeMakerStatistics:
    """刷量交易统计数据"""

    # 运行状态
    start_time: datetime = field(default_factory=datetime.now)
    running_time: timedelta = field(
        default_factory=lambda: timedelta(seconds=0))
    is_running: bool = False
    is_paused: bool = False

    # 轮次统计
    total_cycles: int = 0                   # 总轮次
    successful_cycles: int = 0              # 成功轮次
    failed_cycles: int = 0                  # 失败轮次
    timeout_cycles: int = 0                 # 超时轮次
    current_cycle: int = 0                  # 当前轮次

    # 交易量统计
    total_buy_volume: Decimal = Decimal("0")    # 总买入量
    total_sell_volume: Decimal = Decimal("0")   # 总卖出量
    total_volume: Decimal = Decimal("0")        # 总交易量

    # 盈亏统计
    total_pnl: Decimal = Decimal("0")           # 总盈亏
    total_fee: Decimal = Decimal("0")           # 总手续费
    net_pnl: Decimal = Decimal("0")             # 净盈亏
    win_rate: float = 0.0                       # 胜率
    avg_pnl_per_cycle: Decimal = Decimal("0")   # 平均每轮盈亏
    profit_cycles: int = 0                      # 盈利订单数量
    loss_cycles: int = 0                        # 亏损订单数量
    profit_rate: float = 0.0                    # 盈利百分比

    # 价格统计
    avg_spread: Decimal = Decimal("0")          # 平均价差
    min_spread: Decimal = Decimal("999999")     # 最小价差
    max_spread: Decimal = Decimal("0")          # 最大价差

    # 最近交易记录
    recent_cycles: List[CycleResult] = field(default_factory=list)

    # 连续失败计数
    consecutive_fails: int = 0

    def update_from_cycle(self, result: CycleResult) -> None:
        """从轮次结果更新统计"""
        self.total_cycles += 1
        self.current_cycle = result.cycle_id

        # 更新状态计数
        if result.status == CycleStatus.SUCCESS:
            self.successful_cycles += 1
            self.consecutive_fails = 0
        elif result.status == CycleStatus.FAILED:
            self.failed_cycles += 1
            self.consecutive_fails += 1
        elif result.status == CycleStatus.TIMEOUT:
            self.timeout_cycles += 1
            self.consecutive_fails += 1

        # 更新交易量
        if result.filled_amount:
            if result.filled_side == 'buy':
                self.total_buy_volume += result.filled_amount
            elif result.filled_side == 'sell':
                self.total_sell_volume += result.filled_amount
            self.total_volume = self.total_buy_volume + self.total_sell_volume

        # 更新盈亏
        self.total_pnl += result.pnl
        self.total_fee += result.fee
        self.net_pnl = self.total_pnl - self.total_fee

        # 更新盈利/亏损订单统计
        if result.status == CycleStatus.SUCCESS and result.pnl != Decimal("0"):
            if result.pnl > Decimal("0"):
                self.profit_cycles += 1
            else:
                self.loss_cycles += 1

        # 更新胜率和盈利率
        if self.total_cycles > 0:
            self.win_rate = self.successful_cycles / self.total_cycles
            self.avg_pnl_per_cycle = self.net_pnl / \
                Decimal(str(self.total_cycles))

        # 计算盈利百分比（只统计有盈亏的订单）
        completed_trades = self.profit_cycles + self.loss_cycles
        if completed_trades > 0:
            self.profit_rate = (self.profit_cycles / completed_trades) * 100

        # 更新价差统计
        if result.spread > 0:
            spread_count = len(
                [c for c in self.recent_cycles if c.spread > 0]) + 1
            total_spread = sum(
                c.spread for c in self.recent_cycles if c.spread > 0) + result.spread
            self.avg_spread = total_spread / Decimal(str(spread_count))
            self.min_spread = min(self.min_spread, result.spread)
            self.max_spread = max(self.max_spread, result.spread)

        # 添加到最近记录
        self.recent_cycles.append(result)
        if len(self.recent_cycles) > 100:  # 保留最近100条
            self.recent_cycles.pop(0)

        # 更新运行时间
        self.running_time = datetime.now() - self.start_time

    def get_success_rate(self) -> float:
        """获取成功率（百分比）"""
        if self.total_cycles == 0:
            return 0.0
        return (self.successful_cycles / self.total_cycles) * 100

    def get_avg_cycle_duration(self) -> timedelta:
        """获取平均轮次时长"""
        if not self.recent_cycles:
            return timedelta(seconds=0)
        total_duration = sum((c.duration.total_seconds()
                             for c in self.recent_cycles), 0.0)
        return timedelta(seconds=total_duration / len(self.recent_cycles))

    def get_recent_pnl(self, count: int = 10) -> Decimal:
        """获取最近N轮的盈亏"""
        recent = self.recent_cycles[-count:]
        return sum(c.pnl for c in recent)

    def reset(self) -> None:
        """重置统计"""
        self.start_time = datetime.now()
        self.running_time = timedelta(seconds=0)
        self.total_cycles = 0
        self.successful_cycles = 0
        self.failed_cycles = 0
        self.timeout_cycles = 0
        self.current_cycle = 0
        self.total_buy_volume = Decimal("0")
        self.total_sell_volume = Decimal("0")
        self.total_volume = Decimal("0")
        self.total_pnl = Decimal("0")
        self.total_fee = Decimal("0")
        self.net_pnl = Decimal("0")
        self.win_rate = 0.0
        self.avg_pnl_per_cycle = Decimal("0")
        self.profit_cycles = 0
        self.loss_cycles = 0
        self.profit_rate = 0.0
        self.avg_spread = Decimal("0")
        self.min_spread = Decimal("999999")
        self.max_spread = Decimal("0")
        self.recent_cycles = []
        self.consecutive_fails = 0
