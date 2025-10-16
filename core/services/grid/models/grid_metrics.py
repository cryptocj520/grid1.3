"""
网格指标模型

定义网格系统的性能指标和统计数据
"""

from dataclasses import dataclass, field
from typing import List, Dict
from decimal import Decimal
from datetime import datetime, timedelta


@dataclass
class GridStatistics:
    """
    网格统计数据

    用于终端界面显示
    """

    # 基本信息
    grid_count: int                         # 总网格数
    grid_interval: Decimal                  # 网格间隔
    price_range: tuple                      # 价格区间 (lower, upper)

    # 当前状态
    current_price: Decimal                  # 当前价格
    current_grid_id: int                    # 当前网格位置
    current_position: Decimal               # 当前持仓
    average_cost: Decimal                   # 平均成本

    # 订单统计
    pending_buy_orders: int                 # 挂单中的买单数
    pending_sell_orders: int                # 挂单中的卖单数
    total_pending_orders: int               # 总挂单数

    # 成交统计
    filled_buy_count: int                   # 买单成交次数
    filled_sell_count: int                  # 卖单成交次数
    completed_cycles: int                   # 完成循环次数

    # 盈亏统计
    realized_profit: Decimal                # 已实现盈亏
    unrealized_profit: Decimal              # 未实现盈亏
    total_profit: Decimal                   # 总盈亏
    total_fees: Decimal                     # 总手续费
    net_profit: Decimal                     # 净利润
    profit_rate: Decimal                    # 收益率

    @property
    def unrealized_pnl(self) -> Decimal:
        """未实现盈亏的别名（用于与PositionData保持一致）"""
        return self.unrealized_profit

    @property
    def realized_pnl(self) -> Decimal:
        """已实现盈亏的别名（用于与PositionData保持一致）"""
        return self.realized_profit

    # 网格利用率
    grid_utilization: float                 # 网格利用率（百分比）

    # 资金信息（Backpack统一账户）
    spot_balance: Decimal                   # 现货余额（未用作保证金的）
    collateral_balance: Decimal             # 抵押品资金（用作保证金的）
    order_locked_balance: Decimal           # 订单冻结资金
    total_balance: Decimal                  # 总资金
    capital_utilization: float              # 资金利用率

    # 时间信息
    running_time: timedelta                 # 运行时长
    last_trade_time: datetime               # 最后成交时间

    # 监控方式
    monitoring_mode: str = "WebSocket"      # 订单监控方式：WebSocket 或 REST轮询
    # 持仓数据来源：WebSocket缓存 / PositionTracker / REST API
    position_data_source: str = "PositionTracker"

    # 本金保护模式状态
    capital_protection_enabled: bool = False  # 是否启用本金保护
    capital_protection_active: bool = False   # 本金保护是否已激活
    initial_capital: Decimal = Decimal('0')   # 初始本金
    capital_profit_loss: Decimal = Decimal('0')  # 本金盈亏

    # 价格脱离监控状态（价格移动网格专用）
    price_escape_active: bool = False          # 是否正在脱离
    price_escape_direction: str = ""           # 脱离方向：up/down
    price_escape_duration: int = 0             # 已脱离时长（秒）
    price_escape_timeout: int = 0              # 脱离超时阈值（秒）
    price_escape_remaining: int = 0            # 剩余时间（秒）

    # 止盈模式状态
    take_profit_enabled: bool = False          # 是否启用止盈模式
    take_profit_active: bool = False           # 止盈模式是否已激活
    take_profit_initial_capital: Decimal = Decimal('0')  # 止盈初始本金
    take_profit_current_profit: Decimal = Decimal('0')   # 当前盈利金额
    take_profit_profit_rate: Decimal = Decimal('0')      # 当前盈利率（百分比）
    take_profit_threshold: Decimal = Decimal('0')        # 止盈阈值（百分比）

    # 价格锁定模式状态
    price_lock_enabled: bool = False           # 是否启用价格锁定模式
    price_lock_active: bool = False            # 价格锁定是否已激活
    price_lock_threshold: Decimal = Decimal('0')  # 价格锁定阈值

    def to_display_dict(self) -> Dict:
        """转换为显示字典"""
        return {
            'grid_count': self.grid_count,
            'grid_interval': float(self.grid_interval),
            'price_range': {
                'lower': float(self.price_range[0]),
                'upper': float(self.price_range[1])
            },
            'current_price': float(self.current_price),
            'current_grid_id': self.current_grid_id,
            'current_position': float(self.current_position),
            'average_cost': float(self.average_cost),
            'pending_orders': {
                'buy': self.pending_buy_orders,
                'sell': self.pending_sell_orders,
                'total': self.total_pending_orders
            },
            'filled_orders': {
                'buy': self.filled_buy_count,
                'sell': self.filled_sell_count,
                'cycles': self.completed_cycles
            },
            'profit': {
                'realized': float(self.realized_profit),
                'unrealized': float(self.unrealized_profit),
                'total': float(self.total_profit),
                'fees': float(self.total_fees),
                'net': float(self.net_profit),
                'rate': float(self.profit_rate)
            },
            'grid_utilization': self.grid_utilization,
            'balance': {
                'spot': float(self.spot_balance),
                'collateral': float(self.collateral_balance),
                'order_locked': float(self.order_locked_balance),
                'total': float(self.total_balance),
                'utilization': self.capital_utilization
            },
            'time': {
                'running_time': str(self.running_time),
                'last_trade': self.last_trade_time.isoformat()
            }
        }


@dataclass
class GridMetrics:
    """
    网格性能指标

    用于分析网格系统的运行效果
    """

    # 收益指标
    total_profit: Decimal = Decimal('0')       # 总利润
    profit_rate: Decimal = Decimal('0')        # 收益率
    daily_profit: Decimal = Decimal('0')       # 日均收益

    # 交易指标
    total_trades: int = 0                      # 总交易次数
    win_trades: int = 0                        # 盈利交易次数
    loss_trades: int = 0                       # 亏损交易次数
    win_rate: float = 0.0                      # 胜率

    # 效率指标
    avg_profit_per_trade: Decimal = Decimal('0')  # 平均每笔收益
    avg_holding_time: timedelta = timedelta()     # 平均持仓时间
    grid_efficiency: float = 0.0                  # 网格效率

    # 风险指标
    max_drawdown: Decimal = Decimal('0')       # 最大回撤
    max_position: Decimal = Decimal('0')       # 最大持仓
    avg_position: Decimal = Decimal('0')       # 平均持仓

    # 成本指标
    total_fees: Decimal = Decimal('0')         # 总手续费
    fee_rate: Decimal = Decimal('0')           # 手续费率

    # 时间指标
    running_days: int = 0                      # 运行天数
    uptime_percentage: float = 100.0           # 运行时间百分比

    def calculate_metrics(self,
                          trades: List,
                          start_time: datetime,
                          end_time: datetime,
                          initial_balance: Decimal):
        """
        计算所有指标

        Args:
            trades: 交易记录列表
            start_time: 开始时间
            end_time: 结束时间
            initial_balance: 初始资金
        """
        if not trades:
            return

        # 计算交易指标
        self.total_trades = len(trades)

        # 计算胜率
        for trade in trades:
            if trade.get('profit', 0) > 0:
                self.win_trades += 1
            elif trade.get('profit', 0) < 0:
                self.loss_trades += 1

        if self.total_trades > 0:
            self.win_rate = (self.win_trades / self.total_trades) * 100

        # 计算收益率
        if initial_balance > 0:
            self.profit_rate = (self.total_profit / initial_balance) * 100

        # 计算运行天数
        running_time = end_time - start_time
        self.running_days = running_time.days

        # 计算日均收益
        if self.running_days > 0:
            self.daily_profit = self.total_profit / \
                Decimal(str(self.running_days))

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'profit': {
                'total': float(self.total_profit),
                'rate': float(self.profit_rate),
                'daily': float(self.daily_profit)
            },
            'trades': {
                'total': self.total_trades,
                'win': self.win_trades,
                'loss': self.loss_trades,
                'win_rate': self.win_rate
            },
            'efficiency': {
                'avg_profit_per_trade': float(self.avg_profit_per_trade),
                'grid_efficiency': self.grid_efficiency
            },
            'risk': {
                'max_drawdown': float(self.max_drawdown),
                'max_position': float(self.max_position),
                'avg_position': float(self.avg_position)
            },
            'cost': {
                'total_fees': float(self.total_fees),
                'fee_rate': float(self.fee_rate)
            },
            'time': {
                'running_days': self.running_days,
                'uptime_percentage': self.uptime_percentage
            }
        }
