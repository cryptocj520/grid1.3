"""
网格交易系统终端界面

使用Rich库实现实时监控界面
"""

import asyncio
from typing import Optional
from datetime import timedelta
from decimal import Decimal

from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text

from ...logging import get_logger
from .models import GridStatistics, GridType
from .coordinator import GridCoordinator


class GridTerminalUI:
    """
    网格交易终端界面

    显示内容：
    1. 运行状态
    2. 订单统计
    3. 持仓信息
    4. 盈亏统计
    5. 最近成交订单
    """

    def __init__(self, coordinator: GridCoordinator):
        """
        初始化终端界面

        Args:
            coordinator: 网格协调器
        """
        self.logger = get_logger(__name__)
        self.coordinator = coordinator
        self.console = Console()

        # 界面配置
        self.refresh_rate = 1  # 刷新频率（次/秒）- 降低刷新率减少闪烁
        self.history_limit = 10  # 显示历史记录数

        # 运行控制
        self._running = False

        # 提取基础货币名称（从交易对符号中提取）
        # 例如: BTC_USDC_PERP -> BTC, HYPE_USDC_PERP -> HYPE
        symbol = self.coordinator.config.symbol
        self.base_currency = symbol.split('_')[0] if '_' in symbol else symbol

    def create_header(self, stats: GridStatistics) -> Panel:
        """创建标题栏"""
        # 判断网格类型（做多/做空）
        is_long = self.coordinator.config.grid_type in [
            GridType.LONG, GridType.MARTINGALE_LONG, GridType.FOLLOW_LONG]
        grid_type_text = "做多网格" if is_long else "做空网格"

        title = Text()
        title.append("🎯 网格交易系统实时监控 ", style="bold cyan")
        title.append("v2.5", style="bold magenta")
        title.append(" - ", style="bold white")
        title.append(
            f"{self.coordinator.config.exchange.upper()}/", style="bold yellow")
        title.append(f"{self.coordinator.config.symbol}", style="bold green")

        return Panel(title, style="bold white on blue")

    def create_status_panel(self, stats: GridStatistics) -> Panel:
        """创建运行状态面板"""
        # 判断网格类型（做多/做空）和模式（普通/马丁/价格移动）
        grid_type = self.coordinator.config.grid_type

        if grid_type == GridType.LONG:
            grid_type_text = "做多网格（普通）"
        elif grid_type == GridType.SHORT:
            grid_type_text = "做空网格（普通）"
        elif grid_type == GridType.MARTINGALE_LONG:
            grid_type_text = "做多网格（马丁）"
        elif grid_type == GridType.MARTINGALE_SHORT:
            grid_type_text = "做空网格（马丁）"
        elif grid_type == GridType.FOLLOW_LONG:
            grid_type_text = "做多网格（价格移动）"
        elif grid_type == GridType.FOLLOW_SHORT:
            grid_type_text = "做空网格（价格移动）"
        else:
            grid_type_text = grid_type.value

        status_text = self.coordinator.get_status_text()

        # 格式化运行时长
        running_time = str(stats.running_time).split('.')[0]  # 移除微秒

        # 🔥 获取剥头皮模式状态
        scalping_enabled = self.coordinator.config.scalping_enabled
        scalping_active = False
        if self.coordinator.scalping_manager:
            scalping_active = self.coordinator.scalping_manager.is_active()

        # 🛡️ 获取本金保护模式状态
        capital_protection_enabled = self.coordinator.config.capital_protection_enabled
        capital_protection_active = False
        if self.coordinator.capital_protection_manager:
            capital_protection_active = self.coordinator.capital_protection_manager.is_active()

        content = Text()
        content.append(
            f"├─ 网格策略: {grid_type_text} ({stats.grid_count}格)   ", style="white")
        content.append(f"状态: {status_text}", style="bold")
        content.append("\n")

        # 📊 显示马丁模式状态（如果启用）
        if self.coordinator.config.martingale_increment and self.coordinator.config.martingale_increment > 0:
            content.append("├─ 马丁模式: ", style="white")
            content.append("✅ 已启用", style="bold green")
            content.append(f"  |  递增: ", style="white")
            content.append(
                f"{self.coordinator.config.martingale_increment} {self.base_currency}", style="bold yellow")
            content.append("\n")

        # 🔥 显示剥头皮模式状态
        if scalping_enabled:
            content.append("├─ 剥头皮: ", style="white")
            if scalping_active:
                content.append("🔴 已激活", style="bold red")
            else:
                content.append("⚪ 待触发", style="bold cyan")
            # 🆕 显示触发次数（从启动就显示，包括0次）
            content.append(f"  |  触发次数: ", style="white")
            content.append(f"{stats.scalping_trigger_count}",
                           style="bold yellow")
            content.append("\n")

        # 🛡️ 显示本金保护模式状态
        if capital_protection_enabled:
            content.append("├─ 本金保护: ", style="white")
            if capital_protection_active:
                content.append("🟢 已触发", style="bold green")
            else:
                content.append("⚪ 待触发", style="bold cyan")
            # 🆕 显示触发次数（从启动就显示，包括0次）
            content.append(f"  |  触发次数: ", style="white")
            content.append(
                f"{stats.capital_protection_trigger_count}", style="bold yellow")
            content.append("\n")

        # 💰 显示止盈模式状态
        if stats.take_profit_enabled:
            content.append("├─ 止盈: ", style="white")
            if stats.take_profit_active:
                content.append("🔴 已触发", style="bold red")
            else:
                # 显示当前盈利率和阈值
                profit_rate = float(stats.take_profit_profit_rate)
                threshold = float(stats.take_profit_threshold)
                content.append("⚪ 待触发  |  ", style="bold cyan")
                if profit_rate >= 0:
                    content.append(
                        f"当前: +{profit_rate:.2f}%  阈值: {threshold:.2f}%", style="bold green")
                else:
                    content.append(
                        f"当前: {profit_rate:.2f}%  阈值: {threshold:.2f}%", style="bold red")
            # 🆕 显示触发次数（从启动就显示，包括0次）
            content.append(f"  |  触发次数: ", style="white")
            content.append(
                f"{stats.take_profit_trigger_count}", style="bold yellow")
            content.append("\n")

        # 🔒 显示价格锁定模式状态
        if stats.price_lock_enabled:
            content.append("├─ 价格锁定: ", style="white")
            if stats.price_lock_active:
                content.append("🔒 已激活 (冻结)", style="bold yellow")
            else:
                threshold = float(stats.price_lock_threshold)
                current = float(stats.current_price)
                content.append("⚪ 待触发  |  ", style="bold cyan")
                content.append(
                    f"当前: ${current:,.2f}  阈值: ${threshold:,.2f}", style="white")
            content.append("\n")

        # 🔄 显示价格脱离倒计时（价格移动网格专用）
        if stats.price_escape_active:
            content.append("├─ 价格脱离: ", style="white")
            direction_text = "⬇️ 向下" if stats.price_escape_direction == "down" else "⬆️ 向上"
            content.append(f"{direction_text} ", style="bold yellow")
            content.append(
                f"⏱️ {stats.price_escape_remaining}s", style="bold red")
            # 🆕 显示触发次数（从启动就显示，包括0次）
            content.append(f"  |  触发次数: ", style="white")
            content.append(
                f"{stats.price_escape_trigger_count}", style="bold yellow")
            content.append("\n")
        # 🆕 即使没有脱离，如果是价格移动网格，也显示历史触发次数
        elif self.coordinator.config.is_follow_mode():
            content.append("├─ 价格脱离: ", style="white")
            content.append("✅ 正常  ", style="bold green")
            content.append(f"|  历史触发次数: ", style="white")
            content.append(
                f"{stats.price_escape_trigger_count}", style="bold yellow")
            content.append("\n")

        content.append(
            f"├─ 价格区间: ${stats.price_range[0]:,.2f} - ${stats.price_range[1]:,.2f}  ", style="white")
        content.append(f"网格间隔: ${stats.grid_interval}  ", style="cyan")
        content.append(
            f"反手距离: {self.coordinator.config.reverse_order_grid_distance}格\n", style="magenta")

        # 🆕 显示单格金额（仅作为显示，无实质功能）
        content.append(f"├─ 单格金额: ", style="white")
        content.append(
            f"{self.coordinator.config.order_amount} {self.base_currency}  ", style="bold cyan")
        content.append(
            f"数量精度: {self.coordinator.config.quantity_precision}位\n", style="white")

        content.append(
            f"├─ 当前价格: ${stats.current_price:,.2f}             ", style="bold yellow")
        content.append(
            f"当前位置: Grid {stats.current_grid_id}/{stats.grid_count}\n", style="white")

        content.append(f"└─ 运行时长: {running_time}", style="white")

        return Panel(content, title="📊 运行状态", border_style="green")

    def create_orders_panel(self, stats: GridStatistics) -> Panel:
        """创建订单统计面板"""
        content = Text()

        # 🔥 显示监控方式
        monitoring_mode = getattr(stats, 'monitoring_mode', 'WebSocket')
        if monitoring_mode == "WebSocket":
            mode_icon = "📡"
            mode_style = "bold cyan"
        else:
            mode_icon = "📊"
            mode_style = "bold yellow"

        content.append(f"├─ 监控方式: ", style="white")
        content.append(f"{mode_icon} {monitoring_mode}", style=mode_style)
        content.append("\n")

        # 🔥 计算网格范围（根据修复后的网格顺序）
        # 做多网格：Grid 1 = 最低价，买单在下方，卖单在上方
        # 做空网格：Grid 1 = 最高价，卖单在上方，买单在下方
        is_long = self.coordinator.config.grid_type in [
            GridType.LONG, GridType.MARTINGALE_LONG, GridType.FOLLOW_LONG]

        if is_long:
            # 做多：买单在下方（Grid 1到current），卖单在上方（current+1到200）
            if stats.pending_buy_orders > 0:
                buy_range = f"Grid 1-{stats.current_grid_id}"
            else:
                buy_range = "无"

            if stats.pending_sell_orders > 0:
                sell_range = f"Grid {stats.current_grid_id + 1}-{stats.grid_count}"
            else:
                sell_range = "无"
        else:
            # 做空：卖单在上方（Grid 1到current），买单在下方（current+1到200）
            if stats.pending_sell_orders > 0:
                sell_range = f"Grid 1-{stats.current_grid_id}"
            else:
                sell_range = "无"

            if stats.pending_buy_orders > 0:
                buy_range = f"Grid {stats.current_grid_id + 1}-{stats.grid_count}"
            else:
                buy_range = "无"

        content.append(
            f"├─ 未成交买单: {stats.pending_buy_orders}个 ({buy_range}) ⏳\n", style="green")
        content.append(
            f"├─ 未成交卖单: {stats.pending_sell_orders}个 ({sell_range}) ⏳\n", style="red")

        # 🔥 显示剥头皮止盈订单（更详细）
        if self.coordinator.config.is_scalping_enabled():
            if self.coordinator.scalping_manager and self.coordinator.scalping_manager.is_active():
                tp_order = self.coordinator.scalping_manager.get_current_take_profit_order()
                if tp_order:
                    content.append(f"├─ 🎯 止盈订单: ", style="white")
                    content.append(
                        f"sell {abs(tp_order.amount):.4f}@${tp_order.price:,.2f} (Grid {tp_order.grid_id})",
                        style="bold yellow"
                    )
                    content.append("\n")
                else:
                    content.append(f"├─ 🎯 止盈订单: ", style="white")
                    content.append("⚠️ 未挂出", style="red")
                    content.append("\n")
            else:
                # 剥头皮模式启用但未激活
                content.append(f"├─ 🎯 止盈订单: ", style="white")
                content.append("⏳ 待触发", style="yellow")
                content.append("\n")

        content.append(
            f"└─ 总挂单数量: {stats.total_pending_orders}个", style="white")

        return Panel(content, title="📋 订单统计", border_style="blue")

    def create_position_panel(self, stats: GridStatistics) -> Panel:
        """创建持仓信息面板"""
        position_color = "green" if stats.current_position > 0 else "red" if stats.current_position < 0 else "white"
        position_type = "做多" if stats.current_position > 0 else "做空" if stats.current_position < 0 else "空仓"

        # 未实现盈亏颜色
        unrealized_color = "green" if stats.unrealized_profit > 0 else "red" if stats.unrealized_profit < 0 else "white"
        unrealized_sign = "+" if stats.unrealized_profit > 0 else ""

        content = Text()
        content.append(f"├─ 当前持仓: ", style="white")
        content.append(
            f"{stats.current_position:+.4f} {self.base_currency} ({position_type})      ", style=f"bold {position_color}")
        content.append(f"平均成本: ${stats.average_cost:,.2f}\n", style="white")

        # 🔥 显示持仓数据来源（实时）
        data_source = stats.position_data_source
        if "WebSocket" in data_source:
            source_color = "bold green"
            source_icon = "📡"
        elif "REST" in data_source:
            source_color = "bold yellow"
            source_icon = "🔄"
        else:
            source_color = "cyan"
            source_icon = "📊"

        content.append(f"├─ 数据来源: ", style="white")
        content.append(f"{source_icon} {data_source}\n", style=source_color)

        # 🛡️ 本金保护模式状态和余额显示
        if stats.capital_protection_enabled:
            # 显示本金保护状态
            if stats.capital_protection_active:
                status_text = "🟢 已触发"
                status_color = "bold green"
            else:
                status_text = "⚪ 待触发"
                status_color = "cyan"

            content.append(f"├─ 本金保护: ", style="white")
            content.append(f"{status_text}\n", style=status_color)

            # 显示初始本金
            content.append(
                f"├─ 初始本金: ${stats.initial_capital:,.3f} USDC      ", style="white")
            content.append(
                f"当前抵押品: ${stats.collateral_balance:,.3f} USDC\n", style="yellow")

            # 计算并显示盈亏
            profit_loss = stats.capital_profit_loss
            if profit_loss >= 0:
                pl_sign = "+"
                pl_color = "bold green"
                pl_emoji = "📈"
            else:
                pl_sign = ""
                pl_color = "bold red"
                pl_emoji = "📉"

            profit_loss_rate = (profit_loss / stats.initial_capital *
                                100) if stats.initial_capital > 0 else Decimal('0')
            content.append(f"├─ 本金盈亏: ", style="white")
            content.append(f"{pl_emoji} ", style=pl_color)
            content.append(
                f"{pl_sign}${profit_loss:,.3f} ({pl_sign}{profit_loss_rate:.2f}%)\n",
                style=pl_color
            )

        # 🔒 价格锁定模式状态
        if stats.price_lock_enabled:
            # 显示价格锁定状态
            if stats.price_lock_active:
                status_text = "🔒 已激活（冻结中）"
                status_color = "bold yellow"
            else:
                status_text = "⚪ 待触发"
                status_color = "cyan"

            content.append(f"├─ 价格锁定: ", style="white")
            content.append(f"{status_text}      ", style=status_color)
            content.append(
                f"阈值: ${stats.price_lock_threshold:,.2f}\n", style="white")

            # 显示其他余额信息
            content.append(
                f"├─ 现货余额: ${stats.spot_balance:,.2f} USDC      ", style="white")
            content.append(
                f"订单冻结: ${stats.order_locked_balance:,.2f} USDC\n", style="white")
            content.append(
                f"├─ 总资金: ${stats.total_balance:,.2f} USDC\n", style="bold cyan")
        else:
            # 未启用本金保护模式，显示常规余额信息
            content.append(
                f"├─ 现货余额: ${stats.spot_balance:,.2f} USDC      ", style="white")
            content.append(
                f"抵押品: ${stats.collateral_balance:,.2f} USDC\n", style="yellow")
            content.append(
                f"├─ 订单冻结: ${stats.order_locked_balance:,.2f} USDC      ", style="white")
            content.append(
                f"总资金: ${stats.total_balance:,.2f} USDC\n", style="cyan")

        # 未实现盈亏（始终显示）
        content.append(f"└─ 未实现盈亏: ", style="white")
        content.append(f"{unrealized_sign}${stats.unrealized_profit:,.2f} ",
                       style=f"bold {unrealized_color}")
        content.append(f"({unrealized_sign}{stats.unrealized_profit/abs(stats.current_position * stats.current_price) * 100 if stats.current_position != 0 else 0:.2f}%)",
                       style=unrealized_color)

        return Panel(content, title="💰 持仓信息", border_style="yellow")

    def create_pnl_panel(self, stats: GridStatistics) -> Panel:
        """创建盈亏统计面板"""
        # 总盈亏颜色
        total_color = "green" if stats.total_profit > 0 else "red" if stats.total_profit < 0 else "white"
        total_sign = "+" if stats.total_profit >= 0 else ""

        # 已实现盈亏颜色
        realized_color = "green" if stats.realized_profit > 0 else "red" if stats.realized_profit < 0 else "white"
        realized_sign = "+" if stats.realized_profit >= 0 else ""

        # 收益率颜色
        rate_color = "green" if stats.profit_rate > 0 else "red" if stats.profit_rate < 0 else "white"
        rate_sign = "+" if stats.profit_rate >= 0 else ""

        content = Text()
        content.append(f"├─ 已实现: ", style="white")
        content.append(
            f"{realized_sign}${stats.realized_profit:,.2f}             ", style=f"bold {realized_color}")
        content.append(
            f"网格收益: {realized_sign}${stats.realized_profit:,.2f}\n", style=realized_color)

        content.append(f"├─ 未实现: ", style="white")
        content.append(f"{'+' if stats.unrealized_profit >= 0 else ''}${stats.unrealized_profit:,.2f}             ",
                       style="cyan" if stats.unrealized_profit >= 0 else "red")
        content.append(f"手续费: -${stats.total_fees:,.2f}\n", style="red")

        content.append(f"└─ 总盈亏: ", style="white")
        content.append(f"{total_sign}${stats.total_profit:,.2f} ",
                       style=f"bold {total_color}")
        content.append(
            f"({rate_sign}{stats.profit_rate:.2f}%)  ", style=f"bold {rate_color}")
        content.append(
            f"净收益: {total_sign}${stats.net_profit:,.2f}", style=total_color)

        return Panel(content, title="🎯 盈亏统计", border_style="magenta")

    def create_trigger_panel(self, stats: GridStatistics) -> Panel:
        """创建触发统计面板"""
        content = Text()

        content.append(
            f"├─ 买单成交: {stats.filled_buy_count}次               ", style="green")
        content.append(f"卖单成交: {stats.filled_sell_count}次\n", style="red")

        content.append(
            f"├─ 完整循环: {stats.completed_cycles}次 (一买一卖)      ", style="yellow")
        content.append(f"网格利用率: {stats.grid_utilization:.1f}%\n", style="cyan")

        # 平均每次循环收益
        avg_cycle_profit = stats.realized_profit / \
            stats.completed_cycles if stats.completed_cycles > 0 else Decimal(
                '0')
        content.append(f"└─ 平均循环收益: ${avg_cycle_profit:,.2f}",
                       style="green" if avg_cycle_profit > 0 else "white")

        return Panel(content, title="🎯 触发统计", border_style="cyan")

    def create_recent_trades_table(self, stats: GridStatistics) -> Panel:
        """创建最近成交订单表格"""
        table = Table(show_header=True, header_style="bold magenta", box=None)

        table.add_column("时间", style="cyan", width=10)
        table.add_column("类型", width=4)
        table.add_column("价格", style="yellow", width=12)
        table.add_column("数量", style="white", width=12)
        table.add_column("网格层级", style="blue", width=10)

        # 获取最近交易记录
        trades = self.coordinator.tracker.get_trade_history(self.history_limit)

        for trade in reversed(trades[-5:]):  # 只显示最新5条
            time_str = trade['time'].strftime("%H:%M:%S")
            side = trade['side']
            side_style = "green" if side == "buy" else "red"
            price = f"${trade['price']:,.2f}"
            amount = f"{trade['amount']:.4f} {self.base_currency}"
            grid_text = f"Grid {trade['grid_id']}"

            table.add_row(
                time_str,
                f"[{side_style}]{side.upper()}[/{side_style}]",
                price,
                amount,
                grid_text
            )

        if not trades:
            table.add_row("--", "--", "--", "--", "--")

        return Panel(table, title="📈 最近成交订单 (最新5条)", border_style="green")

    def create_controls_panel(self) -> Panel:
        """创建控制命令面板"""
        content = Text()
        content.append("[P]", style="bold yellow")
        content.append("暂停  ", style="white")
        content.append("[R]", style="bold green")
        content.append("恢复  ", style="white")
        content.append("[S]", style="bold red")
        content.append("停止  ", style="white")
        content.append("[Q]", style="bold cyan")
        content.append("退出", style="white")

        return Panel(content, title="🔧 控制命令", border_style="white")

    def create_layout(self, stats: GridStatistics) -> Layout:
        """创建完整布局"""
        layout = Layout()

        layout.split_column(
            Layout(self.create_header(stats), size=3),
            Layout(name="main"),
            Layout(self.create_controls_panel(), size=3)
        )

        layout["main"].split_row(
            Layout(name="left"),
            Layout(name="right")
        )

        layout["left"].split_column(
            Layout(self.create_status_panel(stats)),
            Layout(self.create_orders_panel(stats)),
            Layout(self.create_trigger_panel(stats))
        )

        layout["right"].split_column(
            Layout(self.create_position_panel(stats)),
            Layout(self.create_pnl_panel(stats)),
            Layout(self.create_recent_trades_table(stats))
        )

        return layout

    async def run(self):
        """运行终端界面"""
        self._running = True

        # ✅ 在 Live 上下文之前打印启动信息
        self.console.print("\n[bold green]✅ 网格交易系统终端界面已启动[/bold green]")
        self.console.print("[cyan]提示: 使用 Ctrl+C 停止系统[/cyan]\n")

        # 短暂延迟，让启动信息显示
        await asyncio.sleep(1)

        # ✅ 清屏，避免之前的输出干扰
        self.console.clear()

        # 🔥 修复：先获取初始统计数据，避免在Live上下文初始化时阻塞
        self.console.print("[cyan]📊 正在获取初始统计数据...[/cyan]")
        try:
            initial_stats = await self.coordinator.get_statistics()
            self.console.print("[green]✅ 初始统计数据获取成功[/green]")
        except Exception as e:
            self.console.print(f"[red]❌ 获取初始统计数据失败: {e}[/red]")
            import traceback
            self.console.print(f"[yellow]{traceback.format_exc()}[/yellow]")
            # 使用空的统计数据作为fallback
            from .models import GridStatistics
            initial_stats = GridStatistics()

        self.console.print("[cyan]🖥️  正在启动Rich终端界面...[/cyan]")

        # 🔥 修复：检查是否使用全屏模式（可通过环境变量控制）
        import os
        use_fullscreen = os.getenv(
            'GRID_UI_FULLSCREEN', 'true').lower() == 'true'

        # 🔥 修复：使用try-except捕获Live初始化错误
        try:
            self.console.print(
                f"[yellow]📺 创建Live显示对象（全屏模式: {use_fullscreen}）...[/yellow]")
            live_display = Live(
                self.create_layout(initial_stats),
                refresh_per_second=self.refresh_rate,
                console=self.console,
                screen=use_fullscreen,  # 可配置的全屏模式
                transient=False  # 不使用临时显示
            )
            self.console.print("[green]✅ Live对象创建成功[/green]")
        except Exception as e:
            self.console.print(f"[red]❌ 创建Live对象失败: {e}[/red]")
            import traceback
            self.console.print(f"[yellow]{traceback.format_exc()}[/yellow]")

            # 如果全屏模式失败，尝试非全屏模式
            if use_fullscreen:
                self.console.print("[yellow]⚠️ 尝试使用非全屏模式...[/yellow]")
                try:
                    live_display = Live(
                        self.create_layout(initial_stats),
                        refresh_per_second=self.refresh_rate,
                        console=self.console,
                        screen=False,  # 非全屏模式
                        transient=False
                    )
                    self.console.print("[green]✅ 非全屏模式启动成功[/green]")
                except Exception as e2:
                    self.console.print(f"[red]❌ 非全屏模式也失败: {e2}[/red]")
                    return
            else:
                return

        self.console.print("[cyan]🚀 正在进入Live上下文...[/cyan]")

        # 🔥 添加日志，不使用console.print（因为Live会清除）
        self.logger.info("📺 正在进入Live上下文管理器...")

        with live_display as live:
            self.logger.info("✅ Rich Live上下文已启动，开始主循环")

            # 🔥 添加一个变量来跟踪是否成功进入主循环
            loop_started = False

            try:
                while self._running:
                    # 获取最新统计数据
                    try:
                        if not loop_started:
                            self.logger.info("🔄 主循环首次迭代开始...")

                        # 🔥 添加5秒超时保护
                        try:
                            stats = await asyncio.wait_for(
                                self.coordinator.get_statistics(),
                                timeout=5.0
                            )
                            if not loop_started:
                                self.logger.info("✅ 首次统计数据获取成功")
                        except asyncio.TimeoutError:
                            self.logger.error("⏰ 获取统计数据超时（5秒），跳过本次更新")
                            continue

                        # 更新界面
                        live.update(self.create_layout(stats))

                        if not loop_started:
                            self.logger.info("✅ 首次界面更新成功，UI已启动！")
                            loop_started = True
                    except Exception as e:
                        self.logger.error(f"❌ 更新界面失败: {e}")
                        import traceback
                        self.logger.error(f"详细错误: {traceback.format_exc()}")
                        # 继续运行，不要因为单次更新失败而停止

                    # 休眠
                    await asyncio.sleep(1 / self.refresh_rate)

            except KeyboardInterrupt:
                self.console.print("\n[yellow]收到退出信号...[/yellow]")
            finally:
                self._running = False

    def stop(self):
        """停止终端界面"""
        self._running = False
