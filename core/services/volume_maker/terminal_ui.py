"""
刷量交易系统终端界面

使用Rich库实现实时监控界面
"""

import asyncio
from typing import Optional, List
from datetime import datetime
from decimal import Decimal

from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, TextColumn

from .implementations.volume_maker_service_impl import VolumeMakerServiceImpl
from .models.volume_maker_statistics import VolumeMakerStatistics, CycleResult, CycleStatus
from ...adapters.exchanges.models import OrderBookData


class VolumeMakerTerminalUI:
    """
    刷量交易终端界面

    显示内容：
    1. 运行状态
    2. 轮次统计
    3. 交易量统计
    4. 盈亏统计
    5. 最近成交记录
    """

    def __init__(self, service: VolumeMakerServiceImpl):
        """
        初始化终端界面

        Args:
            service: 刷量服务
        """
        self.service = service
        self.console = Console()

        # 界面配置
        self.refresh_rate = 1  # 刷新频率（次/秒）
        self.history_limit = 50  # 显示历史记录数

        # 运行控制
        self._running = False
        self._live: Optional[Live] = None

    def create_header(self) -> Panel:
        """创建标题栏"""
        title = Text()
        title.append("🎯 刷量交易系统实时监控 ", style="bold cyan")
        title.append("v1.0", style="bold magenta")
        title.append(" - ", style="bold white")
        title.append(f"{self.service.config.exchange.upper()}/",
                     style="bold yellow")
        title.append(f"{self.service.config.symbol}", style="bold green")

        return Panel(title, style="bold white on blue")

    def create_status_panel(self, stats: VolumeMakerStatistics) -> Panel:
        """创建运行状态面板"""
        status_text = self.service.get_status_text()

        # 状态颜色
        if status_text == "运行中":
            status_color = "green"
            status_icon = "🟢"
        elif status_text == "已暂停":
            status_color = "yellow"
            status_icon = "🟡"
        else:
            status_color = "red"
            status_icon = "🔴"

        # 格式化运行时长
        running_time = str(stats.running_time).split('.')[0]  # 移除微秒

        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column("Key", style="cyan", width=12)
        table.add_column("Value", style="white")

        table.add_row("状态", f"{status_icon} {status_text}",
                      style=f"bold {status_color}")
        table.add_row("运行时长", running_time)
        table.add_row(
            "当前轮次", f"{stats.current_cycle} / {self.service.config.max_cycles}")
        table.add_row("连续失败", str(stats.consecutive_fails))

        return Panel(table, title="📊 运行状态", border_style="cyan")

    def create_cycle_stats_panel(self, stats: VolumeMakerStatistics) -> Panel:
        """创建轮次统计面板"""
        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column("Key", style="cyan", width=12)
        table.add_column("Value", style="white")

        success_rate = stats.get_success_rate()
        success_color = "green" if success_rate >= 80 else "yellow" if success_rate >= 60 else "red"

        table.add_row("总轮次", str(stats.total_cycles))
        table.add_row("成功", f"{stats.successful_cycles}", style="green")
        table.add_row("失败", f"{stats.failed_cycles}", style="red")
        table.add_row("超时", f"{stats.timeout_cycles}", style="yellow")
        table.add_row("成功率", f"{success_rate:.2f}%",
                      style=f"bold {success_color}")

        # 平均轮次时长
        avg_duration = stats.get_avg_cycle_duration()
        avg_duration_str = str(avg_duration).split('.')[
            0] if avg_duration else "N/A"
        table.add_row("平均时长", avg_duration_str)

        return Panel(table, title="🔄 轮次统计", border_style="blue")

    def create_volume_stats_panel(self, stats: VolumeMakerStatistics) -> Panel:
        """创建交易量统计面板"""
        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column("Key", style="cyan", width=12)
        table.add_column("Value", style="white")

        table.add_row(
            "总交易量", f"{stats.total_volume:.6f} BTC", style="bold yellow")
        table.add_row(
            "买入量", f"{stats.total_buy_volume:.6f} BTC", style="green")
        table.add_row("卖出量", f"{stats.total_sell_volume:.6f} BTC", style="red")

        return Panel(table, title="📈 交易量统计", border_style="yellow")

    def create_pnl_stats_panel(self, stats: VolumeMakerStatistics) -> Panel:
        """创建盈亏统计面板"""
        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column("Key", style="cyan", width=12)
        table.add_column("Value", style="white")

        # 净盈亏颜色
        net_pnl_color = "green" if stats.net_pnl >= 0 else "red"
        net_pnl_icon = "📈" if stats.net_pnl >= 0 else "📉"

        table.add_row(
            "净盈亏",
            f"{net_pnl_icon} ${stats.net_pnl:.6f} USDC",
            style=f"bold {net_pnl_color}"
        )
        table.add_row("总盈亏", f"${stats.total_pnl:.6f} USDC")
        table.add_row("总手续费", f"${stats.total_fee:.6f} USDC", style="dim")
        table.add_row("平均盈亏", f"${stats.avg_pnl_per_cycle:.6f} USDC")

        # 盈利百分比统计（紧凑显示）
        profit_rate_color = "green" if stats.profit_rate >= 50 else "yellow" if stats.profit_rate >= 30 else "red"
        completed_trades = stats.profit_cycles + stats.loss_cycles
        if completed_trades > 0:
            table.add_row(
                "盈亏比",
                f"🎯 {stats.profit_rate:.1f}% ({stats.profit_cycles}/{stats.loss_cycles})",
                style=f"bold {profit_rate_color}"
            )
        else:
            table.add_row("盈亏比", "N/A", style="dim")

        return Panel(table, title="💰 盈亏统计", border_style="green")

    def create_spread_stats_panel(self, stats: VolumeMakerStatistics) -> Panel:
        """创建价差统计面板"""
        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column("Key", style="cyan", width=15)
        table.add_column("Value", style="white")

        if stats.avg_spread > 0:
            table.add_row("平均价差", f"${stats.avg_spread:.2f} USDC")
            table.add_row(
                "最小价差", f"${stats.min_spread:.2f} USDC", style="green")
            table.add_row("最大价差", f"${stats.max_spread:.2f} USDC", style="red")
        else:
            table.add_row("平均价差", "N/A", style="dim")
            table.add_row("最小价差", "N/A", style="dim")
            table.add_row("最大价差", "N/A", style="dim")

        return Panel(table, title="📊 价差统计", border_style="magenta")

    def create_orderbook_panel(self) -> Panel:
        """创建实时订单簿面板"""
        # 获取 WebSocket 订单簿数据
        orderbook = self.service._latest_orderbook

        # 获取订单簿获取方式
        orderbook_method = self.service.config.orderbook_method.upper()

        # 判断数据源状态
        if orderbook_method == "WEBSOCKET":
            ws_status = "🟢 实时" if self.service._ws_orderbook_subscribed else "🔴 未连接"
            data_source = f"WebSocket {ws_status}"
        else:
            data_source = "REST API 📡"

        table = Table(show_header=True, box=None, padding=(0, 1), expand=True)
        table.add_column("卖单", style="red bold", width=25, justify="right")
        table.add_column("", width=2)  # 分隔符
        table.add_column("买单", style="green bold", width=25)

        if orderbook and orderbook.bids and orderbook.asks:
            # 显示卖5到卖1（从上到下）
            asks_to_show = list(reversed(orderbook.asks[:5]))  # 反转，让卖1在最下面

            # 补齐到5行
            while len(asks_to_show) < 5:
                asks_to_show.insert(0, None)

            # 卖单部分（卖5...卖1）
            for i, ask in enumerate(asks_to_show):
                if ask:
                    ask_text = f"${ask.price:,.2f} × {ask.size:.4f}"
                    table.add_row(ask_text, "", "")
                else:
                    table.add_row("-", "", "")

            # 分隔线
            table.add_row("─" * 23, "──", "─" * 23, style="dim")

            # 买单部分（买1...买5）
            bids_to_show = orderbook.bids[:5]

            for bid in bids_to_show:
                if bid:
                    bid_text = f"${bid.price:,.2f} × {bid.size:.4f}"
                    table.add_row("", "", bid_text)

            # 补齐到5行
            while len(bids_to_show) < 5:
                table.add_row("", "", "-")

            # 计算价差
            if orderbook.asks and orderbook.bids:
                spread = orderbook.asks[0].price - orderbook.bids[0].price
                spread_pct = (
                    spread / orderbook.bids[0].price * 100) if orderbook.bids[0].price > 0 else 0

                # 底部显示价差信息
                table.add_row("", "", "", end_section=True)
                spread_info = f"价差: ${spread:.2f} ({spread_pct:.3f}%)"
                table.add_row(
                    Text(spread_info, style="yellow bold", justify="center"),
                    "",
                    Text(f"更新: {orderbook.timestamp.strftime('%H:%M:%S')}",
                         style="dim", justify="center")
                )
        else:
            # 无订单簿数据
            table.add_row("", "", "")
            table.add_row("", "", "")
            table.add_row("", "📊", "", style="dim")
            table.add_row("", Text("暂无数据", style="dim yellow"),
                          "", style="dim")
            table.add_row("", "", "")
            table.add_row("", "", "")
            if orderbook_method == "WEBSOCKET" and not self.service._ws_orderbook_subscribed:
                table.add_row("", Text("等待WebSocket连接...", style="dim"), "")
            elif orderbook_method == "REST":
                table.add_row("", Text("等待数据加载...", style="dim"), "")

        title = f"📖 实时订单簿 ({data_source})"
        return Panel(table, title=title, border_style="blue")

    def create_recent_trades_panel(self, stats: VolumeMakerStatistics) -> Panel:
        """创建最近成交面板"""
        table = Table(show_header=True, box=None, padding=(0, 1))
        table.add_column("轮次", style="cyan", width=6)
        table.add_column("状态", width=10)
        table.add_column("方向", width=6)
        table.add_column("成交价", width=12, justify="right")
        table.add_column("平仓价", width=12, justify="right")
        table.add_column("盈亏", width=12, justify="right")
        table.add_column("时长", width=8)
        table.add_column("等待", width=8, justify="right")  # 新增：等待时间列
        table.add_column("比例", width=8, justify="right")  # 新增：数量比例列
        table.add_column("平仓原因", width=10)  # 新增：平仓原因列

        # 获取最近的记录
        recent_cycles = stats.recent_cycles[-self.history_limit:]

        if not recent_cycles:
            table.add_row("暂无数据", "", "", "", "", "",
                          "", "", "", "", style="dim")
        else:
            for cycle in reversed(recent_cycles):
                # 状态样式
                if cycle.status == CycleStatus.SUCCESS:
                    status_text = "✅ 成功"
                    status_style = "green"
                elif cycle.status == CycleStatus.FAILED:
                    status_text = "❌ 失败"
                    status_style = "red"
                elif cycle.status == CycleStatus.TIMEOUT:
                    status_text = "⏱️ 超时"
                    status_style = "yellow"
                else:
                    status_text = cycle.status.value
                    status_style = "dim"

                # 方向
                side_text = "🟢买" if cycle.filled_side == 'buy' else "🔴卖" if cycle.filled_side else "-"

                # 成交价
                filled_price_text = f"${cycle.filled_price:.2f}" if cycle.filled_price else "-"

                # 平仓价
                close_price_text = f"${cycle.close_price:.2f}" if cycle.close_price else "-"

                # 盈亏
                if cycle.pnl != Decimal("0"):
                    pnl_style = "green" if cycle.pnl >= 0 else "red"
                    pnl_icon = "+" if cycle.pnl >= 0 else ""
                    pnl_text = f"{pnl_icon}${cycle.pnl:.6f}"
                else:
                    pnl_style = "dim"
                    pnl_text = "-"

                # 时长
                duration_seconds = int(cycle.duration.total_seconds())
                duration_text = f"{duration_seconds}s"

                # 等待时间
                if cycle.wait_time is not None:
                    wait_text = f"{cycle.wait_time:.1f}s"
                    wait_style = "green" if cycle.wait_time < 5 else "yellow" if cycle.wait_time < 10 else "red"
                else:
                    wait_text = "-"
                    wait_style = "dim"

                # 数量比例
                if cycle.quantity_ratio is not None:
                    ratio_text = f"{cycle.quantity_ratio:.1f}%"
                    # 比例越大，颜色越深（说明买卖不平衡）
                    if cycle.quantity_ratio >= 200:
                        ratio_style = "bright_green"
                    elif cycle.quantity_ratio >= 150:
                        ratio_style = "green"
                    else:
                        ratio_style = "yellow"
                else:
                    ratio_text = "-"
                    ratio_style = "dim"

                # 平仓原因
                if cycle.close_reason:
                    reason_map = {
                        "price_change": "💹价格变化",
                        "quantity_reversal": "🔄数量反转",
                        "timeout": "⏱️超时",
                        "interval": "⏲️间隔",
                        "immediate": "⚡立即",
                        "error": "❌异常"
                    }
                    reason_text = reason_map.get(
                        cycle.close_reason, cycle.close_reason)
                    # 根据不同原因使用不同颜色
                    if cycle.close_reason == "quantity_reversal":
                        reason_style = "bright_magenta"
                    elif cycle.close_reason == "price_change":
                        reason_style = "bright_cyan"
                    elif cycle.close_reason == "timeout":
                        reason_style = "yellow"
                    elif cycle.close_reason == "immediate":
                        reason_style = "bright_green"
                    else:
                        reason_style = "white"
                else:
                    reason_text = "-"
                    reason_style = "dim"

                table.add_row(
                    f"#{cycle.cycle_id}",
                    status_text,
                    side_text,
                    filled_price_text,
                    close_price_text,
                    Text(pnl_text, style=pnl_style),
                    duration_text,
                    Text(wait_text, style=wait_style),
                    Text(ratio_text, style=ratio_style),
                    Text(reason_text, style=reason_style)
                )

        return Panel(table, title="📝 最近成交记录", border_style="white")

    def create_layout(self) -> Layout:
        """创建界面布局"""
        layout = Layout()

        # 顶部：标题
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body")
        )

        # 主体分为上下两部分：统计区 + 数据区
        layout["body"].split_column(
            Layout(name="stats", size=10),  # 统计区高度减小
            Layout(name="data")  # 数据区（订单簿和成交记录）
        )

        # 统计部分分为4列并排显示
        layout["stats"].split_row(
            Layout(name="col1"),  # 运行状态
            Layout(name="col2"),  # 轮次统计
            Layout(name="col3"),  # 交易量统计
            Layout(name="col4")   # 盈亏统计
        )

        # 数据区分为左右两部分：订单簿 + 成交记录（并排显示）
        layout["data"].split_row(
            Layout(name="orderbook", ratio=1),  # 左侧：订单簿
            Layout(name="trades", ratio=1)      # 右侧：成交记录
        )

        return layout

    def render(self) -> Layout:
        """渲染界面"""
        layout = self.create_layout()
        stats = self.service.get_statistics()

        # 填充内容
        layout["header"].update(self.create_header())
        layout["col1"].update(self.create_status_panel(stats))
        layout["col2"].update(self.create_cycle_stats_panel(stats))
        layout["col3"].update(self.create_volume_stats_panel(stats))
        layout["col4"].update(self.create_pnl_stats_panel(stats))

        # 中间显示实时订单簿
        layout["orderbook"].update(self.create_orderbook_panel())

        # 最下面显示最近成交
        layout["trades"].update(self.create_recent_trades_panel(stats))

        return layout

    async def run(self) -> None:
        """运行终端UI"""
        self._running = True

        try:
            with Live(
                self.render(),
                console=self.console,
                refresh_per_second=self.refresh_rate,
                screen=True
            ) as live:
                self._live = live

                while self._running:
                    try:
                        live.update(self.render())
                        await asyncio.sleep(1.0 / self.refresh_rate)
                    except KeyboardInterrupt:
                        # 🔥 立即响应 Ctrl+C
                        self.console.print(
                            "\n[yellow]检测到 Ctrl+C，正在停止...[/yellow]")
                        self._running = False
                        break

        except KeyboardInterrupt:
            self.console.print("\n[yellow]检测到 Ctrl+C，正在停止...[/yellow]")
        except Exception as e:
            self.console.print(f"\n[red]UI运行错误: {e}[/red]")
        finally:
            self._running = False
            self._live = None

    def stop(self) -> None:
        """停止UI（可以被信号处理器调用）"""
        self._running = False
        # 如果 Live 还在运行，尝试停止它
        if self._live:
            try:
                # Rich Live 会在上下文管理器退出时自动清理
                pass
            except:
                pass
