"""
余额监控模块

提供账户余额定期监控和更新
"""

import asyncio
from typing import Optional
from decimal import Decimal
from datetime import datetime

from ....logging import get_logger


class BalanceMonitor:
    """
    账户余额监控管理器

    职责：
    1. 定期查询账户余额（REST API）
    2. 更新现货余额、抵押品余额、订单冻结余额
    3. 为本金保护、止盈、剥头皮管理器提供初始本金
    """

    def __init__(self, engine, config, coordinator, update_interval: int = 10):
        """
        初始化余额监控器

        Args:
            engine: 执行引擎
            config: 网格配置
            coordinator: 协调器引用（用于访问各种管理器）
            update_interval: 余额更新间隔（秒）
        """
        self.logger = get_logger(__name__)
        self.engine = engine
        self.config = config
        self.coordinator = coordinator
        self._update_interval = update_interval

        # 余额数据
        self._spot_balance: Decimal = Decimal('0')  # 现货余额（未用作保证金）
        self._collateral_balance: Decimal = Decimal('0')  # 抵押品余额（用作保证金）
        self._order_locked_balance: Decimal = Decimal('0')  # 订单冻结余额
        self._last_balance_update: Optional[datetime] = None

        # 监控任务
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None

    async def start_monitoring(self):
        """启动余额监控"""
        if self._running:
            self.logger.warning("余额监控已经在运行")
            return

        self._running = True

        # 立即更新一次余额
        await self.update_balance()

        # 启动监控循环
        self._monitor_task = asyncio.create_task(self._balance_monitor_loop())
        self.logger.info(f"✅ 账户余额轮询已启动（间隔{self._update_interval}秒）")

    async def stop_monitoring(self):
        """停止余额监控"""
        self._running = False

        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self.logger.info("✅ 余额监控已停止")

    async def _balance_monitor_loop(self):
        """余额监控循环"""
        self.logger.info("💰 账户余额监控循环已启动")

        while self._running:
            try:
                await asyncio.sleep(self._update_interval)
                await self.update_balance()
            except asyncio.CancelledError:
                self.logger.info("💰 余额监控循环被取消")
                break
            except Exception as e:
                self.logger.error(f"❌ 余额更新失败: {e}")
                await asyncio.sleep(self._update_interval)

    async def update_balance(self):
        """
        更新账户余额

        从 Backpack collateral API 获取USDC余额
        - spot_balance: availableQuantity（现货余额，未用作保证金）
        - collateral_balance: netEquity（账户总净资产，用于盈亏计算）
        - order_locked_balance: netEquityLocked（订单冻结的净资产）

        🔥 重要：盈亏计算使用 netEquity（总净资产），包含可用+冻结的所有资产
        🔥 不能用 netEquityAvailable，因为它不包含订单冻结资金，会导致盈亏计算错误
        """
        try:
            # 调用交易所API获取所有余额
            balances = await self.engine.exchange.get_balances()

            # 查找USDC余额
            usdc_balance = None
            for balance in balances:
                if balance.currency.upper() == 'USDC':
                    usdc_balance = balance
                    break

            if usdc_balance:
                # 从 raw_data 中提取详细的余额信息
                raw_data = usdc_balance.raw_data

                # 🔥 使用账户级别的净资产字段（用于准确的盈亏计算）
                # netEquity = 总净资产（包含未实现盈亏 + 订单冻结）
                # netEquityLocked = 订单冻结的净资产
                self._spot_balance = self._safe_decimal(
                    raw_data.get('availableQuantity', '0'))
                self._collateral_balance = self._safe_decimal(
                    raw_data.get('_account_netEquity', '0'))  # 🔥 使用总净资产（正确）
                self._order_locked_balance = self._safe_decimal(
                    raw_data.get('_account_netEquityLocked', '0'))  # 🔥 订单冻结资产

                self._last_balance_update = datetime.now()

                # 初始化各个管理器的本金（首次获取时）
                self._initialize_managers_capital()

                # 检查止盈条件（如果启用）
                if self.coordinator.take_profit_manager:
                    if self.coordinator.take_profit_manager.get_initial_capital() > 0:
                        if self.coordinator.take_profit_manager.check_take_profit_condition(
                            self._collateral_balance
                        ):
                            # 触发止盈
                            self.coordinator.take_profit_manager.activate(
                                self._collateral_balance)
                            # 🔥 使用新模块执行止盈重置
                            await self.coordinator.reset_manager.execute_take_profit_reset()

                # 只在首次或有显著变化时输出info，其他用debug
                if self._last_balance_update is None:
                    self.logger.info(
                        f"💰 初始余额: 现货=${self._spot_balance:,.2f}, "
                        f"抵押品=${self._collateral_balance:,.2f}, "
                        f"订单冻结=${self._order_locked_balance:,.2f}"
                    )
                else:
                    self.logger.debug(
                        f"💰 余额查询: 现货=${self._spot_balance:,.2f}, "
                        f"抵押品=${self._collateral_balance:,.2f}, "
                        f"订单冻结=${self._order_locked_balance:,.2f}"
                    )
            else:
                all_currencies = [b.currency for b in balances]
                self.logger.warning(
                    f"⚠️ 未找到USDC余额，所有币种: {', '.join(all_currencies) if all_currencies else '(空)'}"
                )

        except Exception as e:
            self.logger.error(f"❌ 获取账户余额失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    def _initialize_managers_capital(self):
        """初始化各个管理器的本金（首次获取时）"""
        # 本金保护管理器
        if self.coordinator.capital_protection_manager:
            if self.coordinator.capital_protection_manager.get_initial_capital() == Decimal('0'):
                self.coordinator.capital_protection_manager.initialize_capital(
                    self._collateral_balance)

        # 止盈管理器
        if self.coordinator.take_profit_manager:
            if self.coordinator.take_profit_manager.get_initial_capital() == Decimal('0'):
                self.coordinator.take_profit_manager.initialize_capital(
                    self._collateral_balance, is_reinit=False)

        # 剥头皮管理器
        if self.coordinator.scalping_manager:
            if self.coordinator.scalping_manager.get_initial_capital() == Decimal('0'):
                self.coordinator.scalping_manager.initialize_capital(
                    self._collateral_balance)

    def _safe_decimal(self, value, default='0') -> Decimal:
        """安全转换为Decimal"""
        try:
            if value is None:
                return Decimal(default)
            return Decimal(str(value))
        except:
            return Decimal(default)

    def get_balances(self) -> dict:
        """获取当前余额"""
        return {
            'spot_balance': self._spot_balance,
            'collateral_balance': self._collateral_balance,
            'order_locked_balance': self._order_locked_balance,
            'total_balance': self._spot_balance + self._collateral_balance + self._order_locked_balance,
            'last_update': self._last_balance_update
        }

    @property
    def spot_balance(self) -> Decimal:
        """现货余额"""
        return self._spot_balance

    @property
    def collateral_balance(self) -> Decimal:
        """抵押品余额"""
        return self._collateral_balance

    @property
    def order_locked_balance(self) -> Decimal:
        """订单冻结余额"""
        return self._order_locked_balance
