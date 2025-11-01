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

        # 💰 初始本金（独立维护，无论是否启用本金保护都记录）
        self._initial_capital: Decimal = Decimal('0')  # 启动时的初始账户权益

        # 🔥 现货模式专用：记录初始持仓和USDC
        self._initial_spot_position: Decimal = Decimal('0')  # 初始现货持仓数量
        self._initial_spot_usdc: Decimal = Decimal('0')  # 初始USDC余额
        self._initial_capital_with_btc_calculated: bool = False  # 🔥 标记初始本金是否已包含BTC价值

        # 🔥 现货模式专用：缓存基础货币（如BTC/UBTC）的总余额
        self._base_currency_total_balance: Decimal = Decimal(
            '0')  # 基础货币总余额（包括预留）

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

                # 🔥 支持多交易所：Backpack vs Hyperliquid vs Lighter
                exchange_name = self.config.exchange.lower() if hasattr(
                    self.config, 'exchange') else 'backpack'

                if exchange_name == 'hyperliquid':
                    # Hyperliquid格式：直接使用total作为余额
                    self._spot_balance = self._safe_decimal(
                        raw_data.get('free', '0'))
                    self._collateral_balance = self._safe_decimal(
                        raw_data.get('total', '0'))  # Hyperliquid的总余额
                    self._order_locked_balance = self._safe_decimal(
                        raw_data.get('used', '0'))  # 订单冻结资产
                elif exchange_name == 'lighter':
                    # Lighter格式：BalanceData 直接包含 free, used, total
                    # Lighter是合约交易所，只有USDC保证金
                    self._spot_balance = usdc_balance.free  # 可用余额
                    self._collateral_balance = usdc_balance.total  # 总余额（包含冻结）
                    self._order_locked_balance = usdc_balance.used  # 订单冻结资产

                    self.logger.debug(
                        f"📊 Lighter余额: 可用={self._spot_balance}, "
                        f"总额={self._collateral_balance}, 冻结={self._order_locked_balance}"
                    )
                else:
                    # Backpack格式：使用账户级别的净资产字段
                    # netEquity = 总净资产（包含未实现盈亏 + 订单冻结）
                    # netEquityLocked = 订单冻结的净资产
                    self._spot_balance = self._safe_decimal(
                        raw_data.get('availableQuantity', '0'))
                    self._collateral_balance = self._safe_decimal(
                        raw_data.get('_account_netEquity', '0'))  # 🔥 使用总净资产（正确）
                    self._order_locked_balance = self._safe_decimal(
                        raw_data.get('_account_netEquityLocked', '0'))  # 🔥 订单冻结资产

                self._last_balance_update = datetime.now()

                # 🔥 现货模式：同时查询并缓存基础货币（如UBTC）的总余额
                if self._is_spot_mode():
                    symbol_parts = self.config.symbol.split('/')
                    if len(symbol_parts) >= 1:
                        base_currency = symbol_parts[0]  # UBTC、ETH、SOL等
                        # 查询基础货币的总余额
                        for balance in balances:
                            if balance.currency == base_currency:
                                self._base_currency_total_balance = balance.total
                                self.logger.debug(
                                    f"📊 缓存{base_currency}总余额: {self._base_currency_total_balance}"
                                )
                                break

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
        # 💰 首先记录BalanceMonitor自己的初始本金（无论是否启用本金保护）
        if (self._initial_capital == Decimal('0') or not self._initial_capital_with_btc_calculated) and self._collateral_balance > 0:
            # 🔥 区分现货和合约模式
            if self._is_spot_mode():
                # 🔥 现货模式：初始本金 = 初始USDC + 初始BTC总价值（包括预留）
                # 重要：这里要统计所有BTC，不减去预留
                initial_btc_total = self._get_base_currency_total_balance()  # 获取BTC总余额
                initial_price = self._get_current_price()

                # 🔥 如果价格为0，说明系统刚启动，价格还没有更新，暂时只计算USDC
                # 等待下一次余额更新时再补充BTC价值
                if initial_price <= 0:
                    # 只在首次遇到价格为0时输出警告
                    if self._initial_capital == Decimal('0'):
                        self.logger.warning(
                            f"⚠️ 当前价格为0或无效，暂时只使用USDC作为初始本金: ${self._collateral_balance:,.3f} USDC"
                        )
                        self.logger.warning(
                            f"   BTC总量: {initial_btc_total}, 等待价格更新后将重新计算初始本金"
                        )
                        # 暂时只使用USDC作为初始本金
                        self._initial_capital = self._collateral_balance
                        self._initial_spot_usdc = self._collateral_balance
                        self._initial_spot_position = initial_btc_total
                        self._initial_capital_with_btc_calculated = False  # 标记未完整计算
                    # 不return，继续初始化其他管理器
                else:
                    # 价格有效，正常计算初始本金
                    self._initial_spot_usdc = self._collateral_balance  # USDC余额
                    self._initial_spot_position = initial_btc_total  # BTC总余额（包括预留）
                    initial_btc_value = abs(
                        initial_btc_total) * initial_price  # BTC总价值

                    self._initial_capital = self._initial_spot_usdc + initial_btc_value
                    self._initial_capital_with_btc_calculated = True  # 标记已完整计算

                    self.logger.info(
                        f"💰 现货初始本金已记录: ${self._initial_capital:,.3f} USDC "
                        f"(USDC: ${self._initial_spot_usdc:,.3f} + "
                        f"BTC总价值: ${initial_btc_value:,.3f}, BTC总量: {initial_btc_total})"
                    )

                    # 🔥 同步更新剥头皮管理器的本金（包含BTC价值）
                    if self.coordinator.scalping_manager:
                        old_capital = self.coordinator.scalping_manager.get_initial_capital()
                        if old_capital < self._initial_capital:
                            self.coordinator.scalping_manager.initialize_capital(
                                self._initial_capital, is_reinit=True)
                            self.logger.info(
                                f"💰 剥头皮管理器本金已同步更新: "
                                f"${old_capital:,.2f} → ${self._initial_capital:,.2f}"
                            )
            else:
                # 合约模式：初始本金 = 账户权益（保持原逻辑）
                self._initial_capital = self._collateral_balance
                self._initial_capital_with_btc_calculated = True  # 合约模式不需要BTC计算
                self.logger.info(
                    f"💰 初始本金已记录: ${self._initial_capital:,.3f} USDC")

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

        # 剥头皮管理器（首次初始化）
        if self.coordinator.scalping_manager:
            scalping_current_capital = self.coordinator.scalping_manager.get_initial_capital()

            if scalping_current_capital == Decimal('0'):
                # 首次初始化（启动时）
                # 🔥 现货模式：即使价格为0，也先用USDC初始化，后续会自动更新
                if self._is_spot_mode():
                    # 使用 collateral_balance 属性（会包含BTC价值，如果价格可用）
                    self.coordinator.scalping_manager.initialize_capital(
                        self.collateral_balance)
                else:
                    # 合约模式直接用USDC余额
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
        """
        获取当前余额

        🔥 重要：现货模式时，collateral_balance 需要通过属性获取
        因为属性会自动加上持仓价值（USDC + BTC价值）
        """
        return {
            'spot_balance': self._spot_balance,
            'collateral_balance': self.collateral_balance,  # 🔥 使用属性，不是私有变量
            'order_locked_balance': self._order_locked_balance,
            'total_balance': self._spot_balance + self.collateral_balance + self._order_locked_balance,  # 🔥 使用属性
            'last_update': self._last_balance_update
        }

    @property
    def spot_balance(self) -> Decimal:
        """现货余额"""
        return self._spot_balance

    @property
    def collateral_balance(self) -> Decimal:
        """
        抵押品余额（当前权益）

        🔥 现货模式：返回实时权益 = 当前USDC + 当前BTC总价值（包括预留）
        🔥 合约模式：返回账户权益（保持原逻辑）
        """
        if self._is_spot_mode():
            # 🔥 现货模式：实时计算权益（统计所有BTC，包括预留）
            current_btc_total = self._get_base_currency_total_balance()  # 获取BTC总余额
            current_price = self._get_current_price()
            btc_total_value = abs(current_btc_total) * current_price  # BTC总价值
            return self._collateral_balance + btc_total_value
        else:
            # 合约模式：直接返回账户权益
            return self._collateral_balance

    @property
    def order_locked_balance(self) -> Decimal:
        """订单冻结余额"""
        return self._order_locked_balance

    @property
    def initial_capital(self) -> Decimal:
        """初始本金"""
        return self._initial_capital

    def _is_spot_mode(self) -> bool:
        """判断是否是现货模式"""
        try:
            from ....adapters.exchanges.interface import ExchangeType
            if hasattr(self.engine, 'exchange') and hasattr(self.engine.exchange, 'config'):
                return self.engine.exchange.config.exchange_type == ExchangeType.SPOT
        except:
            pass
        return False

    def _get_current_position(self) -> Decimal:
        """获取当前持仓数量"""
        try:
            if hasattr(self.coordinator, 'tracker'):
                return self.coordinator.tracker.get_current_position()
        except:
            pass
        return Decimal('0')

    def _get_current_price(self) -> Decimal:
        """
        获取当前价格

        🔥 优先从state获取，如果为空则主动查询引擎
        """
        try:
            # 优先从state获取（缓存的价格）
            if hasattr(self.coordinator, 'state') and self.coordinator.state.current_price:
                if self.coordinator.state.current_price > 0:
                    return self.coordinator.state.current_price

            # 如果state中没有价格，或者价格为0，则主动查询引擎
            if hasattr(self.engine, 'get_current_price'):
                self.logger.debug("state价格为空或为0，主动查询引擎获取当前价格")
                # 注意：这是一个异步方法，但我们在同步上下文中，需要特殊处理
                # 尝试从引擎的ticker缓存中获取
                if hasattr(self.engine, '_last_ticker_price') and self.engine._last_ticker_price:
                    if self.engine._last_ticker_price > 0:
                        self.logger.debug(
                            f"从引擎ticker缓存获取价格: {self.engine._last_ticker_price}")
                        return self.engine._last_ticker_price
        except Exception as e:
            self.logger.warning(f"获取当前价格失败: {e}")

        return Decimal('0')

    def _get_base_currency_total_balance(self) -> Decimal:
        """
        获取基础货币的总余额（包括预留和交易持仓）

        🔥 重要：这个方法返回账户中所有的基础货币（如BTC/UBTC），
        从缓存中读取，不减去预留，不减去订单冻结，用于计算初始本金和当前权益

        Returns:
            Decimal: 基础货币总余额（从缓存读取）
        """
        return self._base_currency_total_balance
