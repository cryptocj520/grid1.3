"""
止盈管理器

当抵押品余额盈利超过设定百分比时，触发止盈模式。
执行：取消所有挂单 → 市价平仓 → 重置网格 → 重新初始化本金
"""

from decimal import Decimal
from typing import Optional
from datetime import datetime

from core.services.grid.models.grid_config import GridConfig
from core.logging import get_logger


class TakeProfitManager:
    """
    止盈管理器

    功能：
    1. 记录初始本金（每次网格重置后都会更新）
    2. 持续监控抵押品余额，计算盈利率
    3. 当盈利率达到设定百分比时触发止盈
    4. 止盈后重置网格并重新初始化本金
    """

    def __init__(self, config: GridConfig):
        """
        初始化止盈管理器

        Args:
            config: 网格配置
        """
        self.config = config
        self.logger = get_logger(self.__class__.__name__)

        # 状态变量
        self._initial_capital: Decimal = Decimal('0')  # 初始本金
        self._is_active: bool = False                  # 是否已触发
        self._activation_time: Optional[datetime] = None  # 触发时间
        self._last_check_time: Optional[datetime] = None  # 上次检查时间

        # 止盈百分比阈值
        self._take_profit_threshold = config.take_profit_percentage

        self.logger.info(
            f"✅ 止盈管理器初始化: "
            f"止盈阈值={float(self._take_profit_threshold * 100):.2f}%"
        )

    def initialize_capital(self, initial_capital: Decimal, is_reinit: bool = False):
        """
        初始化或重新初始化本金

        Args:
            initial_capital: 初始本金金额（抵押品余额）
            is_reinit: 是否为重新初始化（网格重置后）
        """
        old_capital = self._initial_capital
        self._initial_capital = initial_capital

        # 🔥 重要：重新初始化时必须重置激活状态，否则无法触发第二次止盈
        if is_reinit:
            self._is_active = False
            self._activation_time = None
            self.logger.info(
                f"🔄 本金已重新初始化: "
                f"旧本金=${old_capital:,.3f}, "
                f"新本金=${self._initial_capital:,.3f}, "
                f"止盈状态已重置"
            )
        else:
            self.logger.info(f"💰 初始本金已记录: ${self._initial_capital:,.3f} USDC")

    def get_initial_capital(self) -> Decimal:
        """获取初始本金"""
        return self._initial_capital

    def check_take_profit_condition(self, current_collateral: Decimal) -> bool:
        """
        检查是否满足止盈条件

        Args:
            current_collateral: 当前抵押品余额

        Returns:
            True表示满足止盈条件
        """
        # 记录检查时间
        self._last_check_time = datetime.now()

        # 如果已经激活，不重复触发
        if self._is_active:
            return False

        # 检查初始本金是否已设置
        if self._initial_capital == Decimal('0'):
            self.logger.debug("⚠️ 初始本金未设置，跳过止盈检查")
            return False

        # 计算盈利金额和盈利率
        profit = current_collateral - self._initial_capital
        profit_rate = profit / \
            self._initial_capital if self._initial_capital > 0 else Decimal(
                '0')

        # 判断是否达到止盈阈值
        should_take_profit = profit_rate >= self._take_profit_threshold

        if should_take_profit:
            self.logger.warning(
                f"🎯 触发止盈条件！"
                f"当前抵押品=${current_collateral:,.3f}, "
                f"初始本金=${self._initial_capital:,.3f}, "
                f"盈利=${profit:+,.3f} ({float(profit_rate * 100):+.2f}%), "
                f"阈值={float(self._take_profit_threshold * 100):.2f}%"
            )

        return should_take_profit

    def activate(self, current_collateral: Decimal):
        """
        激活止盈模式

        Args:
            current_collateral: 当前抵押品余额
        """
        self._is_active = True
        self._activation_time = datetime.now()

        profit = current_collateral - self._initial_capital
        profit_rate = profit / \
            self._initial_capital if self._initial_capital > 0 else Decimal(
                '0')

        self.logger.warning(
            f"💰 止盈模式已激活！"
            f"初始本金=${self._initial_capital:,.3f}, "
            f"当前抵押品=${current_collateral:,.3f}, "
            f"盈利=${profit:+,.3f} ({float(profit_rate * 100):+.2f}%)"
        )

    def is_active(self) -> bool:
        """判断止盈模式是否已激活"""
        return self._is_active

    def get_profit_amount(self, current_collateral: Decimal) -> Decimal:
        """
        获取盈利金额

        Args:
            current_collateral: 当前抵押品余额

        Returns:
            盈利金额（正数为盈利，负数为亏损）
        """
        if self._initial_capital == Decimal('0'):
            return Decimal('0')
        return current_collateral - self._initial_capital

    def get_profit_rate(self, current_collateral: Decimal) -> Decimal:
        """
        获取盈利率

        Args:
            current_collateral: 当前抵押品余额

        Returns:
            盈利率（小数形式，如0.01表示1%）
        """
        if self._initial_capital == Decimal('0'):
            return Decimal('0')
        profit = current_collateral - self._initial_capital
        return profit / self._initial_capital

    def get_profit_percentage(self, current_collateral: Decimal) -> Decimal:
        """
        获取盈利百分比

        Args:
            current_collateral: 当前抵押品余额

        Returns:
            盈利百分比（如1.5表示1.5%）
        """
        return self.get_profit_rate(current_collateral) * 100

    def reset(self):
        """重置止盈状态（网格重置后调用）"""
        self._is_active = False
        self._activation_time = None
        self.logger.info("🔄 止盈状态已重置")

    def get_status_summary(self, current_collateral: Decimal) -> dict:
        """
        获取状态摘要（用于UI显示）

        Args:
            current_collateral: 当前抵押品余额

        Returns:
            状态摘要字典
        """
        profit_amount = self.get_profit_amount(current_collateral)
        profit_rate = self.get_profit_rate(current_collateral)

        return {
            'enabled': True,
            'active': self._is_active,
            'initial_capital': float(self._initial_capital),
            'current_collateral': float(current_collateral),
            'profit_amount': float(profit_amount),
            'profit_rate': float(profit_rate * 100),  # 转为百分比
            # 转为百分比
            'threshold_rate': float(self._take_profit_threshold * 100),
            'activation_time': self._activation_time.isoformat() if self._activation_time else None,
            'last_check_time': self._last_check_time.isoformat() if self._last_check_time else None
        }

    def reset(self):
        """
        重置止盈管理器状态

        在网格重置时调用，清除所有状态
        注意：初始本金会在重置后重新初始化，不在这里清零
        """
        self._is_active = False
        self._activation_time = None
        self._last_check_time = None

        self.logger.info("🔄 止盈管理器已重置（等待新的初始本金）")
