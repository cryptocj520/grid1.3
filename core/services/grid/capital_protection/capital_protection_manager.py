"""
本金保护管理器

当价格移动到网格特定百分比位置时，触发本金保护模式。
检查抵押品余额是否回本，如果回本则重置网格。
"""

from decimal import Decimal
from typing import Optional
from datetime import datetime

from core.services.grid.models.grid_config import GridConfig
from core.services.grid.models.grid_order import GridOrder
from core.logging import get_logger


class CapitalProtectionManager:
    """
    本金保护管理器

    功能：
    1. 记录初始本金（首次获取的抵押品余额）
    2. 监控价格，判断是否触发本金保护
    3. 触发后检查抵押品是否>=本金
    4. 满足条件时执行网格重置
    """

    def __init__(self, config: GridConfig):
        """
        初始化本金保护管理器

        Args:
            config: 网格配置
        """
        self.config = config
        self.logger = get_logger(self.__class__.__name__)

        # 状态变量
        self._initial_capital: Decimal = Decimal('0')  # 初始本金
        self._is_active: bool = False                  # 是否已触发
        self._trigger_grid_index: int = 0              # 触发网格索引
        self._activation_time: Optional[datetime] = None  # 触发时间

        # 计算触发网格位置
        self._trigger_grid_index = config.get_capital_protection_trigger_grid()

        self.logger.info(
            f"✅ 本金保护管理器初始化: "
            f"触发阈值={config.capital_protection_trigger_percent}%, "
            f"触发网格={self._trigger_grid_index}"
        )

    def initialize_capital(self, initial_capital: Decimal, is_reinit: bool = False):
        """
        初始化或重新初始化本金

        Args:
            initial_capital: 初始本金金额
            is_reinit: 是否为重新初始化（网格重置后）
        """
        old_capital = self._initial_capital

        if is_reinit:
            # 🔥 重要：重新初始化时必须重置激活状态，否则无法触发第二次本金保护
            self._initial_capital = initial_capital
            self._is_active = False
            self._activation_time = None
            self.logger.info(
                f"🔄 本金已重新初始化: "
                f"旧本金=${old_capital:,.2f}, "
                f"新本金=${self._initial_capital:,.2f}, "
                f"本金保护状态已重置"
            )
        elif self._initial_capital == Decimal('0'):
            # 首次初始化
            self._initial_capital = initial_capital
            self.logger.info(f"💰 初始本金已记录: ${self._initial_capital:,.2f} USDC")
        else:
            # 已有本金且不是重新初始化，忽略
            self.logger.warning(
                f"⚠️ 初始本金已存在(${self._initial_capital:,.2f})，"
                f"忽略新值(${initial_capital:,.2f})"
            )

    def get_initial_capital(self) -> Decimal:
        """获取初始本金"""
        return self._initial_capital

    def should_trigger(self, current_price: Decimal, current_grid_index: int) -> bool:
        """
        检查是否应该触发本金保护

        Args:
            current_price: 当前价格
            current_grid_index: 当前网格索引

        Returns:
            True表示应该触发

        逻辑（已修正）：
            做多网格：Grid 1 = 最低价，价格跌到低位时触发
            做空网格：Grid 1 = 最高价，价格涨到低位时触发
        """
        # 如果已经激活，不重复触发
        if self._is_active:
            return False

        # 🔥 做多网格：Grid 1 = 最低价，价格跌到低位时触发（Grid ID <= trigger）
        # 🔥 做空网格：Grid 1 = 最高价，价格涨到低位时触发（Grid ID <= trigger）
        if self.config.is_long():
            # 做多：当前网格索引 <= 触发索引（价格越低，索引越小）
            should_activate = current_grid_index <= self._trigger_grid_index
        else:
            # 做空：当前网格索引 <= 触发索引（价格越高，索引越小）
            should_activate = current_grid_index <= self._trigger_grid_index

        if should_activate:
            self.logger.info(
                f"🔔 本金保护触发条件满足！"
                f"当前价格=${current_price:,.2f}, "
                f"当前网格={current_grid_index}, "
                f"触发网格={self._trigger_grid_index}"
            )

        return should_activate

    def activate(self):
        """激活本金保护模式"""
        self._is_active = True
        self._activation_time = datetime.now()
        self.logger.warning(
            f"🛡️ 本金保护模式已激活！"
            f"初始本金: ${self._initial_capital:,.2f}"
        )

    def check_capital_recovery(self, current_collateral: Decimal) -> bool:
        """
        检查抵押品是否回本

        Args:
            current_collateral: 当前抵押品余额

        Returns:
            True表示已回本（抵押品>=本金，允许$0.01的容差）
        """
        if self._initial_capital == Decimal('0'):
            self.logger.warning("⚠️ 初始本金未设置，无法检查回本")
            return False

        profit_loss = current_collateral - self._initial_capital
        profit_rate = (profit_loss / self._initial_capital *
                       100) if self._initial_capital > 0 else Decimal('0')

        # 🔥 修复：添加$0.01容差，避免精度问题导致无法回本
        # 如果亏损在 -$0.01 以内，视为已回本
        tolerance = Decimal('0.01')
        is_recovered = profit_loss >= -tolerance

        if is_recovered:
            self.logger.warning(
                f"✅ 抵押品已回本（容差±$0.01）！"
                f"当前: ${current_collateral:,.3f}, "
                f"本金: ${self._initial_capital:,.3f}, "
                f"盈亏: ${profit_loss:+,.3f} ({profit_rate:+.3f}%)"
            )
        else:
            self.logger.info(
                f"⏳ 等待回本... "
                f"当前: ${current_collateral:,.3f}, "
                f"本金: ${self._initial_capital:,.3f}, "
                f"亏损: ${profit_loss:,.3f} ({profit_rate:.3f}%)"
            )

        return is_recovered

    def is_active(self) -> bool:
        """判断本金保护是否已激活"""
        return self._is_active

    def get_profit_loss(self, current_collateral: Decimal) -> Decimal:
        """
        获取盈亏金额

        Args:
            current_collateral: 当前抵押品余额

        Returns:
            盈亏金额（正数为盈利，负数为亏损）
        """
        if self._initial_capital == Decimal('0'):
            return Decimal('0')
        return current_collateral - self._initial_capital

    def get_profit_loss_rate(self, current_collateral: Decimal) -> Decimal:
        """
        获取盈亏率

        Args:
            current_collateral: 当前抵押品余额

        Returns:
            盈亏率（百分比）
        """
        if self._initial_capital == Decimal('0'):
            return Decimal('0')
        profit_loss = current_collateral - self._initial_capital
        return (profit_loss / self._initial_capital * 100)

    def get_status_summary(self, current_collateral: Decimal) -> dict:
        """
        获取状态摘要（用于UI显示）

        Args:
            current_collateral: 当前抵押品余额

        Returns:
            状态摘要字典
        """
        return {
            'enabled': True,
            'active': self._is_active,
            'initial_capital': float(self._initial_capital),
            'current_collateral': float(current_collateral),
            'profit_loss': float(self.get_profit_loss(current_collateral)),
            'profit_loss_rate': float(self.get_profit_loss_rate(current_collateral)),
            'trigger_percent': self.config.capital_protection_trigger_percent,
            'trigger_grid': self._trigger_grid_index,
            'activation_time': self._activation_time.isoformat() if self._activation_time else None
        }

    def reset(self):
        """
        重置本金保护管理器状态

        在网格重置时调用，清除激活状态但保留初始本金
        注意：初始本金会在重置后重新初始化，不在这里清零
        """
        self._is_active = False
        self._activation_time = None

        self.logger.info("🔄 本金保护管理器已重置（等待新的初始本金）")
