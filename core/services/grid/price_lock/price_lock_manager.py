"""
价格锁定管理器

功能：
1. 监控价格是否超过锁定阈值
2. 在有利方向脱离时，如果超过阈值则冻结网格（不平仓、不重置）
3. 监控价格回归，自动解除冻结
"""

from decimal import Decimal
from typing import Optional
from datetime import datetime

from core.services.grid.models.grid_config import GridConfig
from core.logging import get_logger


class PriceLockManager:
    """
    价格锁定管理器
    
    功能：
    1. 检查价格是否达到锁定阈值
    2. 管理锁定/解锁状态
    3. 判断是否应该锁定而不是重置网格
    """
    
    def __init__(self, config: GridConfig):
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
        
        self._is_locked: bool = False              # 是否已锁定
        self._lock_time: Optional[datetime] = None  # 锁定时间
        
        if config.price_lock_threshold is None:
            raise ValueError("价格锁定模式需要设置 price_lock_threshold")
        
        self.logger.info(
            f"✅ 价格锁定管理器初始化: "
            f"阈值={config.price_lock_threshold}, "
            f"网格类型={config.grid_type.value}"
        )
    
    def should_lock_instead_of_reset(
        self, 
        current_price: Decimal, 
        direction: str
    ) -> bool:
        """
        判断是否应该锁定而不是重置网格
        
        Args:
            current_price: 当前价格
            direction: 脱离方向 ("up" 或 "down")
        
        Returns:
            True 表示应该锁定，False 表示应该重置（或平仓）
        """
        # 只在有利方向脱离时才考虑锁定
        is_favorable_direction = False
        
        if self.config.is_long() and direction == "up":
            # 做多 + 向上脱离 = 有利方向
            is_favorable_direction = True
        elif self.config.is_short() and direction == "down":
            # 做空 + 向下脱离 = 有利方向
            is_favorable_direction = True
        
        if not is_favorable_direction:
            # 不利方向脱离，不考虑锁定
            return False
        
        # 检查价格是否达到锁定阈值
        threshold_reached = self._check_threshold(current_price)
        
        if threshold_reached:
            self.logger.info(
                f"🔒 价格锁定条件满足: "
                f"当前价格=${current_price:,.4f}, "
                f"阈值=${self.config.price_lock_threshold:,.4f}, "
                f"方向={direction}"
            )
            return True
        else:
            return False
    
    def _check_threshold(self, current_price: Decimal) -> bool:
        """
        检查价格是否达到锁定阈值
        
        做多：价格 >= 阈值
        做空：价格 <= 阈值
        """
        if self.config.is_long():
            return current_price >= self.config.price_lock_threshold
        else:  # 做空
            return current_price <= self.config.price_lock_threshold
    
    def activate_lock(self, current_price: Decimal):
        """激活价格锁定"""
        if not self._is_locked:
            self._is_locked = True
            self._lock_time = datetime.now()
            self.logger.warning(
                f"🔒 价格锁定已激活！"
                f"当前价格=${current_price:,.4f}, "
                f"阈值=${self.config.price_lock_threshold:,.4f}"
            )
    
    def check_unlock_condition(
        self, 
        current_price: Decimal, 
        lower_price: Decimal, 
        upper_price: Decimal
    ) -> bool:
        """
        检查是否应该解锁
        
        当价格回归到网格范围内时解锁
        
        Args:
            current_price: 当前价格
            lower_price: 网格下限
            upper_price: 网格上限
        
        Returns:
            True 表示应该解锁
        """
        if not self._is_locked:
            return False
        
        # 检查价格是否回归到网格范围内
        is_in_range = lower_price <= current_price <= upper_price
        
        if is_in_range:
            self.logger.info(
                f"🔓 价格回归网格范围内: "
                f"${current_price:,.4f} in [${lower_price:,.4f}, ${upper_price:,.4f}]"
            )
            return True
        
        return False
    
    def deactivate_lock(self):
        """解除价格锁定"""
        if self._is_locked:
            lock_duration = (datetime.now() - self._lock_time).total_seconds()
            self._is_locked = False
            self._lock_time = None
            self.logger.info(
                f"🔓 价格锁定已解除！锁定持续时间: {lock_duration:.1f}秒"
            )
    
    def is_locked(self) -> bool:
        """是否处于锁定状态"""
        return self._is_locked
    
    def get_lock_info(self) -> dict:
        """获取锁定信息"""
        return {
            "is_locked": self._is_locked,
            "lock_time": self._lock_time,
            "threshold": self.config.price_lock_threshold,
            "grid_type": self.config.grid_type.value
        }
    
    def reset(self):
        """
        重置价格锁定管理器状态
        
        在网格重置时调用，清除锁定状态
        """
        self._is_locked = False
        self._lock_time = None
        
        self.logger.info("🔄 价格锁定管理器已重置")

