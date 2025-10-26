"""
刷量交易服务接口定义

定义刷量交易的核心服务接口
"""

from abc import ABC, abstractmethod
from typing import Optional
from decimal import Decimal

from ..models.volume_maker_config import VolumeMakerConfig
from ..models.volume_maker_statistics import VolumeMakerStatistics


class IVolumeMakerService(ABC):
    """刷量交易服务接口"""

    @abstractmethod
    async def initialize(self, config: VolumeMakerConfig) -> bool:
        """
        初始化刷量服务

        Args:
            config: 刷量配置

        Returns:
            bool: 初始化是否成功
        """
        pass

    @abstractmethod
    async def start(self) -> None:
        """启动刷量交易"""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """停止刷量交易"""
        pass

    @abstractmethod
    async def pause(self) -> None:
        """暂停刷量交易"""
        pass

    @abstractmethod
    async def resume(self) -> None:
        """恢复刷量交易"""
        pass

    @abstractmethod
    def get_statistics(self) -> VolumeMakerStatistics:
        """
        获取统计数据

        Returns:
            VolumeMakerStatistics: 统计数据
        """
        pass

    @abstractmethod
    def is_running(self) -> bool:
        """
        检查是否正在运行

        Returns:
            bool: 是否正在运行
        """
        pass

    @abstractmethod
    def is_paused(self) -> bool:
        """
        检查是否已暂停

        Returns:
            bool: 是否已暂停
        """
        pass

    @abstractmethod
    async def emergency_stop(self) -> None:
        """紧急停止（取消所有订单，平仓所有持仓）"""
        pass

    @abstractmethod
    def get_status_text(self) -> str:
        """
        获取状态文本

        Returns:
            str: 状态文本描述
        """
        pass
