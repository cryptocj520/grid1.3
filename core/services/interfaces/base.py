"""
基础服务接口

定义所有服务的基础接口
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import asyncio


class IService(ABC):
    """基础服务接口"""
    
    @abstractmethod
    async def initialize(self) -> bool:
        """初始化服务"""
        pass
    
    @abstractmethod
    async def shutdown(self) -> bool:
        """关闭服务"""
        pass
    
    @abstractmethod
    def get_health_status(self) -> Dict[str, Any]:
        """获取健康状态"""
        pass


class IConfigurable(ABC):
    """可配置接口"""
    
    @abstractmethod
    def configure(self, config: Dict[str, Any]) -> bool:
        """配置服务"""
        pass


class IMonitorable(ABC):
    """可监控接口"""
    
    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        """获取监控指标"""
        pass


class BaseService(IService, IConfigurable, IMonitorable):
    """基础服务实现"""
    
    def __init__(self, name: str):
        self.name = name
        self.initialized = False
        self.config = {}
        self.metrics = {}
    
    async def initialize(self) -> bool:
        """初始化服务"""
        self.initialized = True
        return True
    
    async def shutdown(self) -> bool:
        """关闭服务"""
        self.initialized = False
        return True
    
    def get_health_status(self) -> Dict[str, Any]:
        """获取健康状态"""
        return {
            "service": self.name,
            "initialized": self.initialized,
            "status": "healthy" if self.initialized else "unhealthy"
        }
    
    def configure(self, config: Dict[str, Any]) -> bool:
        """配置服务"""
        self.config.update(config)
        return True
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取监控指标"""
        return self.metrics
