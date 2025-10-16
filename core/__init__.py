"""
Trading System 核心模块

新架构的核心模块，提供依赖注入和服务接口。

主要组件：
- di: 依赖注入容器
- services: 服务层接口和实现
- domain: 领域模型
- infrastructure: 基础设施层
- adapters: 适配器层
"""

__version__ = "2.0.0"
__author__ = "Trading System Team"

# 导入新架构的核心组件
from .di.container import get_container, DIContainer
from .services.interfaces.base import IService, BaseService
# 日志服务已简化，直接使用统一入口
# from .services.interfaces.logging import ILoggingService
from .services.interfaces.monitoring_service import MonitoringService

__all__ = [
    "get_container",
    "DIContainer",
    "IService",
    "BaseService",
    # "ILoggingService",  # 已简化，使用 core.logging 统一入口
    "MonitoringService"
]
