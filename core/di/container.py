"""
依赖注入容器

基于 Python-injector 的依赖注入容器实现
"""

from injector import Injector, Module, singleton, provider
from typing import Dict, Any, Type, Optional, List

# 使用简化的统一日志入口
from ..logging import get_system_logger


class DIContainer:
    """依赖注入容器"""
    
    def __init__(self, modules: List[Module] = None):
        self.modules = modules or []
        self.injector = Injector(self.modules)
        self.logger = get_system_logger()
        self.initialized = False
    
    def register_module(self, module: Module):
        """注册模块"""
        self.modules.append(module)
        self.injector = Injector(self.modules)
        self.logger.info(f"注册模块: {module.__class__.__name__}")
    
    def register_modules(self, modules: List[Module]):
        """注册多个模块"""
        for module in modules:
            self.modules.append(module)
        self.injector = Injector(self.modules)
        self.logger.info(f"注册了 {len(modules)} 个模块")
    
    def get(self, interface: Type):
        """获取实例"""
        return self.injector.get(interface)
    
    def create_child_injector(self, modules: list = None):
        """创建子注入器"""
        child_modules = self.modules + (modules or [])
        return Injector(child_modules)
    
    def initialize(self):
        """初始化容器"""
        if not self.initialized:
            # 自动注册默认模块
            from .modules import ALL_MODULES
            self.register_modules([module() for module in ALL_MODULES])
            self.initialized = True
            self.logger.info("依赖注入容器已初始化")


# 全局容器实例
container = DIContainer()


def get_container() -> DIContainer:
    """获取全局容器"""
    if not container.initialized:
        container.initialize()
    return container
