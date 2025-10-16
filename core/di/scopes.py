"""
依赖注入作用域

定义服务的生命周期作用域
"""

from enum import Enum
from typing import Dict, Any, Type, Optional
import threading


class ServiceScope(Enum):
    """服务作用域"""
    SINGLETON = "singleton"     # 单例
    TRANSIENT = "transient"     # 瞬态
    SCOPED = "scoped"          # 作用域


class ScopeManager:
    """作用域管理器"""
    
    def __init__(self):
        self.scopes: Dict[str, Dict[Type, Any]] = {}
        self.lock = threading.Lock()
    
    def get_instance(self, scope: ServiceScope, service_type: Type, factory: callable):
        """获取服务实例"""
        with self.lock:
            if scope == ServiceScope.SINGLETON:
                return self._get_singleton(service_type, factory)
            elif scope == ServiceScope.TRANSIENT:
                return factory()
            elif scope == ServiceScope.SCOPED:
                return self._get_scoped(service_type, factory)
    
    def _get_singleton(self, service_type: Type, factory: callable):
        """获取单例实例"""
        if service_type not in self.scopes.get("singleton", {}):
            if "singleton" not in self.scopes:
                self.scopes["singleton"] = {}
            self.scopes["singleton"][service_type] = factory()
        return self.scopes["singleton"][service_type]
    
    def _get_scoped(self, service_type: Type, factory: callable):
        """获取作用域实例"""
        # 实现作用域逻辑
        return factory()
