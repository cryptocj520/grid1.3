"""
依赖注入装饰器

提供便捷的依赖注入装饰器
"""

from injector import inject, singleton
from typing import Type, Callable
import functools


def injectable(scope: str = "transient"):
    """注册为可注入的服务"""
    def decorator(cls: Type):
        if scope == "singleton":
            cls = singleton(cls)
        return cls
    return decorator


def service(interface: Type = None, scope: str = "transient"):
    """服务装饰器"""
    def decorator(cls: Type):
        cls = injectable(scope)(cls)
        if interface:
            # 注册接口映射
            pass
        return cls
    return decorator


def auto_inject(func: Callable):
    """自动注入装饰器"""
    @functools.wraps(func)
    @inject
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper
