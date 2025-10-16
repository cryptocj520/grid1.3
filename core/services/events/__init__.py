"""
事件模块

定义新架构中使用的事件基类和简化的事件处理机制
"""

from .event import Event, ComponentStoppedEvent, HealthCheckEvent
from .event_handler import EventHandler, EventCallback

__all__ = ['Event', 'ComponentStoppedEvent', 'HealthCheckEvent', 'EventHandler', 'EventCallback'] 