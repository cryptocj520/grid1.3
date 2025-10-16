"""
简化的事件处理器

提供统一的事件处理机制，替代复杂的事件总线系统
"""

import asyncio
from typing import Dict, List, Callable, Optional, Any, Union
from dataclasses import dataclass
from datetime import datetime

# 使用简化的统一日志入口
from ...logging import get_system_logger

from .event import Event


# 事件回调类型定义
EventCallback = Callable[[Union[Event, Dict[str, Any]]], None]
AsyncEventCallback = Callable[[Union[Event, Dict[str, Any]]], Any]


@dataclass
class EventSubscription:
    """事件订阅记录"""
    event_type: str
    callback: Union[EventCallback, AsyncEventCallback]
    subscriber_id: str
    created_at: datetime


class EventHandler:
    """
    简化的事件处理器
    
    提供统一的事件发布和订阅机制，支持同步和异步回调
    """
    
    def __init__(self, name: str = "EventHandler"):
        self.name = name
        self.logger = get_system_logger()
        
        # 事件订阅管理
        self._subscriptions: Dict[str, List[EventSubscription]] = {}
        self._subscriber_counters: Dict[str, int] = {}
        
        # 统计信息
        self._stats = {
            'events_published': 0,
            'events_processed': 0,
            'errors': 0,
            'subscribers': 0
        }
        
        # 异步任务管理
        self._background_tasks: List[asyncio.Task] = []
        
        self.logger.info(f"事件处理器初始化完成: {name}")
    
    def subscribe(self, event_type: str, callback: Union[EventCallback, AsyncEventCallback], 
                  subscriber_id: Optional[str] = None) -> str:
        """
        订阅事件
        
        Args:
            event_type: 事件类型
            callback: 回调函数（支持同步和异步）
            subscriber_id: 订阅者ID（可选）
            
        Returns:
            订阅ID
        """
        # 生成订阅者ID
        if subscriber_id is None:
            counter = self._subscriber_counters.get(event_type, 0) + 1
            self._subscriber_counters[event_type] = counter
            subscriber_id = f"{event_type}_subscriber_{counter}"
        
        # 创建订阅记录
        subscription = EventSubscription(
            event_type=event_type,
            callback=callback,
            subscriber_id=subscriber_id,
            created_at=datetime.now()
        )
        
        # 添加到订阅列表
        if event_type not in self._subscriptions:
            self._subscriptions[event_type] = []
        
        self._subscriptions[event_type].append(subscription)
        self._stats['subscribers'] += 1
        
        self.logger.debug(f"新增订阅: {event_type} <- {subscriber_id}")
        return subscriber_id
    
    def unsubscribe(self, event_type: str, subscriber_id: str) -> bool:
        """
        取消订阅
        
        Args:
            event_type: 事件类型
            subscriber_id: 订阅者ID
            
        Returns:
            是否成功取消
        """
        if event_type not in self._subscriptions:
            return False
        
        # 查找并移除订阅
        subscriptions = self._subscriptions[event_type]
        for i, subscription in enumerate(subscriptions):
            if subscription.subscriber_id == subscriber_id:
                subscriptions.pop(i)
                self._stats['subscribers'] -= 1
                self.logger.debug(f"取消订阅: {event_type} <- {subscriber_id}")
                
                # 如果没有订阅者了，清理事件类型
                if not subscriptions:
                    del self._subscriptions[event_type]
                
                return True
        
        return False
    
    async def publish(self, event: Union[Event, Dict[str, Any], str], data: Optional[Dict[str, Any]] = None) -> None:
        """
        发布事件
        
        Args:
            event: 事件对象、事件数据字典或事件类型字符串
            data: 事件数据（当event为字符串时使用）
        """
        try:
            self._stats['events_published'] += 1
            
            # 处理不同的事件输入格式
            if isinstance(event, str):
                # 字符串事件类型
                event_type = event
                event_data = data or {}
            elif isinstance(event, dict):
                # 字典格式事件
                event_type = event.get('event_type', 'unknown')
                event_data = event
            elif isinstance(event, Event):
                # Event类实例
                event_type = event.event_type
                event_data = event.to_dict()
            else:
                self.logger.warning(f"不支持的事件类型: {type(event)}")
                return
            
            # 获取订阅者
            subscriptions = self._subscriptions.get(event_type, [])
            if not subscriptions:
                self.logger.debug(f"没有订阅者的事件: {event_type}")
                return
            
            # 并发处理所有订阅者
            tasks = []
            for subscription in subscriptions:
                task = asyncio.create_task(
                    self._safe_callback(subscription, event_data)
                )
                tasks.append(task)
            
            # 等待所有回调完成
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            
            self._stats['events_processed'] += 1
            self.logger.debug(f"事件发布完成: {event_type} -> {len(subscriptions)} 个订阅者")
            
        except Exception as e:
            self._stats['errors'] += 1
            self.logger.error(f"发布事件失败: {e}")
    
    async def _safe_callback(self, subscription: EventSubscription, event_data: Dict[str, Any]) -> None:
        """
        安全执行回调函数
        
        Args:
            subscription: 订阅记录
            event_data: 事件数据
        """
        try:
            callback = subscription.callback
            
            # 检查是否为异步回调
            if asyncio.iscoroutinefunction(callback):
                await callback(event_data)
            else:
                # 同步回调
                callback(event_data)
                
        except Exception as e:
            self._stats['errors'] += 1
            self.logger.error(
                f"回调执行失败 [{subscription.subscriber_id}]: {e}"
            )
    
    async def emit(self, event_type: str, data: Dict[str, Any]) -> None:
        """
        发出事件（兼容旧的接口）
        
        Args:
            event_type: 事件类型
            data: 事件数据
        """
        await self.publish(event_type, data)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'name': self.name,
            'events_published': self._stats['events_published'],
            'events_processed': self._stats['events_processed'],
            'errors': self._stats['errors'],
            'subscribers': self._stats['subscribers'],
            'event_types': list(self._subscriptions.keys()),
            'subscriptions_count': {
                event_type: len(subs) 
                for event_type, subs in self._subscriptions.items()
            }
        }
    
    def get_subscriptions(self) -> Dict[str, List[str]]:
        """获取所有订阅信息"""
        return {
            event_type: [sub.subscriber_id for sub in subscriptions]
            for event_type, subscriptions in self._subscriptions.items()
        }
    
    async def cleanup(self) -> None:
        """清理资源"""
        # 取消所有后台任务
        for task in self._background_tasks:
            if not task.done():
                task.cancel()
        
        # 等待任务完成
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        
        # 清理订阅
        self._subscriptions.clear()
        self._subscriber_counters.clear()
        self._background_tasks.clear()
        
        self.logger.info(f"事件处理器清理完成: {self.name}")


# 全局默认事件处理器
default_event_handler = EventHandler("Default")


def get_event_handler(name: str = "Default") -> EventHandler:
    """获取事件处理器实例"""
    if name == "Default":
        return default_event_handler
    else:
        return EventHandler(name) 