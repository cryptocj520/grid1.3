"""
事件基类

定义新架构中使用的基础事件类型
"""

import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
from abc import ABC
from decimal import Decimal


@dataclass
class Event(ABC):
    """
    事件基类

    所有事件都继承此基类，包含基础的事件元数据。
    事件是不可变的，一旦创建就不能修改。
    """

    # 事件元数据
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = field(init=False)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    correlation_id: Optional[str] = None

    # 事件来源信息
    source: Optional[str] = None
    source_id: Optional[str] = None

    # 事件优先级（1=最高，5=最低）
    priority: int = 3

    # 额外的事件数据
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """设置事件类型为类名"""
        if not hasattr(self, 'event_type') or not self.event_type:
            self.event_type = self.__class__.__name__

    def to_dict(self) -> Dict[str, Any]:
        """将事件转换为字典格式"""
        return {
            'event_id': self.event_id,
            'event_type': self.event_type,
            'timestamp': self.timestamp.isoformat(),
            'correlation_id': self.correlation_id,
            'source': self.source,
            'source_id': self.source_id,
            'priority': self.priority,
            'metadata': self.metadata,
            'data': self._get_data()
        }

    def _get_data(self) -> Dict[str, Any]:
        """获取事件的业务数据，子类可以重写此方法"""
        # 排除基类字段，只返回业务数据
        base_fields = {'event_id', 'event_type', 'timestamp', 'correlation_id',
                       'source', 'source_id', 'priority', 'metadata'}

        data = {}
        for key, value in self.__dict__.items():
            if key not in base_fields:
                # 处理特殊类型的序列化
                if isinstance(value, Decimal):
                    data[key] = float(value)
                elif isinstance(value, datetime):
                    data[key] = value.isoformat()
                else:
                    data[key] = value

        return data


@dataclass
class ComponentStoppedEvent(Event):
    """
    组件停止事件
    
    当系统组件停止时发出此事件
    """
    
    component: str = field(default="unknown")
    
    def __post_init__(self):
        super().__post_init__()
        self.source = self.component


@dataclass
class HealthCheckEvent(Event):
    """
    健康检查事件
    
    当系统组件进行健康检查时发出此事件
    """
    
    component: str = field(default="unknown")
    check_name: str = field(default="health_check")
    status: str = field(default="unknown")
    details: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        super().__post_init__()
        self.source = self.component 