"""
交易所注册器

负责管理交易所适配器的注册和访问
"""

from typing import Dict, List, Optional, Any
from core.logging import get_logger
from core.adapters.exchanges.interface import ExchangeInterface


class ExchangeRegistry:
    """交易所注册器"""
    
    def __init__(self, exchange_adapters: Dict[str, ExchangeInterface] = None):
        """
        初始化交易所注册器
        
        Args:
            exchange_adapters: 初始交易所适配器字典
        """
        self._adapters: Dict[str, ExchangeInterface] = exchange_adapters or {}
        self.logger = get_logger(__name__)
    
    def register_exchange(self, name: str, adapter: ExchangeInterface):
        """
        注册交易所适配器
        
        Args:
            name: 交易所名称
            adapter: 交易所适配器
        """
        self._adapters[name] = adapter
        self.logger.info(f"注册交易所适配器: {name}")
    
    def unregister_exchange(self, name: str) -> bool:
        """
        注销交易所适配器
        
        Args:
            name: 交易所名称
            
        Returns:
            是否成功注销
        """
        if name in self._adapters:
            del self._adapters[name]
            self.logger.info(f"注销交易所适配器: {name}")
            return True
        return False
    
    def get_adapter(self, name: str) -> Optional[ExchangeInterface]:
        """
        获取交易所适配器
        
        Args:
            name: 交易所名称
            
        Returns:
            交易所适配器，如果不存在则返回None
        """
        return self._adapters.get(name)
    
    def get_all_exchanges(self) -> List[str]:
        """
        获取所有已注册的交易所名称
        
        Returns:
            交易所名称列表
        """
        return list(self._adapters.keys())
    
    def get_all_adapters(self) -> Dict[str, ExchangeInterface]:
        """
        获取所有已注册的交易所适配器
        
        Returns:
            交易所适配器字典
        """
        return self._adapters.copy()
    
    def is_registered(self, name: str) -> bool:
        """
        检查交易所是否已注册
        
        Args:
            name: 交易所名称
            
        Returns:
            是否已注册
        """
        return name in self._adapters
    
    async def check_all_health(self) -> Dict[str, bool]:
        """
        检查所有交易所的健康状态
        
        Returns:
            健康状态字典
        """
        health_status = {}
        
        for name, adapter in self._adapters.items():
            try:
                is_healthy = adapter.is_connected()
                if not is_healthy:
                    # 尝试重新连接
                    await adapter.connect()
                    is_healthy = adapter.is_connected()
                
                health_status[name] = is_healthy
                
            except Exception as e:
                self.logger.error(f"检查交易所健康状态失败: {name} - {e}")
                health_status[name] = False
        
        return health_status
    
    def get_adapter_count(self) -> int:
        """
        获取已注册适配器数量
        
        Returns:
            适配器数量
        """
        return len(self._adapters)
    
    def clear_all(self):
        """清空所有适配器"""
        self._adapters.clear()
        self.logger.info("清空所有交易所适配器")
    
    # TODO: 高级功能占位符
    async def auto_discover_exchanges(self, config_path: str):
        """
        自动发现并注册交易所
        
        Args:
            config_path: 配置文件路径
        """
        # TODO: 实现自动发现机制
        pass
    
    async def load_exchange_configs(self, config_data: Dict[str, Any]):
        """
        从配置加载交易所
        
        Args:
            config_data: 配置数据
        """
        # TODO: 实现配置加载
        pass
    
    def get_exchange_capabilities(self, name: str) -> Dict[str, Any]:
        """
        获取交易所能力信息
        
        Args:
            name: 交易所名称
            
        Returns:
            能力信息
        """
        # TODO: 实现能力查询
        return {}
    
    async def test_exchange_connectivity(self, name: str) -> Dict[str, Any]:
        """
        测试交易所连通性
        
        Args:
            name: 交易所名称
            
        Returns:
            连通性测试结果
        """
        # TODO: 实现连通性测试
        return {
            'status': 'not_implemented',
            'exchange': name
        } 