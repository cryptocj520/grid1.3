"""
符号缓存服务接口

定义符号缓存管理的核心接口，支持：
- 一次性获取和缓存交易对
- 计算重叠交易对
- 为各交易所提供固定的符号列表
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

@dataclass
class SymbolCacheData:
    """符号缓存数据结构"""
    exchange_symbols: Dict[str, List[str]]  # 各交易所支持的符号 {"hyperliquid": ["BTC_USDT_PERP", ...]}
    overlap_symbols: List[str]              # 重叠的符号列表
    subscription_symbols: Dict[str, List[str]]  # 各交易所应该订阅的符号列表
    timestamp: float                        # 缓存时间戳
    total_symbols: int                      # 总符号数量
    overlap_count: int                      # 重叠符号数量
    metadata: Dict[str, Any]               # 元数据

@dataclass
class SymbolOverlapConfig:
    """符号重叠配置"""
    min_exchange_count: int = 2             # 最小交易所重叠数量
    use_overlap_only: bool = True           # 是否只使用重叠符号
    max_symbols_per_exchange: int = 0       # 每个交易所最大符号数量（0表示无限制）
    exchange_priority: List[str] = None     # 交易所优先级
    include_patterns: List[str] = None      # 包含模式
    exclude_patterns: List[str] = None      # 排除模式

class ISymbolCacheService(ABC):
    """符号缓存服务接口"""
    
    @abstractmethod
    async def initialize_cache(self, exchange_ids: List[str], config: Optional[SymbolOverlapConfig] = None) -> bool:
        """初始化符号缓存（只在启动时调用一次）
        
        Args:
            exchange_ids: 交易所ID列表
            config: 重叠配置
            
        Returns:
            bool: 初始化是否成功
        """
        pass
    
    @abstractmethod
    def get_symbols_for_exchange(self, exchange_id: str) -> List[str]:
        """获取指定交易所应该订阅的符号列表
        
        Args:
            exchange_id: 交易所ID
            
        Returns:
            List[str]: 符号列表
        """
        pass
    
    @abstractmethod
    def get_overlap_symbols(self) -> List[str]:
        """获取重叠的符号列表
        
        Returns:
            List[str]: 重叠符号列表
        """
        pass
    
    @abstractmethod
    def get_all_exchange_symbols(self) -> Dict[str, List[str]]:
        """获取所有交易所的符号列表
        
        Returns:
            Dict[str, List[str]]: 交易所符号映射
        """
        pass
    
    @abstractmethod
    def is_cache_valid(self) -> bool:
        """检查缓存是否有效
        
        Returns:
            bool: 缓存是否有效
        """
        pass
    
    @abstractmethod
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息
        
        Returns:
            Dict[str, Any]: 缓存统计
        """
        pass
    
    @abstractmethod
    def clear_cache(self) -> None:
        """清空缓存"""
        pass 