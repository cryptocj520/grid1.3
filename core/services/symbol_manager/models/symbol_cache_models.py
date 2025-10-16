"""
符号缓存相关数据模型
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime

@dataclass
class SymbolCacheData:
    """符号缓存数据结构"""
    exchange_symbols: Dict[str, List[str]]          # 各交易所支持的符号
    overlap_symbols: List[str]                      # 重叠的符号列表
    subscription_symbols: Dict[str, List[str]]      # 各交易所应该订阅的符号列表
    timestamp: float                                # 缓存时间戳
    total_symbols: int                              # 总符号数量
    overlap_count: int                              # 重叠符号数量
    metadata: Dict[str, Any] = field(default_factory=dict)  # 元数据

@dataclass
class SymbolOverlapConfig:
    """符号重叠配置"""
    min_exchange_count: int = 2                     # 最小交易所重叠数量
    use_overlap_only: bool = True                   # 是否只使用重叠符号
    max_symbols_per_exchange: int = 0               # 每个交易所最大符号数量（0表示无限制）
    exchange_priority: List[str] = field(default_factory=list)      # 交易所优先级
    include_patterns: List[str] = field(default_factory=list)       # 包含模式
    exclude_patterns: List[str] = field(default_factory=list)       # 排除模式
    
    def __post_init__(self):
        """初始化后处理"""
        if self.exchange_priority is None:
            self.exchange_priority = []
        if self.include_patterns is None:
            self.include_patterns = []
        if self.exclude_patterns is None:
            self.exclude_patterns = []

@dataclass
class SymbolAnalysisResult:
    """符号分析结果"""
    symbol: str                                     # 符号名称
    exchanges: List[str]                            # 支持的交易所
    exchange_count: int                             # 支持的交易所数量
    is_overlap: bool                                # 是否为重叠符号
    metadata: Dict[str, Any] = field(default_factory=dict)  # 元数据
    
    def __post_init__(self):
        """初始化后处理"""
        self.exchange_count = len(self.exchanges)
        self.is_overlap = self.exchange_count >= 2 