"""
精度缓存模块

负责存储和管理不同交易所的精度信息，提供快速查询接口
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple
from decimal import Decimal

from core.logging import get_logger
from .models import PrecisionInfo


class PrecisionCache:
    """精度信息缓存"""
    
    def __init__(self, cache_ttl: int = 3600):
        """
        初始化精度缓存
        
        Args:
            cache_ttl: 缓存过期时间（秒），默认1小时
        """
        self.cache_ttl = cache_ttl
        self._cache: Dict[str, PrecisionInfo] = {}
        self._lock = asyncio.Lock()
        self.logger = get_logger(__name__)
    
    def _get_cache_key(self, exchange: str, symbol: str) -> str:
        """生成缓存键"""
        return f"{exchange}:{symbol}"
    
    async def get(self, exchange: str, symbol: str) -> Optional[PrecisionInfo]:
        """
        获取精度信息
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            
        Returns:
            精度信息，如果不存在或过期则返回None
        """
        cache_key = self._get_cache_key(exchange, symbol)
        
        async with self._lock:
            precision_info = self._cache.get(cache_key)
            
            if precision_info is None:
                return None
            
            # 检查是否过期
            if self._is_expired(precision_info):
                del self._cache[cache_key]
                return None
            
            return precision_info
    
    async def set(self, exchange: str, symbol: str, precision_info: PrecisionInfo):
        """
        设置精度信息
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            precision_info: 精度信息
        """
        cache_key = self._get_cache_key(exchange, symbol)
        
        async with self._lock:
            self._cache[cache_key] = precision_info
        
        self.logger.debug(f"缓存精度信息: {cache_key}")
    
    async def get_all_for_exchange(self, exchange: str) -> List[PrecisionInfo]:
        """
        获取指定交易所的所有精度信息
        
        Args:
            exchange: 交易所名称
            
        Returns:
            精度信息列表
        """
        result = []
        
        async with self._lock:
            for key, precision_info in self._cache.items():
                if key.startswith(f"{exchange}:") and not self._is_expired(precision_info):
                    result.append(precision_info)
        
        return result
    
    async def get_all_for_symbol(self, symbol: str) -> List[PrecisionInfo]:
        """
        获取指定交易对的所有精度信息
        
        Args:
            symbol: 交易对符号
            
        Returns:
            精度信息列表
        """
        result = []
        
        async with self._lock:
            for key, precision_info in self._cache.items():
                if key.endswith(f":{symbol}") and not self._is_expired(precision_info):
                    result.append(precision_info)
        
        return result
    
    async def remove(self, exchange: str, symbol: str) -> bool:
        """
        移除精度信息
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            
        Returns:
            是否成功移除
        """
        cache_key = self._get_cache_key(exchange, symbol)
        
        async with self._lock:
            if cache_key in self._cache:
                del self._cache[cache_key]
                return True
            return False
    
    async def clear(self):
        """清空缓存"""
        async with self._lock:
            self._cache.clear()
        
        self.logger.info("精度缓存已清空")
    
    async def clear_expired(self):
        """清理过期缓存"""
        expired_keys = []
        
        async with self._lock:
            for key, precision_info in self._cache.items():
                if self._is_expired(precision_info):
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self._cache[key]
        
        if expired_keys:
            self.logger.info(f"清理了 {len(expired_keys)} 个过期的精度缓存")
    
    def _is_expired(self, precision_info: PrecisionInfo) -> bool:
        """检查精度信息是否过期"""
        if self.cache_ttl <= 0:
            return False
        
        now = datetime.now()
        return (now - precision_info.last_updated).total_seconds() > self.cache_ttl
    
    async def get_cache_stats(self) -> Dict[str, int]:
        """
        获取缓存统计信息
        
        Returns:
            缓存统计信息
        """
        async with self._lock:
            total_count = len(self._cache)
            expired_count = sum(1 for info in self._cache.values() if self._is_expired(info))
            valid_count = total_count - expired_count
            
            # 按交易所统计
            exchange_stats = {}
            for key in self._cache.keys():
                exchange = key.split(':')[0]
                exchange_stats[exchange] = exchange_stats.get(exchange, 0) + 1
        
        return {
            'total_count': total_count,
            'valid_count': valid_count,
            'expired_count': expired_count,
            'exchange_stats': exchange_stats
        }
    
    async def exists(self, exchange: str, symbol: str) -> bool:
        """
        检查精度信息是否存在且有效
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            
        Returns:
            是否存在有效的精度信息
        """
        precision_info = await self.get(exchange, symbol)
        return precision_info is not None
    
    async def batch_get(self, requests: List[Tuple[str, str]]) -> Dict[str, Optional[PrecisionInfo]]:
        """
        批量获取精度信息
        
        Args:
            requests: 请求列表，每个元素为(exchange, symbol)
            
        Returns:
            结果字典，key为"exchange:symbol"，value为精度信息
        """
        results = {}
        
        for exchange, symbol in requests:
            cache_key = self._get_cache_key(exchange, symbol)
            precision_info = await self.get(exchange, symbol)
            results[cache_key] = precision_info
        
        return results
    
    async def batch_set(self, precision_data: Dict[str, PrecisionInfo]):
        """
        批量设置精度信息
        
        Args:
            precision_data: 精度数据字典，key为"exchange:symbol"
        """
        async with self._lock:
            for cache_key, precision_info in precision_data.items():
                self._cache[cache_key] = precision_info
        
        self.logger.info(f"批量设置了 {len(precision_data)} 个精度信息")


class PrecisionCacheManager:
    """精度缓存管理器"""
    
    def __init__(self, cache_ttl: int = 3600, cleanup_interval: int = 300):
        """
        初始化精度缓存管理器
        
        Args:
            cache_ttl: 缓存过期时间（秒）
            cleanup_interval: 清理间隔（秒）
        """
        self.cache = PrecisionCache(cache_ttl)
        self.cleanup_interval = cleanup_interval
        self._cleanup_task: Optional[asyncio.Task] = None
        self.logger = get_logger(__name__)
    
    async def start(self):
        """启动缓存管理器"""
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            self.logger.info("精度缓存管理器已启动")
    
    async def stop(self):
        """停止缓存管理器"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
            self.logger.info("精度缓存管理器已停止")
    
    async def _cleanup_loop(self):
        """定期清理过期缓存"""
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self.cache.clear_expired()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"清理过期缓存时出错: {e}")
    
    async def get_precision(self, exchange: str, symbol: str) -> Optional[PrecisionInfo]:
        """获取精度信息"""
        return await self.cache.get(exchange, symbol)
    
    async def set_precision(self, exchange: str, symbol: str, precision_info: PrecisionInfo):
        """设置精度信息"""
        await self.cache.set(exchange, symbol, precision_info)
    
    async def get_stats(self) -> Dict[str, int]:
        """获取缓存统计信息"""
        return await self.cache.get_cache_stats() 