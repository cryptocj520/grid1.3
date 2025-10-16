"""
精度管理器

负责从各个交易所适配器获取精度信息并缓存，在套利模块运行前初始化
"""

import asyncio
import math
from datetime import datetime
from typing import Dict, List, Optional, Any
from decimal import Decimal

from core.logging import get_logger
from core.adapters.exchanges.interface import ExchangeInterface
from ..shared.models import PrecisionInfo
from ..shared.precision_cache import PrecisionCacheManager


class PrecisionManager:
    """精度管理器"""
    
    def __init__(self, exchange_adapters: Dict[str, ExchangeInterface]):
        """
        初始化精度管理器
        
        Args:
            exchange_adapters: 交易所适配器字典
        """
        self.exchange_adapters = exchange_adapters
        self.cache_manager = PrecisionCacheManager()
        self.logger = get_logger(__name__)
        self._initialized = False
    
    async def initialize_precision_cache(self, overlapping_symbols: List[str]) -> bool:
        """
        初始化精度缓存
        
        Args:
            overlapping_symbols: 重叠交易对列表
            
        Returns:
            是否成功初始化
        """
        try:
            self.logger.info(f"开始初始化精度缓存，共 {len(overlapping_symbols)} 个交易对")
            
            # 启动缓存管理器
            await self.cache_manager.start()
            
            # 获取所有交易所的精度信息
            total_success = 0
            total_failed = 0
            
            for exchange_name, adapter in self.exchange_adapters.items():
                success_count, failed_count = await self._initialize_exchange_precision(
                    exchange_name, adapter, overlapping_symbols
                )
                total_success += success_count
                total_failed += failed_count
            
            # 记录初始化结果
            self.logger.info(f"精度缓存初始化完成: 成功 {total_success}, 失败 {total_failed}")
            
            # 打印缓存统计信息
            stats = await self.cache_manager.get_stats()
            self.logger.info(f"缓存统计: {stats}")
            
            self._initialized = True
            return total_failed == 0
            
        except Exception as e:
            self.logger.error(f"初始化精度缓存失败: {e}")
            return False
    
    async def _initialize_exchange_precision(
        self, 
        exchange_name: str, 
        adapter: ExchangeInterface, 
        symbols: List[str]
    ) -> tuple[int, int]:
        """
        初始化单个交易所的精度信息
        
        Args:
            exchange_name: 交易所名称
            adapter: 交易所适配器
            symbols: 交易对列表
            
        Returns:
            (成功数量, 失败数量)
        """
        success_count = 0
        failed_count = 0
        
        self.logger.info(f"获取 {exchange_name} 的精度信息...")
        
        try:
            # 确保适配器已连接
            if not adapter.is_connected():
                await adapter.connect()
            
            # 获取交易所信息
            exchange_info = await adapter.get_exchange_info()
            if not exchange_info or not exchange_info.markets:
                self.logger.error(f"{exchange_name} 无法获取交易所信息")
                return 0, len(symbols)
            
            # 为每个交易对获取精度信息
            for symbol in symbols:
                try:
                    precision_info = await self._get_symbol_precision(
                        exchange_name, symbol, exchange_info.markets
                    )
                    
                    if precision_info:
                        await self.cache_manager.set_precision(
                            exchange_name, symbol, precision_info
                        )
                        success_count += 1
                        self.logger.debug(f"获取精度信息成功: {exchange_name}:{symbol}")
                    else:
                        failed_count += 1
                        self.logger.warning(f"获取精度信息失败: {exchange_name}:{symbol}")
                        
                except Exception as e:
                    failed_count += 1
                    self.logger.error(f"获取精度信息异常: {exchange_name}:{symbol} - {e}")
            
            self.logger.info(f"{exchange_name} 精度信息获取完成: 成功 {success_count}, 失败 {failed_count}")
            
        except Exception as e:
            self.logger.error(f"初始化 {exchange_name} 精度信息失败: {e}")
            failed_count = len(symbols)
        
        return success_count, failed_count
    
    async def _get_symbol_precision(
        self, 
        exchange_name: str, 
        symbol: str, 
        markets: Dict[str, Any]
    ) -> Optional[PrecisionInfo]:
        """
        获取单个交易对的精度信息
        
        Args:
            exchange_name: 交易所名称
            symbol: 交易对符号
            markets: 市场信息
            
        Returns:
            精度信息
        """
        try:
            # 从市场信息中获取精度数据
            market_info = markets.get(symbol)
            if not market_info:
                return None
            
            precision_data = market_info.get('precision', {})
            limits = market_info.get('limits', {})
            
            # 转换精度信息
            price_precision = self._convert_precision_to_decimals(
                precision_data.get('price', 0)
            )
            amount_precision = self._convert_precision_to_decimals(
                precision_data.get('amount', 0)
            )
            
            # 获取限制信息
            amount_limits = limits.get('amount', {})
            price_limits = limits.get('price', {})
            
            # 安全地转换为Decimal
            min_value = amount_limits.get('min', 0)
            max_value = amount_limits.get('max', 1000000)
            
            min_order_size = Decimal(str(min_value if min_value is not None else 0))
            max_order_size = Decimal(str(max_value if max_value is not None else 1000000))
            
            # 计算tick_size和step_size
            tick_size = Decimal('0.1') ** price_precision if price_precision > 0 else Decimal('1')
            step_size = Decimal('0.1') ** amount_precision if amount_precision > 0 else Decimal('1')
            
            return PrecisionInfo(
                symbol=symbol,
                exchange=exchange_name,
                price_precision=price_precision,
                amount_precision=amount_precision,
                min_order_size=min_order_size,
                max_order_size=max_order_size,
                tick_size=tick_size,
                step_size=step_size,
                last_updated=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"转换精度信息失败: {exchange_name}:{symbol} - {e}")
            return None
    
    def _convert_precision_to_decimals(self, precision_value: Any) -> int:
        """
        将精度值转换为小数位数
        
        Args:
            precision_value: 精度值
            
        Returns:
            小数位数
        """
        try:
            if precision_value is None:
                return 0
            
            # 如果是科学计数法（如 1e-05）
            if isinstance(precision_value, float):
                if precision_value == 0:
                    return 0
                # 计算小数点后的位数
                decimal_places = -int(math.log10(precision_value))
                return max(0, decimal_places)
            
            # 如果是整数或字符串
            elif isinstance(precision_value, (int, str)):
                return max(0, int(float(precision_value)))
            
            else:
                return 0
                
        except Exception:
            return 0
    
    async def get_symbol_precision(self, exchange: str, symbol: str) -> Optional[PrecisionInfo]:
        """
        获取指定交易对的精度信息
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            
        Returns:
            精度信息
        """
        if not self._initialized:
            self.logger.warning("精度管理器未初始化")
            return None
        
        return await self.cache_manager.get_precision(exchange, symbol)
    
    async def refresh_symbol_precision(self, exchange: str, symbol: str) -> bool:
        """
        刷新指定交易对的精度信息
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            
        Returns:
            是否成功刷新
        """
        try:
            adapter = self.exchange_adapters.get(exchange)
            if not adapter:
                self.logger.error(f"未找到交易所适配器: {exchange}")
                return False
            
            # 获取最新的交易所信息
            exchange_info = await adapter.get_exchange_info()
            if not exchange_info or not exchange_info.markets:
                self.logger.error(f"{exchange} 无法获取交易所信息")
                return False
            
            # 获取最新的精度信息
            precision_info = await self._get_symbol_precision(
                exchange, symbol, exchange_info.markets
            )
            
            if precision_info:
                await self.cache_manager.set_precision(exchange, symbol, precision_info)
                self.logger.info(f"刷新精度信息成功: {exchange}:{symbol}")
                return True
            else:
                self.logger.error(f"刷新精度信息失败: {exchange}:{symbol}")
                return False
                
        except Exception as e:
            self.logger.error(f"刷新精度信息异常: {exchange}:{symbol} - {e}")
            return False
    
    async def get_all_precision_for_symbol(self, symbol: str) -> List[PrecisionInfo]:
        """
        获取指定交易对在所有交易所的精度信息
        
        Args:
            symbol: 交易对符号
            
        Returns:
            精度信息列表
        """
        if not self._initialized:
            self.logger.warning("精度管理器未初始化")
            return []
        
        return await self.cache_manager.cache.get_all_for_symbol(symbol)
    
    async def get_precision_stats(self) -> Dict[str, Any]:
        """
        获取精度缓存统计信息
        
        Returns:
            统计信息
        """
        if not self._initialized:
            return {'status': 'not_initialized'}
        
        stats = await self.cache_manager.get_stats()
        stats['initialized'] = self._initialized
        return stats
    
    async def shutdown(self):
        """关闭精度管理器"""
        try:
            await self.cache_manager.stop()
            self._initialized = False
            self.logger.info("精度管理器已关闭")
        except Exception as e:
            self.logger.error(f"关闭精度管理器时出错: {e}")
    
    # TODO: 高级功能占位符
    async def auto_refresh_precision(self, interval: int = 3600):
        """
        自动刷新精度信息
        
        Args:
            interval: 刷新间隔（秒）
        """
        # TODO: 实现自动刷新机制
        pass
    
    async def validate_precision_consistency(self) -> Dict[str, Any]:
        """
        验证精度一致性
        
        Returns:
            验证结果
        """
        # TODO: 实现精度一致性验证
        return {'status': 'not_implemented'}
    
    async def export_precision_data(self, file_path: str):
        """
        导出精度数据到文件
        
        Args:
            file_path: 文件路径
        """
        # TODO: 实现精度数据导出
        pass
    
    async def import_precision_data(self, file_path: str):
        """
        从文件导入精度数据
        
        Args:
            file_path: 文件路径
        """
        # TODO: 实现精度数据导入
        pass 