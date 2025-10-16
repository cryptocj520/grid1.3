"""
符号缓存服务实现

提供符号缓存的核心功能实现，包括：
1. 一次性获取各交易所交易对
2. 计算重叠交易对（使用符号转换服务）
3. 缓存管理
4. 性能优化
"""

import asyncio
import time
from typing import Dict, List, Optional, Any
from collections import defaultdict
from injector import inject

from ....adapters.exchanges.manager import ExchangeManager
from ..interfaces.symbol_cache import ISymbolCacheService, SymbolCacheData, SymbolOverlapConfig
from ..models.symbol_cache_models import SymbolAnalysisResult
from ....logging import get_system_logger
from ..interfaces.symbol_conversion_service import ISymbolConversionService


class SymbolCacheServiceImpl(ISymbolCacheService):
    """符号缓存服务实现 - 专注于重叠分析和缓存管理"""
    
    @inject
    def __init__(self, 
                 exchange_manager: ExchangeManager,
                 symbol_conversion_service: ISymbolConversionService):
        """初始化符号缓存服务
        
        Args:
            exchange_manager: 交易所管理器
            symbol_conversion_service: 符号转换服务
        """
        self.exchange_manager = exchange_manager
        self.symbol_conversion_service = symbol_conversion_service
        self.logger = get_system_logger()
        
        # 缓存数据
        self._cache_data: Optional[SymbolCacheData] = None
        self._initialized = False
        self._initialization_time = 0
        
        # 预定义安全符号列表（获取失败时使用）
        self._fallback_symbols = [
            "BTC_USDT_PERP", "ETH_USDT_PERP", "SOL_USDT_PERP",
            "AVAX_USDT_PERP", "DOGE_USDT_PERP", "ADA_USDT_PERP",
            "DOT_USDT_PERP", "MATIC_USDT_PERP", "LINK_USDT_PERP",
            "UNI_USDT_PERP"
        ]
    
    async def initialize_cache(self, exchange_ids: List[str], config: Optional[SymbolOverlapConfig] = None) -> bool:
        """初始化符号缓存（只在启动时调用一次）"""
        if self._initialized:
            self.logger.info("符号缓存已初始化，跳过重复初始化")
            return True
            
        self.logger.info(f"🚀 开始初始化符号缓存: {exchange_ids}")
        start_time = time.time()
        
        try:
            # 使用默认配置
            if config is None:
                config = SymbolOverlapConfig()
            
            # 1. 并行获取各交易所的交易对
            self.logger.info("📊 并行获取各交易所支持的交易对...")
            exchange_symbols = await self._fetch_all_exchange_symbols(exchange_ids)
            
            if not exchange_symbols:
                self.logger.error("❌ 获取交易对失败，使用预定义列表")
                return await self._initialize_fallback_cache(exchange_ids, config)
            
            # 2. 计算重叠交易对（使用符号转换服务）
            self.logger.info("🔍 计算重叠交易对...")
            analysis_result = await self._analyze_symbol_overlap(exchange_symbols, config)
            
            # 3. 生成订阅符号列表
            self.logger.info("📋 生成各交易所订阅列表...")
            subscription_symbols = await self._generate_subscription_symbols(
                exchange_symbols, analysis_result, config
            )
            
            # 4. 创建缓存数据
            self._cache_data = SymbolCacheData(
                exchange_symbols=exchange_symbols,
                overlap_symbols=analysis_result['overlap_symbols'],
                subscription_symbols=subscription_symbols,
                timestamp=time.time(),
                total_symbols=analysis_result['total_unique_symbols'],
                overlap_count=len(analysis_result['overlap_symbols']),
                metadata={
                    'config': config,
                    'exchange_ids': exchange_ids,
                    'initialization_time': time.time() - start_time,
                    'analysis_result': analysis_result
                }
            )
            
            self._initialized = True
            self._initialization_time = time.time() - start_time
            
            # 5. 记录初始化结果
            self._log_initialization_result()
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 符号缓存初始化失败: {e}")
            return await self._initialize_fallback_cache(exchange_ids, config)
    
    async def _fetch_all_exchange_symbols(self, exchange_ids: List[str]) -> Dict[str, List[str]]:
        """并行获取所有交易所的交易对"""
        if not self.exchange_manager:
            self.logger.error("❌ 交易所管理器未设置")
            return {}
        
        # 获取连接的适配器
        connected_adapters = self.exchange_manager.get_connected_adapters()
        
        # 创建获取任务
        tasks = []
        valid_exchanges = []
        
        for exchange_id in exchange_ids:
            if exchange_id in connected_adapters:
                adapter = connected_adapters[exchange_id]
                task = self._get_exchange_symbols_with_timeout(exchange_id, adapter)
                tasks.append(task)
                valid_exchanges.append(exchange_id)
            else:
                self.logger.warning(f"⚠️ 交易所 {exchange_id} 未连接，跳过")
        
        if not tasks:
            self.logger.error("❌ 没有有效的交易所连接")
            return {}
        
        # 并行执行
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理结果
        exchange_symbols = {}
        for i, result in enumerate(results):
            exchange_id = valid_exchanges[i]
            if isinstance(result, Exception):
                self.logger.error(f"❌ 获取 {exchange_id} 交易对失败: {result}")
            elif isinstance(result, list) and result:
                exchange_symbols[exchange_id] = result
                self.logger.info(f"✅ {exchange_id}: {len(result)} 个交易对")
            else:
                self.logger.warning(f"⚠️ {exchange_id}: 未获取到有效交易对")
        
        return exchange_symbols
    
    async def _get_exchange_symbols_with_timeout(self, exchange_id: str, adapter, timeout: int = 30) -> List[str]:
        """获取交易所交易对（带超时）"""
        try:
            return await asyncio.wait_for(
                adapter.get_supported_symbols(),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            self.logger.error(f"❌ 获取 {exchange_id} 交易对超时")
            return []
        except Exception as e:
            self.logger.error(f"❌ 获取 {exchange_id} 交易对出错: {e}")
            return []
    
    async def _analyze_symbol_overlap(self, exchange_symbols: Dict[str, List[str]], 
                                    config: SymbolOverlapConfig) -> Dict[str, Any]:
        """分析交易对重叠情况（使用符号转换服务）"""
        # 🔥 重构：使用符号转换服务进行标准化
        self.logger.info("🔍 使用符号转换服务分析重叠情况:")
        
        # 统计每个标准化符号在哪些交易所中存在
        standardized_symbol_exchanges = defaultdict(list)  # 标准化符号 -> 交易所列表
        symbol_mapping = defaultdict(dict)  # 标准化符号 -> {交易所: 原始符号}
        
        for exchange_id, symbols in exchange_symbols.items():
            self.logger.info(f"✅ {exchange_id}: {len(symbols)} 个符号")
            self.logger.info(f"   前10个符号: {symbols[:10]}")
            
            for symbol in symbols:
                # 🔥 使用符号转换服务将交易所格式转换为标准格式
                try:
                    standardized = await self.symbol_conversion_service.convert_from_exchange_format(
                        symbol, exchange_id
                    )
                    if standardized:
                        standardized_symbol_exchanges[standardized].append(exchange_id)
                        symbol_mapping[standardized][exchange_id] = symbol
                except Exception as e:
                    self.logger.warning(f"⚠️ 符号转换失败 {symbol} ({exchange_id}): {e}")
                    # 转换失败时使用原始符号
                    standardized_symbol_exchanges[symbol].append(exchange_id)
                    symbol_mapping[symbol][exchange_id] = symbol
        
        # 分析重叠情况
        overlap_symbols = []
        all_standardized_symbols = list(standardized_symbol_exchanges.keys())
        
        # 详细记录重叠分析过程
        self.logger.info("🔍 重叠分析详情:")
        for standardized_symbol, exchanges in standardized_symbol_exchanges.items():
            if len(exchanges) >= config.min_exchange_count:
                overlap_symbols.append(standardized_symbol)
                self.logger.info(f"✅ 重叠符号: {standardized_symbol} 存在于 {exchanges}")
        
        # 应用过滤条件
        if config.include_patterns:
            overlap_symbols = [s for s in overlap_symbols 
                             if any(self._match_pattern(s, pattern) for pattern in config.include_patterns)]
        
        if config.exclude_patterns:
            overlap_symbols = [s for s in overlap_symbols 
                             if not any(self._match_pattern(s, pattern) for pattern in config.exclude_patterns)]
        
        # 限制数量
        if config.max_symbols_per_exchange > 0:
            overlap_symbols = overlap_symbols[:config.max_symbols_per_exchange]
        
        # 记录标准化效果
        original_total = sum(len(symbols) for symbols in exchange_symbols.values())
        standardized_total = len(all_standardized_symbols)
        
        self.logger.info(f"🔄 符号标准化: {original_total} -> {standardized_total} ({len(overlap_symbols)} 重叠)")
        
        return {
            'overlap_symbols': overlap_symbols,
            'symbol_exchanges': dict(standardized_symbol_exchanges),
            'symbol_mapping': dict(symbol_mapping),
            'total_unique_symbols': len(all_standardized_symbols),
            'overlap_count': len(overlap_symbols),
            'exchange_coverage': {
                symbol: len(exchanges) for symbol, exchanges in standardized_symbol_exchanges.items()
            }
        }
    
    async def _generate_subscription_symbols(self, exchange_symbols: Dict[str, List[str]], 
                                           analysis_result: Dict[str, Any], 
                                           config: SymbolOverlapConfig) -> Dict[str, List[str]]:
        """生成各交易所的订阅符号列表（转换回原始符号）"""
        subscription_symbols = {}
        
        if config.use_overlap_only:
            # 只使用重叠符号模式
            overlap_symbols = set(analysis_result['overlap_symbols'])
            symbol_mapping = analysis_result.get('symbol_mapping', {})
            
            for exchange_id, all_symbols in exchange_symbols.items():
                # 只使用重叠符号，需要转换回原始符号
                symbols = []
                for standardized_symbol in overlap_symbols:
                    if exchange_id in symbol_mapping.get(standardized_symbol, {}):
                        original_symbol = symbol_mapping[standardized_symbol][exchange_id]
                        symbols.append(original_symbol)
                
                # 应用数量限制
                if config.max_symbols_per_exchange > 0 and len(symbols) > config.max_symbols_per_exchange:
                    symbols = symbols[:config.max_symbols_per_exchange]
                
                subscription_symbols[exchange_id] = symbols
        else:
            # 使用各交易所自己的符号列表
            for exchange_id, all_symbols in exchange_symbols.items():
                symbols = all_symbols.copy()
                
                # 应用数量限制
                if config.max_symbols_per_exchange > 0 and len(symbols) > config.max_symbols_per_exchange:
                    symbols = symbols[:config.max_symbols_per_exchange]
                
                subscription_symbols[exchange_id] = symbols
        
        return subscription_symbols
    
    def _match_pattern(self, symbol: str, pattern: str) -> bool:
        """匹配符号模式"""
        # 简单的通配符匹配
        import re
        regex_pattern = pattern.replace('*', '.*')
        return bool(re.match(regex_pattern, symbol))
    
    async def _initialize_fallback_cache(self, exchange_ids: List[str], 
                                       config: SymbolOverlapConfig) -> bool:
        """使用预定义符号列表初始化缓存"""
        self.logger.info("🔄 使用预定义符号列表初始化缓存")
        
        # 为每个交易所分配相同的预定义符号
        exchange_symbols = {exchange_id: self._fallback_symbols.copy() for exchange_id in exchange_ids}
        subscription_symbols = exchange_symbols.copy()
        
        self._cache_data = SymbolCacheData(
            exchange_symbols=exchange_symbols,
            overlap_symbols=self._fallback_symbols.copy(),
            subscription_symbols=subscription_symbols,
            timestamp=time.time(),
            total_symbols=len(self._fallback_symbols),
            overlap_count=len(self._fallback_symbols),
            metadata={
                'config': config,
                'exchange_ids': exchange_ids,
                'is_fallback': True
            }
        )
        
        self._initialized = True
        self.logger.info(f"✅ 预定义缓存初始化完成: {len(self._fallback_symbols)} 个符号")
        return True
    
    def _log_initialization_result(self):
        """记录初始化结果"""
        if not self._cache_data:
            return
        
        self.logger.info("=" * 60)
        self.logger.info("🎉 符号缓存初始化完成")
        self.logger.info("=" * 60)
        
        # 基本统计
        self.logger.info(f"📊 总计: {self._cache_data.total_symbols} 个独特符号")
        self.logger.info(f"🔗 重叠: {self._cache_data.overlap_count} 个符号")
        self.logger.info(f"⏱️  耗时: {self._initialization_time:.2f} 秒")
        
        # 各交易所统计
        self.logger.info("📋 各交易所订阅统计:")
        for exchange_id, symbols in self._cache_data.subscription_symbols.items():
            self.logger.info(f"  - {exchange_id}: {len(symbols)} 个符号")
        
        # 重叠符号示例
        if self._cache_data.overlap_symbols:
            sample_symbols = self._cache_data.overlap_symbols[:10]
            self.logger.info(f"🔍 重叠符号示例: {', '.join(sample_symbols)}")
            if len(self._cache_data.overlap_symbols) > 10:
                self.logger.info(f"   ... 还有 {len(self._cache_data.overlap_symbols) - 10} 个")
        
        self.logger.info("=" * 60)
    
    def get_symbols_for_exchange(self, exchange_id: str) -> List[str]:
        """获取指定交易所应该订阅的符号列表"""
        if not self._cache_data:
            self.logger.warning(f"⚠️ 缓存未初始化，返回空列表")
            return []
        
        return self._cache_data.subscription_symbols.get(exchange_id, [])
    
    def get_overlap_symbols(self) -> List[str]:
        """获取重叠的符号列表"""
        if not self._cache_data:
            return []
        
        return self._cache_data.overlap_symbols.copy()
    
    def get_all_exchange_symbols(self) -> Dict[str, List[str]]:
        """获取所有交易所的符号列表"""
        if not self._cache_data:
            return {}
        
        return {k: v.copy() for k, v in self._cache_data.exchange_symbols.items()}
    
    def is_cache_valid(self) -> bool:
        """检查缓存是否有效"""
        return self._initialized and self._cache_data is not None
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        if not self._cache_data:
            return {"status": "not_initialized"}
        
        return {
            "status": "initialized",
            "total_symbols": self._cache_data.total_symbols,
            "overlap_count": self._cache_data.overlap_count,
            "exchanges": list(self._cache_data.exchange_symbols.keys()),
            "timestamp": self._cache_data.timestamp,
            "initialization_time": self._initialization_time,
            "is_fallback": self._cache_data.metadata.get('is_fallback', False)
        }
    
    def clear_cache(self) -> None:
        """清空缓存"""
        self._cache_data = None
        self._initialized = False
        self._initialization_time = 0
        self.logger.info("🗑️  符号缓存已清空") 