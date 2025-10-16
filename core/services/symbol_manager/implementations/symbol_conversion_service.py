"""
符号转换服务实现

统一处理所有交易所的符号格式转换，消除架构冗余
"""

import re
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from injector import singleton, inject

from core.logging import get_logger
from core.infrastructure.config_manager import ConfigManager
from ..interfaces.symbol_conversion_service import ISymbolConversionService, SymbolFormat


@singleton
class SymbolConversionService(ISymbolConversionService):
    """符号转换服务实现"""
    
    @inject
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.logger = get_logger(__name__)
        
        # 配置和缓存
        self.config = None
        self.symbol_mappings = {}
        self.exchange_formats = {}
        self.cache = {}
        self.cache_timestamps = {}
        
        # 性能统计
        self.conversion_stats = {
            'total_conversions': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'conversion_errors': 0
        }
        
        # 初始化
        self._load_configuration()
        
        self.logger.info(f"✅ 符号转换服务初始化完成，支持 {len(self.exchange_formats)} 个交易所")
    
    def _load_configuration(self) -> None:
        """加载配置文件"""
        try:
            config_path = Path("config/symbol_conversion.yaml")
            if not config_path.exists():
                self.logger.error(f"配置文件不存在: {config_path}")
                self._load_default_configuration()
                return
            
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            
            # 解析配置
            self.exchange_formats = self.config.get('exchange_formats', {})
            self.symbol_mappings = self.config.get('symbol_mappings', {})
            self.validation_rules = self.config.get('validation', {})
            self.cache_config = self.config.get('cache', {})
            
            self.logger.info(f"📋 加载配置完成: {len(self.exchange_formats)} 个交易所格式")
            
        except Exception as e:
            self.logger.error(f"加载配置文件失败: {e}")
            self._load_default_configuration()
    
    def _load_default_configuration(self) -> None:
        """加载默认配置"""
        self.logger.warning("使用默认配置")
        self.exchange_formats = {
            'hyperliquid': {
                'format_type': 'hyperliquid',
                'pattern': '{base}/{quote}:{type}',
                'separator': '/',
                'type_separator': ':',
                'default_quote': 'USDC'
            },
            'backpack': {
                'format_type': 'backpack',
                'pattern': '{base}_{quote}_{type}',
                'separator': '_',
                'default_quote': 'USDC'
            },
            'edgex': {
                'format_type': 'edgex',
                'pattern': '{base}_{quote}_{type}',
                'separator': '_',
                'default_quote': 'USDT'
            }
        }
        self.symbol_mappings = {'standard_to_exchange': {}}
        self.validation_rules = {}
        self.cache_config = {'enabled': True, 'ttl': 3600, 'max_size': 10000}
    
    async def convert_to_exchange_format(self, standard_symbol: str, exchange: str) -> str:
        """将系统标准格式转换为交易所特定格式"""
        try:
            # 性能统计
            self.conversion_stats['total_conversions'] += 1
            
            # 检查缓存
            cache_key = f"to_{exchange}_{standard_symbol}"
            if self._check_cache(cache_key):
                self.conversion_stats['cache_hits'] += 1
                return self.cache[cache_key]
            
            self.conversion_stats['cache_misses'] += 1
            
            # 优先使用直接映射
            direct_mapping = self.symbol_mappings.get('standard_to_exchange', {}).get(exchange, {})
            if standard_symbol in direct_mapping:
                result = direct_mapping[standard_symbol]
                self._set_cache(cache_key, result)
                self.logger.debug(f"🔄 直接映射: {standard_symbol} -> {result} ({exchange})")
                return result
            
            # 使用格式转换
            result = self._convert_using_format(standard_symbol, exchange, to_exchange=True)
            self._set_cache(cache_key, result)
            
            if result != standard_symbol:
                self.logger.debug(f"🔄 格式转换: {standard_symbol} -> {result} ({exchange})")
            
            return result
            
        except Exception as e:
            self.conversion_stats['conversion_errors'] += 1
            self.logger.error(f"转换到交易所格式失败: {standard_symbol} -> {exchange} - {e}")
            return standard_symbol
    
    async def convert_from_exchange_format(self, exchange_symbol: str, exchange: str) -> str:
        """将交易所特定格式转换为系统标准格式"""
        try:
            # 性能统计
            self.conversion_stats['total_conversions'] += 1
            
            # 检查缓存
            cache_key = f"from_{exchange}_{exchange_symbol}"
            if self._check_cache(cache_key):
                self.conversion_stats['cache_hits'] += 1
                return self.cache[cache_key]
            
            self.conversion_stats['cache_misses'] += 1
            
            # 优先使用反向映射
            direct_mapping = self.symbol_mappings.get('standard_to_exchange', {}).get(exchange, {})
            reverse_mapping = {v: k for k, v in direct_mapping.items()}
            if exchange_symbol in reverse_mapping:
                result = reverse_mapping[exchange_symbol]
                self._set_cache(cache_key, result)
                self.logger.debug(f"🔄 反向映射: {exchange_symbol} -> {result} ({exchange})")
                return result
            
            # 使用格式转换
            result = self._convert_using_format(exchange_symbol, exchange, to_exchange=False)
            self._set_cache(cache_key, result)
            
            if result != exchange_symbol:
                self.logger.debug(f"🔄 反向转换: {exchange_symbol} -> {result} ({exchange})")
            
            return result
            
        except Exception as e:
            self.conversion_stats['conversion_errors'] += 1
            self.logger.error(f"从交易所格式转换失败: {exchange_symbol} -> {exchange} - {e}")
            return exchange_symbol
    
    def _convert_using_format(self, symbol: str, exchange: str, to_exchange: bool) -> str:
        """使用格式规则进行转换"""
        try:
            exchange_format = self.exchange_formats.get(exchange.lower())
            if not exchange_format:
                self.logger.warning(f"未找到交易所格式配置: {exchange}")
                return symbol
            
            if to_exchange:
                # 标准格式 -> 交易所格式
                return self._standard_to_exchange_format(symbol, exchange_format)
            else:
                # 交易所格式 -> 标准格式
                return self._exchange_to_standard_format(symbol, exchange_format)
                
        except Exception as e:
            self.logger.error(f"格式转换失败: {symbol} ({exchange}) - {e}")
            return symbol
    
    def _standard_to_exchange_format(self, standard_symbol: str, exchange_format: Dict[str, Any]) -> str:
        """标准格式转换为交易所格式"""
        try:
            # 解析标准格式：BTC-USDC-PERP
            parts = standard_symbol.split('-')
            if len(parts) < 2:
                return standard_symbol
            
            base = parts[0]
            quote = parts[1]
            symbol_type = parts[2] if len(parts) > 2 else 'PERP'
            
            # 应用quote映射
            quote_mapping = exchange_format.get('quote_mapping', {})
            if quote in quote_mapping:
                quote = quote_mapping[quote]
            
            # 应用类型映射
            type_mapping = exchange_format.get('type_mapping', {})
            mapped_type = type_mapping.get(symbol_type, symbol_type)
            
            # 根据交易所格式构建符号
            format_type = exchange_format.get('format_type', 'unknown')
            
            if format_type == 'hyperliquid':
                # BTC/USDC:PERP
                if mapped_type:
                    return f"{base}/{quote}:{mapped_type}"
                else:
                    return f"{base}/{quote}"
            
            elif format_type == 'backpack':
                # BTC_USDC_PERP
                if mapped_type:
                    return f"{base}_{quote}_{mapped_type}"
                else:
                    return f"{base}_{quote}"
            
            elif format_type == 'edgex':
                # BTC_USDT_PERP
                if mapped_type:
                    return f"{base}_{quote}_{mapped_type}"
                else:
                    return f"{base}_{quote}"
            
            elif format_type == 'binance':
                # BTCUSDT
                return f"{base}{quote}"
            
            else:
                return standard_symbol
                
        except Exception as e:
            self.logger.error(f"标准格式转换失败: {standard_symbol} - {e}")
            return standard_symbol
    
    def _exchange_to_standard_format(self, exchange_symbol: str, exchange_format: Dict[str, Any]) -> str:
        """交易所格式转换为标准格式"""
        try:
            format_type = exchange_format.get('format_type', 'unknown')
            
            if format_type == 'hyperliquid':
                # BTC/USDC:USDC -> BTC-USDC-PERP
                if '/' in exchange_symbol:
                    base_part, quote_part = exchange_symbol.split('/', 1)
                    if ':' in quote_part:
                        quote, symbol_type = quote_part.split(':', 1)
                        
                        # 🔥 修复：应用类型映射
                        type_mapping = exchange_format.get('type_mapping', {})
                        if symbol_type in type_mapping:
                            mapped_type = type_mapping[symbol_type]
                            # 如果映射结果是USDC，则转换为PERP
                            if mapped_type == 'USDC':
                                mapped_type = 'PERP'
                            return f"{base_part}-{quote}-{mapped_type}"
                        else:
                            # 🔥 修复：默认情况，如果symbol_type是USDC，转换为PERP
                            if symbol_type == 'USDC':
                                symbol_type = 'PERP'
                            return f"{base_part}-{quote}-{symbol_type}"
                    else:
                        return f"{base_part}-{quote_part}-SPOT"
            
            elif format_type == 'backpack':
                # BTC_USDC_PERP -> BTC-USDC-PERP
                parts = exchange_symbol.split('_')
                if len(parts) >= 3:
                    base = parts[0]
                    quote = parts[1]
                    symbol_type = parts[2]
                    return f"{base}-{quote}-{symbol_type}"
                elif len(parts) == 2:
                    base = parts[0]
                    quote = parts[1]
                    return f"{base}-{quote}-SPOT"
            
            elif format_type == 'edgex':
                # BTC_USDT_PERP -> BTC-USDC-PERP
                parts = exchange_symbol.split('_')
                if len(parts) >= 3:
                    base = parts[0]
                    quote = parts[1]
                    symbol_type = parts[2]
                    # 将USDT映射回USDC
                    if quote == 'USDT':
                        quote = 'USDC'
                    return f"{base}-{quote}-{symbol_type}"
                elif len(parts) == 2:
                    base = parts[0]
                    quote = parts[1]
                    if quote == 'USDT':
                        quote = 'USDC'
                    return f"{base}-{quote}-PERP"
            
            elif format_type == 'binance':
                # BTCUSDT -> BTC-USDC-PERP
                # 简单规则：假设最后4位是USDT，转换为USDC
                if exchange_symbol.endswith('USDT'):
                    base = exchange_symbol[:-4]
                    return f"{base}-USDC-PERP"
                elif exchange_symbol.endswith('USDC'):
                    base = exchange_symbol[:-4]
                    return f"{base}-USDC-PERP"
            
            return exchange_symbol
            
        except Exception as e:
            self.logger.error(f"交易所格式转换失败: {exchange_symbol} - {e}")
            return exchange_symbol
    
    async def batch_convert_to_exchange_format(self, symbols: List[str], exchange: str) -> Dict[str, str]:
        """批量转换符号到交易所格式"""
        results = {}
        for symbol in symbols:
            results[symbol] = await self.convert_to_exchange_format(symbol, exchange)
        return results
    
    async def batch_convert_from_exchange_format(self, symbols: List[str], exchange: str) -> Dict[str, str]:
        """批量转换符号从交易所格式"""
        results = {}
        for symbol in symbols:
            results[symbol] = await self.convert_from_exchange_format(symbol, exchange)
        return results
    
    async def get_supported_exchanges(self) -> List[str]:
        """获取支持的交易所列表"""
        return list(self.exchange_formats.keys())
    
    async def get_exchange_symbol_format(self, exchange: str) -> SymbolFormat:
        """获取交易所的符号格式类型"""
        format_type = self.exchange_formats.get(exchange.lower(), {}).get('format_type', 'unknown')
        try:
            return SymbolFormat(format_type)
        except ValueError:
            return SymbolFormat.STANDARD
    
    async def validate_standard_symbol(self, symbol: str) -> bool:
        """验证标准格式符号是否有效"""
        try:
            validation_rules = self.validation_rules.get('standard_format', {})
            
            # 检查长度
            min_length = validation_rules.get('min_length', 3)
            max_length = validation_rules.get('max_length', 50)
            if not (min_length <= len(symbol) <= max_length):
                return False
            
            # 检查分隔符
            required_separators = validation_rules.get('required_separators', ['-'])
            for sep in required_separators:
                if sep not in symbol:
                    return False
            
            # 检查格式：base-quote-type
            parts = symbol.split('-')
            if len(parts) < 2:
                return False
            
            # 检查有效类型
            if len(parts) > 2:
                symbol_type = parts[2]
                valid_types = validation_rules.get('valid_types', ['PERP', 'SPOT'])
                if symbol_type not in valid_types:
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"验证标准符号失败: {symbol} - {e}")
            return False
    
    async def validate_exchange_symbol(self, symbol: str, exchange: str) -> bool:
        """验证交易所格式符号是否有效"""
        try:
            validation_rules = self.validation_rules.get('exchange_formats', {}).get(exchange.lower(), {})
            if not validation_rules:
                return True  # 如果没有验证规则，默认有效
            
            # 检查长度
            min_length = validation_rules.get('min_length', 3)
            max_length = validation_rules.get('max_length', 50)
            if not (min_length <= len(symbol) <= max_length):
                return False
            
            # 检查必需分隔符
            required_separators = validation_rules.get('required_separators', [])
            for sep in required_separators:
                if sep not in symbol:
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"验证交易所符号失败: {symbol} ({exchange}) - {e}")
            return False
    
    async def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        """获取符号信息"""
        try:
            # 尝试解析为标准格式
            if '-' in symbol:
                parts = symbol.split('-')
                if len(parts) >= 2:
                    return {
                        'symbol': symbol,
                        'format': 'standard',
                        'base': parts[0],
                        'quote': parts[1],
                        'type': parts[2] if len(parts) > 2 else 'PERP',
                        'is_valid': await self.validate_standard_symbol(symbol)
                    }
            
            # 尝试识别交易所格式
            for exchange, format_config in self.exchange_formats.items():
                if await self.validate_exchange_symbol(symbol, exchange):
                    return {
                        'symbol': symbol,
                        'format': exchange,
                        'format_type': format_config.get('format_type', 'unknown'),
                        'is_valid': True
                    }
            
            return {
                'symbol': symbol,
                'format': 'unknown',
                'is_valid': False
            }
            
        except Exception as e:
            self.logger.error(f"获取符号信息失败: {symbol} - {e}")
            return {'symbol': symbol, 'format': 'unknown', 'is_valid': False}
    
    async def reload_configuration(self) -> bool:
        """重新加载配置"""
        try:
            self.logger.info("🔄 重新加载符号转换配置")
            
            # 清空缓存
            self.cache.clear()
            self.cache_timestamps.clear()
            
            # 重新加载配置
            self._load_configuration()
            
            self.logger.info("✅ 配置重新加载完成")
            return True
            
        except Exception as e:
            self.logger.error(f"重新加载配置失败: {e}")
            return False
    
    def _check_cache(self, cache_key: str) -> bool:
        """检查缓存是否有效"""
        if not self.cache_config.get('enabled', True):
            return False
        
        if cache_key not in self.cache:
            return False
        
        # 检查TTL
        ttl = self.cache_config.get('ttl', 3600)
        if cache_key in self.cache_timestamps:
            elapsed = (datetime.now() - self.cache_timestamps[cache_key]).total_seconds()
            if elapsed > ttl:
                # 缓存过期，删除
                del self.cache[cache_key]
                del self.cache_timestamps[cache_key]
                return False
        
        return True
    
    def _set_cache(self, cache_key: str, value: str) -> None:
        """设置缓存"""
        if not self.cache_config.get('enabled', True):
            return
        
        max_size = self.cache_config.get('max_size', 10000)
        
        # 检查缓存大小限制
        if len(self.cache) >= max_size:
            # 删除最旧的缓存项
            oldest_key = min(self.cache_timestamps.keys(), key=lambda k: self.cache_timestamps[k])
            del self.cache[oldest_key]
            del self.cache_timestamps[oldest_key]
        
        self.cache[cache_key] = value
        self.cache_timestamps[cache_key] = datetime.now()
    
    def get_conversion_stats(self) -> Dict[str, Any]:
        """获取转换统计信息"""
        total_conversions = self.conversion_stats['total_conversions']
        cache_hit_rate = 0
        if total_conversions > 0:
            cache_hit_rate = (self.conversion_stats['cache_hits'] / total_conversions) * 100
        
        return {
            'total_conversions': total_conversions,
            'cache_hits': self.conversion_stats['cache_hits'],
            'cache_misses': self.conversion_stats['cache_misses'],
            'cache_hit_rate': round(cache_hit_rate, 2),
            'conversion_errors': self.conversion_stats['conversion_errors'],
            'cache_size': len(self.cache),
            'supported_exchanges': len(self.exchange_formats)
        } 