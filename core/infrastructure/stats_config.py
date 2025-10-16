"""
统计信息配置读取器
根据交易所类型和币种数量动态调整统计信息打印频率
"""

import yaml
import os
from typing import Dict, Any, Optional
from pathlib import Path


class StatsConfigReader:
    """统计信息配置读取器"""
    
    def __init__(self, config_path: str = "config/logging.yaml"):
        """初始化配置读取器"""
        self.config_path = config_path
        self._config = None
        self._load_config()
    
    def _load_config(self) -> None:
        """加载配置文件"""
        try:
            config_file = Path(self.config_path)
            if config_file.exists():
                with open(config_file, 'r', encoding='utf-8') as f:
                    self._config = yaml.safe_load(f)
            else:
                self._config = {}
        except Exception as e:
            print(f"加载统计配置失败: {e}")
            self._config = {}
    
    def get_stats_frequency(self, exchange_id: str, symbol_count: Optional[int] = None) -> Dict[str, int]:
        """获取交易所的统计信息打印频率配置
        
        Args:
            exchange_id: 交易所ID (如: hyperliquid, backpack, edgex)
            symbol_count: 交易所支持的币种数量（可选，用于自适应配置）
            
        Returns:
            Dict[str, int]: 包含各种统计信息打印频率的字典
        """
        if not self._config:
            return self._get_default_frequency()
        
        stats_config = self._config.get('exchange_statistics_frequency', {})
        
        # 1. 优先使用交易所特定配置
        if exchange_id.lower() in stats_config:
            return stats_config[exchange_id.lower()]
        
        # 2. 如果提供了币种数量，使用自适应配置
        if symbol_count is not None:
            adaptive_config = self._get_adaptive_frequency(symbol_count, stats_config)
            if adaptive_config:
                return adaptive_config
        
        # 3. 使用默认配置
        return stats_config.get('default', self._get_default_frequency())
    
    def _get_adaptive_frequency(self, symbol_count: int, stats_config: Dict[str, Any]) -> Optional[Dict[str, int]]:
        """根据币种数量获取自适应频率配置"""
        try:
            adaptive_rules = stats_config.get('adaptive_rules', {})
            thresholds = adaptive_rules.get('thresholds', {})
            default_freq = stats_config.get('default', self._get_default_frequency())
            
            # 确定适用的阈值
            multiplier = 1.0
            for threshold_name, threshold_config in thresholds.items():
                max_symbols = threshold_config.get('max_symbols', 0)
                if symbol_count >= max_symbols:
                    multiplier = threshold_config.get('multiplier', 1.0)
            
            # 应用倍数
            if multiplier > 1.0:
                adapted_freq = {}
                for key, value in default_freq.items():
                    adapted_freq[key] = int(value * multiplier)
                return adapted_freq
            
            return None
        except Exception as e:
            print(f"自适应频率配置失败: {e}")
            return None
    
    def _get_default_frequency(self) -> Dict[str, int]:
        """获取默认统计信息打印频率"""
        return {
            'message_stats_frequency': 100,
            'callback_stats_frequency': 50,
            'orderbook_stats_frequency': 50,
            'global_callback_frequency': 50
        }
    
    def get_exchange_log_level(self, exchange_id: str) -> str:
        """获取交易所的日志级别"""
        if not self._config:
            return "INFO"
        
        loggers = self._config.get('loggers', {})
        exchange_logger = loggers.get(exchange_id.lower(), {})
        return exchange_logger.get('level', 'INFO')
    
    def should_reduce_logging(self, exchange_id: str) -> bool:
        """判断是否应该减少日志输出"""
        # 对于大型交易所，建议减少日志输出
        large_exchanges = ['hyperliquid', 'binance', 'okx', 'bybit']
        return exchange_id.lower() in large_exchanges
    
    def get_stats_summary(self, exchange_id: str, symbol_count: Optional[int] = None) -> str:
        """获取统计配置摘要信息"""
        freq_config = self.get_stats_frequency(exchange_id, symbol_count)
        
        if symbol_count:
            return (f"📊 {exchange_id.upper()}统计配置 (支持{symbol_count}种币): "
                   f"消息统计每{freq_config['message_stats_frequency']}条, "
                   f"回调统计每{freq_config['callback_stats_frequency']}个, "
                   f"订单簿统计每{freq_config['orderbook_stats_frequency']}个")
        else:
            return (f"📊 {exchange_id.upper()}统计配置: "
                   f"消息统计每{freq_config['message_stats_frequency']}条, "
                   f"回调统计每{freq_config['callback_stats_frequency']}个, "
                   f"订单簿统计每{freq_config['orderbook_stats_frequency']}个")


# 全局配置实例
_stats_config = None

def get_stats_config() -> StatsConfigReader:
    """获取全局统计配置实例"""
    global _stats_config
    if _stats_config is None:
        _stats_config = StatsConfigReader()
    return _stats_config


def get_exchange_stats_frequency(exchange_id: str, symbol_count: Optional[int] = None) -> Dict[str, int]:
    """便捷函数：获取交易所统计频率配置"""
    return get_stats_config().get_stats_frequency(exchange_id, symbol_count)


def get_exchange_stats_summary(exchange_id: str, symbol_count: Optional[int] = None) -> str:
    """便捷函数：获取交易所统计配置摘要"""
    return get_stats_config().get_stats_summary(exchange_id, symbol_count) 