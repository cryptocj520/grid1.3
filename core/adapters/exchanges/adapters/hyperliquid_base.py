"""
Hyperliquid交易所基础模块 - 重构版

包含基础配置、工具方法等共用功能
重构：简化符号映射，推荐使用统一符号转换服务
"""

import time
import asyncio
import yaml
from pathlib import Path
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

from ....logging import get_logger


class HyperliquidBase:
    """Hyperliquid基础类 - 重构版"""

    # API端点配置
    DEFAULT_REST_URL = "https://api.hyperliquid.xyz"
    DEFAULT_WS_URL = "wss://api.hyperliquid.xyz/ws"

    # 支持的时间周期
    SUPPORTED_TIMEFRAMES = {
        '1m': '1m',
        '3m': '3m',
        '5m': '5m',
        '15m': '15m',
        '30m': '30m',
        '1h': '1h',
        '2h': '2h',
        '4h': '4h',
        '6h': '6h',
        '8h': '8h',
        '12h': '12h',
        '1d': '1d',
        '3d': '3d',
        '1w': '1w',
        '1M': '1M'
    }

    # WebSocket黑名单（参考Backpack模式）
    WEBSOCKET_BLACKLIST = [
        # 添加有问题的交易对
    ]

    def __init__(self, config=None):
        """初始化基础配置"""
        self.config = config
        self.logger = None

        # 初始化URL配置
        self._setup_urls()

        # 🚀 加载市场类型配置
        self._load_market_config()

        # 🔥 重构：简化符号映射配置
        self._setup_legacy_symbol_mappings()

        # 支持的交易对缓存
        self._supported_symbols = []
        self._market_info = {}

    def _setup_urls(self):
        """设置API URL"""
        if self.config:
            self.base_url = self.config.base_url or self.DEFAULT_REST_URL
            self.ws_url = self.config.ws_url or self.DEFAULT_WS_URL
        else:
            self.base_url = self.DEFAULT_REST_URL
            self.ws_url = self.DEFAULT_WS_URL

    def _load_market_config(self):
        """加载市场配置"""
        try:
            # 尝试从配置文件加载
            config_path = Path("config/exchanges/hyperliquid_config.yaml")
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    self.market_config = yaml.safe_load(f)

                # 解析市场配置
                markets = self.market_config.get('markets', {})
                self.perpetual_enabled = markets.get(
                    'perpetual', {}).get('enabled', True)
                self.spot_enabled = markets.get(
                    'spot', {}).get('enabled', False)
                self.market_priority = markets.get('priority', 'perpetual')
                self.default_market = markets.get('default', 'perpetual')

                # 解析特定市场的映射
                self.perpetual_mapping = markets.get(
                    'perpetual', {}).get('symbol_mapping', {})
                self.spot_mapping = markets.get(
                    'spot', {}).get('symbol_mapping', {})

                if self.logger:
                    self.logger.info(
                        f"✅ 加载Hyperliquid市场配置: 永续={self.perpetual_enabled}, 现货={self.spot_enabled}")

            else:
                if self.logger:
                    self.logger.warning(f"⚠️ 配置文件不存在: {config_path}")
                self._setup_default_market_config()

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 加载市场配置失败: {e}")
            self._setup_default_market_config()

    def _setup_default_market_config(self):
        """设置默认市场配置"""
        self.market_config = {}
        self.perpetual_enabled = True
        self.spot_enabled = False
        self.market_priority = 'perpetual'
        self.default_market = 'perpetual'
        self.perpetual_mapping = {}
        self.spot_mapping = {}

    def _setup_legacy_symbol_mappings(self):
        """
        设置遗留符号映射（已弃用）

        @deprecated: 建议使用统一的符号转换服务
        """
        # 🔥 重构：简化映射配置，只保留最基本的映射
        self._default_symbol_mapping = {}

        # 只保留配置文件中明确定义的映射
        if hasattr(self, 'market_config') and self.market_config:
            symbol_mappings = self.market_config.get('symbol_mapping', {})
            backpack_mappings = symbol_mappings.get(
                'backpack_to_hyperliquid', {})
            if backpack_mappings:
                self._default_symbol_mapping.update(backpack_mappings)

        # 合并用户配置的符号映射
        if self.config and hasattr(self.config, 'symbol_mapping') and self.config.symbol_mapping:
            self._default_symbol_mapping.update(self.config.symbol_mapping)

    def map_symbol(self, symbol: str) -> str:
        """
        映射交易对符号到Hyperliquid格式

        @deprecated: 建议使用统一的符号转换服务
        """
        if not hasattr(self, '_deprecation_logged_map'):
            if self.logger:
                self.logger.warning("⚠️ map_symbol方法已弃用，建议使用统一的符号转换服务")
            self._deprecation_logged_map = True

        return self._default_symbol_mapping.get(symbol, symbol)

    def reverse_map_symbol(self, exchange_symbol: str) -> str:
        """
        反向映射交易对符号从Hyperliquid格式

        @deprecated: 建议使用统一的符号转换服务
        """
        if not hasattr(self, '_deprecation_logged_reverse'):
            if self.logger:
                self.logger.warning("⚠️ reverse_map_symbol方法已弃用，建议使用统一的符号转换服务")
            self._deprecation_logged_reverse = True

        # 简化的反向映射
        reverse_mapping = {v: k for k,
                           v in self._default_symbol_mapping.items()}
        return reverse_mapping.get(exchange_symbol, exchange_symbol)

    def get_supported_symbols_by_market(self) -> Dict[str, List[str]]:
        """获取按市场类型分组的支持符号"""
        symbols_by_market = {}

        if self.perpetual_enabled:
            symbols_by_market['perpetual'] = self.perpetual_mapping.get(
                'symbols', [])

        if self.spot_enabled:
            symbols_by_market['spot'] = self.spot_mapping.get('symbols', [])

        return symbols_by_market

    def is_market_enabled(self, market_type: str) -> bool:
        """检查市场类型是否启用"""
        if market_type == 'perpetual':
            return self.perpetual_enabled
        elif market_type == 'spot':
            return self.spot_enabled
        return False

    def get_market_priority(self) -> str:
        """获取市场优先级"""
        return self.market_priority

    def get_default_market(self) -> str:
        """获取默认市场"""
        return self.default_market

    def get_enabled_markets(self) -> List[str]:
        """获取启用的市场类型列表"""
        enabled_markets = []

        if self.perpetual_enabled:
            enabled_markets.append('perpetual')

        if self.spot_enabled:
            enabled_markets.append('spot')

        return enabled_markets

    def filter_symbols_by_market_type(self, symbols: List[str]) -> List[str]:
        """根据启用的市场类型过滤符号"""
        if not symbols:
            return []

        filtered_symbols = []

        # 获取启用的市场类型
        enabled_markets = self.get_enabled_markets()

        if not enabled_markets:
            # 如果没有启用任何市场，返回空列表
            if self.logger:
                self.logger.warning("⚠️ 没有启用任何市场类型，返回空符号列表")
            return []

        for symbol in symbols:
            should_include = False

            # 检查是否为永续合约
            if self.perpetual_enabled and ('perpetual' in enabled_markets):
                if ':PERP' in symbol or ':USDC' in symbol or symbol.endswith('PERP'):
                    should_include = True

            # 检查是否为现货交易对
            if self.spot_enabled and ('spot' in enabled_markets):
                if ':SPOT' in symbol or (not ':PERP' in symbol and not ':USDC' in symbol):
                    should_include = True

            if should_include:
                filtered_symbols.append(symbol)

        if self.logger:
            self.logger.debug(
                f"🔍 符号过滤: {len(symbols)} -> {len(filtered_symbols)} (启用市场: {enabled_markets})")

        return filtered_symbols

    def filter_websocket_symbols(self, symbols: List[str]) -> List[str]:
        """过滤WebSocket黑名单符号"""
        if not symbols:
            return []

        # 首先按市场类型过滤
        filtered_symbols = self.filter_symbols_by_market_type(symbols)

        # 然后过滤黑名单符号
        final_symbols = [
            s for s in filtered_symbols if not self.is_websocket_symbol_blacklisted(s)]

        if self.logger and len(final_symbols) != len(symbols):
            filtered_count = len(symbols) - len(final_symbols)
            self.logger.debug(f"🚫 过滤了 {filtered_count} 个符号 (市场类型 + 黑名单)")

        return final_symbols

    def get_market_type_from_symbol(self, symbol: str) -> Optional[str]:
        """根据符号格式判断市场类型"""
        if not symbol:
            return None

        # 检查是否为永续合约
        if ':PERP' in symbol or ':USDC' in symbol or symbol.endswith('PERP'):
            return 'perpetual'

        # 检查是否为现货交易对
        if ':SPOT' in symbol or ('/' in symbol and ':' not in symbol):
            return 'spot'

        # 默认返回永续合约（Hyperliquid主要是永续合约）
        return 'perpetual'

    def is_perpetual_symbol(self, symbol: str) -> bool:
        """判断符号是否为永续合约"""
        return self.get_market_type_from_symbol(symbol) == 'perpetual'

    def is_spot_symbol(self, symbol: str) -> bool:
        """判断符号是否为现货交易对"""
        return self.get_market_type_from_symbol(symbol) == 'spot'

    async def _use_default_symbols(self):
        """使用默认交易对列表"""
        symbols_by_market = self.get_supported_symbols_by_market()

        default_symbols = []
        for market_type, symbols in symbols_by_market.items():
            if self.is_market_enabled(market_type):
                default_symbols.extend(symbols)

        # 如果配置中没有符号，使用硬编码的默认值
        if not default_symbols:
            if self.perpetual_enabled:
                default_symbols.extend([
                    "BTC/USDC:PERP", "ETH/USDC:PERP", "SOL/USDC:PERP",
                    "AVAX/USDC:PERP", "DOGE/USDC:PERP", "ADA/USDC:PERP"
                ])

            if self.spot_enabled:
                default_symbols.extend([
                    "BTC/USDC:SPOT", "ETH/USDC:SPOT", "SOL/USDC:SPOT",
                    "AVAX/USDC:SPOT", "DOGE/USDC:SPOT", "ADA/USDC:SPOT"
                ])

        self._supported_symbols = default_symbols

        if self.logger:
            perp_count = len([s for s in default_symbols if ':PERP' in s])
            spot_count = len([s for s in default_symbols if ':SPOT' in s])
            self.logger.info(f"使用默认交易对列表: {perp_count}个永续合约 + {spot_count}个现货")

    # === 数据转换工具方法 ===

    def _safe_decimal(self, value: Any) -> Optional[Decimal]:
        """安全转换为Decimal"""
        if value is None or value == "":
            return None
        try:
            if isinstance(value, str):
                # 处理科学计数法
                if 'e' in value.lower():
                    return Decimal(value)
                # 移除可能的千分位分隔符
                value = value.replace(',', '')
            return Decimal(str(value))
        except (ValueError, TypeError, InvalidOperation):
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """安全转换为float"""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        """安全转换为int"""
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _safe_str(self, value: Any) -> str:
        """安全转换为str"""
        if value is None:
            return ""
        return str(value)

    def _parse_timestamp(self, timestamp_value: Any) -> Optional[datetime]:
        """
        安全解析时间戳

        支持多种格式:
        - 毫秒时间戳 (int/float): 1698765432000
        - 秒时间戳 (int/float): 1698765432
        - ISO 8601 字符串: "2025-10-26T12:03:07.880Z"
        - datetime 对象: 直接返回
        - None: 返回当前时间
        """
        if timestamp_value is None:
            return datetime.now()

        # 如果已经是datetime对象，直接返回
        if isinstance(timestamp_value, datetime):
            return timestamp_value

        try:
            # 尝试解析为数字时间戳
            if isinstance(timestamp_value, (int, float)):
                # 判断是毫秒还是秒（假设时间戳大于10位数字为毫秒）
                if timestamp_value > 10000000000:
                    # 毫秒时间戳
                    return datetime.fromtimestamp(timestamp_value / 1000)
                else:
                    # 秒时间戳
                    return datetime.fromtimestamp(timestamp_value)

            # 尝试解析为字符串时间戳
            if isinstance(timestamp_value, str):
                # 移除时区标记（Z或+00:00等）
                timestamp_str = timestamp_value.replace(
                    'Z', '').split('+')[0].split('-')[0]
                # 尝试多种日期格式
                for fmt in ['%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']:
                    try:
                        return datetime.strptime(timestamp_str, fmt)
                    except ValueError:
                        continue

                # 如果是纯数字字符串，尝试转换为数字
                try:
                    num_timestamp = float(timestamp_str)
                    return self._parse_timestamp(num_timestamp)
                except ValueError:
                    pass

            # 如果所有方法都失败，返回当前时间
            if self.logger:
                self.logger.debug(
                    f"无法解析时间戳: {timestamp_value} (类型: {type(timestamp_value)})")
            return datetime.now()

        except Exception as e:
            if self.logger:
                self.logger.debug(f"解析时间戳失败: {timestamp_value}, 错误: {e}")
            return datetime.now()

    def get_supported_symbols(self) -> List[str]:
        """获取支持的交易对列表"""
        return self._supported_symbols.copy()

    def get_market_info(self) -> Dict[str, Any]:
        """获取市场信息"""
        return self._market_info.copy()

    def is_symbol_supported(self, symbol: str) -> bool:
        """检查交易对是否支持"""
        return symbol in self._supported_symbols

    def get_timeframe_mapping(self, timeframe: str) -> str:
        """获取时间周期映射"""
        return self.SUPPORTED_TIMEFRAMES.get(timeframe, timeframe)

    def is_timeframe_supported(self, timeframe: str) -> bool:
        """检查时间周期是否支持"""
        return timeframe in self.SUPPORTED_TIMEFRAMES

    def get_websocket_url(self) -> str:
        """获取WebSocket URL"""
        return self.ws_url

    def get_rest_url(self) -> str:
        """获取REST API URL"""
        return self.base_url

    def is_websocket_symbol_blacklisted(self, symbol: str) -> bool:
        """检查符号是否在WebSocket黑名单中"""
        return symbol in self.WEBSOCKET_BLACKLIST

    def get_config(self) -> Any:
        """获取配置"""
        return self.config

    def set_logger(self, logger):
        """设置日志器"""
        self.logger = logger

    def get_logger(self):
        """获取日志器"""
        return self.logger

    # === 回调管理 ===

    async def extended_data_callback(self, data_type: str, data: Any):
        """
        扩展数据回调 - 用于WebSocket数据推送

        Args:
            data_type: 数据类型 ('ticker', 'orderbook', 'trade', 'order', 'balance')
            data: 数据内容
        """
        try:
            # 获取注册的回调函数列表
            callbacks = getattr(self, f'_{data_type}_callbacks', [])

            # 触发所有注册的回调
            for callback_item in callbacks:
                try:
                    # 🔥 修复：正确解包回调元组
                    # 回调可能是三元组 (symbol, original_callback, wrapped_callback)
                    # 或者单个回调函数
                    if isinstance(callback_item, tuple) and len(callback_item) == 3:
                        symbol, original_callback, wrapped_callback = callback_item
                        # 调用 wrapped_callback，传递 symbol 和 data
                        if asyncio.iscoroutinefunction(wrapped_callback):
                            await wrapped_callback(symbol, data)
                        else:
                            wrapped_callback(symbol, data)
                    else:
                        # 直接调用回调函数
                        callback = callback_item
                        if asyncio.iscoroutinefunction(callback):
                            await callback(data)
                        else:
                            callback(data)
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"执行{data_type}回调失败: {e}")

        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"extended_data_callback失败 ({data_type}): {e}")

    def register_callback(self, data_type: str, callback: Any):
        """
        注册回调函数

        Args:
            data_type: 数据类型 ('ticker', 'orderbook', 'trade', 'order', 'balance')
            callback: 回调函数
        """
        callback_attr = f'_{data_type}_callbacks'
        if not hasattr(self, callback_attr):
            setattr(self, callback_attr, [])

        callbacks = getattr(self, callback_attr)
        if callback not in callbacks:
            callbacks.append(callback)

            if self.logger:
                self.logger.debug(
                    f"注册{data_type}回调: {callback.__name__ if hasattr(callback, '__name__') else callback}")
