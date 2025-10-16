"""
配置管理器
负责加载和管理分离的配置文件
"""

import yaml
import os
from typing import Dict, Any, Optional, List
from pathlib import Path
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class ExchangeConfig:
    """交易所配置"""
    name: str
    enabled: bool
    subscription_mode: str
    data_types: List[str]
    symbols: List[str]
    predefined_combinations: Dict[str, Any]
    discovery_settings: Dict[str, Any]
    rate_limit: Dict[str, Any]
    websocket_enabled: bool
    rest_fallback: bool
    max_symbols: int
    priority_symbols: Optional[List[str]] = None
    exchange_info: Optional[Dict[str, Any]] = None

@dataclass
class MonitoringConfig:
    """监控系统配置"""
    enabled: bool
    global_max_symbols: int
    update_interval: int
    health_check_interval: int
    reconnect_delay: int
    max_reconnect_attempts: int
    data_retention_hours: int
    enable_statistics: bool
    enable_symbol_limits: bool
    defaults: Dict[str, Any]
    monitoring: Dict[str, Any]
    performance: Dict[str, Any]
    logging: Dict[str, Any]

class ConfigManager:
    """配置管理器 - 统一管理分离的配置文件"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.monitoring_config: Optional[MonitoringConfig] = None
        self.exchange_configs: Dict[str, ExchangeConfig] = {}
        
    def load_monitoring_config(self) -> MonitoringConfig:
        """加载全局监控配置"""
        config_path = self.config_dir / "monitoring" / "monitoring.yaml"
        
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                config_data = yaml.safe_load(file)
                
            self.monitoring_config = MonitoringConfig(
                enabled=config_data.get('enabled', True),
                global_max_symbols=config_data.get('global_max_symbols', 1000),
                update_interval=config_data.get('update_interval', 1000),
                health_check_interval=config_data.get('health_check_interval', 30),
                reconnect_delay=config_data.get('reconnect_delay', 5),
                max_reconnect_attempts=config_data.get('max_reconnect_attempts', 10),
                data_retention_hours=config_data.get('data_retention_hours', 24),
                enable_statistics=config_data.get('enable_statistics', True),
                enable_symbol_limits=config_data.get('enable_symbol_limits', False),
                defaults=config_data.get('defaults', {}),
                monitoring=config_data.get('monitoring', {}),
                performance=config_data.get('performance', {}),
                logging=config_data.get('logging', {})
            )
            
            logger.info(f"成功加载监控配置: {config_path}")
            return self.monitoring_config
            
        except Exception as e:
            logger.error(f"加载监控配置失败: {e}")
            # 返回默认配置
            return self._get_default_monitoring_config()
    
    def load_exchange_config(self, exchange_name: str) -> Optional[ExchangeConfig]:
        """加载指定交易所的配置"""
        if not self.monitoring_config:
            self.load_monitoring_config()
            
        config_pattern = self.monitoring_config.monitoring.get(
            'config_file_pattern', '{exchange}_config.yaml'
        )
        config_filename = config_pattern.format(exchange=exchange_name)
        config_path = self.config_dir / "exchanges" / config_filename
        
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                config_data = yaml.safe_load(file)
            
            # 🔥 适配现有的复杂配置格式
            exchange_data = config_data.get(exchange_name, {})
            
            # 🔥 修复：检查顶级enabled字段
            enabled = exchange_data.get('enabled', True)
            logger.info(f"📊 {exchange_name} 配置加载: enabled={enabled}")
            
            # 🔥 获取订阅模式配置
            subscription_mode_config = exchange_data.get('subscription_mode', {})
            if isinstance(subscription_mode_config, dict):
                subscription_mode = subscription_mode_config.get('mode', 'predefined')
            else:
                subscription_mode = 'predefined'
            
            # 🔥 获取数据类型配置 - 需要从动态模式中获取
            data_types = []
            dynamic_config = subscription_mode_config.get('dynamic', {})
            if dynamic_config:
                data_types_config = dynamic_config.get('data_types', {})
                if isinstance(data_types_config, dict):
                    # 从布尔值配置转换为字符串列表
                    for data_type, is_enabled in data_types_config.items():
                        if is_enabled:
                            data_types.append(data_type)
                else:
                    data_types = ['orderbook']  # 默认只订阅orderbook
            
            # 如果动态模式没有配置，尝试从预定义模式获取
            if not data_types:
                predefined_config = subscription_mode_config.get('predefined', {})
                if predefined_config:
                    data_types_config = predefined_config.get('data_types', {})
                    if isinstance(data_types_config, dict):
                        for data_type, is_enabled in data_types_config.items():
                            if is_enabled:
                                data_types.append(data_type)
                    else:
                        data_types = ['orderbook']  # 默认只订阅orderbook
            
            # 如果仍然没有数据类型，使用默认值
            if not data_types:
                data_types = ['orderbook']
            
            # 🔥 获取符号列表 - 从预定义或动态配置获取
            symbols = []
            if subscription_mode == 'predefined':
                predefined_config = subscription_mode_config.get('predefined', {})
                symbols = predefined_config.get('symbols', [])
            else:
                # 动态模式时符号为空，将通过发现获取
                symbols = []
            
            # 🔥 获取发现设置
            discovery_settings = {}
            if subscription_mode == 'dynamic':
                dynamic_config = subscription_mode_config.get('dynamic', {})
                discovery_config = dynamic_config.get('discovery', {})
                if discovery_config:
                    discovery_settings = {
                        'enabled': discovery_config.get('enabled', True),
                        'filters': discovery_config.get('filter_criteria', {}),
                        'auto_discover_interval': dynamic_config.get('dynamic_subscription', {}).get('auto_discover_interval', 300),
                        'max_discovery_attempts': 3
                    }
            
            # 🔥 获取预定义组合
            predefined_combinations = {}
            custom_subscriptions = exchange_data.get('custom_subscriptions', {})
            if custom_subscriptions:
                combinations = custom_subscriptions.get('combinations', {})
                for combo_name, combo_config in combinations.items():
                    # 转换数据类型格式
                    combo_data_types = []
                    data_types_dict = combo_config.get('data_types', {})
                    if isinstance(data_types_dict, dict):
                        for dt, is_enabled in data_types_dict.items():  # 🔥 修复：改为is_enabled避免覆盖外部enabled变量
                            if is_enabled:
                                combo_data_types.append(dt)
                    
                    predefined_combinations[combo_name] = {
                        'description': combo_config.get('description', ''),
                        'symbols': combo_config.get('symbols', []),
                        'data_types': combo_data_types
                    }
            
            # 🔥 获取其他配置
            api_config = exchange_data.get('api', {})
            rate_limit_config = exchange_data.get('rate_limits', {})
            
            # 创建ExchangeConfig对象
            exchange_config = ExchangeConfig(
                name=exchange_name,
                enabled=enabled,
                subscription_mode=subscription_mode,
                data_types=data_types,
                symbols=symbols,
                predefined_combinations=predefined_combinations,
                discovery_settings=discovery_settings,
                rate_limit={
                    'requests_per_minute': rate_limit_config.get('rest_api', 100),
                    'burst_limit': 20
                },
                websocket_enabled=exchange_data.get('websocket', {}).get('enabled', True),
                rest_fallback=True,
                max_symbols=discovery_settings.get('filters', {}).get('max_symbols', 50),
                priority_symbols=None,
                exchange_info={
                    'name': exchange_data.get('name', exchange_name),
                    'type': 'perpetual',  # 🔥 只支持永续合约
                    'base_url': api_config.get('base_url', ''),
                    'ws_url': api_config.get('ws_url', ''),
                    'testnet': False
                }
            )
            
            self.exchange_configs[exchange_name] = exchange_config
            logger.info(f"成功加载交易所配置: {exchange_name}")
            logger.info(f"  - 订阅模式: {subscription_mode}")
            logger.info(f"  - 数据类型: {data_types}")
            logger.info(f"  - 符号数量: {len(symbols)}")
            logger.info(f"  - 组合数量: {len(predefined_combinations)}")
            
            return exchange_config
            
        except Exception as e:
            logger.error(f"加载交易所配置失败 {exchange_name}: {e}")
            
            # 尝试使用默认配置
            if self.monitoring_config.monitoring.get('fallback_to_defaults', True):
                return self._get_default_exchange_config(exchange_name)
            return None
    
    def load_all_exchange_configs(self) -> Dict[str, ExchangeConfig]:
        """加载所有启用的交易所配置"""
        if not self.monitoring_config:
            self.load_monitoring_config()
            
        # 🔥 修复：确保正确读取enabled_exchanges
        enabled_exchanges = self.monitoring_config.monitoring.get('enabled_exchanges', [])
        logger.info(f"📊 从监控配置中获取启用的交易所: {enabled_exchanges}")
        
        # 🔥 修复：如果没有找到启用的交易所，记录详细信息
        if not enabled_exchanges:
            logger.warning("⚠️ 监控配置中没有找到启用的交易所列表")
            logger.info(f"📊 监控配置内容: {self.monitoring_config.monitoring}")
            
            # 尝试从默认列表获取
            default_exchanges = ["hyperliquid", "backpack", "edgex"]
            logger.info(f"📊 使用默认的交易所列表: {default_exchanges}")
            enabled_exchanges = default_exchanges
        
        # 🔥 修复：确保清空现有配置再重新加载
        self.exchange_configs.clear()
        
        for exchange_name in enabled_exchanges:
            logger.info(f"📊 加载交易所配置: {exchange_name}")
            try:
                config = self.load_exchange_config(exchange_name)
                if config:
                    logger.info(f"✅ {exchange_name} 配置加载成功: enabled={config.enabled}")
                    # 🔥 修复：即使配置中enabled为False，也要记录配置
                    if config.enabled:
                        self.exchange_configs[exchange_name] = config
                        logger.info(f"✅ {exchange_name} 已添加到启用列表")
                    else:
                        logger.warning(f"⚠️ {exchange_name} 配置中enabled=False，跳过")
                else:
                    logger.error(f"❌ {exchange_name} 配置加载失败")
            except Exception as e:
                logger.error(f"❌ 加载 {exchange_name} 配置时出错: {e}")
                import traceback
                logger.error(traceback.format_exc())
            
        logger.info(f"📊 总共加载了 {len(self.exchange_configs)} 个启用的交易所配置")
        logger.info(f"📊 启用的交易所列表: {list(self.exchange_configs.keys())}")
        
        return self.exchange_configs
    
    def get_exchange_config(self, exchange_name: str) -> Optional[ExchangeConfig]:
        """获取交易所配置"""
        if exchange_name not in self.exchange_configs:
            return self.load_exchange_config(exchange_name)
        return self.exchange_configs[exchange_name]
    
    def get_monitoring_config(self) -> MonitoringConfig:
        """获取监控配置"""
        if not self.monitoring_config:
            return self.load_monitoring_config()
        return self.monitoring_config
    
    def _merge_with_defaults(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """合并默认配置"""
        defaults = self.monitoring_config.defaults if self.monitoring_config else {}
        merged = defaults.copy()
        merged.update(config_data)
        return merged
    
    def _get_default_monitoring_config(self) -> MonitoringConfig:
        """获取默认监控配置"""
        return MonitoringConfig(
            enabled=True,
            global_max_symbols=1000,
            update_interval=1000,
            health_check_interval=30,
            reconnect_delay=5,
            max_reconnect_attempts=10,
            data_retention_hours=24,
            enable_statistics=True,
            enable_symbol_limits=False,
            defaults={
                'data_types': ['ticker'],
                'rate_limit': {'requests_per_minute': 100},
                'websocket_enabled': True,
                'rest_fallback': True,
                'max_symbols': 1000
            },
            monitoring={
                'exchange_configs_dir': 'config/exchanges',
                'enabled_exchanges': [],
                'config_loading': {
                    'use_exchange_configs': True,
                    'config_file_pattern': '{exchange}_config.yaml',
                    'fallback_to_defaults': True
                }
            },
            performance={'enabled': True, 'metrics_interval': 60},
            logging={'level': 'INFO', 'log_subscriptions': True}
        )
    
    def _get_default_exchange_config(self, exchange_name: str) -> ExchangeConfig:
        """获取默认交易所配置"""
        defaults = self.monitoring_config.defaults if self.monitoring_config else {}
        
        return ExchangeConfig(
            name=exchange_name,
            enabled=True,
            subscription_mode='predefined',
            data_types=defaults.get('data_types', ['ticker']),
            symbols=[],
            predefined_combinations={},
            discovery_settings={},
            rate_limit=defaults.get('rate_limit', {'requests_per_minute': 100}),
            websocket_enabled=defaults.get('websocket_enabled', True),
            rest_fallback=defaults.get('rest_fallback', True),
            max_symbols=defaults.get('max_symbols', 1000),
            priority_symbols=None
        )
    
    def is_exchange_enabled(self, exchange_name: str) -> bool:
        """检查交易所是否启用"""
        config = self.get_exchange_config(exchange_name)
        return config.enabled if config else False
    
    def get_exchange_data_types(self, exchange_name: str) -> List[str]:
        """获取交易所的数据类型配置"""
        config = self.get_exchange_config(exchange_name)
        return config.data_types if config else []
    
    def get_exchange_symbols(self, exchange_name: str) -> List[str]:
        """获取交易所的交易对配置"""
        config = self.get_exchange_config(exchange_name)
        return config.symbols if config else []

# 全局配置管理器实例
config_manager = ConfigManager() 