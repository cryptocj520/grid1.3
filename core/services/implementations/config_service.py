"""
配置服务实现

实现配置管理功能，整合监控配置、交易所配置等
🔥 新增：内部使用ConfigManager统一配置源
"""

import os
import yaml
from typing import Dict, Any, Optional, List
from injector import inject, singleton

from ..interfaces.config_service import (
    IConfigurationService, MonitoringConfiguration, 
    ExchangeConfig, SymbolConfig, SubscriptionConfig
)
from ...logging import get_system_logger
from ...domain.models import DataType, MonitoringDataTypeConfig, ExchangeDataTypeConfig, DataTypeConfig
# 🔥 新增：导入新的配置管理器
from ...infrastructure.config_manager import ConfigManager


@singleton
class ConfigurationServiceImpl(IConfigurationService):
    """配置服务实现 - 🔥 统一使用ConfigManager作为配置源"""
    
    def __init__(self):
        self.logger = get_system_logger()
        self.config_path = "config/monitoring/monitoring.yaml"
        self.config: Optional[MonitoringConfiguration] = None
        self.initialized = False
        
        # 🔥 新增：统一配置管理器
        self.config_manager = ConfigManager()
    
    async def initialize(self) -> bool:
        """初始化配置服务"""
        try:
            self.logger.info("🔧 初始化配置服务...")
            
            # 🔥 修改：使用新配置管理器加载配置
            self.config = await self.load_config()
            self.initialized = True
            
            self.logger.info("✅ 配置服务初始化成功")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 配置服务初始化失败: {e}")
            return False
    
    async def load_config(self, config_path: str = None) -> MonitoringConfiguration:
        """🔥 修改：统一使用ConfigManager加载配置"""
        try:
            # 加载全局监控配置
            monitoring_config = self.config_manager.load_monitoring_config()
            
            # 加载所有交易所配置
            exchange_configs = self.config_manager.load_all_exchange_configs()
            
            # 转换为旧配置格式（保持兼容性）
            exchanges = {}
            subscriptions = {}
            
            for exchange_id, exchange_config in exchange_configs.items():
                if exchange_config.enabled:
                    # 转换为旧的ExchangeConfig格式
                    exchanges[exchange_id] = ExchangeConfig(
                        exchange_id=exchange_id,
                        name=exchange_config.name,
                        enabled=exchange_config.enabled,
                        base_url=exchange_config.exchange_info.get('base_url', '') if exchange_config.exchange_info else '',
                        ws_url=exchange_config.exchange_info.get('ws_url', '') if exchange_config.exchange_info else '',
                        testnet=exchange_config.exchange_info.get('testnet', False) if exchange_config.exchange_info else False,
                        max_symbols=exchange_config.max_symbols
                    )
                    
                    # 转换为旧的SubscriptionConfig格式
                    subscriptions[exchange_id] = SubscriptionConfig(
                        exchange_id=exchange_id,
                        data_types=exchange_config.data_types,
                        symbols=exchange_config.symbols,
                        batch_size=10  # 默认值
                    )
            
            # 创建统一的配置对象
            config = MonitoringConfiguration(
                exchanges=exchanges,
                subscriptions=subscriptions,
                symbols={},  # 暂时为空
                global_settings=monitoring_config.defaults
            )
            
            self.logger.info("📄 配置加载成功（使用统一配置管理器）")
            return config
            
        except Exception as e:
            self.logger.error(f"❌ 配置加载失败: {e}")
            return self._create_default_config()
    
    async def save_config(self, config: MonitoringConfiguration, config_path: str = None) -> bool:
        """保存配置"""
        try:
            path = config_path or self.config_path
            
            # 确保目录存在
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            # 转换为字典
            config_data = self._serialize_config(config)
            
            with open(path, 'w', encoding='utf-8') as f:
                yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True)
            
            self.logger.info(f"💾 配置保存成功: {path}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 配置保存失败: {e}")
            return False
    
    async def get_exchange_config(self, exchange_id: str) -> Optional[ExchangeConfig]:
        """🔥 修改：从新配置管理器获取交易所配置"""
        try:
            # 从新配置管理器获取交易所配置
            exchange_config = self.config_manager.get_exchange_config(exchange_id)
            
            if not exchange_config:
                return None
            
            # 转换为旧的ExchangeConfig格式
            return ExchangeConfig(
                exchange_id=exchange_id,
                name=exchange_config.name,
                enabled=exchange_config.enabled,
                base_url=exchange_config.exchange_info.get('base_url', '') if exchange_config.exchange_info else '',
                ws_url=exchange_config.exchange_info.get('ws_url', '') if exchange_config.exchange_info else '',
                testnet=exchange_config.exchange_info.get('testnet', False) if exchange_config.exchange_info else False,
                max_symbols=exchange_config.max_symbols
            )
            
        except Exception as e:
            self.logger.error(f"❌ 获取交易所配置失败 {exchange_id}: {e}")
            # 降级到旧逻辑
            if not self.config:
                return None
            
            return self.config.exchanges.get(exchange_id)
    
    async def get_subscription_config(self, exchange_id: str) -> Optional[SubscriptionConfig]:
        """获取订阅配置"""
        if not self.config:
            return None
        
        return self.config.subscriptions.get(exchange_id)
    
    async def get_symbol_config(self, symbol: str, exchange_id: str) -> Optional[SymbolConfig]:
        """获取交易对配置"""
        if not self.config:
            return None
        
        key = f"{exchange_id}_{symbol}"
        return self.config.symbols.get(key)
    
    async def get_enabled_exchanges(self) -> List[str]:
        """🔥 修改：从新配置管理器获取启用的交易所"""
        try:
            self.logger.info("📊 开始获取启用的交易所...")
            
            # 从新配置管理器获取所有交易所配置
            exchange_configs = self.config_manager.load_all_exchange_configs()
            self.logger.info(f"📊 配置管理器返回 {len(exchange_configs)} 个交易所配置")
            
            # 过滤出启用的交易所
            enabled_exchanges = [
                exchange_id for exchange_id, config in exchange_configs.items()
                if config.enabled
            ]
            
            self.logger.info(f"📊 获取启用的交易所: {enabled_exchanges}")
            
            # 🔥 修复：如果没有找到启用的交易所，尝试降级处理
            if not enabled_exchanges:
                self.logger.warning("⚠️ 没有找到启用的交易所，尝试降级处理...")
                # 直接从监控配置获取启用的交易所列表
                monitoring_config = self.config_manager.get_monitoring_config()
                fallback_exchanges = monitoring_config.monitoring.get('enabled_exchanges', [])
                self.logger.info(f"📊 降级获取启用的交易所: {fallback_exchanges}")
                
                # 为每个交易所创建基本配置
                for exchange_id in fallback_exchanges:
                    try:
                        config = self.config_manager.load_exchange_config(exchange_id)
                        if config and config.enabled:
                            enabled_exchanges.append(exchange_id)
                    except Exception as ex:
                        self.logger.warning(f"⚠️ 加载 {exchange_id} 配置失败: {ex}")
                        # 即使配置加载失败，也认为这个交易所是启用的
                        enabled_exchanges.append(exchange_id)
                
                self.logger.info(f"📊 降级处理后的启用交易所: {enabled_exchanges}")
            
            return enabled_exchanges
            
        except Exception as e:
            self.logger.error(f"❌ 获取启用交易所失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            # 降级到旧逻辑
            if not self.config:
                # 如果完全失败，返回硬编码的交易所列表
                fallback_list = ["hyperliquid", "backpack", "edgex"]
                self.logger.warning(f"⚠️ 使用硬编码的交易所列表: {fallback_list}")
                return fallback_list
            
            return [
                exchange_id for exchange_id, config in self.config.exchanges.items()
                if config.enabled
            ]
    
    async def get_symbols_for_exchange(self, exchange_id: str) -> List[str]:
        """获取交易所的交易对"""
        if not self.config:
            return []
        
        subscription_config = self.config.subscriptions.get(exchange_id)
        if subscription_config:
            return subscription_config.symbols
        
        return []
    
    async def update_exchange_config(self, exchange_id: str, config: ExchangeConfig) -> bool:
        """更新交易所配置"""
        if not self.config:
            return False
        
        self.config.exchanges[exchange_id] = config
        await self.save_config(self.config)
        
        self.logger.info(f"🔄 更新交易所配置: {exchange_id}")
        return True
    
    async def update_subscription_config(self, exchange_id: str, config: SubscriptionConfig) -> bool:
        """更新订阅配置"""
        if not self.config:
            return False
        
        self.config.subscriptions[exchange_id] = config
        await self.save_config(self.config)
        
        self.logger.info(f"🔄 更新订阅配置: {exchange_id}")
        return True
    
    async def reload_config(self) -> bool:
        """重新加载配置"""
        try:
            self.config = await self.load_config()
            self.logger.info("🔄 配置重新加载成功")
            return True
        except Exception as e:
            self.logger.error(f"❌ 配置重新加载失败: {e}")
            return False
    
    def get_config_snapshot(self) -> Dict[str, Any]:
        """获取配置快照"""
        if not self.config:
            return {}
        
        return {
            "exchanges": {
                exchange_id: {
                    "name": config.name,
                    "enabled": config.enabled,
                    "testnet": config.testnet,
                    "max_symbols": config.max_symbols
                }
                for exchange_id, config in self.config.exchanges.items()
            },
            "subscriptions": {
                exchange_id: {
                    "data_types": config.data_types,
                    "symbols_count": len(config.symbols),
                    "batch_size": config.batch_size
                }
                for exchange_id, config in self.config.subscriptions.items()
            },
            "global_settings": self.config.global_settings
        }
    
    async def get_monitoring_data_type_config(self) -> MonitoringDataTypeConfig:
        """🔥 修改：从新配置管理器获取监控数据类型配置"""
        try:
            # 从新配置管理器获取所有交易所配置
            exchange_configs = self.config_manager.load_all_exchange_configs()
            
            monitoring_config = MonitoringDataTypeConfig()
            
            # 为每个启用的交易所创建数据类型配置
            for exchange_id, exchange_config in exchange_configs.items():
                if exchange_config.enabled:
                    # 解析数据类型列表
                    enabled_types = set()
                    for data_type_str in exchange_config.data_types:
                        try:
                            data_type = DataType.from_string(data_type_str)
                            enabled_types.add(data_type)
                        except ValueError:
                            self.logger.warning(f"⚠️  不支持的数据类型: {data_type_str} 在交易所 {exchange_id}")
                    
                    # 创建数据类型配置
                    data_type_config = DataTypeConfig(enabled_types=enabled_types)
                    
                    # 创建交易所数据类型配置
                    exchange_data_config = ExchangeDataTypeConfig(
                        exchange_id=exchange_id,
                        data_types=data_type_config,
                        priority_symbols=exchange_config.symbols[:3]  # 取前3个作为优先级符号
                    )
                    
                    # 设置每种数据类型的最大符号数
                    max_symbols = exchange_config.max_symbols
                    for data_type in enabled_types:
                        exchange_data_config.set_max_symbols(data_type, max_symbols)
                    
                    monitoring_config.set_exchange_config(exchange_id, exchange_data_config)
            
            return monitoring_config
            
        except Exception as e:
            self.logger.error(f"❌ 获取监控数据类型配置失败: {e}")
            # 降级到旧逻辑
            if not self.config:
                return MonitoringDataTypeConfig()
            
            monitoring_config = MonitoringDataTypeConfig()
            
            # 设置每个交易所的数据类型配置
            for exchange_id, subscription_config in self.config.subscriptions.items():
                if exchange_id in self.config.exchanges and self.config.exchanges[exchange_id].enabled:
                    # 解析数据类型列表
                    enabled_types = set()
                    for data_type_str in subscription_config.data_types:
                        try:
                            data_type = DataType.from_string(data_type_str)
                            enabled_types.add(data_type)
                        except ValueError:
                            self.logger.warning(f"⚠️  不支持的数据类型: {data_type_str} 在交易所 {exchange_id}")
                    
                    # 创建数据类型配置
                    data_type_config = DataTypeConfig(enabled_types=enabled_types)
                    
                    # 创建交易所数据类型配置
                    exchange_config = ExchangeDataTypeConfig(
                        exchange_id=exchange_id,
                        data_types=data_type_config,
                        priority_symbols=subscription_config.symbols[:3]  # 取前3个作为优先级符号
                    )
                    
                    # 设置每种数据类型的最大符号数
                    max_symbols = self.config.exchanges[exchange_id].max_symbols
                    for data_type in enabled_types:
                        exchange_config.set_max_symbols(data_type, max_symbols)
                    
                    monitoring_config.set_exchange_config(exchange_id, exchange_config)
            
            return monitoring_config
    
    async def get_enabled_data_types_for_exchange(self, exchange_id: str) -> List[DataType]:
        """获取指定交易所的启用数据类型"""
        monitoring_config = await self.get_monitoring_data_type_config()
        return monitoring_config.get_enabled_types_for_exchange(exchange_id)
    
    async def is_data_type_enabled(self, exchange_id: str, data_type: DataType) -> bool:
        """检查指定交易所的数据类型是否启用"""
        enabled_types = await self.get_enabled_data_types_for_exchange(exchange_id)
        return data_type in enabled_types
    
    def _create_default_config(self) -> MonitoringConfiguration:
        """创建默认配置"""
        exchanges = {
            "hyperliquid": ExchangeConfig(
                exchange_id="hyperliquid",
                name="Hyperliquid",
                enabled=True,
                base_url="https://api.hyperliquid.xyz",
                ws_url="wss://api.hyperliquid.xyz/ws",
                testnet=False,
                max_symbols=20
            ),
            "backpack": ExchangeConfig(
                exchange_id="backpack",
                name="Backpack Exchange",
                enabled=True,
                base_url="https://api.backpack.exchange",
                ws_url="wss://ws.backpack.exchange",
                testnet=True,
                max_symbols=15
            ),
            "binance": ExchangeConfig(
                exchange_id="binance",
                name="Binance",
                enabled=True,
                base_url="https://api.binance.com",
                ws_url="wss://stream.binance.com:9443",
                testnet=False,
                max_symbols=25
            )
        }
        
        subscriptions = {
            "hyperliquid": SubscriptionConfig(
                exchange_id="hyperliquid",
                data_types=["ticker"],
                symbols=["BTC", "ETH", "SOL"],
                batch_size=10
            ),
            "backpack": SubscriptionConfig(
                exchange_id="backpack",
                data_types=["ticker", "orderbook"],
                symbols=["SOL_USDC", "BTC_USDC"],
                batch_size=5
            ),
            "binance": SubscriptionConfig(
                exchange_id="binance",
                data_types=["ticker"],
                symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
                batch_size=15
            )
        }
        
        symbols = {}
        global_settings = {
            "max_connections": 10,
            "heartbeat_interval": 30,
            "reconnect_delay": 5,
            "data_retention_hours": 24
        }
        
        return MonitoringConfiguration(
            exchanges=exchanges,
            subscriptions=subscriptions,
            symbols=symbols,
            global_settings=global_settings
        )
    
    def _parse_config(self, config_data: Dict[str, Any]) -> MonitoringConfiguration:
        """解析配置数据"""
        exchanges = {}
        subscriptions = {}
        symbols = {}
        
        # 🔥 修复：处理嵌套的配置格式
        # 解析交易所配置 - 适配实际的配置文件格式
        for exchange_id, exchange_data in config_data.get("exchanges", {}).items():
            # 创建交易所配置
            exchanges[exchange_id] = ExchangeConfig(
                exchange_id=exchange_id,
                name=exchange_data.get("name", exchange_id),
                enabled=exchange_data.get("enabled", True),
                base_url=exchange_data.get("base_url", ""),
                ws_url=exchange_data.get("ws_url", ""),
                testnet=exchange_data.get("testnet", False),
                max_symbols=exchange_data.get("max_symbols", 50)
            )
            
            # 🔥 新增：从交易所配置中提取订阅信息
            if exchange_data.get("enabled", True):
                subscriptions[exchange_id] = SubscriptionConfig(
                    exchange_id=exchange_id,
                    data_types=exchange_data.get("data_types", ["ticker"]),
                    symbols=exchange_data.get("symbols", []),
                    batch_size=exchange_data.get("batch_size", 10)
                )
        
        # 解析独立的订阅配置（如果存在）
        for exchange_id, sub_data in config_data.get("subscriptions", {}).items():
            subscriptions[exchange_id] = SubscriptionConfig(
                exchange_id=exchange_id,
                data_types=sub_data.get("data_types", ["ticker"]),
                symbols=sub_data.get("symbols", []),
                batch_size=sub_data.get("batch_size", 10)
            )
        
        # 解析交易对配置
        for symbol_key, symbol_data in config_data.get("symbols", {}).items():
            symbols[symbol_key] = SymbolConfig(
                symbol=symbol_data.get("symbol", ""),
                exchange_id=symbol_data.get("exchange_id", ""),
                enabled=symbol_data.get("enabled", True),
                priority=symbol_data.get("priority", 1)
            )
        
        global_settings = config_data.get("global_settings", {})
        
        return MonitoringConfiguration(
            exchanges=exchanges,
            subscriptions=subscriptions,
            symbols=symbols,
            global_settings=global_settings
        )
    
    def _serialize_config(self, config: MonitoringConfiguration) -> Dict[str, Any]:
        """序列化配置为字典"""
        exchanges = {}
        for exchange_id, exchange_config in config.exchanges.items():
            exchanges[exchange_id] = {
                "name": exchange_config.name,
                "enabled": exchange_config.enabled,
                "base_url": exchange_config.base_url,
                "ws_url": exchange_config.ws_url,
                "testnet": exchange_config.testnet,
                "max_symbols": exchange_config.max_symbols
            }
        
        subscriptions = {}
        for exchange_id, sub_config in config.subscriptions.items():
            subscriptions[exchange_id] = {
                "data_types": sub_config.data_types,
                "symbols": sub_config.symbols,
                "batch_size": sub_config.batch_size
            }
        
        symbols = {}
        for symbol_key, symbol_config in config.symbols.items():
            symbols[symbol_key] = {
                "symbol": symbol_config.symbol,
                "exchange_id": symbol_config.exchange_id,
                "enabled": symbol_config.enabled,
                "priority": symbol_config.priority
            }
        
        return {
            "exchanges": exchanges,
            "subscriptions": subscriptions,
            "symbols": symbols,
            "global_settings": config.global_settings
        } 