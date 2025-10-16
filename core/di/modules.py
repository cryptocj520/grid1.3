"""
依赖注入模块配置

定义各个模块的依赖注入绑定规则
"""

from injector import Module, singleton, provider

# 服务接口和实现
from ..services.interfaces.config_service import IConfigurationService
from ..services.implementations.config_service import ConfigurationServiceImpl
from ..services.interfaces.monitoring_service import MonitoringService
from ..services.implementations.enhanced_monitoring_service import EnhancedMonitoringServiceImpl
from ..services.events.event_handler import EventHandler

# 符号服务 - 统一从symbol_manager模块导入
from ..services.symbol_manager.interfaces.symbol_conversion_service import ISymbolConversionService
from ..services.symbol_manager.implementations.symbol_conversion_service import SymbolConversionService
from ..services.symbol_manager.interfaces.symbol_cache import ISymbolCacheService
from ..services.symbol_manager.implementations.symbol_cache_service import SymbolCacheServiceImpl

# 适配器和数据模块
from ..adapters.exchanges.manager import ExchangeManager
from ..adapters.exchanges.factory import ExchangeFactory
from ..data_aggregator import DataAggregator


class ConfigModule(Module):
    """配置模块"""
    
    def configure(self, binder):
        binder.bind(IConfigurationService, to=ConfigurationServiceImpl, scope=singleton)


class EventModule(Module):
    """事件模块"""
    
    def configure(self, binder):
        binder.bind(EventHandler, to=EventHandler, scope=singleton)


class ExchangeModule(Module):
    """交易所模块"""
    
    def configure(self, binder):
        binder.bind(ExchangeManager, to=ExchangeManager, scope=singleton)
        binder.bind(ExchangeFactory, to=ExchangeFactory, scope=singleton)


class SymbolModule(Module):
    """符号服务模块"""
    
    def configure(self, binder):
        # 符号转换服务 - 使用singleton绑定
        binder.bind(ISymbolConversionService, to=SymbolConversionService, scope=singleton)
        
    @singleton
    @provider
    def provide_symbol_cache_service(self, 
                                   exchange_manager: ExchangeManager,
                                   symbol_conversion_service: ISymbolConversionService) -> ISymbolCacheService:
        """提供符号缓存服务实例"""
        return SymbolCacheServiceImpl(exchange_manager, symbol_conversion_service)


class DataModule(Module):
    """数据模块"""
    
    def configure(self, binder):
        binder.bind(DataAggregator, to=DataAggregator, scope=singleton)


class MonitoringModule(Module):
    """监控模块"""
    
    def configure(self, binder):
        binder.bind(MonitoringService, to=EnhancedMonitoringServiceImpl, scope=singleton)


# 所有模块的集合 - 按依赖顺序排列
ALL_MODULES = [
    ConfigModule,       # 配置服务 - 基础服务
    EventModule,        # 事件处理
    ExchangeModule,     # 交易所管理
    SymbolModule,       # 符号转换和缓存服务
    DataModule,         # 数据聚合
    MonitoringModule    # 监控服务 - 依赖其他服务
] 