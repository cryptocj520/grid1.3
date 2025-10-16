"""
套利系统初始化器

负责协调整个套利系统的启动和集成
"""

import asyncio
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal

from core.logging import get_logger
from core.adapters.exchanges.interface import ExchangeInterface
from ..coordinator.arbitrage_coordinator import ArbitrageCoordinator
from ..decision.opportunity_processor import OpportunityProcessor
from ..shared.config import ArbitrageConfigManager, ArbitrageSystemConfig


class ArbitrageInitializer:
    """套利系统初始化器"""
    
    def __init__(self, exchange_adapters: Dict[str, ExchangeInterface]):
        """
        初始化套利系统初始化器
        
        Args:
            exchange_adapters: 交易所适配器字典
        """
        self.exchange_adapters = exchange_adapters
        self.logger = get_logger(__name__)
        
        # 核心组件
        self.config_manager = ArbitrageConfigManager()
        self.arbitrage_coordinator: Optional[ArbitrageCoordinator] = None
        self.opportunity_processor: Optional[OpportunityProcessor] = None
        
        # 系统状态
        self.initialized = False
        self.running = False
        
        # 集成接口
        self.integration_callbacks: Dict[str, Callable] = {}
    
    async def initialize(self, config_path: Optional[str] = None, 
                        overlapping_symbols: Optional[List[str]] = None) -> bool:
        """
        初始化套利系统
        
        Args:
            config_path: 配置文件路径
            overlapping_symbols: 重叠交易对列表
            
        Returns:
            是否成功初始化
        """
        try:
            self.logger.info("开始初始化套利系统...")
            
            # 1. 加载配置
            config = self.config_manager.load_config(config_path)
            
            if not config.enabled:
                self.logger.info("套利系统已禁用")
                return False
            
            # 2. 验证配置
            if not self.config_manager.validate_config(config):
                self.logger.error("配置验证失败")
                return False
            
            # 3. 初始化套利协调器
            self.arbitrage_coordinator = ArbitrageCoordinator(
                self.exchange_adapters,
                config.to_dict()
            )
            
            # 4. 初始化精度管理和核心组件
            if overlapping_symbols:
                init_success = await self.arbitrage_coordinator.initialize(overlapping_symbols)
                if not init_success:
                    self.logger.error("套利协调器初始化失败")
                    return False
            
            # 5. 初始化机会处理器
            self.opportunity_processor = OpportunityProcessor(self.arbitrage_coordinator)
            
            # 6. 设置集成回调
            self._setup_integration_callbacks()
            
            self.initialized = True
            self.logger.info("套利系统初始化完成")
            return True
            
        except Exception as e:
            self.logger.error(f"初始化套利系统失败: {e}")
            return False
    
    async def start(self) -> bool:
        """
        启动套利系统
        
        Returns:
            是否成功启动
        """
        try:
            if not self.initialized:
                self.logger.error("系统未初始化")
                return False
            
            if self.running:
                self.logger.warning("系统已经在运行中")
                return True
            
            # 启动套利协调器
            await self.arbitrage_coordinator.start()
            
            self.running = True
            self.logger.info("套利系统已启动")
            return True
            
        except Exception as e:
            self.logger.error(f"启动套利系统失败: {e}")
            return False
    
    async def stop(self):
        """停止套利系统"""
        try:
            if not self.running:
                return
            
            self.running = False
            
            # 停止套利协调器
            if self.arbitrage_coordinator:
                await self.arbitrage_coordinator.stop()
            
            self.logger.info("套利系统已停止")
            
        except Exception as e:
            self.logger.error(f"停止套利系统失败: {e}")
    
    def _setup_integration_callbacks(self):
        """设置集成回调"""
        config = self.config_manager.get_config()
        
        # 设置市场数据回调
        if self.arbitrage_coordinator:
            self.arbitrage_coordinator.set_market_data_callback(
                self._on_market_data_processed
            )
            
            self.arbitrage_coordinator.set_execution_callback(
                self._on_execution_completed
            )
    
    async def _on_market_data_processed(self, market_data: Dict[str, Any], trade_plan: Optional[Any]):
        """市场数据处理回调"""
        if 'market_data_callback' in self.integration_callbacks:
            await self.integration_callbacks['market_data_callback'](market_data, trade_plan)
    
    async def _on_execution_completed(self, trade_plan: Any, result: Any):
        """执行完成回调"""
        if 'execution_callback' in self.integration_callbacks:
            await self.integration_callbacks['execution_callback'](trade_plan, result)
    
    def register_integration_callback(self, callback_type: str, callback: Callable):
        """
        注册集成回调
        
        Args:
            callback_type: 回调类型
            callback: 回调函数
        """
        self.integration_callbacks[callback_type] = callback
        self.logger.info(f"注册集成回调: {callback_type}")
    
    def get_opportunity_processor(self) -> Optional[OpportunityProcessor]:
        """获取机会处理器"""
        return self.opportunity_processor
    
    def get_arbitrage_coordinator(self) -> Optional[ArbitrageCoordinator]:
        """获取套利协调器"""
        return self.arbitrage_coordinator
    
    def get_config(self) -> ArbitrageSystemConfig:
        """获取配置"""
        return self.config_manager.get_config()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = {
            'initialized': self.initialized,
            'running': self.running,
            'exchange_count': len(self.exchange_adapters),
            'integration_callbacks': len(self.integration_callbacks)
        }
        
        if self.arbitrage_coordinator:
            stats['arbitrage_stats'] = self.arbitrage_coordinator.get_stats()
        
        if self.opportunity_processor:
            stats['processor_stats'] = self.opportunity_processor.get_stats()
        
        return stats
    
    # 与现有监视器模块集成的便捷方法
    async def handle_spread_analysis_result(self, spread_data: Dict[str, Any]):
        """
        处理价差分析结果（便捷方法）
        
        Args:
            spread_data: 价差分析结果
        """
        if self.opportunity_processor and self.running:
            await self.opportunity_processor.process_spread_analysis_result(spread_data)
    
    async def handle_ticker_update(self, ticker_data: Dict[str, Any]):
        """
        处理ticker更新（便捷方法）
        
        Args:
            ticker_data: ticker数据
        """
        if self.opportunity_processor and self.running:
            await self.opportunity_processor.process_ticker_update(ticker_data)
    
    async def handle_price_monitor_data(self, price_data: Dict[str, Any]):
        """
        处理价格监控数据（便捷方法）
        
        Args:
            price_data: 价格监控数据
        """
        if self.opportunity_processor and self.running:
            await self.opportunity_processor.process_price_monitor_data(price_data)
    
    async def handle_generic_market_data(self, data: Dict[str, Any], data_type: str = 'generic'):
        """
        处理通用市场数据（便捷方法）
        
        Args:
            data: 市场数据
            data_type: 数据类型
        """
        if self.opportunity_processor and self.running:
            await self.opportunity_processor.process_generic_data(data, data_type)
    
    # TODO: 高级功能占位符
    async def reload_config(self, config_path: Optional[str] = None):
        """
        重新加载配置
        
        Args:
            config_path: 配置文件路径
        """
        # TODO: 实现配置重载
        pass
    
    async def health_check(self) -> Dict[str, Any]:
        """
        健康检查
        
        Returns:
            健康检查结果
        """
        # TODO: 实现健康检查
        return {
            'status': 'healthy',
            'initialized': self.initialized,
            'running': self.running
        }
    
    async def backup_state(self, backup_path: str):
        """
        备份系统状态
        
        Args:
            backup_path: 备份路径
        """
        # TODO: 实现状态备份
        pass
    
    async def restore_state(self, backup_path: str):
        """
        恢复系统状态
        
        Args:
            backup_path: 备份路径
        """
        # TODO: 实现状态恢复
        pass
    
    async def get_performance_metrics(self) -> Dict[str, Any]:
        """
        获取性能指标
        
        Returns:
            性能指标
        """
        # TODO: 实现性能指标收集
        return {
            'status': 'not_implemented'
        } 