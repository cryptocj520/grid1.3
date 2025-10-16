"""
数据聚合器模块

统一收集和处理所有交易所的实时数据，使用依赖注入的ExchangeManager和简化的事件处理
"""

import asyncio
from typing import Dict, List, Optional, Any, Callable, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from decimal import Decimal
from injector import inject, singleton

# 使用统一日志入口
from .logging import get_data_logger

from .adapters.exchanges.models import TickerData, OrderBookData, TradeData
from .adapters.exchanges.adapter import ExchangeAdapter
from .adapters.exchanges.manager import ExchangeManager
from .services.events.event_handler import EventHandler
from .services.events.event import Event
from .domain.models import DataType, MonitoringDataTypeConfig, ExchangeDataTypeConfig, DataTypeConfig, SubscriptionStatus, SubscriptionSummary
from .services.symbol_manager import ISymbolCacheService, SymbolCacheServiceImpl, SymbolOverlapConfig


@dataclass
class AggregatedData:
    """聚合数据结构"""
    exchange: str
    symbol: str
    data_type: DataType
    data: Any
    timestamp: datetime
    
    
@dataclass
class MarketSnapshot:
    """市场快照"""
    symbol: str
    exchange_data: Dict[str, Any] = field(default_factory=dict)
    last_update: datetime = field(default_factory=datetime.now)


@singleton
class DataAggregator:
    """数据聚合器 - 使用依赖注入的ExchangeManager和简化的事件处理"""
    
    @inject
    def __init__(self, exchange_manager: ExchangeManager, event_handler: EventHandler, symbol_cache_service: ISymbolCacheService):
        # 使用统一日志入口 - 数据专用日志器
        self.logger = get_data_logger("DataAggregator")
        self.exchange_manager = exchange_manager
        self.event_handler = event_handler
        
        # 🔥 修复：通过依赖注入获取符号缓存服务
        self.symbol_cache_service = symbol_cache_service
        
        self.logger.info(f"💡 数据聚合器初始化完成, 使用简化事件处理器: {event_handler.name}")
        
        # 数据存储
        self.market_snapshots: Dict[str, MarketSnapshot] = {}  # symbol -> MarketSnapshot
        self.ticker_data: Dict[str, Dict[str, TickerData]] = {}  # symbol -> {exchange: TickerData}
        self.orderbook_data: Dict[str, Dict[str, OrderBookData]] = {}  # symbol -> {exchange: OrderBookData}
        # 🔥 新增：trades数据存储
        self.trades_data: Dict[str, Dict[str, List[TradeData]]] = {}  # symbol -> {exchange: List[TradeData]}
        
        # 订阅管理
        self.subscribed_symbols: Set[str] = set()
        self.subscribed_exchanges: Set[str] = set()
        
        # 🔥 新增：记录配置信息以便重连时使用
        self.monitoring_config = None
        self.configured_exchanges: Set[str] = set()
        self.exchange_symbol_mapping: Dict[str, List[str]] = {}  # exchange -> symbols
        
        # 回调函数
        self.data_callbacks: Dict[DataType, List[Callable]] = {
            DataType.TICKER: [],
            DataType.ORDERBOOK: [],
            DataType.TRADES: [],
            DataType.USER_DATA: []
        }
        
        # 状态
        self.is_running = False
        
        # 🔥 新增：启动连接状态监控任务
        self._connection_monitor_task = None
    
    # 🗑️ 已删除：旧的动态获取方法已被符号缓存服务替代
    # - get_all_supported_symbols()
    # - get_common_symbols()
    
    def get_cached_symbols(self, exchange_id: str = None) -> Dict[str, List[str]]:
        """获取缓存的交易对列表
        
        Args:
            exchange_id: 交易所ID，如果为None则返回所有交易所
            
        Returns:
            Dict[str, List[str]]: 交易所符号映射
        """
        if exchange_id:
            symbols = self.symbol_cache_service.get_symbols_for_exchange(exchange_id)
            return {exchange_id: symbols}
        else:
            return self.symbol_cache_service.get_all_exchange_symbols()
    
    def get_overlap_symbols(self) -> List[str]:
        """获取重叠的交易对列表"""
        return self.symbol_cache_service.get_overlap_symbols()
    
    def get_symbol_cache_stats(self) -> Dict[str, Any]:
        """获取符号缓存统计信息"""
        return self.symbol_cache_service.get_cache_stats()
    
    async def start_configured_monitoring(self, config_service=None) -> Dict[str, Any]:
        """基于配置启动监控 - 使用符号缓存服务，支持动态重连"""
        try:
            if self.is_running:
                self.logger.warning("数据聚合器已经在运行")
                return {"status": "already_running"}
            
            self.logger.info("🚀 开始基于符号缓存的监控启动...")
            
            # 1. 获取所有配置的交易所（包括未连接的）
            configured_exchanges = self.exchange_manager.get_configured_exchanges()
            connected_adapters = self.exchange_manager.get_connected_adapters()
            
            if not configured_exchanges:
                self.logger.error("❌ 没有配置的交易所")
                return {"status": "no_exchanges"}
            
            # 🔥 新增：记录配置信息以便重连时使用
            self.configured_exchanges = set(configured_exchanges)
            
            # 2. 获取数据类型配置
            if config_service:
                try:
                    self.monitoring_config = await config_service.get_monitoring_data_type_config()
                    self.logger.info("✅ 已获取数据类型配置")
                except Exception as e:
                    self.logger.warning(f"⚠️  获取数据类型配置失败: {e}，使用默认配置")
            
            # 3. 解析符号管理配置
            symbol_config = self._parse_symbol_management_config()
            
            # 4. 初始化符号缓存（使用所有配置的交易所）
            self.logger.info("📊 初始化符号缓存...")
            cache_success = await self.symbol_cache_service.initialize_cache(
                list(self.configured_exchanges), symbol_config
            )
            
            if not cache_success:
                self.logger.error("❌ 符号缓存初始化失败")
                return {"status": "cache_init_failed"}
            
            # 5. 启动结果统计
            results = {
                "status": "started",
                "exchanges": {},
                "total_symbols": 0,
                "total_subscriptions": 0,
                "successful_subscriptions": 0,
                "failed_subscriptions": 0,
                "start_time": datetime.now().isoformat(),
                "subscription_summary": SubscriptionSummary(),
                "cache_stats": self.symbol_cache_service.get_cache_stats()
            }
            
            self.is_running = True
            
            # 6. 为每个配置的交易所启动监控（已连接的直接订阅，未连接的标记为等待）
            for exchange_name in self.configured_exchanges:
                # 获取符号列表
                exchange_symbols = self.symbol_cache_service.get_symbols_for_exchange(exchange_name)
                self.exchange_symbol_mapping[exchange_name] = exchange_symbols
                
                # 获取数据类型配置
                if self.monitoring_config:
                    enabled_data_types = self.monitoring_config.get_enabled_types_for_exchange(exchange_name)
                else:
                    enabled_data_types = [DataType.TICKER]
                
                exchange_result = {
                    "symbols": exchange_symbols,
                    "symbol_count": len(exchange_symbols),
                    "subscriptions": len(exchange_symbols) * len(enabled_data_types),
                    "successful": 0,
                    "failed": 0,
                    "data_types": [dt.value for dt in enabled_data_types],
                    "configured": True
                }
                
                if not exchange_symbols:
                    self.logger.warning(f"⚠️ {exchange_name} 没有可订阅的交易对")
                    exchange_result["status"] = "no_symbols"
                    results["exchanges"][exchange_name] = exchange_result
                    continue
                
                # 检查是否已连接
                if exchange_name in connected_adapters:
                    adapter = connected_adapters[exchange_name]
                    
                    self.logger.info(f"📋 开始订阅 {exchange_name}: {len(exchange_symbols)} 个交易对，数据类型: {[dt.value for dt in enabled_data_types]}")
                    
                    # 立即订阅
                    success = await self._subscribe_exchange_data(exchange_name, adapter, exchange_symbols, enabled_data_types, results)
                    
                    if success:
                        exchange_result["status"] = "subscribed"
                        exchange_result["successful"] = len(exchange_symbols) * len(enabled_data_types)
                    else:
                        exchange_result["status"] = "subscription_failed"
                        exchange_result["failed"] = len(exchange_symbols) * len(enabled_data_types)
                        
                else:
                    # 未连接，标记为待连接
                    self.logger.info(f"⏳ {exchange_name} 未连接，将在连接后自动订阅")
                    exchange_result["status"] = "waiting_for_connection"
                
                results["exchanges"][exchange_name] = exchange_result
                results["total_symbols"] += len(exchange_symbols)
                results["total_subscriptions"] += len(exchange_symbols) * len(enabled_data_types)
            
            # 7. 🔥 新增：启动连接状态监控任务
            if self._connection_monitor_task is None or self._connection_monitor_task.done():
                self._connection_monitor_task = asyncio.create_task(self._monitor_connection_status())
                self.logger.info("🔄 启动连接状态监控任务")
            
            # 8. 统计结果
            self.logger.info("============================================================")
            self.logger.info("🎉 符号缓存驱动的监控启动完成:")
            self.logger.info(f"   - 成功订阅: {results['successful_subscriptions']}/{results['total_subscriptions']}")
            self.logger.info(f"   - 交易所数量: {len(results['exchanges'])}")
            self.logger.info(f"   - 符号数量: {results['total_symbols']}")
            self.logger.info("============================================================")
            
            return results
            
        except Exception as e:
            self.logger.error(f"启动配置监控失败: {e}")
            import traceback
            self.logger.error(f"错误堆栈: {traceback.format_exc()}")
            self.is_running = False
            return {"status": "error", "error": str(e)}
    
    def _parse_symbol_management_config(self) -> SymbolOverlapConfig:
        """解析符号管理配置"""
        try:
            import yaml
            from pathlib import Path
            
            # 从监控配置文件中读取符号管理配置
            config_path = Path("config/monitoring/monitoring.yaml")
            
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    config_data = yaml.safe_load(f)
                
                symbol_mgmt = config_data.get('symbol_management', {})
                overlap_settings = symbol_mgmt.get('overlap_settings', {})
                
                config = SymbolOverlapConfig(
                    min_exchange_count=overlap_settings.get('min_exchange_count', 2),
                    use_overlap_only=overlap_settings.get('use_overlap_only', True),
                    max_symbols_per_exchange=overlap_settings.get('max_symbols_per_exchange', 0)
                )
                
                self.logger.info(f"📋 符号管理配置加载成功: use_overlap_only={config.use_overlap_only}, max_symbols_per_exchange={config.max_symbols_per_exchange}")
                return config
                
        except Exception as e:
            self.logger.warning(f"⚠️ 读取符号管理配置失败: {e}，使用默认配置")
        
        # 使用默认配置作为后备
        return SymbolOverlapConfig(
            min_exchange_count=2,
            use_overlap_only=False,  # 🔥 修改默认值为False
            max_symbols_per_exchange=20  # 🔥 修改默认值为20
        )

    async def start_configured_monitoring_with_config_manager(self, config_manager) -> Dict[str, Any]:
        """🔥 已移除：使用新配置管理器启动监控（此方法已被统一配置系统替代）"""
        self.logger.warning("⚠️ start_configured_monitoring_with_config_manager 方法已废弃，请使用 start_configured_monitoring")
        
        # 降级到统一的配置方法
        return await self.start_configured_monitoring()
    
    # 🗑️ 已删除：旧的批量监控方法已被符号缓存方法替代
    # - start_batch_monitoring()
    # 
    # 请使用 start_configured_monitoring() 方法
    
    async def _start_ticker_monitoring(self, exchange_name: str, adapter: ExchangeAdapter, symbols: List[str]) -> None:
        """启动ticker监控"""
        try:
            # 创建ticker数据回调
            async def ticker_callback(symbol: str, ticker_data: TickerData):
                await self._handle_ticker_data(exchange_name, symbol, ticker_data)
            
            # 批量订阅ticker
            if hasattr(adapter, 'batch_subscribe_tickers'):
                await adapter.batch_subscribe_tickers(symbols, ticker_callback)
            else:
                # 如果不支持批量订阅，逐个订阅
                for symbol in symbols:
                    # 使用闭包正确捕获symbol
                    async def create_symbol_callback(sym):
                        async def callback(data):
                            await ticker_callback(sym, data)
                        return callback
                    
                    callback = await create_symbol_callback(symbol)
                    await adapter.subscribe_ticker(symbol, callback)
                    
        except Exception as e:
            self.logger.error(f"启动 {exchange_name} ticker监控时出错: {e}")
    
    async def _start_orderbook_monitoring(self, exchange_name: str, adapter: ExchangeAdapter, symbols: List[str]) -> None:
        """启动orderbook监控"""
        try:
            # 创建orderbook数据回调
            async def orderbook_callback(symbol: str, orderbook_data: OrderBookData):
                await self._handle_orderbook_data(exchange_name, symbol, orderbook_data)
            
            # 批量订阅orderbook
            if hasattr(adapter, 'batch_subscribe_orderbooks'):
                # 使用关键字参数明确传递callback，避免参数错位
                await adapter.batch_subscribe_orderbooks(symbols, callback=orderbook_callback)
            else:
                # 如果不支持批量订阅，逐个订阅
                for symbol in symbols:
                    # 使用闭包正确捕获symbol
                    async def create_symbol_callback(sym):
                        async def callback(data):
                            await orderbook_callback(sym, data)
                        return callback
                    
                    callback = await create_symbol_callback(symbol)
                    await adapter.subscribe_orderbook(symbol, callback)
                    
        except Exception as e:
            self.logger.error(f"启动 {exchange_name} orderbook监控时出错: {e}")
    
    async def _start_trades_monitoring(self, exchange_name: str, adapter: ExchangeAdapter, symbols: List[str]) -> None:
        """启动trades监控"""
        try:
            # 创建trades数据回调
            async def trades_callback(symbol: str, trade_data: TradeData):
                await self._handle_trades_data(exchange_name, symbol, trade_data)
            
            # 批量订阅trades（如果支持）
            if hasattr(adapter, 'batch_subscribe_trades'):
                await adapter.batch_subscribe_trades(symbols, callback=trades_callback)
            else:
                # 如果不支持批量订阅，逐个订阅
                for symbol in symbols:
                    # 使用闭包正确捕获symbol
                    async def create_symbol_callback(sym):
                        async def callback(data):
                            await trades_callback(sym, data)
                        return callback
                    
                    callback = await create_symbol_callback(symbol)
                    await adapter.subscribe_trades(symbol, callback)
                    
        except Exception as e:
            self.logger.error(f"启动 {exchange_name} trades监控时出错: {e}")
    
    async def _start_user_data_monitoring(self, exchange_name: str, adapter: ExchangeAdapter) -> None:
        """启动user_data监控"""
        try:
            # 创建user_data数据回调
            async def user_data_callback(user_data: Dict[str, Any]):
                await self._handle_user_data(exchange_name, user_data)
            
            # 订阅user_data
            await adapter.subscribe_user_data(user_data_callback)
                    
        except Exception as e:
            self.logger.error(f"启动 {exchange_name} user_data监控时出错: {e}")
    
    async def _handle_ticker_data(self, exchange_name: str, symbol: str, ticker_data: TickerData) -> None:
        """处理ticker数据 - 直接转发原始数据"""
        try:
            # 记录接收时间
            received_time = datetime.now()
            ticker_data.received_timestamp = received_time
            
            # 记录处理时间
            processed_time = datetime.now()
            ticker_data.processed_timestamp = processed_time
            
            # 更新内部存储（使用原始符号）
            if symbol not in self.ticker_data:
                self.ticker_data[symbol] = {}
            self.ticker_data[symbol][exchange_name] = ticker_data
            
            # 更新市场快照
            self._update_market_snapshot(symbol, exchange_name, 'ticker', ticker_data)
            
            # 记录发送时间
            sent_time = datetime.now()
            ticker_data.sent_timestamp = sent_time
            
            # 创建聚合数据
            aggregated_data = AggregatedData(
                exchange=exchange_name,
                symbol=symbol,  # 发送原始符号
                data_type=DataType.TICKER,
                data=ticker_data,
                timestamp=sent_time
            )
            
            # 调用回调函数
            for callback in self.data_callbacks[DataType.TICKER]:
                await self._safe_callback(callback, aggregated_data)
                
            # 发送事件（简化版本）
            await self._publish_ticker_event(symbol, exchange_name, ticker_data)
            
        except Exception as e:
            self.logger.error(f"处理ticker数据时出错: {e}")
    
    async def _handle_orderbook_data(self, exchange_name: str, symbol: str, orderbook_data: OrderBookData) -> None:
        """处理orderbook数据 - 直接转发原始数据"""
        try:
            # 记录接收时间
            received_time = datetime.now()
            orderbook_data.received_timestamp = received_time
            
            # 记录处理时间
            processed_time = datetime.now()
            orderbook_data.processed_timestamp = processed_time
            
            # 更新内部存储（使用原始符号）
            if symbol not in self.orderbook_data:
                self.orderbook_data[symbol] = {}
            self.orderbook_data[symbol][exchange_name] = orderbook_data
            
            # 更新市场快照
            self._update_market_snapshot(symbol, exchange_name, 'orderbook', orderbook_data)
            
            # 记录发送时间
            sent_time = datetime.now()
            orderbook_data.sent_timestamp = sent_time
            
            # 创建聚合数据
            aggregated_data = AggregatedData(
                exchange=exchange_name,
                symbol=symbol,  # 发送原始符号
                data_type=DataType.ORDERBOOK,
                data=orderbook_data,
                timestamp=sent_time
            )
            
            # 调用回调函数
            for callback in self.data_callbacks[DataType.ORDERBOOK]:
                await self._safe_callback(callback, aggregated_data)
                
            # 发送事件（简化版本）
            await self._publish_orderbook_event(symbol, exchange_name, orderbook_data)
            
        except Exception as e:
            self.logger.error(f"处理orderbook数据时出错: {e}")
    
    async def _handle_trades_data(self, exchange_name: str, symbol: str, trade_data: TradeData) -> None:
        """处理trades数据 - 直接转发原始数据"""
        try:
            # 记录接收时间
            received_time = datetime.now()
            trade_data.received_timestamp = received_time
            
            # 记录处理时间
            processed_time = datetime.now()
            trade_data.processed_timestamp = processed_time
            
            # 🔥 新增：更新内部存储（使用原始符号）
            if symbol not in self.trades_data:
                self.trades_data[symbol] = {}
            if exchange_name not in self.trades_data[symbol]:
                self.trades_data[symbol][exchange_name] = []
            
            # 添加新的trade数据，保持最近的100条记录
            self.trades_data[symbol][exchange_name].append(trade_data)
            if len(self.trades_data[symbol][exchange_name]) > 100:
                self.trades_data[symbol][exchange_name] = self.trades_data[symbol][exchange_name][-100:]
            
            # 更新市场快照
            self._update_market_snapshot(symbol, exchange_name, 'trades', trade_data)
            
            # 记录发送时间
            sent_time = datetime.now()
            trade_data.sent_timestamp = sent_time
            
            # 创建聚合数据
            aggregated_data = AggregatedData(
                exchange=exchange_name,
                symbol=symbol,  # 发送原始符号
                data_type=DataType.TRADES,
                data=trade_data,
                timestamp=sent_time
            )
            
            # 调用回调函数
            for callback in self.data_callbacks[DataType.TRADES]:
                await self._safe_callback(callback, aggregated_data)
                
            # 发送事件（简化版本）
            await self._publish_trades_event(symbol, exchange_name, trade_data)
            
        except Exception as e:
            self.logger.error(f"处理trades数据时出错: {e}")
    
    async def _handle_user_data(self, exchange_name: str, user_data: Dict[str, Any]) -> None:
        """处理user_data数据 - 直接转发原始数据"""
        try:
            # 记录接收时间
            received_time = datetime.now()
            user_data['received_timestamp'] = received_time.isoformat()
            
            # 记录处理时间
            processed_time = datetime.now()
            user_data['processed_timestamp'] = processed_time.isoformat()
            
            # 更新市场快照
            self._update_market_snapshot("", exchange_name, 'user_data', user_data)
            
            # 记录发送时间
            sent_time = datetime.now()
            user_data['sent_timestamp'] = sent_time.isoformat()
            
            # 创建聚合数据
            aggregated_data = AggregatedData(
                exchange=exchange_name,
                symbol="",  # user_data不需要symbol
                data_type=DataType.USER_DATA,
                data=user_data,
                timestamp=sent_time
            )
            
            # 调用回调函数
            for callback in self.data_callbacks[DataType.USER_DATA]:
                await self._safe_callback(callback, aggregated_data)
                
            # 发送事件（简化版本）
            await self._publish_user_data_event(exchange_name, user_data)
            
        except Exception as e:
            self.logger.error(f"处理user_data数据时出错: {e}")
    
    async def _publish_ticker_event(self, symbol: str, exchange_name: str, ticker_data: TickerData) -> None:
        """发布ticker事件 - 使用简化的事件处理器"""
        try:
            # 创建ticker事件数据
            event_data = {
                'event_type': 'ticker_updated',
                'symbol': symbol,
                'exchange': exchange_name,
                'bid': float(ticker_data.bid or 0),
                'ask': float(ticker_data.ask or 0),
                'last': float(ticker_data.last or 0),
                'volume': float(ticker_data.volume or 0),
                'high': float(ticker_data.high or 0),
                'low': float(ticker_data.low or 0),
                'open_price': float(ticker_data.open or 0),
                'close_price': float(ticker_data.close or 0),
                'change': float(ticker_data.change or 0),
                'percentage': float(ticker_data.percentage or 0),
                'timestamp': datetime.now().isoformat()
            }
            
            # 发布事件
            await self.event_handler.publish('ticker_updated', event_data)
            
        except Exception as e:
            self.logger.warning(f"发布ticker事件失败: {e}")
    
    async def _publish_orderbook_event(self, symbol: str, exchange_name: str, orderbook_data: OrderBookData) -> None:
        """发布orderbook事件 - 使用简化的事件处理器"""
        try:
            # 转换订单簿数据格式
            bids_data = [[float(level.price), float(level.size)] for level in orderbook_data.bids]
            asks_data = [[float(level.price), float(level.size)] for level in orderbook_data.asks]
            
            # 创建orderbook事件数据
            event_data = {
                'event_type': 'orderbook_updated',
                'symbol': symbol,
                'exchange': exchange_name,
                'bids': bids_data,
                'asks': asks_data,
                'sequence': orderbook_data.nonce,
                'timestamp': datetime.now().isoformat()
            }
            
            # 发布事件
            await self.event_handler.publish('orderbook_updated', event_data)
            
        except Exception as e:
            self.logger.warning(f"发布orderbook事件失败: {e}")
    
    async def _publish_trades_event(self, symbol: str, exchange_name: str, trade_data: TradeData) -> None:
        """发布trades事件 - 使用简化的事件处理器"""
        try:
            # 创建trades事件数据
            event_data = {
                'event_type': 'trades_updated',
                'symbol': symbol,
                'exchange': exchange_name,
                'price': float(trade_data.price or 0),
                'quantity': float(trade_data.quantity or 0),
                'side': trade_data.side.value if trade_data.side else 'unknown',
                'timestamp': datetime.now().isoformat()
            }
            
            # 发布事件
            await self.event_handler.publish('trades_updated', event_data)
            
        except Exception as e:
            self.logger.warning(f"发布trades事件失败: {e}")
    
    async def _publish_user_data_event(self, exchange_name: str, user_data: Dict[str, Any]) -> None:
        """发布user_data事件 - 使用简化的事件处理器"""
        try:
            # 创建user_data事件数据
            event_data = {
                'event_type': 'user_data_updated',
                'exchange': exchange_name,
                'data': user_data,
                'timestamp': datetime.now().isoformat()
            }
            
            # 发布事件
            await self.event_handler.publish('user_data_updated', event_data)
            
        except Exception as e:
            self.logger.warning(f"发布user_data事件失败: {e}")
    
    def _update_market_snapshot(self, symbol: str, exchange_name: str, data_type: str, data: Any) -> None:
        """更新市场快照"""
        if symbol not in self.market_snapshots:
            self.market_snapshots[symbol] = MarketSnapshot(symbol=symbol)
            
        snapshot = self.market_snapshots[symbol]
        if exchange_name not in snapshot.exchange_data:
            snapshot.exchange_data[exchange_name] = {}
            
        snapshot.exchange_data[exchange_name][data_type] = data
        snapshot.last_update = datetime.now()
    
    def register_data_callback(self, data_type: DataType, callback: Callable[[AggregatedData], None]) -> None:
        """注册数据回调"""
        self.data_callbacks[data_type].append(callback)
    
    def get_market_snapshot(self, symbol: str) -> Optional[MarketSnapshot]:
        """获取市场快照"""
        return self.market_snapshots.get(symbol)
    
    def get_all_market_snapshots(self) -> Dict[str, MarketSnapshot]:
        """获取所有市场快照"""
        return self.market_snapshots.copy()
    
    def get_ticker_data(self, symbol: str = None, exchange: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """获取ticker数据"""
        if symbol and exchange:
            # 获取特定交易所的特定符号数据
            symbol_data = self.ticker_data.get(symbol, {})
            return {exchange: symbol_data.get(exchange)} if exchange in symbol_data else {}
        elif symbol:
            # 获取特定符号的所有交易所数据
            return self.ticker_data.get(symbol, {})
        else:
            # 获取所有数据
            return self.ticker_data
    
    def get_orderbook_data(self, symbol: str = None, exchange: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """获取orderbook数据"""
        if symbol and exchange:
            # 获取特定交易所的特定符号数据
            symbol_data = self.orderbook_data.get(symbol, {})
            return {exchange: symbol_data.get(exchange)} if exchange in symbol_data else {}
        elif symbol:
            # 获取特定符号的所有交易所数据
            return self.orderbook_data.get(symbol, {})
        else:
            # 获取所有数据
            return self.orderbook_data
    
    def get_trades_data(self, symbol: str = None, exchange: Optional[str] = None) -> Dict[str, Dict[str, List[TradeData]]]:
        """获取trades数据"""
        if symbol and exchange:
            # 获取特定交易所的特定符号数据
            symbol_data = self.trades_data.get(symbol, {})
            return {exchange: symbol_data.get(exchange, [])} if exchange in symbol_data else {}
        elif symbol:
            # 获取特定符号的所有交易所数据
            return self.trades_data.get(symbol, {})
        else:
            # 获取所有数据
            return self.trades_data
    
    async def stop(self) -> None:
        """停止数据聚合器"""
        try:
            self.is_running = False
            
            # 🔥 新增：取消连接状态监控任务
            if self._connection_monitor_task and not self._connection_monitor_task.done():
                self._connection_monitor_task.cancel()
                try:
                    await self._connection_monitor_task
                except asyncio.CancelledError:
                    pass
                self._connection_monitor_task = None
                self.logger.info("连接状态监控任务已取消")
            
            # 从ExchangeManager获取连接的适配器并取消订阅
            connected_adapters = self.exchange_manager.get_connected_adapters()
            for exchange_name, adapter in connected_adapters.items():
                try:
                    if hasattr(adapter, 'unsubscribe'):
                        await adapter.unsubscribe()
                except Exception as e:
                    self.logger.error(f"取消 {exchange_name} 订阅时出错: {e}")
            
            # 清空数据
            self.market_snapshots.clear()
            self.ticker_data.clear()
            self.orderbook_data.clear()
            self.trades_data.clear()  # 🔥 新增：清理trades数据
            self.subscribed_symbols.clear()
            self.subscribed_exchanges.clear()
            
            # 🔥 新增：清理配置信息
            self.monitoring_config = None
            self.configured_exchanges.clear()
            self.exchange_symbol_mapping.clear()
            
            self.logger.info("数据聚合器已停止")
            
        except Exception as e:
            self.logger.error(f"停止数据聚合器时出错: {e}")
    
    async def _safe_callback(self, callback: Callable, data: Any) -> None:
        """安全调用回调函数"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            self.logger.error(f"回调函数执行出错: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        # 从ExchangeManager获取交易所列表
        connected_adapters = self.exchange_manager.get_connected_adapters()
        
        # 获取事件处理器统计
        event_stats = self.event_handler.get_stats()
        
        return {
            "exchanges": list(connected_adapters.keys()),
            "subscribed_symbols": list(self.subscribed_symbols),
            "total_symbols": len(self.subscribed_symbols),
            "total_exchanges": len(self.subscribed_exchanges),
            "ticker_data_count": sum(len(data) for data in self.ticker_data.values()),
            "orderbook_data_count": sum(len(data) for data in self.orderbook_data.values()),
            "trades_data_count": sum(len(data) for data in self.trades_data.values()),
            "is_running": self.is_running,
            "event_stats": event_stats
        }
    
    async def subscribe_ticker(self, exchange_id: str, symbols: List[str]) -> bool:
        """订阅单个交易所的ticker数据"""
        try:
            # 从ExchangeManager获取适配器
            connected_adapters = self.exchange_manager.get_connected_adapters()
            
            if exchange_id not in connected_adapters:
                self.logger.error(f"未找到交易所: {exchange_id}")
                return False
            
            adapter = connected_adapters[exchange_id]
            
            # 创建ticker回调
            async def ticker_callback(symbol: str, ticker_data: TickerData):
                await self._handle_ticker_data(exchange_id, symbol, ticker_data)
            
            # 批量订阅
            if hasattr(adapter, 'batch_subscribe_tickers'):
                await adapter.batch_subscribe_tickers(symbols, ticker_callback)
            else:
                # 逐个订阅
                for symbol in symbols:
                    async def create_callback(sym):
                        async def callback(data):
                            await ticker_callback(sym, data)
                        return callback
                    
                    callback = await create_callback(symbol)
                    await adapter.subscribe_ticker(symbol, callback)
            
            # 更新订阅状态
            self.subscribed_symbols.update(symbols)
            self.subscribed_exchanges.add(exchange_id)
            
            self.logger.info(f"✅ {exchange_id} ticker订阅成功: {len(symbols)} 个交易对")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ {exchange_id} ticker订阅失败: {e}")
            return False
    
    async def subscribe_orderbook(self, exchange_id: str, symbols: List[str]) -> bool:
        """订阅单个交易所的orderbook数据"""
        try:
            # 从ExchangeManager获取适配器
            connected_adapters = self.exchange_manager.get_connected_adapters()
            
            if exchange_id not in connected_adapters:
                self.logger.error(f"未找到交易所: {exchange_id}")
                return False
            
            adapter = connected_adapters[exchange_id]
            
            # 创建orderbook回调
            async def orderbook_callback(symbol: str, orderbook_data: OrderBookData):
                await self._handle_orderbook_data(exchange_id, symbol, orderbook_data)
            
            # 批量订阅
            if hasattr(adapter, 'batch_subscribe_orderbooks'):
                await adapter.batch_subscribe_orderbooks(symbols, callback=orderbook_callback)
            else:
                # 逐个订阅
                for symbol in symbols:
                    async def create_callback(sym):
                        async def callback(data):
                            await orderbook_callback(sym, data)
                        return callback
                    
                    callback = await create_callback(symbol)
                    await adapter.subscribe_orderbook(symbol, callback)
            
            # 更新订阅状态
            self.subscribed_symbols.update(symbols)
            self.subscribed_exchanges.add(exchange_id)
            
            self.logger.info(f"✅ {exchange_id} orderbook订阅成功: {len(symbols)} 个交易对")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ {exchange_id} orderbook订阅失败: {e}")
            return False
    
    async def unsubscribe_ticker(self, exchange_id: str, symbols: List[str]) -> bool:
        """取消订阅ticker数据"""
        try:
            # 从ExchangeManager获取适配器
            connected_adapters = self.exchange_manager.get_connected_adapters()
            
            if exchange_id not in connected_adapters:
                self.logger.error(f"未找到交易所: {exchange_id}")
                return False
            
            adapter = connected_adapters[exchange_id]
            
            # 取消订阅
            for symbol in symbols:
                if hasattr(adapter, 'unsubscribe'):
                    await adapter.unsubscribe(symbol)
            
            # 更新订阅状态
            self.subscribed_symbols.difference_update(symbols)
            
            self.logger.info(f"✅ {exchange_id} 取消ticker订阅: {len(symbols)} 个交易对")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ {exchange_id} 取消ticker订阅失败: {e}")
            return False
    
    async def unsubscribe_orderbook(self, exchange_id: str, symbols: List[str]) -> bool:
        """取消订阅orderbook数据"""
        try:
            # 从ExchangeManager获取适配器
            connected_adapters = self.exchange_manager.get_connected_adapters()
            
            if exchange_id not in connected_adapters:
                self.logger.error(f"未找到交易所: {exchange_id}")
                return False
            
            adapter = connected_adapters[exchange_id]
            
            # 取消订阅
            for symbol in symbols:
                if hasattr(adapter, 'unsubscribe'):
                    await adapter.unsubscribe(symbol)
            
            # 更新订阅状态
            self.subscribed_symbols.difference_update(symbols)
            
            self.logger.info(f"✅ {exchange_id} 取消orderbook订阅: {len(symbols)} 个交易对")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ {exchange_id} 取消orderbook订阅失败: {e}")
            return False 
    
    async def _subscribe_exchange_data(self, exchange_name: str, adapter: ExchangeAdapter, 
                                     exchange_symbols: List[str], enabled_data_types: List[DataType], 
                                     results: Dict[str, Any]) -> bool:
        """为单个交易所订阅所有数据类型"""
        try:
            success_count = 0
            total_count = len(exchange_symbols) * len(enabled_data_types)
            
            # 根据配置启动不同的数据类型监控
            for data_type in enabled_data_types:
                try:
                    if data_type == DataType.TICKER:
                        await self._start_ticker_monitoring(exchange_name, adapter, exchange_symbols)
                        success_count += len(exchange_symbols)
                        self.logger.info(f"✅ {exchange_name} ticker订阅成功: {len(exchange_symbols)} 个交易对")
                        
                        # 记录订阅状态
                        for symbol in exchange_symbols:
                            status = SubscriptionStatus(
                                exchange_id=exchange_name,
                                symbol=symbol,
                                data_type=data_type,
                                status="active"
                            )
                            results["subscription_summary"].update_from_status(status)
                    
                    elif data_type == DataType.ORDERBOOK:
                        await self._start_orderbook_monitoring(exchange_name, adapter, exchange_symbols)
                        success_count += len(exchange_symbols)
                        self.logger.info(f"✅ {exchange_name} orderbook订阅成功: {len(exchange_symbols)} 个交易对")
                        
                        # 记录订阅状态
                        for symbol in exchange_symbols:
                            status = SubscriptionStatus(
                                exchange_id=exchange_name,
                                symbol=symbol,
                                data_type=data_type,
                                status="active"
                            )
                            results["subscription_summary"].update_from_status(status)
                    
                    elif data_type == DataType.TRADES:
                        await self._start_trades_monitoring(exchange_name, adapter, exchange_symbols)
                        success_count += len(exchange_symbols)
                        self.logger.info(f"✅ {exchange_name} trades订阅成功: {len(exchange_symbols)} 个交易对")
                        
                        # 记录订阅状态
                        for symbol in exchange_symbols:
                            status = SubscriptionStatus(
                                exchange_id=exchange_name,
                                symbol=symbol,
                                data_type=data_type,
                                status="active"
                            )
                            results["subscription_summary"].update_from_status(status)
                    
                    elif data_type == DataType.USER_DATA:
                        await self._start_user_data_monitoring(exchange_name, adapter)
                        success_count += 1
                        self.logger.info(f"✅ {exchange_name} user_data订阅成功")
                        
                        # 记录订阅状态
                        status = SubscriptionStatus(
                            exchange_id=exchange_name,
                            symbol="",  # user_data不需要symbol
                            data_type=data_type,
                            status="active"
                        )
                        results["subscription_summary"].update_from_status(status)
                    
                except Exception as e:
                    self.logger.error(f"❌ {exchange_name} {data_type.value}订阅失败: {e}")
                    
                    # 记录失败状态
                    for symbol in exchange_symbols:
                        status = SubscriptionStatus(
                            exchange_id=exchange_name,
                            symbol=symbol,
                            data_type=data_type,
                            status="error",
                            error_message=str(e)
                        )
                        results["subscription_summary"].update_from_status(status)
            
            # 更新订阅状态
            if success_count > 0:
                self.subscribed_symbols.update(exchange_symbols)
                self.subscribed_exchanges.add(exchange_name)
            
            # 更新统计
            results["successful_subscriptions"] += success_count
            results["failed_subscriptions"] += (total_count - success_count)
            
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"❌ {exchange_name} 订阅数据失败: {e}")
            return False
    
    async def _monitor_connection_status(self) -> None:
        """监控交易所连接状态变化，自动处理重连后的订阅"""
        monitor_interval = 10  # 每10秒检查一次
        
        while self.is_running:
            try:
                # 获取当前连接状态
                connected_adapters = self.exchange_manager.get_connected_adapters()
                
                # 检查是否有新连接的交易所
                for exchange_name in self.configured_exchanges:
                    if (exchange_name in connected_adapters and 
                        exchange_name not in self.subscribed_exchanges):
                        
                        # 发现新连接的交易所，自动订阅
                        self.logger.info(f"🔄 检测到 {exchange_name} 重新连接，开始自动订阅...")
                        
                        adapter = connected_adapters[exchange_name]
                        exchange_symbols = self.exchange_symbol_mapping.get(exchange_name, [])
                        
                        if not exchange_symbols:
                            self.logger.warning(f"⚠️ {exchange_name} 没有可订阅的交易对")
                            continue
                        
                        # 获取数据类型配置
                        if self.monitoring_config:
                            enabled_data_types = self.monitoring_config.get_enabled_types_for_exchange(exchange_name)
                        else:
                            enabled_data_types = [DataType.TICKER]
                        
                        # 创建临时结果对象
                        temp_results = {
                            "successful_subscriptions": 0,
                            "failed_subscriptions": 0,
                            "subscription_summary": SubscriptionSummary()
                        }
                        
                        # 尝试订阅
                        success = await self._subscribe_exchange_data(
                            exchange_name, adapter, exchange_symbols, enabled_data_types, temp_results
                        )
                        
                        if success:
                            self.logger.info(f"🎉 {exchange_name} 重连后自动订阅成功")
                        else:
                            self.logger.error(f"❌ {exchange_name} 重连后自动订阅失败")
                
                # 等待下次检查
                await asyncio.sleep(monitor_interval)
                
            except asyncio.CancelledError:
                self.logger.info("连接状态监控任务已取消")
                break
            except Exception as e:
                self.logger.error(f"连接状态监控任务异常: {e}")
                await asyncio.sleep(monitor_interval)
    
    def _is_perpetual_contract(self, exchange_name: str, symbol: str) -> bool:
        """🔥 新增：判断是否为永续合约"""
        try:
            symbol_upper = symbol.upper()
            
            # 根据不同交易所的符号格式判断
            if exchange_name.lower() == "hyperliquid":
                # Hyperliquid: BTC/USDC:PERP (永续) vs BTC/USDC:SPOT (现货)
                return ":PERP" in symbol_upper and ":SPOT" not in symbol_upper
            
            elif exchange_name.lower() == "backpack":
                # Backpack: SOL_USDC_PERP (永续) vs SOL_USDC (现货)
                return "_PERP" in symbol_upper or "PERP" in symbol_upper
            
            elif exchange_name.lower() == "edgex":
                # EdgeX: BTC_USDT_PERP (永续合约)
                return "_PERP" in symbol_upper
            
            elif exchange_name.lower() == "binance":
                # Binance: BTCUSDT (现货) vs BTCUSDT_PERP (永续，但实际可能是不同的格式)
                return "PERP" in symbol_upper or not symbol_upper.endswith("USDT")
            
            else:
                # 默认情况：包含PERP关键词的为永续合约
                return "PERP" in symbol_upper
                
        except Exception as e:
            self.logger.warning(f"⚠️ 判断永续合约失败 {exchange_name}:{symbol}: {e}")
            return False 