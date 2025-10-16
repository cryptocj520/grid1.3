"""
增强监控服务实现

使用依赖注入的ExchangeManager，专注于监控服务功能
"""

import asyncio
import time
import json
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from collections import defaultdict
from injector import inject, singleton

from ..interfaces.monitoring_service import (
    MonitoringService, MonitoringStats, MonitoringConfig, 
    SubscriptionStrategy, ExchangeSubscriptionConfig
)
from ..interfaces.config_service import IConfigurationService
from ..symbol_manager.interfaces.symbol_conversion_service import ISymbolConversionService
from ...domain.models import ExchangeData, PriceData, SpreadData, ExchangeStatus
from ...adapters.exchanges.manager import ExchangeManager
from ...data_aggregator import DataAggregator


@singleton
class EnhancedMonitoringServiceImpl(MonitoringService):
    """增强监控服务实现 - 依赖注入版本，使用ExchangeManager和DataAggregator"""
    
    @inject
    def __init__(self, 
                 exchange_manager: ExchangeManager,
                 data_aggregator: DataAggregator,
                 config_service: IConfigurationService,
                 symbol_conversion_service: ISymbolConversionService):
        # 使用简化的统一日志入口
        from ...logging import get_system_logger
        self.logger = get_system_logger()
        self.exchange_manager = exchange_manager
        self.data_aggregator = data_aggregator
        self.config_service = config_service
        self.symbol_conversion_service = symbol_conversion_service
        self.config = MonitoringConfig()
        
        # 核心状态
        self.running = False
        self.start_time = None
        
        # 统计信息
        self.stats = MonitoringStats()
        self.stats.exchange_messages = defaultdict(int)
        
        # 数据更新回调
        self.update_callbacks: List[Callable[[Dict[str, Any]], None]] = []
        
        # SocketIO服务器
        self.socketio_server = None
        
        # 监控任务
        self.monitoring_tasks = []
        
        # 订阅状态跟踪
        self.subscription_status = {}
        
        # 初始化默认订阅配置
        self._initialize_default_config()
    
    def _initialize_default_config(self):
        """初始化默认订阅配置"""
        # 为每个交易所设置默认配置
        for exchange_id in self.config.exchanges.keys():
            if exchange_id not in self.config.exchange_configs:
                # 根据交易所特性设置默认策略
                if exchange_id == 'backpack':
                    default_strategy = SubscriptionStrategy.BOTH
                elif exchange_id == 'hyperliquid':
                    default_strategy = SubscriptionStrategy.TICKER_ONLY
                else:
                    default_strategy = SubscriptionStrategy.TICKER_ONLY
                
                self.config.exchange_configs[exchange_id] = ExchangeSubscriptionConfig(
                    exchange_id=exchange_id,
                    strategy=default_strategy,
                    enabled=True
                )
    
    async def start(self) -> bool:
        """启动监控服务"""
        try:
            self.logger.info("🚀 启动增强监控服务...")
            self.start_time = time.time()
            
            # 初始化配置服务
            self.logger.info("🔧 初始化配置服务...")
            if not await self.config_service.initialize():
                self.logger.error("❌ 配置服务初始化失败")
                return False
            
            # 初始化并启动交易所管理器
            self.logger.info("🔌 初始化交易所管理器...")
            if not await self._initialize_exchange_manager():
                self.logger.error("❌ 交易所管理器初始化失败")
                return False
            
            # 启动配置驱动的监控
            self.logger.info("📊 启动配置驱动的监控...")
            await self._start_configured_monitoring()
            
            # 启动SocketIO服务器
            self.logger.info("🌐 启动SocketIO服务器...")
            try:
                await self._start_socketio_server()
                self.logger.info("✅ SocketIO服务器启动成功")
            except Exception as e:
                self.logger.error(f"❌ SocketIO服务器启动异常: {e}", exc_info=True)
                self.socketio_server = None
            
            # 启动监控任务
            self.logger.info("🔄 启动监控任务...")
            await self._start_monitoring_tasks()
            
            self.running = True
            self.logger.info("✅ 增强监控服务启动成功")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 增强监控服务启动失败: {e}", exc_info=True)
            return False
    
    async def stop(self) -> None:
        """停止监控服务"""
        if not self.running:
            return
            
        self.logger.info("🛑 停止增强监控服务...")
        self.running = False
        
        try:
            # 停止监控任务
            await self._stop_monitoring_tasks()
            
            # 停止SocketIO服务器
            await self._stop_socketio_server()
            
            # 停止数据聚合器
            if self.data_aggregator.is_running:
                await self.data_aggregator.stop()
            
            # 停止交易所管理器
            if self.exchange_manager.is_running():
                await self.exchange_manager.stop()
                self.logger.info("✅ 交易所管理器已停止")
            
            self.logger.info("✅ 增强监控服务已停止")
            
        except Exception as e:
            self.logger.error(f"❌ 停止增强监控服务失败: {e}", exc_info=True)
    
    async def get_stats(self) -> MonitoringStats:
        """获取监控统计信息"""
        if self.start_time:
            self.stats.uptime = time.time() - self.start_time
        
        # 从交易所管理器获取连接的交易所数量
        if self.exchange_manager:
            connected_adapters = self.exchange_manager.get_connected_adapters()
            self.stats.connected_exchanges = len(connected_adapters)
        else:
            self.stats.connected_exchanges = 0
        
        # 从数据聚合器获取统计
        aggregator_stats = self.data_aggregator.get_statistics()
        self.stats.total_messages = aggregator_stats.get('ticker_data_count', 0) + aggregator_stats.get('orderbook_data_count', 0)
        
        return self.stats
    
    async def get_price_data(self) -> Dict[str, PriceData]:
        """获取价格数据 - 从DataAggregator获取"""
        # 从DataAggregator获取ticker数据
        ticker_data = self.data_aggregator.get_ticker_data()
        price_data = {}
        
        # DataAggregator返回的格式是: {symbol: {exchange: TickerData}}
        for symbol, exchange_data in ticker_data.items():
            if isinstance(exchange_data, dict):
                for exchange_id, ticker_obj in exchange_data.items():
                    if ticker_obj and hasattr(ticker_obj, 'last'):
                        key = f"{exchange_id}_{symbol}"
                        price_data[key] = PriceData(
                            symbol=symbol,
                            exchange=exchange_id,
                            price=float(ticker_obj.last or 0),
                            volume=float(ticker_obj.volume or 0),
                            timestamp=datetime.now(),
                            last_update=datetime.now()
                        )
        
        return price_data
    
    async def get_spread_data(self) -> Dict[str, SpreadData]:
        """获取价差数据 - 基于DataAggregator的数据计算，使用统一符号转换服务"""
        price_data = await self.get_price_data()
        
        # 🔥 重构：使用统一的符号转换服务进行标准化
        symbols_data = defaultdict(dict)
        for key, data in price_data.items():
            try:
                # 将交易所格式转换为系统标准格式
                normalized_symbol = await self.symbol_conversion_service.convert_from_exchange_format(
                    data.symbol, data.exchange
                )
                symbols_data[normalized_symbol][data.exchange] = data
            except Exception as e:
                self.logger.warning(f"符号转换失败 {data.symbol} ({data.exchange}): {e}")
                # 转换失败时使用原始符号
                symbols_data[data.symbol][data.exchange] = data
        
        # 计算价差
        spreads = {}
        for symbol, exchanges in symbols_data.items():
            if len(exchanges) >= 2:
                exchange_pairs = list(exchanges.keys())
                for i in range(len(exchange_pairs)):
                    for j in range(i + 1, len(exchange_pairs)):
                        exchange1 = exchange_pairs[i]
                        exchange2 = exchange_pairs[j]
                        
                        data1 = exchanges[exchange1]
                        data2 = exchanges[exchange2]
                        
                        if data1.price > 0 and data2.price > 0:
                            spread = data1.price - data2.price
                            spread_pct = (spread / data2.price) * 100
                            
                            spreads[f"{symbol}_{exchange1}_{exchange2}"] = SpreadData(
                                symbol=symbol,
                                exchange1=exchange1,
                                exchange2=exchange2,
                                price1=data1.price,
                                price2=data2.price,
                                spread=spread,
                                spread_pct=spread_pct,
                                volume1=data1.volume,
                                volume2=data2.volume,
                                timestamp=datetime.now()
                            )
        
        return spreads
    
    async def subscribe_updates(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """订阅数据更新"""
        self.update_callbacks.append(callback)
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        # 获取交易所管理器健康状态
        exchange_health = {}
        if self.exchange_manager:
            exchange_health = await self.exchange_manager.health_check_all()
        
        # 获取数据聚合器统计
        aggregator_stats = self.data_aggregator.get_statistics()
        
        return {
            "status": "healthy" if self.running else "stopped",
            "uptime": time.time() - self.start_time if self.start_time else 0,
            "subscribed_symbols": len(aggregator_stats.get('subscribed_symbols', [])),
            "price_data_count": aggregator_stats.get('ticker_data_count', 0),
            "message_count": self.stats.total_messages,
            "error_count": self.stats.errors,
            "exchange_health": exchange_health,
            "data_aggregator_running": self.data_aggregator.is_running
        }
    
    # === 订阅方法实现 ===
    
    async def subscribe_ticker(self, exchange_id: str, symbols: List[str]) -> bool:
        """订阅ticker数据"""
        try:
            # 委托给数据聚合器
            await self.data_aggregator.subscribe_ticker(exchange_id, symbols)
            self.logger.info(f"📊 {exchange_id} Ticker订阅成功: {len(symbols)} 个交易对")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 订阅ticker失败 ({exchange_id}): {e}")
            return False
    
    async def subscribe_orderbook(self, exchange_id: str, symbols: List[str]) -> bool:
        """订阅orderbook数据"""
        try:
            # 委托给数据聚合器
            await self.data_aggregator.subscribe_orderbook(exchange_id, symbols)
            self.logger.info(f"📊 {exchange_id} Orderbook订阅成功: {len(symbols)} 个交易对")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 订阅orderbook失败 ({exchange_id}): {e}")
            return False
    
    async def unsubscribe_ticker(self, exchange_id: str, symbols: List[str]) -> bool:
        """取消订阅ticker数据"""
        try:
            # 委托给数据聚合器
            await self.data_aggregator.unsubscribe_ticker(exchange_id, symbols)
            self.logger.info(f"📊 {exchange_id} 取消Ticker订阅: {symbols}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 取消ticker订阅失败 ({exchange_id}): {e}")
            return False
    
    async def unsubscribe_orderbook(self, exchange_id: str, symbols: List[str]) -> bool:
        """取消订阅orderbook数据"""
        try:
            # 委托给数据聚合器
            await self.data_aggregator.unsubscribe_orderbook(exchange_id, symbols)
            self.logger.info(f"📊 {exchange_id} 取消Orderbook订阅: {symbols}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 取消orderbook订阅失败 ({exchange_id}): {e}")
            return False
    
    async def configure_exchange_subscription(self, config: ExchangeSubscriptionConfig) -> bool:
        """配置交易所订阅策略"""
        try:
            self.config.exchange_configs[config.exchange_id] = config
            self.logger.info(f"📊 {config.exchange_id} 订阅策略已配置: {config.strategy.value}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 配置订阅策略失败 ({config.exchange_id}): {e}")
            return False
    
    async def get_subscription_status(self) -> Dict[str, Dict[str, Any]]:
        """获取订阅状态"""
        # 从数据聚合器获取订阅状态
        aggregator_stats = self.data_aggregator.get_statistics()
        
        # 转换为接口期望的格式
        status = {}
        for exchange_id in aggregator_stats.get('exchanges', []):
            status[exchange_id] = {
                "strategy": self.config.exchange_configs.get(exchange_id, {}).strategy.value if exchange_id in self.config.exchange_configs else "ticker_only",
                "ticker_symbols": [],  # 从数据聚合器获取
                "orderbook_symbols": [],  # 从数据聚合器获取
                "total_subscriptions": 0  # 从数据聚合器获取
            }
        
        return status
    
    # === 私有方法 ===
    
    async def _start_socketio_server(self) -> None:
        """启动SocketIO服务器"""
        try:
            import socketio
            from aiohttp import web
            
            # 创建SocketIO服务器
            sio = socketio.AsyncServer(
                cors_allowed_origins="*",
                ping_timeout=60,
                ping_interval=25,
                logger=False,
                engineio_logger=False
            )
            
            # 事件处理器
            @sio.event
            async def connect(sid, environ):
                self.logger.debug(f"🔗 客户端连接: {sid}")
                
                # 🔥 修复：发送完整的数据快照，包含所有类型的数据
                snapshot_data = {
                    'price_data': {},      # 兼容旧版本
                    'ticker_data': {},     # ticker数据
                    'orderbook_data': {},  # orderbook数据
                    'trades_data': {},     # trades数据
                    'user_data': {}        # user_data数据
                }
                
                # 添加ticker数据（从price_data获取）
                price_data = await self.get_price_data()
                for key, data in price_data.items():
                    # 兼容格式
                    snapshot_data['price_data'][key] = {
                        'symbol': data.symbol,
                        'exchange': data.exchange,
                        'price': data.price,
                        'volume': data.volume,
                        'timestamp': data.timestamp.isoformat(),
                        'last_update': data.last_update.isoformat()
                    }
                    
                    # 新格式
                    snapshot_data['ticker_data'][key] = {
                        'symbol': data.symbol,
                        'exchange': data.exchange,
                        'price': data.price,
                        'volume': data.volume,
                        'timestamp': data.timestamp.isoformat(),
                        'last_update': data.last_update.isoformat()
                    }
                
                # 🔥 新增：添加orderbook数据
                try:
                    # 从数据聚合器获取orderbook数据
                    orderbook_data = self.data_aggregator.get_orderbook_data()
                    for symbol, exchange_data in orderbook_data.items():
                        if isinstance(exchange_data, dict):
                            for exchange_id, orderbook_obj in exchange_data.items():
                                if orderbook_obj and hasattr(orderbook_obj, 'bids') and hasattr(orderbook_obj, 'asks'):
                                    key = f"{exchange_id}_{symbol}"
                                    snapshot_data['orderbook_data'][key] = {
                                        'symbol': symbol,
                                        'exchange': exchange_id,
                                        'bids': [[float(level.price), float(level.size)] for level in orderbook_obj.bids[:5]],
                                        'asks': [[float(level.price), float(level.size)] for level in orderbook_obj.asks[:5]],
                                        'timestamp': datetime.now().isoformat(),
                                        'last_update': datetime.now().isoformat()
                                    }
                except Exception as e:
                    self.logger.warning(f"⚠️ 获取orderbook快照数据失败: {e}")
                
                # 🔥 新增：添加trades数据
                try:
                    # 从数据聚合器获取trades数据
                    trades_data = self.data_aggregator.get_trades_data()
                    for symbol, exchange_data in trades_data.items():
                        if isinstance(exchange_data, dict):
                            for exchange_id, trades_list in exchange_data.items():
                                if trades_list and len(trades_list) > 0:
                                    key = f"{exchange_id}_{symbol}"
                                    latest_trade = trades_list[-1]  # 获取最新交易
                                    snapshot_data['trades_data'][key] = {
                                        'symbol': symbol,
                                        'exchange': exchange_id,
                                        'price': float(latest_trade.price or 0),
                                        'quantity': float(latest_trade.quantity or 0),
                                        'side': latest_trade.side.value if latest_trade.side else 'unknown',
                                        'timestamp': latest_trade.timestamp.isoformat() if latest_trade.timestamp else datetime.now().isoformat(),
                                        'last_update': datetime.now().isoformat()
                                    }
                except Exception as e:
                    self.logger.warning(f"⚠️ 获取trades快照数据失败: {e}")
                
                # 添加统计信息
                snapshot_data['stats'] = {
                    'total_messages': self.stats.total_messages,
                    'exchange_messages': dict(self.stats.exchange_messages)
                }
                
                await sio.emit('data_snapshot', snapshot_data, room=sid)
                
                # 🔥 更新：记录发送的数据快照统计（包含所有数据类型）
                total_data_points = (
                    len(snapshot_data['price_data']) + 
                    len(snapshot_data['ticker_data']) + 
                    len(snapshot_data['orderbook_data']) + 
                    len(snapshot_data['trades_data']) + 
                    len(snapshot_data['user_data'])
                )
                self.logger.info(f"📊 发送数据快照给客户端 {sid}: {total_data_points} 个数据点 "
                               f"(价格:{len(snapshot_data['price_data'])}, "
                               f"ticker:{len(snapshot_data['ticker_data'])}, "
                               f"orderbook:{len(snapshot_data['orderbook_data'])}, "
                               f"trades:{len(snapshot_data['trades_data'])}, "
                               f"user:{len(snapshot_data['user_data'])})")
            
            @sio.event
            async def disconnect(sid):
                self.logger.debug(f"🔌 客户端断开: {sid}")
            
            @sio.event
            async def subscribe(sid, data):
                self.logger.debug(f"📡 订阅请求: {sid} - {data}")
                stats = self.data_aggregator.get_statistics()
                await sio.emit('subscription_success', {
                    'subscribed_symbols': stats.get('subscribed_symbols', []),
                    'price_data_count': stats.get('ticker_data_count', 0)
                }, room=sid)
            
            # 创建应用
            app = web.Application()
            sio.attach(app)
            
            # 健康检查端点
            async def health_check(request):
                health_data = await self.health_check()
                return web.json_response(health_data)
            
            app.router.add_get('/health', health_check)
            
            # 启动服务器
            from aiohttp.web import AppRunner, TCPSite
            runner = AppRunner(app)
            await runner.setup()
            
            site = TCPSite(runner, "0.0.0.0", self.config.socketio_port)
            await site.start()
            
            # 保存引用
            self.socketio_server = runner
            self.socketio_site = site
            self.sio = sio
            
            self.logger.info(f"🌐 SocketIO服务器启动: 端口 {self.config.socketio_port}")
            
        except Exception as e:
            self.logger.error(f"❌ SocketIO服务器启动失败: {e}")
            raise
    
    async def _start_monitoring_tasks(self) -> None:
        """启动监控任务"""
        # 启动统计任务
        self.monitoring_tasks.append(
            asyncio.create_task(self._stats_loop())
        )
        
        # 启动价差更新任务
        self.monitoring_tasks.append(
            asyncio.create_task(self._spread_update_loop())
        )
    
    async def _stop_monitoring_tasks(self) -> None:
        """停止监控任务"""
        for task in self.monitoring_tasks:
            task.cancel()
        
        await asyncio.gather(*self.monitoring_tasks, return_exceptions=True)
        self.monitoring_tasks.clear()
    
    async def _stop_socketio_server(self) -> None:
        """停止SocketIO服务器"""
        try:
            if hasattr(self, 'socketio_site') and self.socketio_site:
                await self.socketio_site.stop()
                
            if hasattr(self, 'socketio_server') and self.socketio_server:
                await self.socketio_server.cleanup()
                
            self.logger.info("🛑 SocketIO服务器已停止")
            
        except Exception as e:
            self.logger.error(f"❌ 停止SocketIO服务器失败: {e}")
    
    async def _stats_loop(self) -> None:
        """统计循环"""
        while self.running:
            try:
                await asyncio.sleep(10)
                
                if self.stats.total_messages > 0 and self.stats.total_messages % 100 == 0:
                    price_data = await self.get_price_data()
                    self.logger.info(
                        f"📊 监控统计 - 总消息: {self.stats.total_messages}, "
                        f"价格数据: {len(price_data)}"
                    )
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"❌ 统计循环错误: {e}")
    
    async def _spread_update_loop(self) -> None:
        """价差更新循环"""
        while self.running:
            try:
                await asyncio.sleep(5)  # 每5秒更新一次价差
                
                # 计算并发送价差数据
                spreads = await self.get_spread_data()
                
                if spreads and hasattr(self, 'sio') and self.sio:
                    await self.sio.emit('spread_update', {
                        'spreads': {
                            key: {
                                'symbol': spread.symbol,
                                'exchange1': spread.exchange1,
                                'exchange2': spread.exchange2,
                                'price1': spread.price1,
                                'price2': spread.price2,
                                'spread': spread.spread,
                                'spread_pct': spread.spread_pct,
                                'volume1': spread.volume1,
                                'volume2': spread.volume2,
                                'timestamp': spread.timestamp.isoformat()
                            }
                            for key, spread in spreads.items()
                        },
                        'timestamp': datetime.now().isoformat()
                    })
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"❌ 价差更新循环错误: {e}") 
    
    async def _start_configured_monitoring(self) -> None:
        """启动配置驱动的监控"""
        try:
            # 🔥 修改：统一使用配置服务（现在内部使用ConfigManager）
            
            # 获取启用的交易所
            enabled_exchanges = await self.config_service.get_enabled_exchanges()
            self.logger.info(f"📊 启用的交易所: {enabled_exchanges}")
            
            if not enabled_exchanges:
                self.logger.warning("⚠️ 没有启用的交易所")
                return
            
            # 获取数据类型配置
            from ...domain.models import DataType
            monitoring_config = await self.config_service.get_monitoring_data_type_config()
            
            # 记录每个交易所的数据类型配置
            for exchange_id in enabled_exchanges:
                enabled_types = monitoring_config.get_enabled_types_for_exchange(exchange_id)
                self.logger.info(f"📊 {exchange_id} 启用的数据类型: {[dt.value for dt in enabled_types]}")
            
            # 🔥 修改：统一使用配置服务启动数据聚合器
            if not self.data_aggregator.is_running:
                self.logger.info("📊 使用统一配置服务启动数据聚合器...")
                result = await self.data_aggregator.start_configured_monitoring(self.config_service)
                
                self.logger.info(f"📊 数据聚合器启动结果: {result.get('status', 'unknown')}")
                
                # 记录订阅摘要
                if 'subscription_summary' in result:
                    summary = result['subscription_summary']
                    self.logger.info(f"📊 订阅摘要: 总计{summary.total_subscriptions}, 活跃{summary.active_subscriptions}, 错误{summary.error_subscriptions}")
            
            # 注册数据回调，确保接收到的数据能够推送到SocketIO
            self.data_aggregator.register_data_callback(
                DataType.TICKER, 
                self._handle_ticker_data_callback
            )
            self.data_aggregator.register_data_callback(
                DataType.ORDERBOOK, 
                self._handle_orderbook_data_callback
            )
            self.data_aggregator.register_data_callback(
                DataType.TRADES, 
                self._handle_trades_data_callback
            )
            self.data_aggregator.register_data_callback(
                DataType.USER_DATA, 
                self._handle_user_data_callback
            )
            
            self.logger.info("✅ 配置驱动的监控启动完成")
            
        except Exception as e:
            self.logger.error(f"❌ 启动配置驱动监控失败: {e}")
            raise
    
    # 🔥 新增：数据回调处理方法
    async def _handle_ticker_data_callback(self, aggregated_data) -> None:
        """处理ticker数据回调"""
        try:
            # 更新消息统计
            self.stats.total_messages += 1
            self.stats.exchange_messages[aggregated_data.exchange] += 1
            
            # 推送到SocketIO客户端
            if hasattr(self, 'sio') and self.sio:
                ticker_data = {
                    'symbol': aggregated_data.symbol,
                    'exchange': aggregated_data.exchange,
                    'price': float(aggregated_data.data.last or 0),
                    'volume': float(aggregated_data.data.volume or 0),
                    'timestamp': aggregated_data.timestamp.isoformat(),
                    'last_update': datetime.now().isoformat()
                }
                
                # 🔥 修复：使用终端监控工具期望的事件名称
                await self.sio.emit('batch_update', {
                    'ticker_data': {
                        f"{aggregated_data.exchange}_{aggregated_data.symbol}": ticker_data
                    },
                    'timestamp': datetime.now().isoformat()
                })
            
            # 调用用户回调
            for callback in self.update_callbacks:
                await self._safe_callback(callback, {
                    'type': 'ticker',
                    'data': aggregated_data
                })
                
        except Exception as e:
            self.logger.error(f"❌ 处理ticker数据回调失败: {e}")
    
    async def _handle_orderbook_data_callback(self, aggregated_data) -> None:
        """处理orderbook数据回调"""
        try:
            # 更新消息统计
            self.stats.total_messages += 1
            self.stats.exchange_messages[aggregated_data.exchange] += 1
            
            # 推送到SocketIO客户端
            if hasattr(self, 'sio') and self.sio:
                orderbook_data = {
                    'symbol': aggregated_data.symbol,
                    'exchange': aggregated_data.exchange,
                    'bids': [[float(level.price), float(level.size)] for level in aggregated_data.data.bids[:5]],
                    'asks': [[float(level.price), float(level.size)] for level in aggregated_data.data.asks[:5]],
                    'timestamp': aggregated_data.timestamp.isoformat(),
                    'last_update': datetime.now().isoformat()
                }
                
                # 🔥 修复：使用终端监控工具期望的事件名称
                await self.sio.emit('batch_update', {
                    'orderbook_data': {
                        f"{aggregated_data.exchange}_{aggregated_data.symbol}": orderbook_data
                    },
                    'timestamp': datetime.now().isoformat()
                })
            
            # 调用用户回调
            for callback in self.update_callbacks:
                await self._safe_callback(callback, {
                    'type': 'orderbook',
                    'data': aggregated_data
                })
                
        except Exception as e:
            self.logger.error(f"❌ 处理orderbook数据回调失败: {e}")
    
    async def _handle_trades_data_callback(self, aggregated_data) -> None:
        """处理trades数据回调"""
        try:
            # 更新消息统计
            self.stats.total_messages += 1
            self.stats.exchange_messages[aggregated_data.exchange] += 1
            
            # 推送到SocketIO客户端
            if hasattr(self, 'sio') and self.sio:
                trades_data = {
                    'symbol': aggregated_data.symbol,
                    'exchange': aggregated_data.exchange,
                    'price': float(aggregated_data.data.price or 0),
                    'quantity': float(aggregated_data.data.quantity or 0),
                    'side': aggregated_data.data.side.value if aggregated_data.data.side else 'unknown',
                    'timestamp': aggregated_data.timestamp.isoformat(),
                    'last_update': datetime.now().isoformat()
                }
                
                # 使用终端监控工具期望的事件名称
                await self.sio.emit('batch_update', {
                    'trades_data': {
                        f"{aggregated_data.exchange}_{aggregated_data.symbol}": trades_data
                    },
                    'timestamp': datetime.now().isoformat()
                })
            
            # 调用用户回调
            for callback in self.update_callbacks:
                await self._safe_callback(callback, {
                    'type': 'trades',
                    'data': aggregated_data
                })
                
        except Exception as e:
            self.logger.error(f"❌ 处理trades数据回调失败: {e}")
    
    async def _handle_user_data_callback(self, aggregated_data) -> None:
        """处理user_data数据回调"""
        try:
            # 更新消息统计
            self.stats.total_messages += 1
            self.stats.exchange_messages[aggregated_data.exchange] += 1
            
            # 推送到SocketIO客户端
            if hasattr(self, 'sio') and self.sio:
                user_data = {
                    'exchange': aggregated_data.exchange,
                    'data': aggregated_data.data,
                    'timestamp': aggregated_data.timestamp.isoformat(),
                    'last_update': datetime.now().isoformat()
                }
                
                # 使用终端监控工具期望的事件名称
                await self.sio.emit('batch_update', {
                    'user_data': {
                        f"{aggregated_data.exchange}": user_data
                    },
                    'timestamp': datetime.now().isoformat()
                })
            
            # 调用用户回调
            for callback in self.update_callbacks:
                await self._safe_callback(callback, {
                    'type': 'user_data',
                    'data': aggregated_data
                })
                
        except Exception as e:
            self.logger.error(f"❌ 处理user_data数据回调失败: {e}")
    
    async def _safe_callback(self, callback: Callable, data: Any) -> None:
        """安全调用回调函数"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            self.logger.error(f"❌ 回调函数执行失败: {e}")
    
    async def _initialize_exchange_manager(self) -> bool:
        """初始化并启动交易所管理器"""
        try:
            # 🔥 修复：在获取启用交易所之前，确保配置服务完全初始化
            if not hasattr(self.config_service, 'config_manager') or not self.config_service.config_manager:
                self.logger.warning("⚠️ 配置服务未完全初始化，重新初始化...")
                await self.config_service.initialize()
            
            # 获取启用的交易所
            enabled_exchanges = await self.config_service.get_enabled_exchanges()
            self.logger.info(f"📊 启用的交易所: {enabled_exchanges}")
            
            if not enabled_exchanges:
                self.logger.warning("⚠️ 没有启用的交易所")
                return True
            
                            # 为每个启用的交易所创建配置并注册
            for exchange_id in enabled_exchanges:
                try:
                    # 获取交易所配置
                    exchange_config = await self.config_service.get_exchange_config(exchange_id)
                    if not exchange_config:
                        self.logger.warning(f"⚠️ {exchange_id} 配置不存在，跳过")
                        continue
                    
                    # 创建ExchangeConfig对象
                    from ...adapters.exchanges.interface import ExchangeConfig
                    from ...adapters.exchanges.models import ExchangeType
                    
                    # 🔥 修复：从配置文件获取认证信息
                    api_key = ""
                    api_secret = ""
                    wallet_address = ""
                    
                    # 尝试从配置管理器获取认证信息
                    if hasattr(self.config_service, 'config_manager') and self.config_service.config_manager:
                        try:
                            # 直接从配置文件获取认证信息
                            raw_config = self.config_service.config_manager.load_exchange_config(exchange_id)
                            if raw_config and hasattr(raw_config, 'exchange_info') and raw_config.exchange_info:
                                auth_info = raw_config.exchange_info.get('authentication', {})
                                if auth_info:
                                    api_key = auth_info.get('private_key', '')
                                    api_secret = auth_info.get('api_secret', '')
                                    wallet_address = auth_info.get('wallet_address', '')
                                    
                                    if api_key:
                                        self.logger.info(f"🔑 {exchange_id} 使用认证模式")
                                    else:
                                        self.logger.info(f"🔓 {exchange_id} 使用公共访问模式")
                                        
                        except Exception as e:
                            self.logger.warning(f"⚠️ 获取 {exchange_id} 认证信息失败: {e}")
                    
                    # 创建适配器配置
                    adapter_config = ExchangeConfig(
                        exchange_id=exchange_id,
                        name=exchange_config.name,
                        exchange_type=ExchangeType.PERPETUAL,  # 默认为永续合约
                        api_key=api_key,  # 🔥 修复：使用配置文件中的API密钥
                        api_secret=api_secret,
                        wallet_address=wallet_address,  # 🔥 修复：添加钱包地址
                        testnet=exchange_config.testnet,
                        base_url=exchange_config.base_url,
                        ws_url=exchange_config.ws_url,
                        enable_websocket=True,
                        enable_auto_reconnect=True,
                        connect_timeout=60,  # 🔥 修复：增加连接超时时间
                        request_timeout=15   # 🔥 修复：增加请求超时时间
                    )
                    
                    # 注册交易所适配器
                    self.exchange_manager.register_exchange(exchange_id, adapter_config)
                    self.logger.info(f"✅ {exchange_id} 已注册")
                    
                except Exception as e:
                    self.logger.error(f"❌ 注册 {exchange_id} 失败: {e}")
                    continue
            
            # 启动交易所管理器
            if not self.exchange_manager.is_running():
                await self.exchange_manager.start()
                self.logger.info("✅ 交易所管理器已启动")
            
            # 等待连接完成
            await asyncio.sleep(2)
            
            # 检查连接状态
            connected_adapters = self.exchange_manager.get_connected_adapters()
            self.logger.info(f"✅ 已连接的交易所: {list(connected_adapters.keys())}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 交易所管理器初始化失败: {e}", exc_info=True)
            return False
    
    # 🗑️ 已删除：原有的符号标准化方法已被统一的符号转换服务替代
    # def _normalize_symbol(self, symbol: str, exchange_id: str) -> str:
    #     """此方法已被统一的符号转换服务替代"""
    #     pass 