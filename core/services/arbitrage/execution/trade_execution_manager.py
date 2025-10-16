"""
统一交易执行管理器 - 重构版

负责统一所有交易所的REST API操作，标准化交易执行流程
重构：移除内部符号转换逻辑，使用统一的符号转换服务
"""

import asyncio
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal
from injector import inject

from core.logging import get_logger
from core.adapters.exchanges.interface import ExchangeInterface
from core.adapters.exchanges.models import (
    OrderData, OrderSide, OrderType, OrderStatus,
    PositionData, BalanceData, TickerData, OHLCVData,
    OrderBookData, TradeData, ExchangeInfo
)
from ..shared.models import (
    TradePlan, OrderInfo, ExecutionResult, PrecisionInfo, 
    adjust_precision, OrderType as ArbitrageOrderType
)
from ..initialization.precision_manager import PrecisionManager
from .exchange_registry import ExchangeRegistry
from core.services.symbol_manager.interfaces.symbol_conversion_service import ISymbolConversionService


class TradeExecutionManager:
    """
    统一交易执行管理器 - 重构版
    
    重构说明：
    1. 移除内部符号转换逻辑
    2. 使用统一的符号转换服务
    3. 保持其他功能不变
    
    功能范围：
    1. 交易执行：订单创建、取消、查询
    2. 市场数据：行情、订单簿、成交记录、K线
    3. 账户管理：余额、持仓信息
    4. 交易设置：杠杆、保证金模式
    5. 系统管理：健康检查、连接管理
    """
    
    @inject
    def __init__(
        self, 
        exchange_adapters: Dict[str, ExchangeInterface],
        precision_manager: PrecisionManager,
        symbol_conversion_service: ISymbolConversionService,
        config: Dict[str, Any] = None
    ):
        """
        初始化交易执行管理器
        
        Args:
            exchange_adapters: 交易所适配器字典
            precision_manager: 精度管理器
            symbol_conversion_service: 符号转换服务
            config: 配置参数
        """
        self.precision_manager = precision_manager
        self.symbol_conversion_service = symbol_conversion_service
        self.exchange_registry = ExchangeRegistry(exchange_adapters)
        self.config = config or {}
        
        # 执行参数
        self.default_timeout = self.config.get('default_timeout', 30)
        self.max_retries = self.config.get('max_retries', 3)
        self.retry_delay = self.config.get('retry_delay', 1.0)
        
        # 活跃订单跟踪
        self.active_orders: Dict[str, OrderInfo] = {}
        
        # 统计信息
        self.execution_stats = {
            'total_orders': 0,
            'successful_orders': 0,
            'failed_orders': 0,
            'total_volume': Decimal('0')
        }
        
        self.logger = get_logger(__name__)
        
        self.logger.info(f"✅ 交易执行管理器重构完成，使用统一符号转换服务")
        
    # === 核心交易执行功能 ===
    
    async def execute_trade_plan(self, trade_plan: TradePlan) -> ExecutionResult:
        """
        执行交易计划 - 重构为批量下单方法
        
        Args:
            trade_plan: 交易计划
            
        Returns:
            执行结果
        """
        start_time = datetime.now()
        
        try:
            self.logger.info(f"开始执行交易计划: {trade_plan.plan_id}")
            
            # 验证交易计划
            if not await self._validate_trade_plan(trade_plan):
                return ExecutionResult(
                    plan_id=trade_plan.plan_id,
                    success=False,
                    error_message="交易计划验证失败"
                )
            
            # 重构：不再主观决定套利策略，而是根据交易计划执行具体订单
            orders = []
            
            # 检查是否是同一个交易所
            if trade_plan.long_exchange == trade_plan.short_exchange:
                # 单个交易所：只执行买单作为测试
                self.logger.info(f"📋 单个交易所测试: {trade_plan.long_exchange}")
                
                order = await self.create_order(
                    exchange=trade_plan.long_exchange,
                    symbol=trade_plan.symbol,
                    side='buy',
                    order_type='limit',
                    amount=trade_plan.quantity,
                    price=None  # 让create_order方法内部计算安全价格
                )
                
                if order:
                    orders.append(order)
                    self.logger.info(f"✅ 测试订单创建成功: {order.order_id}")
                else:
                    raise Exception("测试订单创建失败")
            else:
                # 不同交易所：执行外部策略指定的交易
                self.logger.info(f"📋 跨交易所交易: {trade_plan.long_exchange} -> {trade_plan.short_exchange}")
                
                # 创建买单
                buy_order = await self.create_order(
                    exchange=trade_plan.long_exchange,
                    symbol=trade_plan.symbol,
                    side='buy',
                    order_type='limit',
                    amount=trade_plan.quantity,
                    price=None
                )
                
                # 创建卖单
                sell_order = await self.create_order(
                    exchange=trade_plan.short_exchange,
                    symbol=trade_plan.symbol,
                    side='sell',
                    order_type='limit',
                    amount=trade_plan.quantity,
                    price=None
                )
                
                if buy_order and sell_order:
                    orders.extend([buy_order, sell_order])
                    self.logger.info(f"✅ 跨交易所订单创建成功: 买单={buy_order.order_id}, 卖单={sell_order.order_id}")
                else:
                    raise Exception("跨交易所订单创建失败")
            
            # 计算执行时间
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # 更新统计信息
            orders_count = len(orders)
            self.execution_stats['total_orders'] += orders_count
            self.execution_stats['successful_orders'] += orders_count
            self.execution_stats['total_volume'] += trade_plan.quantity * orders_count
            
            result = ExecutionResult(
                plan_id=trade_plan.plan_id,
                success=True,
                long_order=orders[0] if orders else None,
                short_order=orders[1] if len(orders) > 1 else None,
                actual_profit=Decimal('0'),  # 不再计算利润，交由外部策略处理
                execution_time=execution_time
            )
            
            self.logger.info(f"交易计划执行完成: {trade_plan.plan_id}, 订单数: {orders_count}")
            return result
            
        except Exception as e:
            self.logger.error(f"交易计划执行失败: {trade_plan.plan_id} - {e}")
            
            # 根据预期的订单数量更新统计
            expected_orders = 1 if trade_plan.long_exchange == trade_plan.short_exchange else 2
            self.execution_stats['total_orders'] += expected_orders
            self.execution_stats['failed_orders'] += expected_orders
            
            return ExecutionResult(
                plan_id=trade_plan.plan_id,
                success=False,
                error_message=str(e)
            )
    
    # === 新增：市场数据功能 ===
    
    async def get_ticker(self, exchange: str, symbol: str) -> Optional[TickerData]:
        """
        获取单个交易对行情
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            
        Returns:
            行情数据
        """
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return None
            
            ticker = await adapter.get_ticker(symbol)
            return ticker
            
        except Exception as e:
            self.logger.error(f"获取行情失败: {exchange} {symbol} - {e}")
            return None
    
    async def get_tickers(self, exchange: str, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """
        获取多个交易对行情
        
        Args:
            exchange: 交易所名称
            symbols: 交易对符号列表，None表示获取所有
            
        Returns:
            行情数据列表
        """
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return []
            
            tickers = await adapter.get_tickers(symbols)
            return tickers
            
        except Exception as e:
            self.logger.error(f"获取行情列表失败: {exchange} - {e}")
            return []
    
    async def get_orderbook(self, exchange: str, symbol: str, limit: Optional[int] = None) -> Optional[OrderBookData]:
        """
        获取订单簿
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            limit: 深度限制
            
        Returns:
            订单簿数据
        """
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return None
            
            orderbook = await adapter.get_orderbook(symbol, limit)
            return orderbook
            
        except Exception as e:
            self.logger.error(f"获取订单簿失败: {exchange} {symbol} - {e}")
            return None
    
    async def get_ohlcv(
        self, 
        exchange: str, 
        symbol: str, 
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OHLCVData]:
        """
        获取K线数据
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            timeframe: 时间框架
            since: 开始时间
            limit: 数据条数限制
            
        Returns:
            K线数据列表
        """
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return []
            
            ohlcv = await adapter.get_ohlcv(symbol, timeframe, since, limit)
            return ohlcv
            
        except Exception as e:
            self.logger.error(f"获取K线数据失败: {exchange} {symbol} - {e}")
            return []
    
    async def get_trades(
        self, 
        exchange: str, 
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """
        获取成交记录
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            since: 开始时间
            limit: 数据条数限制
            
        Returns:
            成交数据列表
        """
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return []
            
            trades = await adapter.get_trades(symbol, since, limit)
            return trades
            
        except Exception as e:
            self.logger.error(f"获取成交记录失败: {exchange} {symbol} - {e}")
            return []
    
    async def get_exchange_info(self, exchange: str) -> Optional[ExchangeInfo]:
        """
        获取交易所信息
        
        Args:
            exchange: 交易所名称
            
        Returns:
            交易所信息
        """
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return None
            
            info = await adapter.get_exchange_info()
            return info
            
        except Exception as e:
            self.logger.error(f"获取交易所信息失败: {exchange} - {e}")
            return None
    
    # === 新增：持仓管理功能 ===
    
    async def get_positions(self, exchange: str, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """
        获取持仓信息
        
        Args:
            exchange: 交易所名称
            symbols: 交易对符号列表，None表示获取所有
            
        Returns:
            持仓数据列表
        """
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return []
            
            positions = await adapter.get_positions(symbols)
            return positions
            
        except Exception as e:
            self.logger.error(f"获取持仓信息失败: {exchange} - {e}")
            return []
    
    async def get_all_positions(self) -> Dict[str, List[PositionData]]:
        """
        获取所有交易所的持仓信息
        
        Returns:
            按交易所分组的持仓数据
        """
        all_positions = {}
        
        for exchange in self.exchange_registry.get_all_exchanges():
            try:
                positions = await self.get_positions(exchange)
                all_positions[exchange] = positions
            except Exception as e:
                self.logger.error(f"获取 {exchange} 持仓信息失败: {e}")
                all_positions[exchange] = []
        
        return all_positions
    
    # === 新增：交易设置功能 ===
    
    async def set_leverage(self, exchange: str, symbol: str, leverage: int) -> bool:
        """
        设置杠杆倍数
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            leverage: 杠杆倍数
            
        Returns:
            是否成功设置
        """
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return False
            
            result = await adapter.set_leverage(symbol, leverage)
            return result is not None
            
        except Exception as e:
            self.logger.error(f"设置杠杆失败: {exchange} {symbol} {leverage}x - {e}")
            return False
    
    async def set_margin_mode(self, exchange: str, symbol: str, margin_mode: str) -> bool:
        """
        设置保证金模式
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            margin_mode: 保证金模式（'cross' 或 'isolated'）
            
        Returns:
            是否成功设置
        """
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return False
            
            result = await adapter.set_margin_mode(symbol, margin_mode)
            return result is not None
            
        except Exception as e:
            self.logger.error(f"设置保证金模式失败: {exchange} {symbol} {margin_mode} - {e}")
            return False
    
    # === 新增：订单管理功能 ===
    
    async def get_open_orders(self, exchange: str, symbol: Optional[str] = None) -> List[OrderData]:
        """
        获取活跃订单
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号，None表示获取所有
            
        Returns:
            活跃订单列表
        """
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return []
            
            orders = await adapter.get_open_orders(symbol)
            return orders
            
        except Exception as e:
            self.logger.error(f"获取活跃订单失败: {exchange} {symbol} - {e}")
            return []
    
    async def get_order_history(
        self, 
        exchange: str, 
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OrderData]:
        """
        获取历史订单
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            since: 开始时间
            limit: 数据条数限制
            
        Returns:
            历史订单列表
        """
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return []
            
            orders = await adapter.get_order_history(symbol, since, limit)
            return orders
            
        except Exception as e:
            self.logger.error(f"获取历史订单失败: {exchange} {symbol} - {e}")
            return []
    
    async def create_order(self, exchange: str, symbol: str, side: str, 
                          order_type: str, amount: Decimal, price: Decimal) -> OrderInfo:
        """
        创建订单 - 重构版：使用统一符号转换服务
        
        Args:
            exchange: 交易所名称
            symbol: 交易对（系统标准格式，如 BTC-USDC-PERP）
            side: 交易方向 ('buy' 或 'sell')
            order_type: 订单类型 ('limit' 或 'market')
            amount: 数量
            price: 价格 (由决策引擎提供，不由执行器计算)
            
        Returns:
            订单信息
        """
        # 验证必要参数
        if not price or price <= 0:
            raise ValueError("价格必须由决策引擎提供，不能为空或小于等于0")
        
        # 获取适配器
        adapter = self.exchange_registry.get_adapter(exchange)
        if not adapter:
            raise ValueError(f"交易所 {exchange} 未注册")
        
        # 🔥 重构：使用统一符号转换服务
        try:
            exchange_symbol = await self.symbol_conversion_service.convert_to_exchange_format(symbol, exchange)
        except Exception as e:
            self.logger.error(f"符号转换失败: {symbol} -> {exchange} - {e}")
            exchange_symbol = symbol  # 转换失败时使用原始符号
        
        # 获取精度信息
        precision = await self.precision_manager.get_symbol_precision(exchange, exchange_symbol)
        
        # 调整精度
        formatted_quantity = adjust_precision(amount, precision.amount_precision if precision else 8)
        formatted_price = adjust_precision(price, precision.price_precision if precision else 8)
        
        # 转换为CCXT格式
        ccxt_side = 'buy' if side.lower() == 'buy' else 'sell'
        ccxt_order_type = 'limit' if order_type.lower() == 'limit' else 'market'
        
        # 记录订单信息
        order_desc = f"{'多单' if side == 'buy' else '空单'}"
        self.logger.info(f"📋 创建{order_desc}: {exchange} {symbol} -> {exchange_symbol} {side.upper()} {formatted_quantity} @ {formatted_price}")
        
        # 重试机制
        for attempt in range(self.max_retries):
            try:
                # 下单
                order_result = await adapter.create_order(
                    symbol=exchange_symbol,  # 使用转换后的交易所特定格式
                    side=ccxt_side,
                    order_type=ccxt_order_type,
                    amount=formatted_quantity,
                    price=formatted_price if ccxt_order_type == 'limit' else None
                )
                
                # 创建订单信息
                order_info = OrderInfo(
                    order_id=order_result.id,
                    exchange=exchange,
                    symbol=symbol,  # 保存系统标准格式符号
                    side=side,
                    order_type=order_type,
                    amount=formatted_quantity,
                    price=formatted_price,
                    filled_amount=order_result.filled or 0,
                    status=order_result.status,
                    created_at=order_result.timestamp or datetime.now()
                )
                
                self.logger.info(f"✅ {order_desc}创建成功: {order_info.order_id}")
                return order_info
                
            except Exception as e:
                self.logger.warning(f"下单失败 (尝试 {attempt + 1}/{self.max_retries}): {e}")
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(1)  # 重试前等待
    
    # === 新增：批量操作功能 ===
    
    async def batch_create_orders(self, orders: List[Dict[str, Any]]) -> List[OrderInfo]:
        """
        批量创建订单 - 重构版：使用统一符号转换服务
        
        Args:
            orders: 订单列表，每个订单必须包含:
                - exchange: 交易所名称
                - symbol: 交易对（系统标准格式）
                - side: 交易方向 ('buy' 或 'sell')
                - order_type: 订单类型 ('limit' 或 'market')
                - amount: 数量
                - price: 价格 (由决策引擎提供，必须明确指定)
                
        Returns:
            订单信息列表
        """
        results = []
        self.logger.info(f"📋 开始批量创建 {len(orders)} 个订单")
        
        # 验证所有订单都包含必要的价格信息
        for i, order in enumerate(orders):
            if 'price' not in order or not order['price'] or order['price'] <= 0:
                raise ValueError(f"订单 {i+1} 缺少有效的价格信息，价格必须由决策引擎提供")
        
        # 并发创建订单
        tasks = []
        for order in orders:
            task = self.create_order(
                exchange=order['exchange'],
                symbol=order['symbol'],  # 使用系统标准格式符号
                side=order['side'],
                order_type=order['order_type'],
                amount=order['amount'],
                price=order['price']
            )
            tasks.append(task)
        
        # 等待所有订单完成
        for i, task in enumerate(tasks):
            try:
                order_info = await task
                results.append(order_info)
                self.execution_stats['total_orders'] += 1
                self.execution_stats['successful_orders'] += 1
            except Exception as e:
                self.logger.error(f"批量订单 {i+1} 创建失败: {e}")
                self.execution_stats['total_orders'] += 1
                self.execution_stats['failed_orders'] += 1
        
        self.logger.info(f"✅ 批量创建订单完成: {len(results)}/{len(orders)} 成功")
        return results
    
    async def batch_cancel_orders(self, orders: List[Tuple[str, str, str]]) -> Dict[str, bool]:
        """
        批量取消订单
        
        Args:
            orders: 订单列表，每个元素为(exchange, order_id, symbol)
            
        Returns:
            取消结果字典
        """
        results = {}
        
        # 按交易所分组
        exchange_orders = {}
        for exchange, order_id, symbol in orders:
            if exchange not in exchange_orders:
                exchange_orders[exchange] = []
            exchange_orders[exchange].append((order_id, symbol))
        
        # 并发取消订单
        tasks = []
        for exchange, order_list in exchange_orders.items():
            for order_id, symbol in order_list:
                task = self.cancel_order(order_id, exchange, symbol)
                tasks.append((f"{exchange}:{order_id}", task))
        
        # 等待所有任务完成
        for order_key, task in tasks:
            try:
                result = await task
                results[order_key] = result
            except Exception as e:
                self.logger.error(f"批量取消订单失败: {order_key} - {e}")
                results[order_key] = False
        
        return results
    
    async def get_execution_stats(self) -> Dict[str, Any]:
        """
        获取执行统计信息
        
        Returns:
            统计信息
        """
        active_orders_count = len(self.active_orders)
        success_rate = (
            self.execution_stats['successful_orders'] / 
            max(self.execution_stats['total_orders'], 1)
        ) * 100
        
        # 获取符号转换服务统计
        conversion_stats = {}
        try:
            conversion_stats = self.symbol_conversion_service.get_conversion_stats()
        except Exception as e:
            self.logger.warning(f"获取符号转换统计失败: {e}")
        
        return {
            'active_orders': active_orders_count,
            'total_orders': self.execution_stats['total_orders'],
            'successful_orders': self.execution_stats['successful_orders'],
            'failed_orders': self.execution_stats['failed_orders'],
            'success_rate': round(success_rate, 2),
            'total_volume': float(self.execution_stats['total_volume']),
            'registered_exchanges': len(self.exchange_registry.get_all_exchanges()),
            'supported_symbols': await self._get_total_supported_symbols(),
            'symbol_conversion_stats': conversion_stats
        }
    
    async def _get_total_supported_symbols(self) -> int:
        """获取所有交易所支持的交易对总数"""
        total = 0
        for exchange in self.exchange_registry.get_all_exchanges():
            try:
                symbols = await self.get_supported_symbols(exchange)
                total += len(symbols)
            except:
                pass
        return total

    # === 移除的功能 ===
    # 以下符号转换相关方法已移除，因为现在使用统一的符号转换服务：
    # - _convert_symbol_format
    # - _convert_to_hyperliquid_format
    # - _convert_to_backpack_format
    # - _convert_to_edgex_format
    # - symbol_format_mapping
    
    # === 保留的核心功能 ===
    
    async def _validate_trade_plan(self, trade_plan: TradePlan) -> bool:
        """验证交易计划"""
        # 检查交易所是否可用
        if not self.exchange_registry.is_registered(trade_plan.long_exchange):
            self.logger.error(f"交易所未注册: {trade_plan.long_exchange}")
            return False
        
        if not self.exchange_registry.is_registered(trade_plan.short_exchange):
            self.logger.error(f"交易所未注册: {trade_plan.short_exchange}")
            return False
        
        # 检查数量是否有效
        if trade_plan.quantity <= 0:
            self.logger.error("交易数量必须大于0")
            return False
        
        return True
    
    def _convert_order_type(self, order_type: ArbitrageOrderType) -> OrderType:
        """转换订单类型"""
        if order_type == ArbitrageOrderType.MARKET:
            return OrderType.MARKET
        elif order_type == ArbitrageOrderType.LIMIT:
            return OrderType.LIMIT
        else:
            return OrderType.MARKET
    
    async def check_exchange_health(self, exchange: str) -> bool:
        """检查交易所健康状态"""
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return False
            
            # 检查连接状态
            if not adapter.is_connected():
                await adapter.connect()
            
            # 简单的健康检查
            # TODO: 实现更复杂的健康检查
            return adapter.is_connected()
            
        except Exception as e:
            self.logger.error(f"检查交易所健康状态失败: {exchange} - {e}")
            return False
    
    async def cancel_order(self, order_id: str, exchange: str, symbol: str) -> bool:
        """取消订单"""
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return False
            
            result = await adapter.cancel_order(order_id, symbol)
            
            # 更新订单状态
            if order_id in self.active_orders:
                self.active_orders[order_id].status = "cancelled"
                self.active_orders[order_id].updated_at = datetime.now()
            
            return result
            
        except Exception as e:
            self.logger.error(f"取消订单失败: {order_id} - {e}")
            return False
    
    async def cancel_all_orders(self, exchange: str, symbol: Optional[str] = None) -> List[OrderData]:
        """
        取消所有订单
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号，None表示取消所有交易对的订单
            
        Returns:
            取消的订单列表
        """
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return []
            
            cancelled_orders = await adapter.cancel_all_orders(symbol)
            
            # 更新本地订单状态
            for order in cancelled_orders:
                if order.id in self.active_orders:
                    self.active_orders[order.id].status = "cancelled"
                    self.active_orders[order.id].updated_at = datetime.now()
            
            return cancelled_orders
            
        except Exception as e:
            self.logger.error(f"取消所有订单失败: {exchange} {symbol} - {e}")
            return []
    
    async def get_order(self, exchange: str, order_id: str, symbol: str) -> Optional[OrderData]:
        """
        获取订单信息
        
        Args:
            exchange: 交易所名称
            order_id: 订单ID
            symbol: 交易对符号
            
        Returns:
            订单数据
        """
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return None
            
            order = await adapter.get_order(order_id, symbol)
            return order
            
        except Exception as e:
            self.logger.error(f"获取订单信息失败: {exchange} {order_id} - {e}")
            return None

    async def get_order_status(self, order_id: str, exchange: str, symbol: str) -> Optional[OrderInfo]:
        """获取订单状态"""
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return None
            
            order_status = await adapter.get_order_status(symbol, order_id)
            
            # 更新本地订单信息
            if order_id in self.active_orders:
                order_info = self.active_orders[order_id]
                order_info.filled_amount = order_status.filled
                order_info.status = order_status.status.value
                order_info.updated_at = datetime.now()
                return order_info
            
            # 创建新的订单信息
            return OrderInfo(
                order_id=order_id,
                exchange=exchange,
                symbol=symbol,
                side="unknown",
                amount=order_status.amount,
                price=order_status.price,
                filled_amount=order_status.filled,
                status=order_status.status.value,
                updated_at=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"获取订单状态失败: {order_id} - {e}")
            return None
    
    async def get_account_balance(self, exchange: str) -> Optional[Dict[str, Any]]:
        """获取账户余额"""
        try:
            adapter = self.exchange_registry.get_adapter(exchange)
            if not adapter:
                return None
            
            balances = await adapter.get_balances()
            
            # 转换为字典格式
            balance_dict = {}
            for balance in balances:
                balance_dict[balance.currency] = {
                    'free': float(balance.free),
                    'used': float(balance.used),
                    'total': float(balance.total)
                }
            
            return balance_dict
            
        except Exception as e:
            self.logger.error(f"获取账户余额失败: {exchange} - {e}")
            return None
    
    # TODO: 高级功能占位符
    async def execute_twap_order(self, trade_plan: TradePlan) -> ExecutionResult:
        """执行TWAP订单"""
        # TODO: 实现TWAP执行策略
        return ExecutionResult(
            plan_id=trade_plan.plan_id,
            success=False,
            error_message="TWAP执行策略未实现"
        )
    
    async def execute_smart_routing(self, trade_plan: TradePlan) -> ExecutionResult:
        """智能路由执行"""
        # TODO: 实现智能路由策略
        return ExecutionResult(
            plan_id=trade_plan.plan_id,
            success=False,
            error_message="智能路由策略未实现"
        ) 