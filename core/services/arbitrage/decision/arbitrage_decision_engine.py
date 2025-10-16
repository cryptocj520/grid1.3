"""
套利决策引擎

负责分析市场数据，生成交易计划，执行套利决策
支持先后下单模式、盈亏计算、平仓逻辑、精度处理等功能
"""

import asyncio
import uuid
import yaml
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Tuple
from decimal import Decimal
from pathlib import Path

from core.logging import get_logger
from ..shared.models import (
    TradePlan, MarketSnapshot, ArbitrageOpportunity, ArbitrageDirection,
    RiskAssessment, PrecisionInfo, calculate_spread_percentage, determine_direction,
    OrderInfo
)
from ..initialization.precision_manager import PrecisionManager
from ..risk_manager.risk_manager import RiskManager
from ..execution.trade_execution_manager import TradeExecutionManager
from ..execution.exchange_registry import ExchangeRegistry


class ArbitrageDecisionEngine:
    """套利决策引擎"""
    
    def __init__(
        self,
        precision_manager: PrecisionManager,
        execution_manager: TradeExecutionManager,
        exchange_registry: ExchangeRegistry,
        config_path: str = "config/arbitrage/decision_engine.yaml"
    ):
        """
        初始化决策引擎
        
        Args:
            precision_manager: 精度管理器
            execution_manager: 执行管理器
            exchange_registry: 交易所注册表
            config_path: 配置文件路径
        """
        self.precision_manager = precision_manager
        self.execution_manager = execution_manager
        self.exchange_registry = exchange_registry
        self.config_path = config_path
        self.logger = get_logger(__name__)
        
        # 加载配置
        self.config = self._load_config()
        
        # 决策参数
        self.decision_params = self.config.get('decision_params', {})
        self.position_management = self.config.get('position_management', {})
        self.exchange_weights = self.config.get('exchange_weights', {})
        self.order_execution = self.config.get('order_execution', {})
        self.profit_management = self.config.get('profit_management', {})
        self.precision_management = self.config.get('precision_management', {})
        
        # 内部状态
        self.current_positions: Dict[str, Dict[str, Any]] = {}  # 当前仓位
        self.position_history: List[Dict[str, Any]] = []  # 仓位历史
        self.decision_history: List[Dict[str, Any]] = []  # 决策历史
        self.total_position_usdc = Decimal('0')  # 总仓位（USDC）
        
        # 回调函数
        self.opportunity_callback: Optional[Callable] = None
        
        self.logger.info("套利决策引擎已初始化")
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                self.logger.warning(f"配置文件不存在: {self.config_path}，使用默认配置")
                return self._get_default_config()
            
            with open(config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            self.logger.info(f"配置文件加载成功: {self.config_path}")
            return config
            
        except Exception as e:
            self.logger.error(f"加载配置文件失败: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            'decision_params': {
                'min_spread_threshold': 0.05,
                'max_spread_threshold': 5.0,
                'min_volume_threshold': 1000,
                'risk_score_threshold': 0.8,
                'max_holding_time': 300
            },
            'position_management': {
                'order_amount_usdc': 100.0,
                'max_total_position_usdc': 1000.0,
                'max_single_exchange_position_usdc': 500.0,
                'position_sizing_mode': 'fixed'
            },
            'exchange_weights': {
                'backpack': 1,
                'hyperliquid': 3,
                'weight_mode': 'priority'
            },
            'order_execution': {
                'execution_mode': 'sequential',
                'first_exchange': {
                    'order_type': 'limit',
                    'price_offset_percent': 0.01,
                    'max_wait_time': 30,
                    'polling_interval': 2
                },
                'second_exchange': {
                    'order_type': 'market',
                    'price_offset_percent': 0.0,
                    'max_wait_time': 10,
                    'polling_interval': 1
                }
            },
            'profit_management': {
                'target_profit_usdc': 2.0,
                'stop_loss_usdc': -5.0,
                'close_mode': 'target_profit',
                'close_check_interval': 5,
                'fee_rate': {
                    'backpack': 0.002,
                    'hyperliquid': 0.0003
                }
            },
            'precision_management': {
                'compatibility_mode': 'lowest',
                'default_precision': {
                    'price': 2,
                    'amount': 4
                },
                'adjustment_strategy': 'conservative'
            }
        }
    
    async def analyze_market_data(self, market_data: Dict[str, Any]) -> Optional[TradePlan]:
        """
        分析市场数据并生成交易计划
        
        Args:
            market_data: 市场数据（从监视器模块传来）
            
        Returns:
            交易计划，如果没有机会则返回None
        """
        try:
            # 从监视器数据中提取关键信息
            symbol = market_data.get('symbol')
            exchanges_data = market_data.get('exchanges', {})
            timestamp = market_data.get('timestamp', datetime.now())
            
            if not symbol or len(exchanges_data) < 2:
                return None
            
            # 检查仓位限制
            if not self._check_position_limits():
                self.logger.warning("仓位限制检查失败，跳过此次机会")
                return None
            
            # 创建市场快照
            market_snapshot = await self._create_market_snapshot(
                symbol, exchanges_data, timestamp
            )
            
            if not market_snapshot:
                return None
            
            # 识别套利机会
            opportunity = await self._identify_arbitrage_opportunity(market_snapshot)
            
            if not opportunity:
                return None
            
            # 生成交易计划
            trade_plan = await self._generate_trade_plan(opportunity)
            
            if trade_plan:
                self.logger.info(f"生成交易计划: {trade_plan.plan_id} - {symbol}")
                
                # 记录决策历史
                self._record_decision(opportunity, trade_plan)
                
                # 触发机会回调
                if self.opportunity_callback:
                    await self.opportunity_callback(opportunity, trade_plan)
                
                # 执行交易计划
                success = await self._execute_trade_plan(trade_plan)
                if success:
                    self.logger.info(f"交易计划执行成功: {trade_plan.plan_id}")
                else:
                    self.logger.error(f"交易计划执行失败: {trade_plan.plan_id}")
            
            return trade_plan
            
        except Exception as e:
            self.logger.error(f"分析市场数据失败: {e}")
            return None
    
    async def _execute_trade_plan(self, trade_plan: TradePlan) -> bool:
        """
        执行交易计划
        
        Args:
            trade_plan: 交易计划
            
        Returns:
            是否执行成功
        """
        try:
            self.logger.info(f"开始执行交易计划: {trade_plan.plan_id}")
            
            # 获取交易所权重排序
            exchanges = self._get_sorted_exchanges([trade_plan.long_exchange, trade_plan.short_exchange])
            
            if self.order_execution.get('execution_mode') == 'sequential':
                # 先后下单模式
                return await self._execute_sequential_orders(trade_plan, exchanges)
            else:
                # 同时下单模式（占位符）
                self.logger.info("同时下单模式暂未实现，使用先后下单模式")
                return await self._execute_sequential_orders(trade_plan, exchanges)
                
        except Exception as e:
            self.logger.error(f"执行交易计划失败: {e}")
            return False
    
    async def _execute_sequential_orders(self, trade_plan: TradePlan, exchanges: List[str]) -> bool:
        """
        执行先后下单
        
        Args:
            trade_plan: 交易计划
            exchanges: 按权重排序的交易所列表
            
        Returns:
            是否执行成功
        """
        try:
            # 计算统一精度的订单数量
            unified_precision = await self._get_unified_precision(trade_plan.symbol, exchanges)
            order_amount = self._calculate_order_amount(trade_plan, unified_precision)
            
            # 计算订单价格
            prices = await self._calculate_order_prices(trade_plan, exchanges)
            
            # 第一个交易所下单（权重高）
            first_exchange = exchanges[0]
            first_side = self._get_order_side(trade_plan, first_exchange)
            first_order_type = self.order_execution['first_exchange']['order_type']
            first_price = prices[first_exchange]
            
            self.logger.info(f"第一步：{first_exchange} {first_side} {order_amount} @ {first_price}")
            
            # 创建第一个订单
            first_order = await self.execution_manager.create_order(
                exchange=first_exchange,
                symbol=trade_plan.symbol,
                side=first_side,
                order_type=first_order_type,
                amount=order_amount,
                price=first_price
            )
            
            if not first_order:
                self.logger.error("第一个订单创建失败")
                return False
            
            # 等待第一个订单完全成交
            first_order_filled = await self._wait_for_order_completion(
                first_order, self.order_execution['first_exchange']
            )
            
            if not first_order_filled:
                self.logger.error("第一个订单未完全成交")
                # TODO: 取消订单
                return False
            
            # 第二个交易所下单（权重低）
            second_exchange = exchanges[1]
            second_side = self._get_order_side(trade_plan, second_exchange)
            second_order_type = self.order_execution['second_exchange']['order_type']
            second_price = prices[second_exchange]
            
            self.logger.info(f"第二步：{second_exchange} {second_side} {order_amount} @ {second_price}")
            
            # 创建第二个订单
            second_order = await self.execution_manager.create_order(
                exchange=second_exchange,
                symbol=trade_plan.symbol,
                side=second_side,
                order_type=second_order_type,
                amount=order_amount,
                price=second_price if second_order_type == 'limit' else None
            )
            
            if not second_order:
                self.logger.error("第二个订单创建失败")
                return False
            
            # 等待第二个订单完全成交
            second_order_filled = await self._wait_for_order_completion(
                second_order, self.order_execution['second_exchange']
            )
            
            if not second_order_filled:
                self.logger.error("第二个订单未完全成交")
                return False
            
            # 记录仓位
            position_info = {
                'plan_id': trade_plan.plan_id,
                'symbol': trade_plan.symbol,
                'exchanges': exchanges,
                'orders': [first_order, second_order],
                'open_time': datetime.now(),
                'status': 'open',
                'amount': order_amount,
                'open_prices': {
                    first_exchange: first_order.price,
                    second_exchange: second_order.price
                }
            }
            
            self.current_positions[trade_plan.plan_id] = position_info
            self.total_position_usdc += self.position_management['order_amount_usdc']
            
            self.logger.info(f"仓位已建立: {trade_plan.plan_id}")
            
            # 启动盈亏监控
            asyncio.create_task(self._monitor_position_profit(trade_plan.plan_id))
            
            return True
            
        except Exception as e:
            self.logger.error(f"执行先后下单失败: {e}")
            return False
    
    async def _wait_for_order_completion(self, order: OrderInfo, config: Dict[str, Any]) -> bool:
        """
        等待订单完全成交
        
        Args:
            order: 订单信息
            config: 订单配置
            
        Returns:
            是否完全成交
        """
        try:
            max_wait_time = config.get('max_wait_time', 30)
            polling_interval = config.get('polling_interval', 2)
            
            start_time = datetime.now()
            
            while (datetime.now() - start_time).seconds < max_wait_time:
                # 查询订单状态
                updated_order = await self.execution_manager.get_order_status(
                    order.order_id, order.exchange, order.symbol
                )
                
                if updated_order and updated_order.status == 'filled':
                    self.logger.info(f"订单完全成交: {order.order_id}")
                    return True
                
                await asyncio.sleep(polling_interval)
            
            self.logger.warning(f"订单等待超时: {order.order_id}")
            return False
            
        except Exception as e:
            self.logger.error(f"等待订单完成失败: {e}")
            return False
    
    async def _monitor_position_profit(self, plan_id: str):
        """
        监控仓位盈亏
        
        Args:
            plan_id: 交易计划ID
        """
        try:
            while plan_id in self.current_positions:
                position = self.current_positions[plan_id]
                
                if position['status'] != 'open':
                    break
                
                # 计算当前盈亏
                current_profit = await self._calculate_position_profit(plan_id)
                
                if current_profit is None:
                    await asyncio.sleep(5)
                    continue
                
                target_profit = Decimal(str(self.profit_management['target_profit_usdc']))
                stop_loss = Decimal(str(self.profit_management['stop_loss_usdc']))
                
                self.logger.info(f"仓位 {plan_id} 当前盈亏: {current_profit} USDC")
                
                # 检查是否达到平仓条件
                if current_profit >= target_profit:
                    self.logger.info(f"达到目标利润，执行平仓: {plan_id}")
                    await self._close_position(plan_id, "target_profit")
                    break
                elif current_profit <= stop_loss:
                    self.logger.info(f"达到止损线，执行平仓: {plan_id}")
                    await self._close_position(plan_id, "stop_loss")
                    break
                
                await asyncio.sleep(self.profit_management.get('close_check_interval', 5))
                
        except Exception as e:
            self.logger.error(f"监控仓位盈亏失败: {e}")
    
    async def _calculate_position_profit(self, plan_id: str) -> Optional[Decimal]:
        """
        计算仓位盈亏
        
        Args:
            plan_id: 交易计划ID
            
        Returns:
            盈亏金额（USDC）
        """
        try:
            if plan_id not in self.current_positions:
                return None
            
            position = self.current_positions[plan_id]
            symbol = position['symbol']
            exchanges = position['exchanges']
            amount = position['amount']
            open_prices = position['open_prices']
            
            # 获取当前价格
            current_prices = {}
            for exchange in exchanges:
                adapter = self.exchange_registry.get_adapter(exchange)
                if adapter:
                    ticker = await adapter.get_ticker(symbol)
                    if ticker and ticker.last_price:
                        current_prices[exchange] = Decimal(str(ticker.last_price))
            
            if len(current_prices) < 2:
                return None
            
            # 计算盈亏
            total_profit = Decimal('0')
            
            for i, exchange in enumerate(exchanges):
                if exchange not in current_prices:
                    continue
                
                open_price = open_prices[exchange]
                current_price = current_prices[exchange]
                
                # 第一个交易所（权重高）的盈亏
                if i == 0:
                    # 根据交易方向计算盈亏
                    if exchange == position.get('long_exchange', exchanges[0]):
                        profit = (current_price - open_price) * amount
                    else:
                        profit = (open_price - current_price) * amount
                else:
                    # 第二个交易所（权重低）的盈亏
                    if exchange == position.get('short_exchange', exchanges[1]):
                        profit = (open_price - current_price) * amount
                    else:
                        profit = (current_price - open_price) * amount
                
                # 扣除手续费
                fee_rate = self.profit_management['fee_rate'].get(exchange, 0.001)
                fee = current_price * amount * Decimal(str(fee_rate))
                profit -= fee
                
                total_profit += profit
            
            return total_profit
            
        except Exception as e:
            self.logger.error(f"计算仓位盈亏失败: {e}")
            return None
    
    async def _close_position(self, plan_id: str, reason: str):
        """
        平仓
        
        Args:
            plan_id: 交易计划ID
            reason: 平仓原因
        """
        try:
            if plan_id not in self.current_positions:
                return
            
            position = self.current_positions[plan_id]
            symbol = position['symbol']
            exchanges = position['exchanges']
            amount = position['amount']
            
            self.logger.info(f"开始平仓: {plan_id} - 原因: {reason}")
            
            # 平仓就是开反向仓位
            close_orders = []
            
            for exchange in exchanges:
                # 获取原始订单方向
                original_order = None
                for order in position['orders']:
                    if order.exchange == exchange:
                        original_order = order
                        break
                
                if not original_order:
                    continue
                
                # 反向操作
                close_side = 'sell' if original_order.side == 'buy' else 'buy'
                
                # 获取当前价格
                adapter = self.exchange_registry.get_adapter(exchange)
                if adapter:
                    ticker = await adapter.get_ticker(symbol)
                    if ticker and ticker.last_price:
                        close_price = Decimal(str(ticker.last_price))
                        
                        # 创建平仓订单
                        close_order = await self.execution_manager.create_order(
                            exchange=exchange,
                            symbol=symbol,
                            side=close_side,
                            order_type='market',  # 平仓使用市价单
                            amount=amount,
                            price=close_price
                        )
                        
                        if close_order:
                            close_orders.append(close_order)
                            self.logger.info(f"平仓订单创建成功: {exchange} {close_side} {amount}")
            
            # 更新仓位状态
            position['status'] = 'closed'
            position['close_time'] = datetime.now()
            position['close_reason'] = reason
            position['close_orders'] = close_orders
            
            # 计算最终盈亏
            final_profit = await self._calculate_position_profit(plan_id)
            position['final_profit'] = final_profit
            
            # 移动到历史记录
            self.position_history.append(position)
            del self.current_positions[plan_id]
            
            # 更新总仓位
            self.total_position_usdc -= self.position_management['order_amount_usdc']
            
            self.logger.info(f"平仓完成: {plan_id} - 最终盈亏: {final_profit} USDC")
            
        except Exception as e:
            self.logger.error(f"平仓失败: {e}")
    
    def _get_sorted_exchanges(self, exchanges: List[str]) -> List[str]:
        """
        按权重排序交易所
        
        Args:
            exchanges: 交易所列表
            
        Returns:
            排序后的交易所列表
        """
        exchange_weights = self.exchange_weights
        
        # 按权重排序（权重越小优先级越高）
        sorted_exchanges = sorted(exchanges, key=lambda x: exchange_weights.get(x, 999))
        
        return sorted_exchanges
    
    def _get_order_side(self, trade_plan: TradePlan, exchange: str) -> str:
        """
        获取订单方向
        
        Args:
            trade_plan: 交易计划
            exchange: 交易所
            
        Returns:
            订单方向 ('buy' 或 'sell')
        """
        if exchange == trade_plan.long_exchange:
            return 'buy'
        else:
            return 'sell'
    
    async def _get_unified_precision(self, symbol: str, exchanges: List[str]) -> Dict[str, int]:
        """
        获取统一精度
        
        Args:
            symbol: 交易对
            exchanges: 交易所列表
            
        Returns:
            统一精度信息
        """
        try:
            precisions = {}
            
            for exchange in exchanges:
                precision = await self.precision_manager.get_symbol_precision(exchange, symbol)
                if precision:
                    precisions[exchange] = {
                        'price': precision.price_precision,
                        'amount': precision.amount_precision
                    }
            
            if not precisions:
                # 使用默认精度
                default_precision = self.precision_management['default_precision']
                return {
                    'price': default_precision['price'],
                    'amount': default_precision['amount']
                }
            
            # 向下兼容到最低精度
            if self.precision_management['compatibility_mode'] == 'lowest':
                min_price_precision = min(p['price'] for p in precisions.values())
                min_amount_precision = min(p['amount'] for p in precisions.values())
                
                return {
                    'price': min_price_precision,
                    'amount': min_amount_precision
                }
            else:
                # 向上兼容到最高精度
                max_price_precision = max(p['price'] for p in precisions.values())
                max_amount_precision = max(p['amount'] for p in precisions.values())
                
                return {
                    'price': max_price_precision,
                    'amount': max_amount_precision
                }
                
        except Exception as e:
            self.logger.error(f"获取统一精度失败: {e}")
            default_precision = self.precision_management['default_precision']
            return {
                'price': default_precision['price'],
                'amount': default_precision['amount']
            }
    
    def _calculate_order_amount(self, trade_plan: TradePlan, precision: Dict[str, int]) -> Decimal:
        """
        计算订单数量
        
        Args:
            trade_plan: 交易计划
            precision: 精度信息
            
        Returns:
            订单数量
        """
        try:
            # 从配置获取订单金额（USDC）
            order_amount_usdc = Decimal(str(self.position_management['order_amount_usdc']))
            
            # 估算价格（使用trade_plan中的价格信息）
            estimated_price = trade_plan.quantity  # 这里需要根据实际情况调整
            
            # 计算数量
            amount = order_amount_usdc / estimated_price
            
            # 调整精度
            amount_precision = precision['amount']
            amount = amount.quantize(Decimal('0.1') ** amount_precision)
            
            return amount
            
        except Exception as e:
            self.logger.error(f"计算订单数量失败: {e}")
            return Decimal('0.1')  # 默认数量
    
    async def _calculate_order_prices(self, trade_plan: TradePlan, exchanges: List[str]) -> Dict[str, Decimal]:
        """
        计算订单价格
        
        Args:
            trade_plan: 交易计划
            exchanges: 交易所列表
            
        Returns:
            每个交易所的订单价格
        """
        try:
            prices = {}
            
            for exchange in exchanges:
                adapter = self.exchange_registry.get_adapter(exchange)
                if adapter:
                    ticker = await adapter.get_ticker(trade_plan.symbol)
                    if ticker and ticker.last_price:
                        market_price = Decimal(str(ticker.last_price))
                        
                        # 根据订单类型调整价格
                        if exchange == exchanges[0]:  # 第一个交易所
                            config = self.order_execution['first_exchange']
                        else:  # 第二个交易所
                            config = self.order_execution['second_exchange']
                        
                        if config['order_type'] == 'limit':
                            offset_percent = Decimal(str(config['price_offset_percent']))
                            side = self._get_order_side(trade_plan, exchange)
                            
                            if side == 'buy':
                                # 买单价格略低于市价
                                price = market_price * (1 - offset_percent)
                            else:
                                # 卖单价格略高于市价
                                price = market_price * (1 + offset_percent)
                        else:
                            # 市价单使用当前价格
                            price = market_price
                        
                        prices[exchange] = price
            
            return prices
            
        except Exception as e:
            self.logger.error(f"计算订单价格失败: {e}")
            return {}
    
    def _check_position_limits(self) -> bool:
        """
        检查仓位限制
        
        Returns:
            是否可以开仓
        """
        try:
            max_total_position = Decimal(str(self.position_management['max_total_position_usdc']))
            order_amount = Decimal(str(self.position_management['order_amount_usdc']))
            
            if self.total_position_usdc + order_amount > max_total_position:
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"检查仓位限制失败: {e}")
            return False
    
    def _record_decision(self, opportunity: ArbitrageOpportunity, trade_plan: TradePlan):
        """
        记录决策历史
        
        Args:
            opportunity: 套利机会
            trade_plan: 交易计划
        """
        try:
            if self.config.get('monitoring', {}).get('decision_history', True):
                decision_record = {
                    'timestamp': datetime.now(),
                    'opportunity_id': opportunity.opportunity_id,
                    'plan_id': trade_plan.plan_id,
                    'symbol': trade_plan.symbol,
                    'spread_percentage': float(opportunity.spread_percentage),
                    'expected_profit': float(opportunity.expected_profit),
                    'exchanges': [trade_plan.long_exchange, trade_plan.short_exchange]
                }
                
                self.decision_history.append(decision_record)
                
                # 限制历史记录数量
                max_records = self.config.get('monitoring', {}).get('max_history_records', 1000)
                if len(self.decision_history) > max_records:
                    self.decision_history = self.decision_history[-max_records:]
                    
        except Exception as e:
            self.logger.error(f"记录决策历史失败: {e}")
    
    # 继承原有方法...
    async def _create_market_snapshot(self, symbol: str, exchanges_data: Dict[str, Any], timestamp: datetime) -> Optional[MarketSnapshot]:
        """创建市场快照"""
        # 保持原有实现
        try:
            prices = {}
            volumes = {}
            
            for exchange_name, data in exchanges_data.items():
                price = None
                volume = None
                
                if 'price' in data:
                    price = Decimal(str(data['price']))
                elif 'ticker' in data and 'last' in data['ticker']:
                    price = Decimal(str(data['ticker']['last']))
                elif 'best_bid' in data and 'best_ask' in data:
                    bid = Decimal(str(data['best_bid']))
                    ask = Decimal(str(data['best_ask']))
                    price = (bid + ask) / 2
                
                if 'volume' in data:
                    volume = Decimal(str(data['volume']))
                elif 'ticker' in data and 'baseVolume' in data['ticker']:
                    volume = Decimal(str(data['ticker']['baseVolume']))
                
                if price and volume:
                    prices[exchange_name] = price
                    volumes[exchange_name] = volume
            
            if len(prices) < 2:
                return None
            
            max_price = max(prices.values())
            min_price = min(prices.values())
            spread_percentage = calculate_spread_percentage(max_price, min_price)
            direction = determine_direction(max_price, min_price)
            
            return MarketSnapshot(
                symbol=symbol,
                timestamp=timestamp,
                exchanges_data=exchanges_data,
                spread_percentage=spread_percentage,
                direction=direction,
                best_bid=min_price,
                best_ask=max_price,
                volume_info=volumes
            )
            
        except Exception as e:
            self.logger.error(f"创建市场快照失败: {e}")
            return None
    
    async def _identify_arbitrage_opportunity(self, market_snapshot: MarketSnapshot) -> Optional[ArbitrageOpportunity]:
        """识别套利机会"""
        # 保持原有实现
        try:
            min_spread = Decimal(str(self.decision_params.get('min_spread_threshold', 0.05)))
            max_spread = Decimal(str(self.decision_params.get('max_spread_threshold', 5.0)))
            
            if (market_snapshot.spread_percentage < min_spread or
                market_snapshot.spread_percentage > max_spread):
                return None
            
            if market_snapshot.direction == ArbitrageDirection.NEUTRAL:
                return None
            
            risk_assessment = await self._assess_risk(market_snapshot)
            
            if not risk_assessment.can_execute:
                return None
            
            expected_profit = await self._calculate_expected_profit(
                market_snapshot, risk_assessment.recommended_size
            )
            
            if expected_profit <= 0:
                return None
            
            opportunity = ArbitrageOpportunity(
                opportunity_id=str(uuid.uuid4()),
                symbol=market_snapshot.symbol,
                direction=market_snapshot.direction,
                spread_percentage=market_snapshot.spread_percentage,
                expected_profit=expected_profit,
                confidence=self._calculate_confidence(market_snapshot),
                urgency=self._calculate_urgency(market_snapshot),
                market_snapshot=market_snapshot,
                risk_assessment=risk_assessment,
                expires_at=datetime.now() + timedelta(seconds=30)
            )
            
            return opportunity
            
        except Exception as e:
            self.logger.error(f"识别套利机会失败: {e}")
            return None
    
    async def _assess_risk(self, market_snapshot: MarketSnapshot) -> RiskAssessment:
        """评估风险"""
        # 保持原有实现
        try:
            risk_score = 0.0
            risk_factors = {}
            warnings = []
            
            max_spread = Decimal(str(self.decision_params.get('max_spread_threshold', 5.0)))
            min_volume = Decimal(str(self.decision_params.get('min_volume_threshold', 1000)))
            
            if market_snapshot.spread_percentage > max_spread * Decimal('0.8'):
                risk_score += 0.3
                risk_factors['high_spread'] = float(market_snapshot.spread_percentage)
                warnings.append("价差过高，可能存在异常")
            
            total_volume = sum(market_snapshot.volume_info.values())
            if total_volume < min_volume:
                risk_score += 0.2
                risk_factors['low_volume'] = float(total_volume)
                warnings.append("成交量过低，流动性不足")
            
            volume_factor = min(1.0, float(total_volume) / float(min_volume))
            base_size = Decimal(str(self.position_management['order_amount_usdc'])) * Decimal(str(volume_factor))
            max_position = Decimal(str(self.position_management['max_single_exchange_position_usdc']))
            recommended_size = min(base_size, max_position)
            
            if risk_score > 0.5:
                recommended_size = recommended_size * Decimal('0.5')
            
            return RiskAssessment(
                symbol=market_snapshot.symbol,
                risk_score=risk_score,
                max_position_size=max_position,
                recommended_size=recommended_size,
                warnings=warnings,
                risk_factors=risk_factors
            )
            
        except Exception as e:
            self.logger.error(f"风险评估失败: {e}")
            return RiskAssessment(
                symbol=market_snapshot.symbol,
                risk_score=1.0,
                max_position_size=Decimal('0'),
                recommended_size=Decimal('0'),
                warnings=["风险评估失败"],
                risk_factors={}
            )
    
    async def _calculate_expected_profit(self, market_snapshot: MarketSnapshot, position_size: Decimal) -> Decimal:
        """计算预期利润"""
        # 保持原有实现
        try:
            price_diff = market_snapshot.best_ask - market_snapshot.best_bid
            gross_profit = price_diff * position_size
            return gross_profit
            
        except Exception as e:
            self.logger.error(f"计算预期利润失败: {e}")
            return Decimal('0')
    
    def _calculate_confidence(self, market_snapshot: MarketSnapshot) -> float:
        """计算置信度"""
        # 保持原有实现
        try:
            confidence = 0.5
            min_spread = Decimal(str(self.decision_params.get('min_spread_threshold', 0.05)))
            min_volume = Decimal(str(self.decision_params.get('min_volume_threshold', 1000)))
            
            if market_snapshot.spread_percentage > min_spread * 2:
                confidence += 0.2
            
            total_volume = sum(market_snapshot.volume_info.values())
            if total_volume > min_volume * 2:
                confidence += 0.2
            
            return min(1.0, confidence)
            
        except Exception:
            return 0.5
    
    def _calculate_urgency(self, market_snapshot: MarketSnapshot) -> float:
        """计算紧急程度"""
        # 保持原有实现
        try:
            urgency = min(1.0, float(market_snapshot.spread_percentage) / 2.0)
            return urgency
            
        except Exception:
            return 0.5
    
    async def _generate_trade_plan(self, opportunity: ArbitrageOpportunity) -> Optional[TradePlan]:
        """生成交易计划"""
        # 保持原有实现
        try:
            exchanges = list(opportunity.market_snapshot.exchanges_data.keys())
            if len(exchanges) < 2:
                return None
            
            prices = {}
            for exchange_name, data in opportunity.market_snapshot.exchanges_data.items():
                if 'price' in data:
                    prices[exchange_name] = Decimal(str(data['price']))
                elif 'ticker' in data and 'last' in data['ticker']:
                    prices[exchange_name] = Decimal(str(data['ticker']['last']))
            
            if len(prices) < 2:
                return None
            
            max_price_exchange = max(prices.items(), key=lambda x: x[1])[0]
            min_price_exchange = min(prices.items(), key=lambda x: x[1])[0]
            
            trade_plan = TradePlan(
                plan_id=str(uuid.uuid4()),
                symbol=opportunity.symbol,
                direction=opportunity.direction,
                long_exchange=min_price_exchange,
                short_exchange=max_price_exchange,
                quantity=opportunity.risk_assessment.recommended_size,
                expected_profit=opportunity.expected_profit,
                created_at=datetime.now()
            )
            
            return trade_plan
            
        except Exception as e:
            self.logger.error(f"生成交易计划失败: {e}")
            return None
    
    def set_opportunity_callback(self, callback: Callable):
        """设置套利机会回调函数"""
        self.opportunity_callback = callback
    
    async def handle_monitor_data(self, data: Dict[str, Any]):
        """处理监视器数据"""
        trade_plan = await self.analyze_market_data(data)
        return trade_plan
    
    def get_current_positions(self) -> Dict[str, Dict[str, Any]]:
        """获取当前仓位"""
        return self.current_positions.copy()
    
    def get_position_history(self) -> List[Dict[str, Any]]:
        """获取仓位历史"""
        return self.position_history.copy()
    
    def get_decision_history(self) -> List[Dict[str, Any]]:
        """获取决策历史"""
        return self.decision_history.copy()
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'current_positions': len(self.current_positions),
            'total_position_usdc': float(self.total_position_usdc),
            'position_history_count': len(self.position_history),
            'decision_history_count': len(self.decision_history),
            'total_profit': sum(p.get('final_profit', 0) for p in self.position_history if p.get('final_profit')),
            'avg_holding_time': self._calculate_avg_holding_time()
        }
    
    def _calculate_avg_holding_time(self) -> float:
        """计算平均持仓时间"""
        try:
            closed_positions = [p for p in self.position_history if p.get('close_time')]
            if not closed_positions:
                return 0.0
            
            total_time = sum(
                (p['close_time'] - p['open_time']).total_seconds()
                for p in closed_positions
            )
            
            return total_time / len(closed_positions)
            
        except Exception:
            return 0.0 