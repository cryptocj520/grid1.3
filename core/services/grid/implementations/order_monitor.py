"""
订单监控器 - REST API轮询方式

由于Backpack WebSocket不支持用户数据流订阅，
采用REST API轮询方式监控订单成交情况
"""
import asyncio
from typing import Dict, Set, Callable, List, Optional
from decimal import Decimal
from datetime import datetime

from ....logging import get_logger
from ....adapters.exchanges import ExchangeInterface, OrderStatus
from ..models import GridOrder, GridOrderStatus


class OrderMonitor:
    """
    订单监控器
    
    功能：
    1. 定期轮询检查订单状态
    2. 检测订单成交
    3. 触发成交回调
    """
    
    def __init__(
        self,
        exchange: ExchangeInterface,
        symbol: str,
        poll_interval: float = 2.0  # 默认2秒轮询一次
    ):
        """
        初始化订单监控器
        
        Args:
            exchange: 交易所适配器
            symbol: 交易对
            poll_interval: 轮询间隔（秒）
        """
        self.logger = get_logger(__name__)
        self.exchange = exchange
        self.symbol = symbol
        self.poll_interval = poll_interval
        
        # 监控的订单
        self._monitored_orders: Dict[str, GridOrder] = {}  # order_id -> GridOrder
        
        # 订单成交回调
        self._fill_callbacks: List[Callable] = []
        
        # 运行状态
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None
        
        # 统计信息
        self._total_checks = 0
        self._total_fills = 0
        self._last_check_time: Optional[datetime] = None
        
        self.logger.info(
            f"订单监控器初始化: {symbol}, "
            f"轮询间隔={poll_interval}秒"
        )
    
    def add_order(self, order: GridOrder):
        """
        添加订单到监控列表
        
        Args:
            order: 网格订单
        """
        if not order.order_id or order.order_id == "pending":
            self.logger.warning(f"订单ID无效，跳过监控: {order.order_id}")
            return
        
        self._monitored_orders[order.order_id] = order
        self.logger.debug(
            f"添加订单监控: {order.order_id} "
            f"(Grid {order.grid_id}, {order.side.value} {order.amount}@{order.price})"
        )
    
    def remove_order(self, order_id: str):
        """
        从监控列表移除订单
        
        Args:
            order_id: 订单ID
        """
        if order_id in self._monitored_orders:
            del self._monitored_orders[order_id]
            self.logger.debug(f"移除订单监控: {order_id}")
    
    def add_fill_callback(self, callback: Callable):
        """
        添加订单成交回调
        
        Args:
            callback: 回调函数，接收 GridOrder 参数
        """
        self._fill_callbacks.append(callback)
        self.logger.debug(f"添加成交回调: {callback}")
    
    async def start(self):
        """启动监控"""
        if self._running:
            self.logger.warning("订单监控器已在运行")
            return
        
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        
        self.logger.info("✅ 订单监控器已启动")
    
    async def stop(self):
        """停止监控"""
        if not self._running:
            return
        
        self._running = False
        
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("⏹️ 订单监控器已停止")
    
    async def _monitor_loop(self):
        """监控循环"""
        self.logger.info(f"订单监控循环启动，间隔={self.poll_interval}秒")
        
        while self._running:
            try:
                await self._check_orders()
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"监控循环异常: {e}")
                await asyncio.sleep(self.poll_interval)
        
        self.logger.info("订单监控循环已退出")
    
    async def _check_orders(self):
        """检查所有监控的订单"""
        if not self._monitored_orders:
            return
        
        self._total_checks += 1
        self._last_check_time = datetime.now()
        
        # 批量查询所有挂单
        try:
            open_orders = await self.exchange.get_open_orders(self.symbol)
            open_order_ids = {order.id for order in open_orders if order.id}
            
            # 检查哪些订单已成交（不在挂单列表中）
            filled_order_ids: Set[str] = set()
            
            for order_id, grid_order in list(self._monitored_orders.items()):
                # 如果订单不在挂单列表中，说明已成交或取消
                if order_id not in open_order_ids:
                    filled_order_ids.add(order_id)
            
            # 处理已成交订单
            if filled_order_ids:
                await self._process_filled_orders(filled_order_ids)
            
            # 日志统计
            if self._total_checks % 30 == 0:  # 每30次检查记录一次
                self.logger.debug(
                    f"监控统计: "
                    f"检查次数={self._total_checks}, "
                    f"成交数={self._total_fills}, "
                    f"当前监控={len(self._monitored_orders)}个订单"
                )
            
        except Exception as e:
            self.logger.error(f"检查订单失败: {e}")
    
    async def _process_filled_orders(self, filled_order_ids: Set[str]):
        """
        处理已成交订单
        
        Args:
            filled_order_ids: 已成交的订单ID集合
        """
        for order_id in filled_order_ids:
            if order_id not in self._monitored_orders:
                continue
            
            grid_order = self._monitored_orders[order_id]
            
            try:
                # 查询订单详情（获取实际成交价格和数量）
                exchange_order = await self.exchange.get_order(order_id, self.symbol)
                
                # 检查订单状态
                if exchange_order.status == OrderStatus.FILLED:
                    # 获取成交信息
                    filled_price = exchange_order.average or exchange_order.price or grid_order.price
                    filled_amount = exchange_order.filled or grid_order.amount
                    
                    # 标记订单已成交
                    grid_order.mark_filled(filled_price, filled_amount)
                    
                    self.logger.info(
                        f"✅ 订单成交: {grid_order.side.value} "
                        f"{filled_amount}@{filled_price} "
                        f"(Grid {grid_order.grid_id}, Order {order_id})"
                    )
                    
                    # 从监控列表移除
                    del self._monitored_orders[order_id]
                    
                    # 更新统计
                    self._total_fills += 1
                    
                    # 触发成交回调
                    await self._trigger_fill_callbacks(grid_order)
                    
                elif exchange_order.status == OrderStatus.CANCELED:
                    # 订单被取消
                    self.logger.warning(
                        f"订单已取消: {order_id} "
                        f"(Grid {grid_order.grid_id})"
                    )
                    del self._monitored_orders[order_id]
                    
                else:
                    # 其他状态（部分成交等）
                    self.logger.debug(
                        f"订单状态={exchange_order.status.value}: {order_id}"
                    )
                    
            except Exception as e:
                self.logger.error(
                    f"处理成交订单失败 {order_id}: {e}"
                )
    
    async def _trigger_fill_callbacks(self, filled_order: GridOrder):
        """
        触发成交回调
        
        Args:
            filled_order: 已成交订单
        """
        for callback in self._fill_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(filled_order)
                else:
                    callback(filled_order)
            except Exception as e:
                self.logger.error(f"成交回调执行失败: {e}")
    
    def get_statistics(self) -> Dict:
        """
        获取监控统计信息
        
        Returns:
            统计字典
        """
        return {
            "total_checks": self._total_checks,
            "total_fills": self._total_fills,
            "monitored_orders": len(self._monitored_orders),
            "last_check_time": self._last_check_time,
            "poll_interval": self.poll_interval,
            "is_running": self._running
        }
    
    def __repr__(self) -> str:
        return (
            f"OrderMonitor("
            f"symbol={self.symbol}, "
            f"monitored={len(self._monitored_orders)}, "
            f"checks={self._total_checks}, "
            f"fills={self._total_fills})"
        )

