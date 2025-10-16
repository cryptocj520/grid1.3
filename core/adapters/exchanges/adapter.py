"""
交易所适配器基类

提供交易所适配器的公共功能实现，包括事件集成、错误处理、
重试机制、数据转换等通用功能。
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal
import traceback

from ...services.events import Event, HealthCheckEvent
from .interface import ExchangeInterface, ExchangeConfig, ExchangeStatus
from .models import (
    OrderData,
    PositionData,
    BalanceData,
    TickerData,
    OrderBookData,
    TradeData,
    OrderSide,
    OrderType,
    ExchangeInfo
)


class ExchangeAdapter(ExchangeInterface):
    """
    交易所适配器基类

    提供通用的功能实现，包括：
    - 事件驱动集成
    - 错误处理和重试
    - 连接管理和健康检查
    - 数据标准化
    - 限频控制
    """

    def __init__(self, config: ExchangeConfig, event_bus: Optional[Any] = None):
        """
        初始化适配器

        Args:
            config: 交易所配置
            event_bus: 事件总线实例（在新架构中可选）
        """
        super().__init__(config)
        self.event_bus = event_bus
        
        # 修复：使用统一的日志服务而不是直接创建logger
        try:
            from core.infrastructure.logging import get_logger
            self.logger = get_logger("exchanges", f"ExchangeAdapter.{config.exchange_id}")
        except ImportError:
            # 回退到基本配置，但确保有正确的格式化器
            self.logger = logging.getLogger(f"ExchangeAdapter.{config.exchange_id}")
            if not self.logger.handlers:
                # 添加控制台处理器和格式化器
                handler = logging.StreamHandler()
                formatter = logging.Formatter(
                    '[%(asctime)s] %(levelname)-8s %(name)-20s | %(message)s',
                    datefmt='%H:%M:%S'
                )
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
                self.logger.setLevel(logging.INFO)
                self.logger.propagate = False

        # 连接管理
        self._connection_lock = asyncio.Lock()
        self._reconnect_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._last_error: Optional[Exception] = None
        self._consecutive_errors = 0

        # 订阅管理
        self._subscriptions: Dict[str, List[Callable]] = {}
        self._ws_connection = None

        # 限频控制
        self._rate_limits: Dict[str, List[float]] = {}
        self._request_timestamps: Dict[str, List[datetime]] = {}

        # 健康监控
        self._health_metrics = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'last_request_time': None,
            'last_error_time': None,
            'uptime_start': datetime.now()
        }

        self.logger.info(f"初始化交易所适配器: {config.name}")

    # === 生命周期管理 ===

    async def connect(self) -> bool:
        """连接到交易所"""
        async with self._connection_lock:
            try:
                self.status = ExchangeStatus.CONNECTING
                self.logger.info("正在连接到交易所...")

                # 子类实现具体连接逻辑
                success = await self._do_connect()

                if success:
                    self.status = ExchangeStatus.CONNECTED
                    self.logger.info("交易所连接成功")

                    # 启动心跳检测
                    if self.config.enable_heartbeat:
                        await self._start_heartbeat()

                    # 记录连接成功事件
                    self.logger.info(f"交易所适配器已启动: {self.config.exchange_id}")

                    return True
                else:
                    self.status = ExchangeStatus.ERROR
                    self.logger.error("交易所连接失败")
                    return False

            except Exception as e:
                self.status = ExchangeStatus.ERROR
                self._last_error = e
                self._consecutive_errors += 1
                self.logger.error(f"连接异常: {str(e)}")

                # 记录错误事件
                self.logger.error(f"交易所适配器连接失败: {self.config.exchange_id}, 错误: {e}")

                return False

    async def disconnect(self) -> None:
        """断开连接"""
        async with self._connection_lock:
            try:
                self.logger.info("正在断开交易所连接...")

                # 停止心跳检测
                if self._heartbeat_task:
                    self._heartbeat_task.cancel()
                    self._heartbeat_task = None

                # 停止自动重连
                if self._reconnect_task:
                    self._reconnect_task.cancel()
                    self._reconnect_task = None

                # 关闭WebSocket连接
                if self._ws_connection:
                    await self._close_websocket()

                # 子类实现具体断开逻辑
                await self._do_disconnect()

                self.status = ExchangeStatus.DISCONNECTED
                self.logger.info("交易所连接已断开")

                # 记录断开连接事件
                self.logger.info(f"交易所适配器已停止: {self.config.exchange_id}")

            except Exception as e:
                self.logger.error(f"断开连接异常: {str(e)}")

    async def authenticate(self) -> bool:
        """进行身份认证"""
        try:
            self.logger.info("正在进行身份认证...")

            # 子类实现具体认证逻辑
            success = await self._do_authenticate()

            if success:
                self.status = ExchangeStatus.AUTHENTICATED
                self.logger.info("身份认证成功")
                return True
            else:
                self.logger.error("身份认证失败")
                return False

        except Exception as e:
            self.logger.error(f"认证异常: {str(e)}")
            return False

    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            # 执行具体的健康检查
            health_data = await self._do_health_check()

            # 添加通用健康指标
            health_data.update({
                'status': self.status.value,
                'exchange_id': self.config.exchange_id,
                'last_heartbeat': self._last_heartbeat.isoformat() if hasattr(self, '_last_heartbeat') else None,
                'consecutive_errors': self._consecutive_errors,
                'metrics': self._health_metrics.copy(),
                'is_connected': self.is_connected(),
                'is_authenticated': self.is_authenticated()
            })

            # 发出健康检查事件
            await self._emit_event(HealthCheckEvent(
                component=f"exchange_{self.config.exchange_id}",
                check_name="adapter_health_check",
                status="healthy" if self.is_connected() else "unhealthy",
                timestamp=datetime.now(),
                details=health_data
            ))

            return health_data

        except Exception as e:
            self.logger.error(f"健康检查异常: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'exchange_id': self.config.exchange_id
            }

    # === 错误处理和重试 ===

    async def _execute_with_retry(
        self,
        func: Callable,
        *args,
        operation_name: str = "operation",
        **kwargs
    ) -> Any:
        """
        带重试的操作执行

        Args:
            func: 要执行的函数
            *args: 函数参数
            operation_name: 操作名称（用于日志）
            **kwargs: 函数关键字参数

        Returns:
            Any: 函数执行结果
        """
        last_exception = None

        for attempt in range(self.config.max_retry_attempts):
            try:
                # 检查限频
                await self._check_rate_limit(operation_name)

                # 执行操作
                result = await func(*args, **kwargs)

                # 记录成功
                self._health_metrics['successful_requests'] += 1
                self._health_metrics['last_request_time'] = datetime.now()

                # 重置错误计数
                if self._consecutive_errors > 0:
                    self._consecutive_errors = 0
                    self.logger.info(f"操作恢复正常: {operation_name}")

                return result

            except Exception as e:
                last_exception = e
                self._consecutive_errors += 1
                self._health_metrics['failed_requests'] += 1
                self._health_metrics['last_error_time'] = datetime.now()

                self.logger.warning(
                    f"操作失败 {operation_name} (尝试 {attempt + 1}/{self.config.max_retry_attempts}): {str(e)}"
                )

                # 如果不是最后一次尝试，等待后重试
                if attempt < self.config.max_retry_attempts - 1:
                    delay = self.config.retry_delay * (2 ** attempt)  # 指数退避
                    await asyncio.sleep(delay)

                    # 检查是否需要重连
                    if self._should_reconnect(e):
                        await self._attempt_reconnect()

        # 所有重试都失败，抛出最后的异常
        self.logger.error(f"操作最终失败 {operation_name}: {str(last_exception)}")

        # 记录错误事件
        self.logger.error(f"交易所操作失败: {self.config.exchange_id}, 操作: {operation_name}, 错误: {last_exception}")

        raise last_exception

    def _should_reconnect(self, error: Exception) -> bool:
        """
        判断是否需要重连

        Args:
            error: 发生的异常

        Returns:
            bool: 是否需要重连
        """
        # 检查错误类型，判断是否是连接相关错误
        error_str = str(error).lower()
        connection_errors = [
            'connection', 'timeout', 'network', 'socket',
            'disconnected', 'reset', 'broken pipe'
        ]

        return any(err in error_str for err in connection_errors)

    async def _attempt_reconnect(self) -> None:
        """尝试重连"""
        if not self.config.enable_auto_reconnect:
            return

        if self._reconnect_task and not self._reconnect_task.done():
            return  # 重连任务已在进行中

        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        """重连循环"""
        max_attempts = 5
        base_delay = 5.0

        for attempt in range(max_attempts):
            try:
                self.logger.info(f"尝试重连 (第 {attempt + 1} 次)...")

                # 断开现有连接
                await self.disconnect()

                # 等待一段时间
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)

                # 尝试重连
                success = await self.connect()
                if success:
                    self.logger.info("重连成功")
                    return

            except Exception as e:
                self.logger.error(f"重连失败: {str(e)}")

        self.logger.error("重连最终失败，停止尝试")
        self.status = ExchangeStatus.ERROR

    # === 限频控制 ===

    async def _check_rate_limit(self, operation: str) -> None:
        """
        检查限频

        Args:
            operation: 操作类型
        """
        if operation not in self.config.rate_limits:
            return

        limit_config = self.config.rate_limits[operation]
        max_requests = limit_config.get('max_requests', 100)
        time_window = limit_config.get('time_window', 60)  # 秒

        now = datetime.now()

        # 初始化时间戳列表
        if operation not in self._request_timestamps:
            self._request_timestamps[operation] = []

        timestamps = self._request_timestamps[operation]

        # 清理过期的时间戳
        cutoff_time = now - timedelta(seconds=time_window)
        timestamps[:] = [ts for ts in timestamps if ts > cutoff_time]

        # 检查是否超过限制
        if len(timestamps) >= max_requests:
            # 计算需要等待的时间
            oldest_timestamp = timestamps[0]
            wait_time = (oldest_timestamp +
                         timedelta(seconds=time_window) - now).total_seconds()

            if wait_time > 0:
                self.logger.warning(f"触发限频，等待 {wait_time:.2f} 秒")
                await asyncio.sleep(wait_time)

        # 记录当前请求时间戳
        timestamps.append(now)
        self._health_metrics['total_requests'] += 1

    # === 心跳检测 ===

    async def _start_heartbeat(self) -> None:
        """启动心跳检测"""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        """心跳检测循环"""
        while self.status in [ExchangeStatus.CONNECTED, ExchangeStatus.AUTHENTICATED]:
            try:
                await asyncio.sleep(self.config.heartbeat_interval)

                # 执行心跳检测
                await self._do_heartbeat()
                self._update_heartbeat()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"心跳检测异常: {str(e)}")
                if self._should_reconnect(e):
                    await self._attempt_reconnect()
                    break

    # === 事件处理 ===

    async def _emit_event(self, event: Event) -> None:
        """发出事件到事件总线"""
        if self.event_bus:
            await self.event_bus.publish(event)

    async def _handle_ticker_update(self, ticker_data: TickerData) -> None:
        """处理行情更新"""
        # 在新架构中简化事件处理
        self.logger.debug(f"行情更新: {ticker_data.symbol}@{self.config.exchange_id}, 价格: {ticker_data.last}")

    async def _handle_orderbook_update(self, orderbook_data: OrderBookData) -> None:
        """处理订单簿更新"""
        # 在新架构中简化事件处理
        self.logger.debug(f"订单簿更新: {orderbook_data.symbol}@{self.config.exchange_id}")

    async def _handle_order_update(self, order_data: OrderData) -> None:
        """处理订单更新"""
        # 在新架构中简化事件处理
        if order_data.status.value == 'filled':
            self.logger.info(f"订单已成交: {order_data.id}@{self.config.exchange_id}, 数量: {order_data.filled}")
        elif order_data.status.value == 'canceled':
            self.logger.info(f"订单已取消: {order_data.id}@{self.config.exchange_id}")
        else:
            self.logger.debug(f"订单状态更新: {order_data.id}@{self.config.exchange_id}, 状态: {order_data.status.value}")

    # === 抽象方法（子类必须实现） ===

    async def _do_connect(self) -> bool:
        """执行具体的连接逻辑"""
        raise NotImplementedError("子类必须实现 _do_connect 方法")

    async def _do_disconnect(self) -> None:
        """执行具体的断开连接逻辑"""
        raise NotImplementedError("子类必须实现 _do_disconnect 方法")

    async def _do_authenticate(self) -> bool:
        """执行具体的认证逻辑"""
        raise NotImplementedError("子类必须实现 _do_authenticate 方法")

    async def _do_health_check(self) -> Dict[str, Any]:
        """执行具体的健康检查逻辑"""
        raise NotImplementedError("子类必须实现 _do_health_check 方法")

    async def _do_heartbeat(self) -> None:
        """执行具体的心跳检测逻辑"""
        raise NotImplementedError("子类必须实现 _do_heartbeat 方法")

    async def _close_websocket(self) -> None:
        """关闭WebSocket连接"""
        # 子类可以重写此方法
        pass

    # === 工具方法 ===

    def _safe_decimal(self, value: Any, default: Decimal = Decimal('0')) -> Decimal:
        """安全转换为Decimal"""
        try:
            if value is None or value == '':
                return default
            return Decimal(str(value))
        except (ValueError, TypeError, Exception):
            return default

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        """安全转换为float"""
        try:
            if value is None:
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    def _safe_int(self, value: Any, default: int = 0) -> int:
        """安全转换为int"""
        try:
            if value is None:
                return default
            return int(value)
        except (ValueError, TypeError):
            return default
