"""
交易所管理器

负责统一管理所有交易所适配器的生命周期，包括：
- 适配器注册和配置
- 连接管理和健康监控
- 批量操作和状态同步
- 事件协调和错误处理
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set, Callable
from dataclasses import dataclass

# 使用简化的统一日志入口
from ...logging import get_system_logger

from ...services.events import Event, ComponentStoppedEvent
# 事件类型在新架构中简化处理
from .interface import ExchangeInterface, ExchangeConfig, ExchangeStatus
from .factory import ExchangeFactory, get_exchange_factory
from .models import ExchangeType


@dataclass
class ExchangeManagerConfig:
    """交易所管理器配置"""
    health_check_interval: int = 60        # 健康检查间隔（秒）
    connection_timeout: int = 30           # 连接超时（秒）
    max_concurrent_connections: int = 10   # 最大并发连接数
    auto_reconnect: bool = True            # 自动重连
    startup_delay: float = 1.0             # 启动延迟（秒）
    shutdown_timeout: int = 30             # 关闭超时（秒）


class ExchangeManager:
    """
    交易所管理器

    提供统一的交易所适配器管理功能：
    - 生命周期管理（启动、停止、重连）
    - 健康监控和状态管理
    - 批量操作和事件协调
    - 错误处理和恢复
    """

    def __init__(
        self,
        event_bus: Optional[Any] = None,
        factory: Optional[ExchangeFactory] = None,
        config: Optional[ExchangeManagerConfig] = None
    ):
        """
        初始化交易所管理器

        Args:
            event_bus: 事件总线实例
            factory: 交易所工厂实例
            config: 管理器配置
        """
        self.event_bus = event_bus
        self.factory = factory or get_exchange_factory()
        self.config = config or ExchangeManagerConfig()
        self.logger = get_system_logger()

        # 交易所适配器管理
        self._adapters: Dict[str, ExchangeInterface] = {}
        self._adapter_configs: Dict[str, ExchangeConfig] = {}
        self._startup_order: List[str] = []

        # 状态管理
        self._running = False
        self._starting_up = False
        self._shutting_down = False

        # 监控任务
        self._health_check_task: Optional[asyncio.Task] = None
        self._connection_monitor_task: Optional[asyncio.Task] = None

        # 连接信号量
        self._connection_semaphore = asyncio.Semaphore(
            self.config.max_concurrent_connections)

        # 事件回调
        self._event_callbacks: Dict[str, List[Callable]] = {}

        self.logger.info("交易所管理器初始化完成")

    # === 适配器注册和配置 ===

    def register_exchange(
        self,
        exchange_id: str,
        config: ExchangeConfig,
        priority: int = 0
    ) -> None:
        """
        注册交易所适配器

        Args:
            exchange_id: 交易所ID
            config: 交易所配置
            priority: 启动优先级（数字越小优先级越高）
        """
        if exchange_id in self._adapter_configs:
            self.logger.warning(f"交易所已注册，覆盖配置: {exchange_id}")

        self._adapter_configs[exchange_id] = config

        # 更新启动顺序
        self._update_startup_order(exchange_id, priority)

        self.logger.info(f"注册交易所: {exchange_id} (优先级: {priority})")

    def unregister_exchange(self, exchange_id: str) -> None:
        """
        注销交易所适配器

        Args:
            exchange_id: 交易所ID
        """
        if exchange_id in self._adapter_configs:
            del self._adapter_configs[exchange_id]

        if exchange_id in self._adapters:
            del self._adapters[exchange_id]

        if exchange_id in self._startup_order:
            self._startup_order.remove(exchange_id)

        self.logger.info(f"注销交易所: {exchange_id}")

    def _update_startup_order(self, exchange_id: str, priority: int) -> None:
        """更新启动顺序"""
        # 移除现有条目
        if exchange_id in self._startup_order:
            self._startup_order.remove(exchange_id)

        # 插入到正确位置（按优先级排序）
        inserted = False
        for i, existing_id in enumerate(self._startup_order):
            existing_config = self._adapter_configs.get(existing_id)
            existing_priority = getattr(existing_config, 'priority', 0)

            if priority < existing_priority:
                self._startup_order.insert(i, exchange_id)
                inserted = True
                break

        if not inserted:
            self._startup_order.append(exchange_id)

    # === 生命周期管理 ===

    async def start(self) -> None:
        """启动所有交易所适配器"""
        if self._running or self._starting_up:
            self.logger.warning("交易所管理器已在运行或正在启动")
            return

        self._starting_up = True

        try:
            self.logger.info("开始启动交易所管理器...")

            # 创建适配器实例
            await self._create_adapters()

            # 按优先级启动适配器
            await self._start_adapters()

            # 启动监控任务
            await self._start_monitoring()

            self._running = True
            self._starting_up = False

            self.logger.info(f"交易所管理器启动完成，管理 {len(self._adapters)} 个交易所")

            # 记录启动事件
            self.logger.info(f"交易所管理器启动成功，管理交易所: {list(self._adapters.keys())}")

        except Exception as e:
            self._starting_up = False
            self.logger.error(f"启动交易所管理器失败: {str(e)}")

            # 清理已启动的适配器
            await self._cleanup_adapters()

            # 记录错误事件
            self.logger.error(f"交易所管理器启动失败: {e}")

            raise

    async def stop(self) -> None:
        """停止所有交易所适配器"""
        if not self._running or self._shutting_down:
            self.logger.warning("交易所管理器未运行或正在关闭")
            return

        self._shutting_down = True

        try:
            self.logger.info("开始停止交易所管理器...")

            # 停止监控任务
            await self._stop_monitoring()

            # 停止所有适配器
            await self._stop_adapters()

            # 清理资源
            await self._cleanup_adapters()

            self._running = False
            self._shutting_down = False

            self.logger.info("交易所管理器停止完成")

            # 发出停止事件
            if self.event_bus:
                await self.event_bus.publish(ComponentStoppedEvent(
                    component="exchange_manager",
                    timestamp=datetime.now(),
                    metadata={"total_exchanges": len(self._adapters)}
                ))

        except Exception as e:
            self._shutting_down = False
            self.logger.error(f"停止交易所管理器失败: {str(e)}")
            raise

    async def restart(self, exchange_id: Optional[str] = None) -> None:
        """
        重启交易所适配器

        Args:
            exchange_id: 交易所ID，None表示重启所有
        """
        if exchange_id:
            self.logger.info(f"重启交易所: {exchange_id}")
            await self._restart_single_adapter(exchange_id)
        else:
            self.logger.info("重启所有交易所")
            await self.stop()
            await self.start()

    async def _create_adapters(self) -> None:
        """创建适配器实例"""
        for exchange_id in self._startup_order:
            config = self._adapter_configs[exchange_id]

            try:
                adapter = self.factory.create_adapter(
                    exchange_id,
                    config,
                    self.event_bus
                )
                self._adapters[exchange_id] = adapter

                self.logger.info(f"创建适配器实例: {exchange_id}")

            except Exception as e:
                self.logger.error(f"创建适配器实例失败 {exchange_id}: {str(e)}")
                raise

    async def _start_adapters(self) -> None:
        """启动适配器"""
        startup_tasks = []

        for exchange_id in self._startup_order:
            if exchange_id in self._adapters:
                task = self._start_single_adapter(exchange_id)
                startup_tasks.append(task)

                # 添加启动延迟
                if self.config.startup_delay > 0:
                    await asyncio.sleep(self.config.startup_delay)

        # 等待所有启动任务完成
        if startup_tasks:
            await asyncio.gather(*startup_tasks, return_exceptions=True)

    async def _start_single_adapter(self, exchange_id: str) -> None:
        """启动单个适配器"""
        async with self._connection_semaphore:
            adapter = self._adapters.get(exchange_id)
            if not adapter:
                return

            try:
                self.logger.info(f"启动交易所适配器: {exchange_id}")

                # 连接
                success = await asyncio.wait_for(
                    adapter.connect(),
                    timeout=self.config.connection_timeout
                )

                if success:
                    # 认证
                    await adapter.authenticate()
                    self.logger.info(f"交易所适配器启动成功: {exchange_id}")
                else:
                    self.logger.error(f"交易所适配器连接失败: {exchange_id}")

            except asyncio.TimeoutError:
                self.logger.error(f"交易所适配器连接超时: {exchange_id}")
            except Exception as e:
                self.logger.error(f"启动交易所适配器异常 {exchange_id}: {str(e)}")

    async def _stop_adapters(self) -> None:
        """停止适配器"""
        stop_tasks = []

        # 按逆序停止（后启动的先停止）
        for exchange_id in reversed(self._startup_order):
            if exchange_id in self._adapters:
                task = self._stop_single_adapter(exchange_id)
                stop_tasks.append(task)

        # 等待所有停止任务完成
        if stop_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*stop_tasks, return_exceptions=True),
                    timeout=self.config.shutdown_timeout
                )
            except asyncio.TimeoutError:
                self.logger.warning("部分适配器停止超时")

    async def _stop_single_adapter(self, exchange_id: str) -> None:
        """停止单个适配器"""
        adapter = self._adapters.get(exchange_id)
        if not adapter:
            return

        try:
            self.logger.info(f"停止交易所适配器: {exchange_id}")
            await adapter.disconnect()

        except Exception as e:
            self.logger.error(f"停止交易所适配器异常 {exchange_id}: {str(e)}")

    async def _restart_single_adapter(self, exchange_id: str) -> None:
        """重启单个适配器"""
        if exchange_id not in self._adapters:
            self.logger.warning(f"适配器不存在: {exchange_id}")
            return

        try:
            # 停止
            await self._stop_single_adapter(exchange_id)

            # 等待一段时间
            await asyncio.sleep(2.0)

            # 启动
            await self._start_single_adapter(exchange_id)

            self.logger.info(f"重启交易所适配器完成: {exchange_id}")

        except Exception as e:
            self.logger.error(f"重启交易所适配器失败 {exchange_id}: {str(e)}")

    async def _cleanup_adapters(self) -> None:
        """清理适配器资源"""
        self._adapters.clear()

    # === 监控和健康检查 ===

    async def _start_monitoring(self) -> None:
        """启动监控任务"""
        self._health_check_task = asyncio.create_task(
            self._health_check_loop())
        self._connection_monitor_task = asyncio.create_task(
            self._connection_monitor_loop())

    async def _stop_monitoring(self) -> None:
        """停止监控任务"""
        if self._health_check_task:
            self._health_check_task.cancel()
            self._health_check_task = None

        if self._connection_monitor_task:
            self._connection_monitor_task.cancel()
            self._connection_monitor_task = None

    async def _health_check_loop(self) -> None:
        """健康检查循环"""
        while self._running:
            try:
                await asyncio.sleep(self.config.health_check_interval)
                await self._perform_health_checks()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"健康检查异常: {str(e)}")

    async def _perform_health_checks(self) -> None:
        """执行健康检查"""
        health_tasks = []

        for exchange_id, adapter in self._adapters.items():
            task = self._check_adapter_health(exchange_id, adapter)
            health_tasks.append(task)

        if health_tasks:
            results = await asyncio.gather(*health_tasks, return_exceptions=True)

            # 处理健康检查结果
            for i, (exchange_id, result) in enumerate(zip(self._adapters.keys(), results)):
                if isinstance(result, Exception):
                    self.logger.error(f"健康检查失败 {exchange_id}: {str(result)}")
                elif result and result.get('status') not in ['healthy', 'connected', 'ok', 'authenticated']:
                    self.logger.warning(f"交易所状态异常 {exchange_id}: {result}")

    async def _check_adapter_health(self, exchange_id: str, adapter: ExchangeInterface) -> Dict[str, Any]:
        """检查单个适配器健康状态"""
        try:
            health_data = await adapter.health_check()
            return health_data

        except Exception as e:
            self.logger.error(f"适配器健康检查异常 {exchange_id}: {str(e)}")
            return {'status': 'error', 'error': str(e)}

    async def _connection_monitor_loop(self) -> None:
        """连接监控循环"""
        while self._running:
            try:
                await asyncio.sleep(30)  # 每30秒检查一次
                await self._monitor_connections()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"连接监控异常: {str(e)}")

    async def _monitor_connections(self) -> None:
        """监控连接状态"""
        for exchange_id, adapter in self._adapters.items():
            if not self._check_adapter_connected(adapter) and self.config.auto_reconnect:
                self.logger.warning(f"检测到连接断开，尝试重连: {exchange_id}")
                asyncio.create_task(self._restart_single_adapter(exchange_id))

    # === 查询接口 ===

    def get_adapter(self, exchange_id: str) -> Optional[ExchangeInterface]:
        """获取适配器实例"""
        return self._adapters.get(exchange_id)

    def get_all_adapters(self) -> Dict[str, ExchangeInterface]:
        """获取所有适配器实例"""
        return self._adapters.copy()

    def get_connected_adapters(self) -> Dict[str, ExchangeInterface]:
        """获取已连接的适配器"""
        return {
            exchange_id: adapter
            for exchange_id, adapter in self._adapters.items()
            if self._check_adapter_connected(adapter)
        }
    
    def _check_adapter_connected(self, adapter: ExchangeInterface) -> bool:
        """安全检查适配器是否连接"""
        try:
            # 检查is_connected是方法还是属性
            if callable(getattr(adapter, 'is_connected', None)):
                return adapter.is_connected()
            elif hasattr(adapter, 'is_connected'):
                return bool(adapter.is_connected)
            else:
                # 回退到状态检查
                return adapter.get_status() in ['connected', 'authenticated']
        except Exception as e:
            self.logger.error(f"检查适配器连接状态时出错: {e}")
            return False

    def get_adapter_status(self, exchange_id: str) -> Optional[ExchangeStatus]:
        """获取适配器状态"""
        adapter = self._adapters.get(exchange_id)
        return adapter.get_status() if adapter else None

    def get_all_statuses(self) -> Dict[str, ExchangeStatus]:
        """获取所有适配器状态"""
        return {
            exchange_id: adapter.get_status()
            for exchange_id, adapter in self._adapters.items()
        }

    def is_running(self) -> bool:
        """检查管理器是否正在运行"""
        return self._running

    def get_registered_exchanges(self) -> List[str]:
        """获取已注册的交易所列表"""
        return list(self._adapter_configs.keys())
    
    def get_configured_exchanges(self) -> List[str]:
        """获取所有配置的交易所列表（包括未连接的）"""
        return list(self._adapter_configs.keys())

    def get_active_exchanges(self) -> List[str]:
        """获取活跃的交易所列表"""
        return [
            exchange_id for exchange_id, adapter in self._adapters.items()
            if self._check_adapter_connected(adapter)
        ]

    # === 批量操作 ===

    async def connect_all(self) -> Dict[str, bool]:
        """连接所有交易所"""
        results = {}

        for exchange_id, adapter in self._adapters.items():
            try:
                success = await adapter.connect()
                results[exchange_id] = success
            except Exception as e:
                self.logger.error(f"连接失败 {exchange_id}: {str(e)}")
                results[exchange_id] = False

        return results

    async def disconnect_all(self) -> None:
        """断开所有交易所连接"""
        tasks = []

        for exchange_id, adapter in self._adapters.items():
            task = adapter.disconnect()
            tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def health_check_all(self) -> Dict[str, Dict[str, Any]]:
        """对所有交易所进行健康检查"""
        results = {}

        health_tasks = []
        exchange_ids = list(self._adapters.keys())

        for exchange_id, adapter in self._adapters.items():
            task = adapter.health_check()
            health_tasks.append(task)

        if health_tasks:
            health_results = await asyncio.gather(*health_tasks, return_exceptions=True)

            for exchange_id, result in zip(exchange_ids, health_results):
                if isinstance(result, Exception):
                    results[exchange_id] = {
                        'status': 'error', 'error': str(result)}
                else:
                    results[exchange_id] = result

        return results

    # === 上下文管理器支持 ===

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.stop()
