"""
统一系统启动器

消除重复初始化代码，提供统一的服务生命周期管理
支持多种启动模式：API、监控、混合模式
"""

import asyncio
import signal
import sys
import time
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, Any

# 使用统一的日志入口
from .logging import get_system_logger, initialize

from .di.container import get_container, DIContainer
from .services.interfaces.monitoring_service import MonitoringService


class StartupMode(Enum):
    """启动模式枚举"""
    API = "api"           # 纯API服务器模式
    MONITOR = "monitor"   # 纯监控系统模式  
    HYBRID = "hybrid"     # 混合模式（API + 监控）


class SystemLauncher:
    """统一系统启动器"""
    
    def __init__(self, mode: StartupMode = StartupMode.HYBRID):
        self.mode = mode
        self.container: Optional[DIContainer] = None
        self.monitoring_service: Optional[MonitoringService] = None
        
        # 使用统一日志入口
        self.logger = get_system_logger("SystemLauncher")
        
        # 状态管理
        self.services_started = False
        self.start_time: Optional[datetime] = None
        self.running = False
    
    async def initialize_services(self) -> bool:
        """统一的服务初始化逻辑"""
        try:
            self.logger.info(f"🚀 初始化系统服务 - 模式: {self.mode.value}")
            
            # 1. 确保统一日志系统已初始化
            log_success = initialize()
            if not log_success:
                self.logger.error("❌ 统一日志系统初始化失败")
                return False
            
            self.logger.info("✅ 统一日志系统已就绪")
            
            # 2. 初始化DI容器
            self.container = get_container()
            self.logger.info("✅ DI容器初始化成功")
            
            # 3. 根据模式初始化监控服务
            if self.mode in [StartupMode.MONITOR, StartupMode.HYBRID]:
                self.monitoring_service = self.container.get(MonitoringService)
                self.logger.info("✅ 监控服务已注入", component="SystemLauncher")
            
            self.services_started = True
            self.start_time = datetime.now()
            self.logger.info(f"✅ 系统服务初始化完成 - 模式: {self.mode.value}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 服务初始化失败: {e}")
            return False
    
    async def start_monitoring_service(self) -> bool:
        """启动监控服务"""
        if not self.monitoring_service:
            self.logger.warning("监控服务未初始化")
            return False
            
        try:
            self.logger.info("启动监控服务...")
            success = await self.monitoring_service.start()
            
            if success:
                self.logger.info("✅ 监控服务启动成功")
                return True
            else:
                self.logger.error("❌ 监控服务启动失败")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 监控服务启动异常: {e}")
            return False
    
    async def stop_services(self) -> None:
        """统一的服务停止逻辑"""
        if not self.services_started:
            return
            
        self.logger.info("🛑 正在停止系统服务...")
        self.running = False
        
        try:
            # 停止监控服务
            if self.monitoring_service:
                await self.monitoring_service.stop()
                self.logger.info("✅ 监控服务已停止")
            
            # 记录运行时间
            if self.start_time:
                uptime = (datetime.now() - self.start_time).total_seconds()
                self.logger.info(f"⏱️ 系统运行时间: {uptime:.1f}秒")
            
            self.services_started = False
            self.logger.info("✅ 系统服务已完全停止")
            
        except Exception as e:
            self.logger.error(f"❌ 停止服务时出现异常: {e}")
    
    def get_system_info(self) -> Dict[str, Any]:
        """获取系统信息"""
        return {
            "mode": self.mode.value,
            "services_started": self.services_started,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "uptime": (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
            "has_monitoring": self.monitoring_service is not None,
            "unified_logging": True  # 统一日志系统始终可用
        }
    
    def print_startup_banner(self, extra_info: Optional[Dict[str, Any]] = None):
        """打印启动横幅"""
        print("\n" + "="*70)
        print("🚀 交易策略系统平台 - 统一启动器")
        print("="*70)
        print(f"📋 启动模式: {self.mode.value.upper()}")
        print(f"📊 系统状态:")
        print(f"   - DI容器: ✅ 已初始化")
        print(f"   - 日志服务: ✅ 已启动")
        
        if self.mode in [StartupMode.MONITOR, StartupMode.HYBRID]:
            print(f"   - 监控服务: ✅ 已启动")
        
        print()
        print("🔗 服务端点:")
        
        if self.mode in [StartupMode.API, StartupMode.HYBRID]:
            print(f"   - API网关: http://localhost:8000")
            print(f"   - API文档: http://localhost:8000/docs")
        
        if self.mode in [StartupMode.MONITOR, StartupMode.HYBRID]:
            print(f"   - SocketIO服务: ws://localhost:8765")
            print(f"   - Web控制台: http://localhost:5173")
        
        print()
        print("📝 日志文件:")
        print(f"   - 系统日志: logs/trading_system.log")
        print(f"   - 错误日志: logs/error.log")
        
        if extra_info:
            print()
            print("📊 额外信息:")
            for key, value in extra_info.items():
                print(f"   - {key}: {value}")
        
        print()
        print("🎯 控制说明:")
        print("   - 停止系统: Ctrl+C")
        if self.mode == StartupMode.MONITOR:
            print("   - 终端客户端: python3 terminal_monitor.py")
        print("="*70)
        print()
    
    async def run_monitoring_loop(self):
        """运行监控循环"""
        self.running = True
        
        # 设置信号处理
        def signal_handler(signum, frame):
            self.logger.info(f"接收到停止信号 ({signum})")
            self.running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            loop_count = 0
            while self.running:
                await asyncio.sleep(5)
                loop_count += 1
                
                # 定期显示状态信息 
                if loop_count % 12 == 0:  # 每分钟显示一次 (12 * 5秒)
                    await self._show_periodic_stats()
                    
        except KeyboardInterrupt:
            self.logger.info("用户中断程序")
        except Exception as e:
            self.logger.error(f"监控循环异常: {e}")
        finally:
            await self.stop_services()
    
    async def _show_periodic_stats(self):
        """显示定期统计信息"""
        try:
            if self.monitoring_service:
                stats = await self.monitoring_service.get_stats()
                self.logger.info(
                    f"运行状态 - 连接交易所: {stats.connected_exchanges}, "
                    f"总消息: {stats.total_messages}, "
                    f"错误: {stats.errors}"
                )
        except Exception as e:
            self.logger.warning(f"获取统计信息失败: {e}")
    
    # 便捷启动方法
    async def start_api_server_mode(self):
        """启动API服务器模式"""
        self.mode = StartupMode.API
        
        if not await self.initialize_services():
            raise RuntimeError("服务初始化失败")
        
        self.print_startup_banner({
            "API模式": "仅提供HTTP API服务",
            "监控服务": "通过API调用"
        })
        
        return True
    
    async def start_monitor_daemon_mode(self):
        """启动监控守护进程模式"""
        self.mode = StartupMode.MONITOR
        
        if not await self.initialize_services():
            raise RuntimeError("服务初始化失败")
        
        if not await self.start_monitoring_service():
            raise RuntimeError("监控服务启动失败")
        
        self.print_startup_banner({
            "监控模式": "后台数据采集和分析",
            "数据源": "EdgeX + Backpack + Hyperliquid"
        })
        
        # 运行监控循环
        await self.run_monitoring_loop()
        
        return True
    
    async def start_hybrid_mode(self):
        """启动混合模式"""
        self.mode = StartupMode.HYBRID
        
        if not await self.initialize_services():
            raise RuntimeError("服务初始化失败")
        
        if not await self.start_monitoring_service():
            raise RuntimeError("监控服务启动失败")
        
        self.print_startup_banner({
            "混合模式": "API服务 + 监控守护进程",
            "完整功能": "所有功能可用"
        })
        
        return True 