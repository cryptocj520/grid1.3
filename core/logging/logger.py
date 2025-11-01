"""
MESA交易系统 - 统一日志模块核心实现

整合所有日志功能，提供：
- 基础日志器类
- 专用日志器（交易、数据、错误、系统等）
- 简单配置
- 文件和控制台输出
- 统一的日志格式
"""

import logging
import os
import json
import time
from typing import Dict, Any, Optional, Union, List
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler


class LogConfig:
    """日志配置类"""

    def __init__(self,
                 log_dir: str = "logs",
                 level: str = "INFO",
                 console_level: str = "INFO",
                 file_level: str = "DEBUG",
                 max_file_size: int = 5 * 1024 *
                 1024,  # 5MB (与 logging.yaml 保持一致)
                 backup_count: int = 3,  # 3个备份 (与 logging.yaml 保持一致)
                 enable_console: bool = True):  # 🔥 新增：是否启用控制台输出
        self.log_dir = log_dir
        self.level = getattr(logging, level.upper())
        self.console_level = getattr(logging, console_level.upper())
        self.file_level = getattr(logging, file_level.upper())
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        self.enable_console = enable_console

        # 确保日志目录存在
        Path(log_dir).mkdir(parents=True, exist_ok=True)


class BaseLogger:
    """基础日志器类"""

    def __init__(self, name: str, config: Optional[LogConfig] = None):
        self.name = name
        self.config = config or LogConfig()
        self.logger = logging.getLogger(name)
        self._setup_logger()

    def _setup_logger(self):
        """设置日志器"""
        self.logger.setLevel(self.config.level)

        # 清除现有处理器
        self.logger.handlers.clear()

        # 🔥 只有启用控制台输出时才添加控制台处理器
        if self.config.enable_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(self.config.console_level)
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)

        # 添加文件处理器（始终启用）
        log_file = os.path.join(self.config.log_dir, f"{self.name}.log")
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=self.config.max_file_size,
            backupCount=self.config.backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(self.config.file_level)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)

    def debug(self, message: str, **kwargs):
        """调试日志"""
        extra_info = f" | {self._format_extra(**kwargs)}" if kwargs else ""
        self.logger.debug(f"{message}{extra_info}")

    def info(self, message: str, **kwargs):
        """信息日志"""
        extra_info = f" | {self._format_extra(**kwargs)}" if kwargs else ""
        self.logger.info(f"{message}{extra_info}")

    def warning(self, message: str, **kwargs):
        """警告日志"""
        extra_info = f" | {self._format_extra(**kwargs)}" if kwargs else ""
        self.logger.warning(f"{message}{extra_info}")

    def error(self, message: str, **kwargs):
        """错误日志"""
        extra_info = f" | {self._format_extra(**kwargs)}" if kwargs else ""
        self.logger.error(f"{message}{extra_info}")

    def critical(self, message: str, **kwargs):
        """严重错误日志"""
        extra_info = f" | {self._format_extra(**kwargs)}" if kwargs else ""
        self.logger.critical(f"{message}{extra_info}")

    def _format_extra(self, **kwargs) -> str:
        """格式化额外信息"""
        return " | ".join([f"{k}={v}" for k, v in kwargs.items()])


class SystemLogger(BaseLogger):
    """系统日志器"""

    def __init__(self, config: Optional[LogConfig] = None):
        super().__init__("system", config)

    def startup(self, component: str, version: str = "", **kwargs):
        """记录组件启动"""
        self.info(f"🚀 组件启动: {component} {version}",
                  component=component, version=version, **kwargs)

    def shutdown(self, component: str, reason: str = "", **kwargs):
        """记录组件关闭"""
        self.info(f"🛑 组件关闭: {component} ({reason})",
                  component=component, reason=reason, **kwargs)

    def config_change(self, component: str, key: str, old_value: Any, new_value: Any):
        """记录配置变更"""
        self.info(f"⚙️ 配置变更: {component}.{key} {old_value} -> {new_value}")


class TradingLogger(BaseLogger):
    """交易日志器"""

    def __init__(self, config: Optional[LogConfig] = None):
        super().__init__("trading", config)

    def order_placed(self, exchange: str, symbol: str, side: str, amount: float, price: float, **kwargs):
        """记录下单"""
        self.info(f"📝 下单: {exchange} {symbol} {side} {amount}@{price}",
                  exchange=exchange, symbol=symbol, side=side, amount=amount, price=price, **kwargs)

    def order_filled(self, exchange: str, symbol: str, order_id: str, filled_amount: float, **kwargs):
        """记录成交"""
        self.info(f"✅ 成交: {exchange} {symbol} {order_id} {filled_amount}",
                  exchange=exchange, symbol=symbol, order_id=order_id, filled_amount=filled_amount, **kwargs)

    def arbitrage_opportunity(self, buy_exchange: str, sell_exchange: str, symbol: str, profit: float, **kwargs):
        """记录套利机会"""
        self.info(f"💰 套利机会: {symbol} {buy_exchange}->{sell_exchange} 利润:{profit:.4f}",
                  symbol=symbol, buy_exchange=buy_exchange, sell_exchange=sell_exchange, profit=profit, **kwargs)

    def trade(self, action: str, symbol: str, amount: float, **kwargs):
        """记录交易行为（向后兼容）"""
        self.info(f"📊 交易: {action} {symbol} {amount}",
                  action=action, symbol=symbol, amount=amount, **kwargs)


class DataLogger(BaseLogger):
    """数据日志器"""

    def __init__(self, config: Optional[LogConfig] = None):
        super().__init__("data", config)

    def price_update(self, exchange: str, symbol: str, bid: float, ask: float, **kwargs):
        """记录价格更新"""
        self.debug(f"📊 价格更新: {exchange} {symbol} bid:{bid} ask:{ask}",
                   exchange=exchange, symbol=symbol, bid=bid, ask=ask, **kwargs)

    def websocket_connected(self, exchange: str, **kwargs):
        """记录WebSocket连接"""
        self.info(f"🔌 WebSocket连接: {exchange}", exchange=exchange, **kwargs)

    def websocket_disconnected(self, exchange: str, reason: str = "", **kwargs):
        """记录WebSocket断开"""
        self.warning(f"❌ WebSocket断开: {exchange} ({reason})",
                     exchange=exchange, reason=reason, **kwargs)


class ErrorLogger(BaseLogger):
    """错误日志器"""

    def __init__(self, config: Optional[LogConfig] = None):
        super().__init__("error", config)

    def exception(self, error: Exception, context: str = "", **kwargs):
        """记录异常"""
        self.error(f"⚠️ 异常: {context} {type(error).__name__}: {str(error)}",
                   error_type=type(error).__name__, error_message=str(error), context=context, **kwargs)

    def api_error(self, exchange: str, endpoint: str, status_code: int, error_message: str, **kwargs):
        """记录API错误"""
        self.error(f"🔴 API错误: {exchange} {endpoint} {status_code} {error_message}",
                   exchange=exchange, endpoint=endpoint, status_code=status_code, error_message=error_message, **kwargs)

    def connection_error(self, exchange: str, error_type: str, error_message: str, **kwargs):
        """记录连接错误"""
        self.error(f"🚫 连接错误: {exchange} {error_type} {error_message}",
                   exchange=exchange, error_type=error_type, error_message=error_message, **kwargs)


class ExchangeLogger(BaseLogger):
    """交易所日志器"""

    def __init__(self, exchange_name: str, config: Optional[LogConfig] = None):
        super().__init__(f"exchange.{exchange_name}", config)
        self.exchange_name = exchange_name

    def adapter_start(self, **kwargs):
        """记录适配器启动"""
        self.info(f"🏪 {self.exchange_name} 适配器启动",
                  exchange=self.exchange_name, **kwargs)

    def adapter_stop(self, reason: str = "", **kwargs):
        """记录适配器停止"""
        self.info(f"🛑 {self.exchange_name} 适配器停止 ({reason})",
                  exchange=self.exchange_name, reason=reason, **kwargs)

    def rate_limit(self, endpoint: str, wait_time: float, **kwargs):
        """记录限流"""
        self.warning(f"⏰ {self.exchange_name} 限流: {endpoint} 等待{wait_time}s",
                     exchange=self.exchange_name, endpoint=endpoint, wait_time=wait_time, **kwargs)


class PerformanceLogger(BaseLogger):
    """性能日志器"""

    def __init__(self, config: Optional[LogConfig] = None):
        super().__init__("performance", config)

    def execution_time(self, function_name: str, duration: float, **kwargs):
        """记录执行时间"""
        if duration > 1.0:  # 只记录超过1秒的操作
            self.info(f"⏱️ 执行时间: {function_name} {duration:.3f}s",
                      function=function_name, duration=duration, **kwargs)

    def memory_usage(self, component: str, memory_mb: float, **kwargs):
        """记录内存使用"""
        if memory_mb > 100:  # 只记录超过100MB的组件
            self.info(f"💾 内存使用: {component} {memory_mb:.1f}MB",
                      component=component, memory_mb=memory_mb, **kwargs)


# 全局日志器实例缓存
_loggers: Dict[str, BaseLogger] = {}
_config: Optional[LogConfig] = None


def get_config() -> LogConfig:
    """获取全局日志配置"""
    global _config
    if _config is None:
        _config = LogConfig()
    return _config


def set_config(config: LogConfig):
    """设置全局日志配置"""
    global _config
    _config = config


def get_logger(name: str) -> BaseLogger:
    """获取通用日志器"""
    if name not in _loggers:
        _loggers[name] = BaseLogger(name, get_config())
    return _loggers[name]


def get_system_logger(name: str = "system") -> SystemLogger:
    """获取系统日志器"""
    logger_key = f"system.{name}" if name != "system" else "system"
    if logger_key not in _loggers:
        _loggers[logger_key] = SystemLogger(get_config())
        # 如果有自定义名称，修改内部日志器的名称
        if name != "system":
            _loggers[logger_key].logger.name = logger_key
    return _loggers[logger_key]


def get_trading_logger() -> TradingLogger:
    """获取交易日志器"""
    if "trading" not in _loggers:
        _loggers["trading"] = TradingLogger(get_config())
    return _loggers["trading"]


def get_data_logger(name: str = "data") -> DataLogger:
    """获取数据日志器"""
    logger_key = f"data.{name}" if name != "data" else "data"
    if logger_key not in _loggers:
        _loggers[logger_key] = DataLogger(get_config())
        # 如果有自定义名称，修改内部日志器的名称
        if name != "data":
            _loggers[logger_key].logger.name = logger_key
    return _loggers[logger_key]


def get_error_logger() -> ErrorLogger:
    """获取错误日志器"""
    if "error" not in _loggers:
        _loggers["error"] = ErrorLogger(get_config())
    return _loggers["error"]


def get_exchange_logger(exchange_name: str) -> ExchangeLogger:
    """获取交易所日志器"""
    key = f"exchange.{exchange_name}"
    if key not in _loggers:
        _loggers[key] = ExchangeLogger(exchange_name, get_config())
    return _loggers[key]


def get_performance_logger() -> PerformanceLogger:
    """获取性能日志器"""
    if "performance" not in _loggers:
        _loggers["performance"] = PerformanceLogger(get_config())
    return _loggers["performance"]


def initialize_logging(log_dir: str = "logs", level: str = "INFO", enable_console: bool = True) -> bool:
    """初始化日志系统

    Args:
        log_dir: 日志目录
        level: 日志级别
        enable_console: 是否启用控制台输出（默认True）
    """
    try:
        config = LogConfig(log_dir=log_dir, level=level,
                           enable_console=enable_console)
        set_config(config)

        # 清理已有实例，使用新配置
        _loggers.clear()

        # 获取系统日志器并记录启动
        system_logger = get_system_logger()
        system_logger.startup("UnifiedLoggingSystem", "v3.0")

        return True
    except Exception as e:
        print(f"Failed to initialize logging: {e}")
        return False


def shutdown_logging():
    """关闭日志系统"""
    try:
        system_logger = get_system_logger()
        system_logger.shutdown("UnifiedLoggingSystem", "正常关闭")

        # 关闭所有处理器
        for logger in _loggers.values():
            for handler in logger.logger.handlers:
                handler.close()

        _loggers.clear()
    except Exception as e:
        print(f"Failed to shutdown logging: {e}")


def get_health_status() -> Dict[str, Any]:
    """获取日志系统健康状态"""
    return {
        "status": "healthy",
        "version": "v3.0",
        "active_loggers": len(_loggers),
        "config": {
            "log_dir": get_config().log_dir,
            "level": logging.getLevelName(get_config().level)
        }
    }
