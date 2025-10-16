"""
MESA交易系统 - 统一日志入口 (v3.0)

这是日志系统的统一入口，提供：
- 简洁的API接口
- 自动初始化
- 完整的日志功能

推荐使用方式：
    from core.logging import get_logger, get_system_logger, get_trading_logger
    
    # 通用日志器
    logger = get_logger(__name__)
    logger.info("这是一条信息日志")
    
    # 系统日志器
    system_logger = get_system_logger("MyService")
    system_logger.startup("MyService", "1.0")
    
    # 交易日志器
    trading_logger = get_trading_logger()
    trading_logger.trade("买入", "BTC", 1.0)
"""

import os
from typing import Dict, Any

# 直接导入logger.py中的所有功能，重命名避免冲突
from .logger import (
    # 配置类
    LogConfig,
    
    # 核心日志器类
    BaseLogger,
    SystemLogger,
    TradingLogger,
    DataLogger,
    ErrorLogger,
    ExchangeLogger,
    PerformanceLogger,
    
    # 配置管理
    get_config,
    set_config,
    
    # 便捷函数（重命名避免冲突）
    get_logger as _get_logger,
    get_system_logger as _get_system_logger,
    get_trading_logger as _get_trading_logger,
    get_data_logger as _get_data_logger,
    get_error_logger as _get_error_logger,
    get_exchange_logger as _get_exchange_logger,
    get_performance_logger as _get_performance_logger,
    
    # 生命周期管理
    initialize_logging,
    shutdown_logging,
    
    # 健康状态
    get_health_status
)

# 自动初始化标记
_auto_initialized = False


def _ensure_initialized():
    """确保日志系统已初始化"""
    global _auto_initialized
    if not _auto_initialized:
        # 检查是否有配置文件
        config_path = "config/logging.yaml"
        if os.path.exists(config_path):
            # 使用默认配置（YAML配置暂不支持）
            initialize_logging()
        else:
            # 使用默认配置
            initialize_logging()
        _auto_initialized = True


# 统一入口函数 - 自动初始化
def get_logger(name: str) -> BaseLogger:
    """获取通用日志器（自动初始化）"""
    _ensure_initialized()
    return _get_logger(name)


def get_system_logger(name: str = "system") -> SystemLogger:
    """获取系统日志器（自动初始化）"""
    _ensure_initialized()
    return _get_system_logger(name)


def get_trading_logger() -> TradingLogger:
    """获取交易日志器（自动初始化）"""
    _ensure_initialized()
    return _get_trading_logger()


def get_data_logger(name: str = "data") -> DataLogger:
    """获取数据日志器（自动初始化）"""
    _ensure_initialized()
    return _get_data_logger(name)


def get_error_logger() -> ErrorLogger:
    """获取错误日志器（自动初始化）"""
    _ensure_initialized()
    return _get_error_logger()


def get_exchange_logger(exchange_name: str) -> ExchangeLogger:
    """获取交易所日志器（自动初始化）"""
    _ensure_initialized()
    return _get_exchange_logger(exchange_name)


def get_performance_logger() -> PerformanceLogger:
    """获取性能日志器（自动初始化）"""
    _ensure_initialized()
    return _get_performance_logger()


# 向后兼容的别名
initialize = initialize_logging
shutdown = shutdown_logging


# 导出所有核心功能
__all__ = [
    # 配置类
    'LogConfig',
    
    # 核心日志器类
    'BaseLogger',
    'SystemLogger',
    'TradingLogger',
    'DataLogger',
    'ErrorLogger',
    'ExchangeLogger',
    'PerformanceLogger',
    
    # 便捷函数（推荐使用）
    'get_logger',
    'get_system_logger',
    'get_trading_logger',
    'get_data_logger',
    'get_error_logger',
    'get_exchange_logger',
    'get_performance_logger',
    
    # 生命周期管理
    'initialize_logging',
    'shutdown_logging',
    
    # 向后兼容别名
    'initialize',
    'shutdown',
    
    # 配置管理
    'get_config',
    'set_config',
    
    # 健康状态
    'get_health_status'
] 