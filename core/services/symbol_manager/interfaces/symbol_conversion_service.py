"""
符号转换服务接口

统一处理所有交易所的符号格式转换，消除架构冗余
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from enum import Enum


class SymbolFormat(Enum):
    """符号格式枚举"""
    STANDARD = "standard"          # 系统标准格式：BTC-USDC-PERP
    HYPERLIQUID = "hyperliquid"    # Hyperliquid格式：BTC/USDC:PERP
    BACKPACK = "backpack"          # Backpack格式：BTC_USDC_PERP
    EDGEX = "edgex"               # EdgeX格式：BTC_USDT_PERP
    BINANCE = "binance"           # Binance格式：BTCUSDT


class ISymbolConversionService(ABC):
    """符号转换服务接口"""
    
    @abstractmethod
    async def convert_to_exchange_format(self, standard_symbol: str, exchange: str) -> str:
        """
        将系统标准格式转换为交易所特定格式
        
        Args:
            standard_symbol: 系统标准格式符号（如 BTC-USDC-PERP）
            exchange: 交易所名称（如 'hyperliquid', 'backpack', 'edgex'）
            
        Returns:
            交易所特定格式符号
        """
        pass
    
    @abstractmethod
    async def convert_from_exchange_format(self, exchange_symbol: str, exchange: str) -> str:
        """
        将交易所特定格式转换为系统标准格式
        
        Args:
            exchange_symbol: 交易所特定格式符号
            exchange: 交易所名称
            
        Returns:
            系统标准格式符号
        """
        pass
    
    @abstractmethod
    async def batch_convert_to_exchange_format(self, symbols: List[str], exchange: str) -> Dict[str, str]:
        """
        批量转换符号到交易所格式
        
        Args:
            symbols: 系统标准格式符号列表
            exchange: 交易所名称
            
        Returns:
            符号映射字典 {标准格式: 交易所格式}
        """
        pass
    
    @abstractmethod
    async def batch_convert_from_exchange_format(self, symbols: List[str], exchange: str) -> Dict[str, str]:
        """
        批量转换符号从交易所格式
        
        Args:
            symbols: 交易所特定格式符号列表
            exchange: 交易所名称
            
        Returns:
            符号映射字典 {交易所格式: 标准格式}
        """
        pass
    
    @abstractmethod
    async def get_supported_exchanges(self) -> List[str]:
        """
        获取支持的交易所列表
        
        Returns:
            支持的交易所名称列表
        """
        pass
    
    @abstractmethod
    async def get_exchange_symbol_format(self, exchange: str) -> SymbolFormat:
        """
        获取交易所的符号格式类型
        
        Args:
            exchange: 交易所名称
            
        Returns:
            符号格式枚举
        """
        pass
    
    @abstractmethod
    async def validate_standard_symbol(self, symbol: str) -> bool:
        """
        验证标准格式符号是否有效
        
        Args:
            symbol: 标准格式符号
            
        Returns:
            是否有效
        """
        pass
    
    @abstractmethod
    async def validate_exchange_symbol(self, symbol: str, exchange: str) -> bool:
        """
        验证交易所格式符号是否有效
        
        Args:
            symbol: 交易所格式符号
            exchange: 交易所名称
            
        Returns:
            是否有效
        """
        pass
    
    @abstractmethod
    async def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        """
        获取符号信息
        
        Args:
            symbol: 符号（标准格式或交易所格式）
            
        Returns:
            符号信息字典
        """
        pass
    
    @abstractmethod
    async def reload_configuration(self) -> bool:
        """
        重新加载配置
        
        Returns:
            是否成功重新加载
        """
        pass 