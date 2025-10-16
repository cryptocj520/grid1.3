"""
价格锁定模块

提供价格锁定功能，防止在有利方向脱离时过早平仓
"""

from .price_lock_manager import PriceLockManager

__all__ = ['PriceLockManager']

