"""
止盈模式管理

当抵押品余额盈利超过设定百分比时，触发止盈模式
"""

from .take_profit_manager import TakeProfitManager

__all__ = ['TakeProfitManager']

