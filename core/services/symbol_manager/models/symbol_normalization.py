"""
符号标准化模块

提供智能的符号标准化逻辑，能够正确识别不同交易所的相同交易对
"""

import re
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum


class ContractType(Enum):
    """合约类型枚举"""
    SPOT = "SPOT"
    PERP = "PERP"
    FUTURES = "FUTURES"
    SWAP = "SWAP"
    UNKNOWN = "UNKNOWN"


class QuoteCurrency(Enum):
    """计价货币枚举"""
    USDC = "USDC"
    USDT = "USDT"
    USD = "USD"
    BTC = "BTC"
    ETH = "ETH"
    UNKNOWN = "UNKNOWN"


@dataclass
class StandardizedSymbol:
    """标准化符号结构"""
    base_asset: str              # 基础资产 (BTC, ETH, SOL)
    quote_currency: QuoteCurrency # 计价货币
    contract_type: ContractType   # 合约类型
    original_symbol: str         # 原始符号
    exchange_id: str             # 交易所ID
    
    def to_comparison_key(self) -> str:
        """生成用于比较的标准化键"""
        # 使用base_asset + contract_type作为比较键
        # 忽略计价货币差异（USDC vs USDT视为相同）
        return f"{self.base_asset}_{self.contract_type.value}"
    
    def to_display_format(self) -> str:
        """生成显示格式"""
        if self.contract_type == ContractType.SPOT:
            return f"{self.base_asset}/{self.quote_currency.value}"
        else:
            return f"{self.base_asset}/{self.quote_currency.value}:{self.contract_type.value}"


class SymbolNormalizer:
    """符号标准化器"""
    
    def __init__(self):
        # 计价货币映射（将不同的计价货币映射到标准格式）
        self.quote_mapping = {
            'USDC': QuoteCurrency.USDC,
            'USDT': QuoteCurrency.USDT,
            'USD': QuoteCurrency.USD,
            'BTC': QuoteCurrency.BTC,
            'ETH': QuoteCurrency.ETH,
        }
        
        # 合约类型映射
        self.contract_mapping = {
            'PERP': ContractType.PERP,
            'PERPETUAL': ContractType.PERP,
            'SWAP': ContractType.SWAP,
            'FUTURES': ContractType.FUTURES,
            'SPOT': ContractType.SPOT,
        }
        
        # 等价计价货币组（用于重叠分析）
        self.equivalent_quotes = {
            frozenset([QuoteCurrency.USDC, QuoteCurrency.USDT, QuoteCurrency.USD])
        }
    
    def normalize_symbol(self, symbol: str, exchange_id: str) -> StandardizedSymbol:
        """标准化符号"""
        try:
            symbol = symbol.upper().strip()
            
            # 根据交易所类型进行解析
            if exchange_id.lower() == 'hyperliquid':
                return self._parse_hyperliquid_symbol(symbol, exchange_id)
            elif exchange_id.lower() == 'backpack':
                return self._parse_backpack_symbol(symbol, exchange_id)
            elif exchange_id.lower() == 'edgex':
                return self._parse_edgex_symbol(symbol, exchange_id)
            else:
                return self._parse_generic_symbol(symbol, exchange_id)
                
        except Exception as e:
            # 解析失败时返回默认值
            return StandardizedSymbol(
                base_asset=symbol.split('_')[0].split('/')[0].split('-')[0],
                quote_currency=QuoteCurrency.UNKNOWN,
                contract_type=ContractType.UNKNOWN,
                original_symbol=symbol,
                exchange_id=exchange_id
            )
    
    def _parse_hyperliquid_symbol(self, symbol: str, exchange_id: str) -> StandardizedSymbol:
        """解析Hyperliquid格式符号: BTC/USDC:PERP, ETH/USDC:PERP"""
        # 处理格式：BTC/USDC:PERP 或 BTC/USDC
        if '/' in symbol:
            parts = symbol.split('/')
            base_asset = parts[0]
            
            if len(parts) > 1:
                quote_part = parts[1]
                
                # 检查是否有合约类型
                if ':' in quote_part:
                    quote_currency, contract_type = quote_part.split(':', 1)
                    contract_type = self.contract_mapping.get(contract_type, ContractType.PERP)
                else:
                    quote_currency = quote_part
                    contract_type = ContractType.SPOT
                
                quote_currency = self.quote_mapping.get(quote_currency, QuoteCurrency.UNKNOWN)
            else:
                quote_currency = QuoteCurrency.USDC  # 默认
                contract_type = ContractType.PERP
        else:
            # 处理简单格式
            base_asset = symbol
            quote_currency = QuoteCurrency.USDC
            contract_type = ContractType.PERP
        
        return StandardizedSymbol(
            base_asset=base_asset,
            quote_currency=quote_currency,
            contract_type=contract_type,
            original_symbol=symbol,
            exchange_id=exchange_id
        )
    
    def _parse_backpack_symbol(self, symbol: str, exchange_id: str) -> StandardizedSymbol:
        """解析Backpack格式符号: BTC_USDC_PERP, SOL_USDC_PERP"""
        parts = symbol.split('_')
        
        if len(parts) >= 3:
            base_asset = parts[0]
            quote_currency = self.quote_mapping.get(parts[1], QuoteCurrency.UNKNOWN)
            contract_type = self.contract_mapping.get(parts[2], ContractType.UNKNOWN)
        elif len(parts) == 2:
            base_asset = parts[0]
            quote_currency = self.quote_mapping.get(parts[1], QuoteCurrency.UNKNOWN)
            contract_type = ContractType.SPOT
        else:
            base_asset = parts[0]
            quote_currency = QuoteCurrency.USDC
            contract_type = ContractType.PERP
        
        return StandardizedSymbol(
            base_asset=base_asset,
            quote_currency=quote_currency,
            contract_type=contract_type,
            original_symbol=symbol,
            exchange_id=exchange_id
        )
    
    def _parse_edgex_symbol(self, symbol: str, exchange_id: str) -> StandardizedSymbol:
        """解析EdgeX格式符号: BTC_USDT_PERP, SOL_USDT_PERP"""
        parts = symbol.split('_')
        
        if len(parts) >= 3:
            base_asset = parts[0]
            quote_currency = self.quote_mapping.get(parts[1], QuoteCurrency.UNKNOWN)
            contract_type = self.contract_mapping.get(parts[2], ContractType.UNKNOWN)
        elif len(parts) == 2:
            base_asset = parts[0]
            quote_currency = self.quote_mapping.get(parts[1], QuoteCurrency.UNKNOWN)
            contract_type = ContractType.PERP  # EdgeX主要是永续合约
        else:
            base_asset = parts[0]
            quote_currency = QuoteCurrency.USDT  # EdgeX主要使用USDT
            contract_type = ContractType.PERP
        
        return StandardizedSymbol(
            base_asset=base_asset,
            quote_currency=quote_currency,
            contract_type=contract_type,
            original_symbol=symbol,
            exchange_id=exchange_id
        )
    
    def _parse_generic_symbol(self, symbol: str, exchange_id: str) -> StandardizedSymbol:
        """解析通用格式符号"""
        # 处理通用格式，优先尝试下划线分隔
        if '_' in symbol:
            parts = symbol.split('_')
        elif '/' in symbol:
            parts = symbol.replace('/', '_').split('_')
        elif '-' in symbol:
            parts = symbol.replace('-', '_').split('_')
        else:
            parts = [symbol]
        
        base_asset = parts[0]
        
        # 尝试识别计价货币
        quote_currency = QuoteCurrency.UNKNOWN
        for i, part in enumerate(parts[1:], 1):
            if part in self.quote_mapping:
                quote_currency = self.quote_mapping[part]
                break
        
        # 尝试识别合约类型
        contract_type = ContractType.UNKNOWN
        for part in parts:
            if part in self.contract_mapping:
                contract_type = self.contract_mapping[part]
                break
        
        return StandardizedSymbol(
            base_asset=base_asset,
            quote_currency=quote_currency,
            contract_type=contract_type,
            original_symbol=symbol,
            exchange_id=exchange_id
        )
    
    def find_overlapping_symbols(self, symbols_by_exchange: Dict[str, List[str]]) -> Dict[str, Dict[str, StandardizedSymbol]]:
        """查找重叠的符号
        
        Returns:
            Dict[comparison_key, Dict[exchange_id, StandardizedSymbol]]
        """
        # 标准化所有符号
        standardized_symbols = {}
        for exchange_id, symbols in symbols_by_exchange.items():
            standardized_symbols[exchange_id] = []
            for symbol in symbols:
                standardized = self.normalize_symbol(symbol, exchange_id)
                standardized_symbols[exchange_id].append(standardized)
        
        # 按比较键分组
        comparison_groups = {}
        for exchange_id, symbols in standardized_symbols.items():
            for symbol in symbols:
                comparison_key = symbol.to_comparison_key()
                if comparison_key not in comparison_groups:
                    comparison_groups[comparison_key] = {}
                comparison_groups[comparison_key][exchange_id] = symbol
        
        # 过滤出重叠的符号（至少在2个交易所中存在）
        overlapping_symbols = {}
        for comparison_key, exchanges in comparison_groups.items():
            if len(exchanges) >= 2:
                overlapping_symbols[comparison_key] = exchanges
        
        return overlapping_symbols
    
    def is_equivalent_quote(self, quote1: QuoteCurrency, quote2: QuoteCurrency) -> bool:
        """检查两个计价货币是否等价"""
        if quote1 == quote2:
            return True
        
        # 检查是否在等价组中
        for equiv_group in self.equivalent_quotes:
            if quote1 in equiv_group and quote2 in equiv_group:
                return True
        
        return False
    
    def generate_overlap_analysis_report(self, symbols_by_exchange: Dict[str, List[str]]) -> str:
        """生成重叠分析报告"""
        overlapping = self.find_overlapping_symbols(symbols_by_exchange)
        
        report = []
        report.append("=" * 60)
        report.append("符号重叠分析报告")
        report.append("=" * 60)
        
        # 统计信息
        total_symbols = sum(len(symbols) for symbols in symbols_by_exchange.values())
        overlap_count = len(overlapping)
        
        report.append(f"总符号数: {total_symbols}")
        report.append(f"重叠符号数: {overlap_count}")
        report.append(f"重叠率: {overlap_count/total_symbols*100:.1f}%")
        report.append("")
        
        # 详细重叠信息
        report.append("重叠符号详情:")
        for comparison_key, exchanges in overlapping.items():
            report.append(f"\n📊 {comparison_key}:")
            for exchange_id, symbol in exchanges.items():
                report.append(f"  {exchange_id}: {symbol.original_symbol}")
        
        # 各交易所符号数量
        report.append("\n各交易所符号数量:")
        for exchange_id, symbols in symbols_by_exchange.items():
            report.append(f"  {exchange_id}: {len(symbols)}")
        
        return "\n".join(report) 