"""
机会处理器

作为与现有监视器模块集成的接口，处理从监视器传来的价差数据
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal

from core.logging import get_logger
from ..coordinator.arbitrage_coordinator import ArbitrageCoordinator


class OpportunityProcessor:
    """
    机会处理器
    
    作为套利系统与现有监视器模块的集成接口
    """
    
    def __init__(self, arbitrage_coordinator: ArbitrageCoordinator):
        """
        初始化机会处理器
        
        Args:
            arbitrage_coordinator: 套利协调器
        """
        self.arbitrage_coordinator = arbitrage_coordinator
        self.logger = get_logger(__name__)
        
        # 数据适配器
        self.data_adapters = {
            'spread_analysis': self._adapt_spread_analysis_data,
            'ticker_data': self._adapt_ticker_data,
            'price_monitor': self._adapt_price_monitor_data,
            'generic': self._adapt_generic_data
        }
        
        # 统计信息
        self.processed_count = 0
        self.forwarded_count = 0
        self.error_count = 0
    
    async def process_spread_analysis_result(self, spread_data: Dict[str, Any]):
        """
        处理价差分析结果（与现有价差分析模块集成）
        
        Args:
            spread_data: 价差分析结果
        """
        try:
            self.processed_count += 1
            
            # 适配数据格式
            market_data = await self._adapt_spread_analysis_data(spread_data)
            
            if market_data:
                # 转发给套利协调器
                await self.arbitrage_coordinator.handle_market_data(market_data)
                self.forwarded_count += 1
                
                self.logger.debug(f"处理价差分析结果: {spread_data.get('symbol', 'unknown')}")
            else:
                self.logger.warning("价差分析数据格式不正确")
                
        except Exception as e:
            self.error_count += 1
            self.logger.error(f"处理价差分析结果失败: {e}")
    
    async def process_ticker_update(self, ticker_data: Dict[str, Any]):
        """
        处理ticker更新（与现有ticker监控集成）
        
        Args:
            ticker_data: ticker数据
        """
        try:
            self.processed_count += 1
            
            # 适配数据格式
            market_data = await self._adapt_ticker_data(ticker_data)
            
            if market_data:
                # 转发给套利协调器
                await self.arbitrage_coordinator.handle_market_data(market_data)
                self.forwarded_count += 1
                
                self.logger.debug(f"处理ticker更新: {ticker_data.get('symbol', 'unknown')}")
            else:
                self.logger.warning("ticker数据格式不正确")
                
        except Exception as e:
            self.error_count += 1
            self.logger.error(f"处理ticker更新失败: {e}")
    
    async def process_price_monitor_data(self, price_data: Dict[str, Any]):
        """
        处理价格监控数据（与现有价格监控模块集成）
        
        Args:
            price_data: 价格监控数据
        """
        try:
            self.processed_count += 1
            
            # 适配数据格式
            market_data = await self._adapt_price_monitor_data(price_data)
            
            if market_data:
                # 转发给套利协调器
                await self.arbitrage_coordinator.handle_market_data(market_data)
                self.forwarded_count += 1
                
                self.logger.debug(f"处理价格监控数据: {price_data.get('symbol', 'unknown')}")
            else:
                self.logger.warning("价格监控数据格式不正确")
                
        except Exception as e:
            self.error_count += 1
            self.logger.error(f"处理价格监控数据失败: {e}")
    
    async def process_generic_data(self, data: Dict[str, Any], data_type: str = 'generic'):
        """
        处理通用数据格式
        
        Args:
            data: 数据
            data_type: 数据类型
        """
        try:
            self.processed_count += 1
            
            # 选择适配器
            adapter = self.data_adapters.get(data_type, self._adapt_generic_data)
            
            # 适配数据格式
            market_data = await adapter(data)
            
            if market_data:
                # 转发给套利协调器
                await self.arbitrage_coordinator.handle_market_data(market_data)
                self.forwarded_count += 1
                
                self.logger.debug(f"处理通用数据: {data.get('symbol', 'unknown')}")
            else:
                self.logger.warning(f"通用数据格式不正确: {data_type}")
                
        except Exception as e:
            self.error_count += 1
            self.logger.error(f"处理通用数据失败: {e}")
    
    async def _adapt_spread_analysis_data(self, spread_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        适配价差分析数据格式
        
        Args:
            spread_data: 价差分析数据
            
        Returns:
            适配后的市场数据
        """
        try:
            # 提取基本信息
            symbol = spread_data.get('symbol')
            if not symbol:
                return None
            
            # 提取交易所数据
            exchanges_data = {}
            
            # 方式1: 直接从exchanges字段获取
            if 'exchanges' in spread_data:
                exchanges_data = spread_data['exchanges']
            
            # 方式2: 从spread_data中提取
            elif 'spread_data' in spread_data:
                spread_info = spread_data['spread_data']
                if 'exchange_a' in spread_info and 'exchange_b' in spread_info:
                    exchanges_data[spread_info['exchange_a']] = {
                        'price': spread_info.get('price_a'),
                        'volume': spread_info.get('volume_a', 0),
                        'timestamp': spread_info.get('timestamp')
                    }
                    exchanges_data[spread_info['exchange_b']] = {
                        'price': spread_info.get('price_b'),
                        'volume': spread_info.get('volume_b', 0),
                        'timestamp': spread_info.get('timestamp')
                    }
            
            # 方式3: 从ticker数据中提取
            elif 'tickers' in spread_data:
                tickers = spread_data['tickers']
                for exchange, ticker in tickers.items():
                    exchanges_data[exchange] = {
                        'ticker': ticker,
                        'price': ticker.get('last'),
                        'volume': ticker.get('baseVolume', 0),
                        'timestamp': ticker.get('timestamp')
                    }
            
            if not exchanges_data or len(exchanges_data) < 2:
                return None
            
            # 构建标准市场数据格式
            market_data = {
                'symbol': symbol,
                'exchanges': exchanges_data,
                'timestamp': spread_data.get('timestamp', datetime.now()),
                'spread_percentage': spread_data.get('spread_percentage'),
                'source': 'spread_analysis',
                'raw_data': spread_data
            }
            
            return market_data
            
        except Exception as e:
            self.logger.error(f"适配价差分析数据失败: {e}")
            return None
    
    async def _adapt_ticker_data(self, ticker_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        适配ticker数据格式
        
        Args:
            ticker_data: ticker数据
            
        Returns:
            适配后的市场数据
        """
        try:
            # 提取基本信息
            symbol = ticker_data.get('symbol')
            if not symbol:
                return None
            
            # 提取交易所数据
            exchanges_data = {}
            
            # 方式1: 单个交易所的ticker
            if 'exchange' in ticker_data:
                exchange = ticker_data['exchange']
                exchanges_data[exchange] = {
                    'ticker': ticker_data,
                    'price': ticker_data.get('last'),
                    'volume': ticker_data.get('baseVolume', 0),
                    'timestamp': ticker_data.get('timestamp')
                }
            
            # 方式2: 多个交易所的ticker集合
            elif 'tickers' in ticker_data:
                tickers = ticker_data['tickers']
                for exchange, ticker in tickers.items():
                    exchanges_data[exchange] = {
                        'ticker': ticker,
                        'price': ticker.get('last'),
                        'volume': ticker.get('baseVolume', 0),
                        'timestamp': ticker.get('timestamp')
                    }
            
            if not exchanges_data:
                return None
            
            # 构建标准市场数据格式
            market_data = {
                'symbol': symbol,
                'exchanges': exchanges_data,
                'timestamp': ticker_data.get('timestamp', datetime.now()),
                'source': 'ticker_data',
                'raw_data': ticker_data
            }
            
            return market_data
            
        except Exception as e:
            self.logger.error(f"适配ticker数据失败: {e}")
            return None
    
    async def _adapt_price_monitor_data(self, price_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        适配价格监控数据格式
        
        Args:
            price_data: 价格监控数据
            
        Returns:
            适配后的市场数据
        """
        try:
            # 提取基本信息
            symbol = price_data.get('symbol')
            if not symbol:
                return None
            
            # 提取交易所数据
            exchanges_data = {}
            
            # 方式1: 从prices字段获取
            if 'prices' in price_data:
                prices = price_data['prices']
                for exchange, price_info in prices.items():
                    exchanges_data[exchange] = {
                        'price': price_info.get('price') if isinstance(price_info, dict) else price_info,
                        'volume': price_info.get('volume', 0) if isinstance(price_info, dict) else 0,
                        'timestamp': price_info.get('timestamp') if isinstance(price_info, dict) else datetime.now()
                    }
            
            # 方式2: 从监控数据中提取
            elif 'monitor_data' in price_data:
                monitor_data = price_data['monitor_data']
                for exchange, data in monitor_data.items():
                    exchanges_data[exchange] = {
                        'price': data.get('price'),
                        'volume': data.get('volume', 0),
                        'best_bid': data.get('best_bid'),
                        'best_ask': data.get('best_ask'),
                        'timestamp': data.get('timestamp')
                    }
            
            if not exchanges_data or len(exchanges_data) < 2:
                return None
            
            # 构建标准市场数据格式
            market_data = {
                'symbol': symbol,
                'exchanges': exchanges_data,
                'timestamp': price_data.get('timestamp', datetime.now()),
                'source': 'price_monitor',
                'raw_data': price_data
            }
            
            return market_data
            
        except Exception as e:
            self.logger.error(f"适配价格监控数据失败: {e}")
            return None
    
    async def _adapt_generic_data(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        适配通用数据格式
        
        Args:
            data: 通用数据
            
        Returns:
            适配后的市场数据
        """
        try:
            # 提取基本信息
            symbol = data.get('symbol')
            if not symbol:
                return None
            
            # 尝试从多个可能的字段中提取交易所数据
            exchanges_data = None
            
            # 尝试exchanges字段
            if 'exchanges' in data:
                exchanges_data = data['exchanges']
            
            # 尝试tickers字段
            elif 'tickers' in data:
                exchanges_data = {}
                for exchange, ticker in data['tickers'].items():
                    exchanges_data[exchange] = {
                        'ticker': ticker,
                        'price': ticker.get('last'),
                        'volume': ticker.get('baseVolume', 0)
                    }
            
            # 尝试prices字段
            elif 'prices' in data:
                exchanges_data = {}
                for exchange, price in data['prices'].items():
                    exchanges_data[exchange] = {
                        'price': price,
                        'volume': 0
                    }
            
            if not exchanges_data or len(exchanges_data) < 2:
                return None
            
            # 构建标准市场数据格式
            market_data = {
                'symbol': symbol,
                'exchanges': exchanges_data,
                'timestamp': data.get('timestamp', datetime.now()),
                'source': 'generic',
                'raw_data': data
            }
            
            return market_data
            
        except Exception as e:
            self.logger.error(f"适配通用数据失败: {e}")
            return None
    
    def get_stats(self) -> Dict[str, Any]:
        """获取处理统计信息"""
        return {
            'processed_count': self.processed_count,
            'forwarded_count': self.forwarded_count,
            'error_count': self.error_count,
            'success_rate': self.forwarded_count / self.processed_count if self.processed_count > 0 else 0
        }
    
    def reset_stats(self):
        """重置统计信息"""
        self.processed_count = 0
        self.forwarded_count = 0
        self.error_count = 0
    
    # TODO: 高级功能占位符
    async def register_custom_adapter(self, data_type: str, adapter_func: Callable):
        """
        注册自定义数据适配器
        
        Args:
            data_type: 数据类型
            adapter_func: 适配器函数
        """
        # TODO: 实现自定义适配器注册
        self.data_adapters[data_type] = adapter_func
    
    async def validate_data_format(self, data: Dict[str, Any], data_type: str) -> bool:
        """
        验证数据格式
        
        Args:
            data: 数据
            data_type: 数据类型
            
        Returns:
            是否格式正确
        """
        # TODO: 实现数据格式验证
        return True
    
    async def filter_opportunities(self, market_data: Dict[str, Any]) -> bool:
        """
        过滤套利机会
        
        Args:
            market_data: 市场数据
            
        Returns:
            是否应该处理
        """
        # TODO: 实现机会过滤逻辑
        return True 