"""
Backpack交易所适配器

基于MESA架构重新实现的Backpack适配器，提供统一的交易接口。
使用ED25519签名方式直接调用Backpack API。
"""

import asyncio
import aiohttp
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal
import json

from ..adapter import ExchangeAdapter
from ..interface import ExchangeConfig
from ..models import *


class BackpackAdapter(ExchangeAdapter):
    """Backpack交易所适配器"""

    DEFAULT_BASE_URL = "https://api.backpack.exchange/"

    def __init__(self, config: ExchangeConfig, event_bus=None):
        super().__init__(config, event_bus)
        self.session = None
        # 确保base_url以斜杠结尾，避免URL拼接错误
        base_url = config.base_url or self.DEFAULT_BASE_URL
        self.base_url = base_url.rstrip('/') + '/'

        # 实际支持的交易对（将从API动态获取）
        self._supported_symbols = []
        self._market_info = {}  # 存储市场信息

        # 符号映射 - 修复：移除默认映射，统一使用Backpack原生格式
        self._symbol_mapping = {}

        if config.symbol_mapping:
            self._symbol_mapping.update(config.symbol_mapping)

    async def get_supported_symbols(self) -> List[str]:
        """获取交易所实际支持的交易对列表"""
        if not self._supported_symbols:
            await self._fetch_supported_symbols()
        return self._supported_symbols.copy()

    async def _fetch_supported_symbols(self) -> None:
        """通过API获取支持的交易对"""
        try:
            self.logger.info("开始获取Backpack支持的交易对列表...")
            
            # 调用市场API获取所有交易对
            async with self.session.get(f"{self.base_url}api/v1/markets") as response:
                if response.status == 200:
                    markets_data = await response.json()
                    
                    supported_symbols = []
                    market_info = {}
                    
                    # 统计数据
                    total_markets = len(markets_data)
                    perpetual_count = 0
                    spot_count = 0
                    
                    for market in markets_data:
                        symbol = market.get("symbol")
                        if symbol:
                            # 只获取永续合约（严格以_PERP结尾的）
                            if symbol.endswith('_PERP'):
                                # 标准化符号格式
                                normalized_symbol = self._normalize_backpack_symbol(symbol)
                                supported_symbols.append(normalized_symbol)
                                market_info[normalized_symbol] = market
                                perpetual_count += 1
                                
                                self.logger.debug(f"添加永续合约: {normalized_symbol}")
                            else:
                                spot_count += 1
                                self.logger.debug(f"跳过现货交易对: {symbol}")
                    
                    self._supported_symbols = supported_symbols
                    self._market_info = market_info
                    
                    self.logger.info(f"✅ Backpack备用适配器市场数据统计:")
                    self.logger.info(f"  - 总市场数量: {len(markets_data)}")
                    self.logger.info(f"  - 永续合约: {len([s for s in supported_symbols if s.endswith('_PERP')])}")
                    self.logger.info(f"  - 现货交易对: {spot_count} (已跳过)")
                    self.logger.info(f"  - 最终获取: {len(supported_symbols)} 个永续合约")
                    
                else:
                    self.logger.error(f"获取市场数据失败: {response.status}")
                    await self._use_default_symbols()
                    
        except Exception as e:
            self.logger.error(f"获取支持的交易对时出错: {e}")
            await self._use_default_symbols()

    def _normalize_backpack_symbol(self, symbol: str) -> str:
        """标准化Backpack符号格式"""
        # Backpack可能返回 "SOL_USDC" 或 "BTC_USDC" 等格式
        # 保持原格式或转换为标准格式
        return symbol.upper()

    async def _use_default_symbols(self) -> None:
        """使用默认的交易对配置 - 修复为永续合约格式"""
        self._supported_symbols = [
            "BTC_USDC_PERP", "ETH_USDC_PERP", "SOL_USDC_PERP", 
            "AVAX_USDC_PERP", "DOGE_USDC_PERP", "XRP_USDC_PERP",
            "SUI_USDC_PERP", "JUP_USDC_PERP", "WIF_USDC_PERP",
            "LTC_USDC_PERP", "ADA_USDC_PERP", "LINK_USDC_PERP",
            "BNB_USDC_PERP"
        ]
        self.logger.info(f"使用默认永续合约交易对列表: {self._supported_symbols}")

    async def batch_subscribe_tickers(self, symbols: Optional[List[str]] = None, 
                                     callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """批量订阅多个交易对的ticker数据 - 使用完整符号格式"""
        try:
            # 如果未指定symbols，使用所有支持的交易对
            if symbols is None:
                symbols = await self.get_supported_symbols()
                
            self.logger.info(f"开始批量订阅 {len(symbols)} 个交易对的ticker数据 (使用完整符号格式)")
            
            # 建立WebSocket连接
            await self._setup_websocket_connection()
            
            # 记录订阅的符号（用于数据映射）
            self._subscribed_symbols = set(symbols)
            
            # 逐个发送订阅消息（使用完整符号格式）
            successful_subscriptions = 0
            for i, symbol in enumerate(symbols):
                try:
                    # 修复：直接使用完整符号，不进行映射
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": [f"ticker.{symbol}"],  # 使用完整符号：ticker.SOL_USDC_PERP
                        "id": i + 1
                    }
                    
                    if self._ws_connection and not self._ws_connection.closed:
                        await self._ws_connection.send_str(json.dumps(subscribe_msg))
                        self.logger.debug(f"✅ 已订阅: ticker.{symbol}")
                        successful_subscriptions += 1
                        
                        # 小延迟避免过快
                        await asyncio.sleep(0.1)
                    
                except Exception as e:
                    self.logger.error(f"订阅 {symbol} 时出错: {e}")
                    continue
            
            self.logger.info(f"🎯 已发送 {successful_subscriptions}/{len(symbols)} 个订阅消息 (完整符号格式)")
            self.logger.info("🎯 开始监听数据流（Backpack无订阅确认）")
                    
            # 如果提供了回调函数，保存它
            if callback:
                self.ticker_callback = callback
                
            self.logger.info(f"✅ 批量ticker订阅完成")
            
        except Exception as e:
            self.logger.error(f"批量订阅ticker时出错: {e}")

    async def batch_subscribe_orderbooks(self, symbols: Optional[List[str]] = None,
                                        callback: Optional[Callable[[str, OrderBookData], None]] = None) -> None:
        """批量订阅多个交易对的订单簿数据"""
        try:
            # 如果未指定symbols，使用所有支持的交易对
            if symbols is None:
                symbols = await self.get_supported_symbols()
                
            self.logger.info(f"开始批量订阅 {len(symbols)} 个交易对的订单簿数据")
            
            # 建立WebSocket连接
            await self._setup_websocket_connection()
            
            # 批量订阅订单簿
            for symbol in symbols:
                try:
                    # 订阅orderbook数据
                    subscribe_msg = self._build_subscribe_message("orderbook", symbol)
                    
                    if self._ws_connection and not self._ws_connection.closed:
                        await self._ws_connection.send_str(subscribe_msg)
                        self.logger.debug(f"已订阅 {symbol} 的订单簿")
                    
                    # 小延迟避免过于频繁的请求
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    self.logger.error(f"订阅 {symbol} 订单簿时出错: {e}")
                    continue
                    
            # 如果提供了回调函数，保存它
            if callback:
                self.orderbook_callback = callback
                
            self.logger.info(f"批量订单簿订阅完成")
            
        except Exception as e:
            self.logger.error(f"批量订阅订单簿时出错: {e}")

    async def batch_subscribe_all_tickers(self, callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """批量订阅所有支持交易对的ticker数据"""
        try:
            # 获取所有支持的交易对
            symbols = await self.get_supported_symbols()
            self.logger.info(f"开始批量订阅所有 {len(symbols)} 个交易对的ticker数据")
            
            # 使用batch_subscribe_tickers方法
            await self.batch_subscribe_tickers(symbols, callback)
            
            self.logger.info(f"✅ 已成功批量订阅所有ticker数据")
            
        except Exception as e:
            self.logger.error(f"批量订阅所有ticker数据失败: {e}")
            raise

    async def get_market_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取市场信息"""
        if not self._market_info:
            await self._fetch_supported_symbols()
        return self._market_info.get(symbol)

    async def _do_connect(self) -> bool:
        """连接实现 - 优化版本，避免重复API调用"""
        try:
            # 创建HTTP session
            self.session = aiohttp.ClientSession()

            # 1. 测试API连接并获取市场数据（一次性完成）
            self.logger.info("测试Backpack API连接并获取市场数据...")
            async with self.session.get(f"{self.base_url}api/v1/markets", timeout=10) as response:
                if response.status == 200:
                    self.logger.info("Backpack API连接成功")
                    
                    # 2. 解析响应数据并直接处理
                    try:
                        markets_data = await response.json()
                        self.logger.info(f"获取到 {len(markets_data)} 个市场数据")
                        
                        # 3. 直接处理市场数据，避免重复API调用
                        supported_symbols = []
                        market_info = {}
                        
                        # 统计数据
                        total_markets = len(markets_data)
                        perpetual_count = 0
                        spot_count = 0
                        
                        for market in markets_data:
                            symbol = market.get("symbol")
                            if symbol and symbol.endswith('_PERP'):
                                # 标准化符号格式
                                normalized_symbol = self._normalize_backpack_symbol(symbol)
                                supported_symbols.append(normalized_symbol)
                                market_info[normalized_symbol] = market
                                perpetual_count += 1
                            elif symbol:
                                # 现货交易对 - 跳过
                                spot_count += 1
                        
                        # 4. 更新内部状态
                        self._supported_symbols = supported_symbols
                        self._market_info = market_info
                        
                        self.logger.info(f"✅ Backpack备用适配器连接成功，市场数据统计:")
                        self.logger.info(f"  - 总市场数量: {total_markets}")
                        self.logger.info(f"  - 永续合约: {perpetual_count}")
                        self.logger.info(f"  - 现货交易对: {spot_count} (已跳过)")
                        self.logger.info(f"  - 最终订阅: {len(supported_symbols)} 个永续合约")
                        
                        if len(supported_symbols) > 0:
                            self.logger.info("Backpack连接和初始化成功")
                            return True
                        else:
                            self.logger.error("未找到任何永续合约")
                            return False
                            
                    except Exception as parse_e:
                        self.logger.error(f"解析市场数据失败: {parse_e}")
                        import traceback
                        self.logger.error(f"详细错误: {traceback.format_exc()}")
                        return False
                else:
                    error_text = await response.text()
                    self.logger.error(f"API连接失败，状态码: {response.status}, 响应: {error_text[:200]}")
                    return False

        except Exception as timeout_e:
            if "timeout" in str(timeout_e).lower():
                self.logger.error("Backpack API连接超时")
            else:
                self.logger.error(f"Backpack连接异常: {type(timeout_e).__name__}: {timeout_e}")
            import traceback
            self.logger.error(f"详细错误: {traceback.format_exc()}")
            return False
        except Exception as e:
            self.logger.error(f"Backpack连接失败: {type(e).__name__}: {e}")
            import traceback
            self.logger.error(f"详细错误: {traceback.format_exc()}")
            return False

        return False

    async def _do_disconnect(self) -> None:
        """断开连接实现"""
        if self.session:
            await self.session.close()
            self.session = None

    async def _do_authenticate(self) -> bool:
        """认证实现"""
        try:
            # 测试需要认证的API调用
            await self._make_authenticated_request("GET", "/api/v1/account")
            return True
        except Exception as e:
            self.logger.error(f"Backpack认证失败: {e}")
            return False

    async def _do_health_check(self) -> Dict[str, Any]:
        """健康检查实现"""
        try:
            async with self.session.get(f"{self.base_url}api/v1/markets") as response:
                if response.status == 200:
                    return {"status": "healthy", "api_accessible": True}
        except Exception as e:
            return {"status": "error", "error": str(e)}

        return {"status": "unhealthy"}

    async def _do_heartbeat(self) -> None:
        """心跳实现"""
        if self.session:
            await self.session.get(f"{self.base_url}api/v1/markets")

    def _map_symbol(self, symbol: str) -> str:
        """映射交易对符号"""
        # 首先检查是否有显式映射
        if symbol in self._symbol_mapping:
            return self._symbol_mapping[symbol]
        
        # 对于永续合约，Backpack需要保留_PERP后缀
        # 这是因为我们要订阅永续合约市场，而不是现货市场
        # 修复：直接返回完整符号，保留_PERP后缀
        return symbol

    def _reverse_map_symbol(self, exchange_symbol: str) -> str:
        """反向映射交易对符号"""
        # 首先检查显式映射
        reverse_mapping = {v: k for k, v in self._symbol_mapping.items()}
        if exchange_symbol in reverse_mapping:
            return reverse_mapping[exchange_symbol]
        
        # 修复：现在Backpack返回的符号已经包含_PERP后缀
        # 所以不需要额外添加后缀，直接返回原符号
        return exchange_symbol

    async def _make_authenticated_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """发起需要认证的API请求，使用ED25519签名"""
        if not self.is_authenticated:
            raise RuntimeError("Exchange not authenticated")

        try:
            import nacl.signing
            import time
            import json
        except ImportError:
            raise RuntimeError("请安装PyNaCl库: pip install PyNaCl")

        # 准备请求数据
        timestamp = str(int(time.time() * 1000))

        # 构建签名字符串
        if method.upper() in ['GET', 'DELETE'] and params:
            query_string = '&'.join(
                [f"{k}={v}" for k, v in sorted(params.items())])
            full_endpoint = f"{endpoint}?{query_string}" if query_string else endpoint
        else:
            full_endpoint = endpoint

        # 请求体
        body = json.dumps(data, separators=(',', ':')) if data else ""

        # 构建签名字符串: instruction + timestamp + window
        instruction = method.upper() + full_endpoint + body
        window = "5000"  # 5秒窗口
        message = f"instruction={instruction}&timestamp={timestamp}&window={window}"

        # ED25519签名
        try:
            # 从hex私钥创建签名密钥
            private_key_bytes = bytes.fromhex(self.config.api_secret)
            signing_key = nacl.signing.SigningKey(private_key_bytes)

            # 签名
            signed = signing_key.sign(message.encode('utf-8'))
            signature = signed.signature.hex()

        except Exception as e:
            self.logger.error(f"ED25519签名失败: {e}")
            raise

        # 构建请求头
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.config.api_key,
            "X-Timestamp": timestamp,
            "X-Window": window,
            "X-Signature": signature
        }

        # 发送请求
        url = f"{self.base_url.rstrip('/')}{endpoint}"

        async with self.session.request(
            method=method.upper(),
            url=url,
            params=params if method.upper() in ['GET', 'DELETE'] else None,
            json=data if method.upper() in ['POST', 'PUT'] else None,
            headers=headers,
            timeout=30
        ) as response:
            if response.status == 200:
                return await response.json()
            else:
                error_text = await response.text()
                self.logger.error(f"API请求失败 {response.status}: {error_text}")
                raise RuntimeError(
                    f"API request failed: {response.status} - {error_text}")

    # 市场数据接口实现
    async def get_exchange_info(self) -> ExchangeInfo:
        """获取交易所信息"""
        try:
            # 获取支持的交易对列表
            supported_symbols = await self.get_supported_symbols()
            
            # 构建markets字典
            markets = {}
            for symbol in supported_symbols:
                # 解析symbol获取base和quote
                if '_' in symbol:
                    parts = symbol.split('_')
                    if len(parts) >= 2:
                        base = parts[0]
                        quote = '_'.join(parts[1:])  # 处理类似 USDC_PERP 的情况
                    else:
                        base = symbol
                        quote = 'USDC'
                else:
                    # 回退处理
                    if symbol.endswith('PERP'):
                        base = symbol[:-4]
                        quote = 'USDC'
                    else:
                        base = symbol
                        quote = 'USDC'
                
                markets[symbol] = {
                    'id': symbol,
                    'symbol': symbol,
                    'base': base,
                    'quote': quote,
                    'baseId': base,
                    'quoteId': quote,
                    'active': True,
                    'type': 'swap',
                    'spot': False,
                    'margin': False,
                    'future': False,
                    'swap': True,
                    'option': False,
                    'contract': True,
                    'contractSize': 1,
                    'linear': True,
                    'inverse': False,
                    'expiry': None,
                    'expiryDatetime': None,
                    'strike': None,
                    'optionType': None,
                    'precision': {
                        'amount': 8,
                        'price': 8,
                        'cost': 8,
                        'base': 8,
                        'quote': 8
                    },
                    'limits': {
                        'amount': {'min': 0.001, 'max': 1000000},
                        'price': {'min': 0.01, 'max': 1000000},
                        'cost': {'min': 10, 'max': 10000000},
                        'leverage': {'min': 1, 'max': 100}
                    },
                    'info': {
                        'symbol': symbol,
                        'exchange': 'backpack',
                        'type': 'perpetual'
                    }
                }
            
            self.logger.info(f"✅ Backpack交易所信息: {len(markets)}个市场")
            
            return ExchangeInfo(
                name="Backpack",
                id="backpack",
                type=ExchangeType.PERPETUAL,
                supported_features=["trading", "orderbook", "ticker"],
                rate_limits=self.config.rate_limits,
                precision=self.config.precision,
                fees={},
                markets=markets,
                status="active",
                timestamp=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"❌ 获取Backpack交易所信息失败: {e}")
            # 返回空markets的基本信息
            return ExchangeInfo(
                name="Backpack",
                id="backpack",
                type=ExchangeType.PERPETUAL,
                supported_features=["trading", "orderbook", "ticker"],
                rate_limits=self.config.rate_limits,
                precision=self.config.precision,
                fees={},
                markets={},
                status="active",
                timestamp=datetime.now()
            )

    async def get_ticker(self, symbol: str) -> TickerData:
        """获取行情数据"""
        mapped_symbol = self._map_symbol(symbol)

        try:
            # 使用公开API获取ticker数据
            async with self.session.get(f"{self.base_url}api/v1/ticker?symbol={mapped_symbol}") as response:
                if response.status == 200:
                    data = await response.json()
                    return TickerData(
                        symbol=symbol,
                        bid=self._safe_decimal(data.get('bidPrice')),
                        ask=self._safe_decimal(data.get('askPrice')),
                        last=self._safe_decimal(data.get('lastPrice')),
                        open=self._safe_decimal(data.get('openPrice')),
                        high=self._safe_decimal(data.get('highPrice')),
                        low=self._safe_decimal(data.get('lowPrice')),
                        close=self._safe_decimal(data.get('lastPrice')),
                        volume=self._safe_decimal(data.get('volume')),
                        quote_volume=self._safe_decimal(data.get('quoteVolume')),
                        change=self._safe_decimal(data.get('priceChange')),
                        percentage=self._safe_decimal(data.get('priceChangePercent')),
                        timestamp=datetime.now(),
                        raw_data=data
                    )
                else:
                    raise Exception(f"HTTP {response.status}")

        except Exception as e:
            self.logger.error(f"获取行情失败 {symbol}: {e}")
            # 返回空行情数据
            return TickerData(
                symbol=symbol,
                bid=None, ask=None, last=None,
                open=None, high=None, low=None, close=None,
                volume=None, quote_volume=None,
                change=None, percentage=None,
                timestamp=datetime.now(),
                raw_data={}
            )

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """获取多个行情数据"""
        try:
            if symbols:
                # 获取指定交易对的ticker
                tasks = [self.get_ticker(symbol) for symbol in symbols]
                return await asyncio.gather(*tasks)
            else:
                # 获取所有ticker数据
                async with self.session.get(f"{self.base_url}api/v1/tickers") as response:
                    if response.status == 200:
                        data = await response.json()
                        tickers = []
                        for ticker_data in data:
                            symbol = ticker_data.get('symbol', '')
                            if symbol:
                                tickers.append(TickerData(
                                    symbol=symbol,
                                    bid=self._safe_decimal(ticker_data.get('bidPrice')),
                                    ask=self._safe_decimal(ticker_data.get('askPrice')),
                                    last=self._safe_decimal(ticker_data.get('lastPrice')),
                                    open=self._safe_decimal(ticker_data.get('openPrice')),
                                    high=self._safe_decimal(ticker_data.get('highPrice')),
                                    low=self._safe_decimal(ticker_data.get('lowPrice')),
                                    close=self._safe_decimal(ticker_data.get('lastPrice')),
                                    volume=self._safe_decimal(ticker_data.get('volume')),
                                    quote_volume=self._safe_decimal(ticker_data.get('quoteVolume')),
                                    change=self._safe_decimal(ticker_data.get('priceChange')),
                                    percentage=self._safe_decimal(ticker_data.get('priceChangePercent')),
                                    timestamp=datetime.now(),
                                    raw_data=ticker_data
                                ))
                        return tickers
                    else:
                        self.logger.error(f"获取所有ticker失败: HTTP {response.status}")
                        return []
        except Exception as e:
            self.logger.error(f"获取ticker数据失败: {e}")
            return []

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿"""
        mapped_symbol = self._map_symbol(symbol)

        try:
            # TODO: 调用Backpack订单簿API
            data = await self._make_authenticated_request("GET", f"/api/v1/orderbook/{mapped_symbol}")

            bids = [
                OrderBookLevel(
                    price=self._safe_decimal(bid[0]),
                    size=self._safe_decimal(bid[1])
                )
                for bid in data.get('bids', [])
            ]

            asks = [
                OrderBookLevel(
                    price=self._safe_decimal(ask[0]),
                    size=self._safe_decimal(ask[1])
                )
                for ask in data.get('asks', [])
            ]

            return OrderBookData(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=datetime.now(),
                raw_data=data
            )

        except Exception as e:
            self.logger.error(f"获取订单簿失败 {symbol}: {e}")
            return OrderBookData(
                symbol=symbol,
                bids=[],
                asks=[],
                timestamp=datetime.now(),
                raw_data={}
            )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OHLCVData]:
        """获取K线数据"""
        # TODO: 实现K线数据获取
        return []

    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """获取成交数据"""
        # TODO: 实现成交数据获取
        return []

    # 账户接口实现
    async def get_balances(self) -> List[BalanceData]:
        """获取账户余额"""
        try:
            data = await self._make_authenticated_request("GET", "/api/v1/capital")

            balances = []
            for balance_info in data.get('balances', []):
                balance = BalanceData(
                    currency=balance_info.get('asset', ''),
                    free=self._safe_decimal(balance_info.get('available')),
                    used=self._safe_decimal(balance_info.get('locked')),
                    total=self._safe_decimal(balance_info.get(
                        'available', 0)) + self._safe_decimal(balance_info.get('locked', 0)),
                    usd_value=None,
                    timestamp=datetime.now(),
                    raw_data=balance_info
                )
                balances.append(balance)

            return balances

        except Exception as e:
            self.logger.error(f"获取余额失败: {e}")
            return []

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """获取持仓信息"""
        try:
            data = await self._make_authenticated_request("GET", "/api/v1/position")

            positions = []
            for position_info in data.get('positions', []):
                symbol = self._reverse_map_symbol(
                    position_info.get('symbol', ''))

                # 过滤指定符号
                if symbols and symbol not in symbols:
                    continue

                position = PositionData(
                    symbol=symbol,
                    side=PositionSide.LONG if position_info.get(
                        'side') == 'Long' else PositionSide.SHORT,
                    size=self._safe_decimal(position_info.get('size')),
                    entry_price=self._safe_decimal(
                        position_info.get('entryPrice')),
                    mark_price=self._safe_decimal(
                        position_info.get('markPrice')),
                    current_price=self._safe_decimal(
                        position_info.get('markPrice')),
                    unrealized_pnl=self._safe_decimal(
                        position_info.get('unrealizedPnl')),
                    realized_pnl=Decimal('0'),
                    percentage=None,
                    leverage=self._safe_int(position_info.get('leverage', 1)),
                    margin_mode=MarginMode.CROSS,
                    margin=self._safe_decimal(position_info.get('margin')),
                    liquidation_price=self._safe_decimal(
                        position_info.get('liquidationPrice')),
                    timestamp=datetime.now(),
                    raw_data=position_info
                )
                positions.append(position)

            return positions

        except Exception as e:
            self.logger.error(f"获取持仓失败: {e}")
            return []

    # 交易接口实现
    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> OrderData:
        """创建订单"""
        mapped_symbol = self._map_symbol(symbol)

        order_data = {
            "symbol": mapped_symbol,
            "side": side.value.title(),  # Buy/Sell
            "orderType": order_type.value.title(),  # Market/Limit
            "quantity": str(amount)
        }

        if price:
            order_data["price"] = str(price)

        if params:
            order_data.update(params)

        try:
            response = await self._make_authenticated_request("POST", "/api/v1/order", data=order_data)

            order = OrderData(
                id=str(response.get('orderId', '')),
                client_id=response.get('clientId'),
                symbol=symbol,
                side=side,
                type=order_type,
                amount=amount,
                price=price,
                filled=Decimal('0'),
                remaining=amount,
                cost=Decimal('0'),
                average=None,
                status=OrderStatus.OPEN,
                timestamp=datetime.now(),
                updated=None,
                fee=None,
                trades=[],
                params=params or {},
                raw_data=response
            )

            # 触发订单创建事件
            await self._handle_order_update(order)

            return order

        except Exception as e:
            self.logger.error(f"创建订单失败: {e}")
            raise

    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """取消订单"""
        mapped_symbol = self._map_symbol(symbol)

        try:
            response = await self._make_authenticated_request(
                "DELETE",
                "/api/v1/order",
                data={"orderId": order_id, "symbol": mapped_symbol}
            )

            order = OrderData(
                id=order_id,
                client_id=None,
                symbol=symbol,
                side=OrderSide.BUY,  # 这里需要从response或者缓存中获取
                type=OrderType.LIMIT,
                amount=Decimal('0'),
                price=None,
                filled=Decimal('0'),
                remaining=Decimal('0'),
                cost=Decimal('0'),
                average=None,
                status=OrderStatus.CANCELED,
                timestamp=datetime.now(),
                updated=datetime.now(),
                fee=None,
                trades=[],
                params={},
                raw_data=response
            )

            # 触发订单更新事件
            await self._handle_order_update(order)

            return order

        except Exception as e:
            self.logger.error(f"取消订单失败: {e}")
            raise

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """取消所有订单"""
        try:
            data = {"cancelAll": True}
            if symbol:
                data["symbol"] = self._map_symbol(symbol)

            response = await self._make_authenticated_request("DELETE", "/api/v1/orders", data=data)

            # 解析返回的订单列表
            canceled_orders = []
            for order_data in response.get('orders', []):
                order = OrderData(
                    id=str(order_data.get('orderId', '')),
                    client_id=order_data.get('clientId'),
                    symbol=self._reverse_map_symbol(
                        order_data.get('symbol', '')),
                    side=OrderSide.BUY if order_data.get(
                        'side', '').lower() == 'buy' else OrderSide.SELL,
                    type=OrderType.LIMIT if order_data.get(
                        'orderType') == 'Limit' else OrderType.MARKET,
                    amount=self._safe_decimal(order_data.get('quantity')),
                    price=self._safe_decimal(order_data.get('price')),
                    filled=self._safe_decimal(
                        order_data.get('executedQuantity', 0)),
                    remaining=self._safe_decimal(order_data.get(
                        'quantity', 0)) - self._safe_decimal(order_data.get('executedQuantity', 0)),
                    cost=self._safe_decimal(order_data.get(
                        'executedQuantity', 0)) * self._safe_decimal(order_data.get('price', 0)),
                    average=self._safe_decimal(order_data.get('price')),
                    status=OrderStatus.CANCELED,
                    timestamp=datetime.now(),
                    updated=datetime.now(),
                    fee=order_data.get('fee'),
                    trades=order_data.get('trades', []),
                    params={},
                    raw_data=order_data
                )
                canceled_orders.append(order)
                # 触发订单更新事件
                await self._handle_order_update(order)

            return canceled_orders

        except Exception as e:
            self.logger.error(f"取消所有订单失败: {e}")
            return []

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """获取订单信息"""
        mapped_symbol = self._map_symbol(symbol)

        try:
            response = await self._make_authenticated_request(
                "GET",
                f"/api/v1/order/{order_id}",
                params={"symbol": mapped_symbol}
            )

            order_data = response.get('order', {})

            # 状态映射
            status_map = {
                'New': OrderStatus.OPEN,
                'PartiallyFilled': OrderStatus.PARTIALLY_FILLED,
                'Filled': OrderStatus.FILLED,
                'Cancelled': OrderStatus.CANCELED,
                'Rejected': OrderStatus.REJECTED,
                'Expired': OrderStatus.EXPIRED
            }

            status = status_map.get(order_data.get(
                'status'), OrderStatus.UNKNOWN)

            return OrderData(
                id=str(order_data.get('orderId', order_id)),
                client_id=order_data.get('clientId'),
                symbol=symbol,
                side=OrderSide.BUY if order_data.get(
                    'side', '').lower() == 'buy' else OrderSide.SELL,
                type=OrderType.LIMIT if order_data.get(
                    'orderType') == 'Limit' else OrderType.MARKET,
                amount=self._safe_decimal(order_data.get('quantity')),
                price=self._safe_decimal(order_data.get('price')),
                filled=self._safe_decimal(
                    order_data.get('executedQuantity', 0)),
                remaining=self._safe_decimal(order_data.get(
                    'quantity', 0)) - self._safe_decimal(order_data.get('executedQuantity', 0)),
                cost=self._safe_decimal(order_data.get(
                    'executedQuantity', 0)) * self._safe_decimal(order_data.get('price', 0)),
                average=self._safe_decimal(order_data.get('price')),
                status=status,
                timestamp=datetime.fromtimestamp(order_data.get(
                    'timestamp', 0) / 1000) if order_data.get('timestamp') else datetime.now(),
                updated=datetime.fromtimestamp(order_data.get(
                    'updateTime', 0) / 1000) if order_data.get('updateTime') else None,
                fee=order_data.get('fee'),
                trades=order_data.get('trades', []),
                params={},
                raw_data=order_data
            )

        except Exception as e:
            self.logger.error(f"获取订单信息失败 {order_id}: {e}")
            # 返回基础订单信息
            return OrderData(
                id=order_id,
                client_id=None,
                symbol=symbol,
                side=OrderSide.BUY,
                type=OrderType.LIMIT,
                amount=Decimal('0'),
                price=Decimal('0'),
                filled=Decimal('0'),
                remaining=Decimal('0'),
                cost=Decimal('0'),
                average=None,
                status=OrderStatus.UNKNOWN,
                timestamp=datetime.now(),
                updated=None,
                fee=None,
                trades=[],
                params={},
                raw_data={}
            )

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        try:
            endpoint = "/api/v1/orders"
            params = {}
            if symbol:
                params["symbol"] = self._map_symbol(symbol)

            response = await self._make_authenticated_request("GET", endpoint, params=params)

            # 解析订单列表
            orders = []
            for order_data in response.get('orders', []):
                # 只处理未完成的订单
                if order_data.get('status') in ['New', 'PartiallyFilled']:
                    order_symbol = self._reverse_map_symbol(
                        order_data.get('symbol', ''))

                    # 状态映射
                    status_map = {
                        'New': OrderStatus.OPEN,
                        'PartiallyFilled': OrderStatus.PARTIALLY_FILLED,
                        'Filled': OrderStatus.FILLED,
                        'Cancelled': OrderStatus.CANCELED,
                        'Rejected': OrderStatus.REJECTED,
                        'Expired': OrderStatus.EXPIRED
                    }

                    status = status_map.get(order_data.get(
                        'status'), OrderStatus.UNKNOWN)

                    order = OrderData(
                        id=str(order_data.get('orderId', '')),
                        client_id=order_data.get('clientId'),
                        symbol=order_symbol,
                        side=OrderSide.BUY if order_data.get(
                            'side', '').lower() == 'buy' else OrderSide.SELL,
                        type=OrderType.LIMIT if order_data.get(
                            'orderType') == 'Limit' else OrderType.MARKET,
                        amount=self._safe_decimal(order_data.get('quantity')),
                        price=self._safe_decimal(order_data.get('price')),
                        filled=self._safe_decimal(
                            order_data.get('executedQuantity', 0)),
                        remaining=self._safe_decimal(order_data.get(
                            'quantity', 0)) - self._safe_decimal(order_data.get('executedQuantity', 0)),
                        cost=self._safe_decimal(order_data.get(
                            'executedQuantity', 0)) * self._safe_decimal(order_data.get('price', 0)),
                        average=self._safe_decimal(order_data.get('price')),
                        status=status,
                        timestamp=datetime.fromtimestamp(order_data.get(
                            'timestamp', 0) / 1000) if order_data.get('timestamp') else datetime.now(),
                        updated=datetime.fromtimestamp(order_data.get(
                            'updateTime', 0) / 1000) if order_data.get('updateTime') else None,
                        fee=order_data.get('fee'),
                        trades=order_data.get('trades', []),
                        params={},
                        raw_data=order_data
                    )
                    orders.append(order)

            return orders

        except Exception as e:
            self.logger.error(f"获取开放订单失败: {e}")
            return []

    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OrderData]:
        """获取历史订单"""
        try:
            endpoint = "/api/v1/history/orders"
            params = {}

            if symbol:
                params["symbol"] = self._map_symbol(symbol)
            if since:
                params["startTime"] = int(since.timestamp() * 1000)
            if limit:
                params["limit"] = limit

            response = await self._make_authenticated_request("GET", endpoint, params=params)

            # 解析历史订单
            orders = []
            for order_data in response.get('orders', []):
                order_symbol = self._reverse_map_symbol(
                    order_data.get('symbol', ''))

                # 状态映射
                status_map = {
                    'New': OrderStatus.OPEN,
                    'PartiallyFilled': OrderStatus.PARTIALLY_FILLED,
                    'Filled': OrderStatus.FILLED,
                    'Cancelled': OrderStatus.CANCELED,
                    'Rejected': OrderStatus.REJECTED,
                    'Expired': OrderStatus.EXPIRED
                }

                status = status_map.get(order_data.get(
                    'status'), OrderStatus.UNKNOWN)

                order = OrderData(
                    id=str(order_data.get('orderId', '')),
                    client_id=order_data.get('clientId'),
                    symbol=order_symbol,
                    side=OrderSide.BUY if order_data.get(
                        'side', '').lower() == 'buy' else OrderSide.SELL,
                    type=OrderType.LIMIT if order_data.get(
                        'orderType') == 'Limit' else OrderType.MARKET,
                    amount=self._safe_decimal(order_data.get('quantity')),
                    price=self._safe_decimal(order_data.get('price')),
                    filled=self._safe_decimal(
                        order_data.get('executedQuantity', 0)),
                    remaining=self._safe_decimal(order_data.get(
                        'quantity', 0)) - self._safe_decimal(order_data.get('executedQuantity', 0)),
                    cost=self._safe_decimal(order_data.get(
                        'executedQuantity', 0)) * self._safe_decimal(order_data.get('price', 0)),
                    average=self._safe_decimal(order_data.get('price')),
                    status=status,
                    timestamp=datetime.fromtimestamp(order_data.get(
                        'timestamp', 0) / 1000) if order_data.get('timestamp') else datetime.now(),
                    updated=datetime.fromtimestamp(order_data.get(
                        'updateTime', 0) / 1000) if order_data.get('updateTime') else None,
                    fee=order_data.get('fee'),
                    trades=order_data.get('trades', []),
                    params={},
                    raw_data=order_data
                )
                orders.append(order)

            return orders

        except Exception as e:
            self.logger.error(f"获取历史订单失败: {e}")
            return []

    # 设置接口实现
    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数"""
        # Backpack可能不支持动态设置杠杆
        return {"success": True, "message": "Leverage setting not supported"}

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式"""
        # Backpack可能不支持动态设置保证金模式
        return {"success": True, "message": "Margin mode setting not supported"}

    # 订阅接口实现
    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅行情数据流"""
        await self._subscribe_websocket('ticker', symbol, callback)

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """订阅订单簿数据流"""
        await self._subscribe_websocket('orderbook', symbol, callback)

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅成交数据流"""
        await self._subscribe_websocket('trades', symbol, callback)

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """订阅用户数据流"""
        await self._subscribe_websocket('user_data', None, callback)

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        if symbol:
            # 取消特定符号的订阅
            if hasattr(self, '_ws_subscriptions'):
                subscriptions_to_remove = []
                for sub_type, sub_symbol, _ in self._ws_subscriptions:
                    if sub_symbol == symbol:
                        subscriptions_to_remove.append(
                            (sub_type, sub_symbol, _))

                for sub in subscriptions_to_remove:
                    self._ws_subscriptions.remove(sub)
        else:
            # 取消所有订阅
            if hasattr(self, '_ws_subscriptions'):
                self._ws_subscriptions.clear()

            # 关闭WebSocket连接
            if hasattr(self, '_ws_connection') and self._ws_connection:
                await self._ws_connection.close()
                self._ws_connection = None

    async def _subscribe_websocket(self, sub_type: str, symbol: Optional[str], callback: Callable) -> None:
        """WebSocket订阅通用方法"""
        try:
            import aiohttp

            # 初始化订阅列表
            if not hasattr(self, '_ws_subscriptions'):
                self._ws_subscriptions = []

            # 添加订阅
            self._ws_subscriptions.append((sub_type, symbol, callback))

            # 如果还没有WebSocket连接，创建一个
            if not hasattr(self, '_ws_connection') or not self._ws_connection:
                await self._setup_websocket_connection()

            # 发送订阅消息
            if hasattr(self, '_ws_connection') and self._ws_connection:
                subscribe_msg = self._build_subscribe_message(sub_type, symbol)
                await self._ws_connection.send_str(subscribe_msg)

        except Exception as e:
            self.logger.error(f"WebSocket订阅失败 {sub_type} {symbol}: {e}")

    async def _setup_websocket_connection(self) -> None:
        """建立WebSocket连接 - 修复版本（添加心跳和重连）"""
        try:
            import aiohttp
            import json

            # 先停止现有的连接和任务
            if hasattr(self, '_ws_connected') and self._ws_connected:
                self.logger.info("🔧 关闭现有连接后重新建立...")
                await self._close_websocket()

            # 使用正确的Backpack WebSocket URL
            ws_url = "wss://ws.backpack.exchange/"

            if hasattr(self, 'session') and self.session:
                self._ws_connection = await self.session.ws_connect(ws_url)
            else:
                # 创建新的session用于WebSocket
                if not hasattr(self, '_ws_session'):
                    self._ws_session = aiohttp.ClientSession()
                self._ws_connection = await self._ws_session.ws_connect(ws_url)

            self.logger.info(f"✅ Backpack WebSocket连接已建立: {ws_url}")

            # 初始化连接状态
            self._ws_connected = True
            self._last_heartbeat = time.time()
            self._reconnect_attempts = 0
            
            # 启动消息处理任务并保存引用
            if hasattr(self, '_ws_handler_task') and self._ws_handler_task and not self._ws_handler_task.done():
                self._ws_handler_task.cancel()
            self._ws_handler_task = asyncio.create_task(self._websocket_message_handler())
            self.logger.info("✅ Backpack WebSocket消息处理器已启动")
            
            # 启动心跳检测任务
            if hasattr(self, '_heartbeat_task') and self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
            self._heartbeat_task = asyncio.create_task(self._websocket_heartbeat_loop())
            self.logger.info("💓 Backpack WebSocket心跳检测已启动")

        except Exception as e:
            self.logger.error(f"建立Backpack WebSocket连接失败: {type(e).__name__}: {e}")
            self._ws_connected = False
            raise

    def _build_subscribe_message(self, sub_type: str, symbol: Optional[str]) -> str:
        """构建订阅消息 - 修复为使用完整符号格式"""
        import json

        # 使用完整符号，不再进行映射转换
        # 根据测试结果，Backpack支持完整的符号格式
        
        # 添加详细日志
        if symbol:
            self.logger.debug(f"构建订阅消息: {symbol} (类型: {sub_type}) - 使用完整符号格式")
        
        if sub_type == 'ticker':
            # 修复：使用完整符号格式 ticker.SOL_USDC_PERP
            msg = json.dumps({
                "method": "SUBSCRIBE",
                "params": [f"ticker.{symbol}"],
                "id": 1
            })
            self.logger.debug(f"ticker订阅消息: {msg}")
            return msg
        elif sub_type == 'orderbook':
            # 修复：使用完整符号格式 depth.SOL_USDC_PERP
            msg = json.dumps({
                "method": "SUBSCRIBE", 
                "params": [f"depth.{symbol}"],
                "id": 2
            })
            self.logger.debug(f"orderbook订阅消息: {msg}")
            return msg
        elif sub_type == 'trades':
            # 修复：使用完整符号格式 trade.SOL_USDC_PERP
            return json.dumps({
                "method": "SUBSCRIBE",
                "params": [f"trade.{symbol}"],
                "id": 3
            })
        elif sub_type == 'user_data':
            # 用户数据流需要认证
            return json.dumps({
                "method": "SUBSCRIBE",
                "params": ["userData"],
                "id": 4
            })
        else:
            return json.dumps({"method": "SUBSCRIBE", "params": [], "id": 0})

    async def _websocket_heartbeat_loop(self):
        """WebSocket心跳检测循环 - 修复版本"""
        heartbeat_interval = 30  # 30秒检测一次
        max_silence = 120  # 增加到120秒无消息则重连
        
        self.logger.info(f"💓 Backpack心跳检测开始，间隔{heartbeat_interval}s，最大静默{max_silence}s")
        
        while self._ws_connected:
            try:
                await asyncio.sleep(heartbeat_interval)
                
                # 再次检查连接状态
                if not self._ws_connected:
                    self.logger.info("💓 连接已断开，退出心跳检测")
                    break
                
                # 检查上次接收到消息的时间
                silence_time = time.time() - self._last_heartbeat
                
                if silence_time > max_silence:
                    self.logger.warning(f"⚠️ Backpack WebSocket静默时间过长: {silence_time:.1f}s，准备重连...")
                    
                    # 标记连接断开，避免重复重连
                    self._ws_connected = False
                    
                    try:
                        self.logger.info("🔄 [心跳调试] 即将调用Backpack重连方法...")
                        await self._reconnect_websocket()
                        
                        if self._ws_connected:
                            self.logger.info("✅ [心跳调试] Backpack重连成功，继续心跳检测")
                        else:
                            self.logger.error("❌ [心跳调试] Backpack重连失败，退出心跳检测")
                            break
                            
                    except Exception as e:
                        self.logger.error(f"❌ [心跳调试] Backpack重连方法调用失败: {type(e).__name__}: {e}")
                        import traceback
                        self.logger.error(f"[心跳调试] Backpack重连异常堆栈: {traceback.format_exc()}")
                        break  # 重连失败，退出心跳循环
                        
                else:
                    # 定期记录心跳状态（降低频率）
                    if hasattr(self, '_heartbeat_log_count'):
                        self._heartbeat_log_count += 1
                    else:
                        self._heartbeat_log_count = 1
                    
                    # 每5次心跳（2.5分钟）记录一次
                    if self._heartbeat_log_count % 5 == 0:
                        self.logger.info(f"💓 Backpack WebSocket心跳正常: {silence_time:.1f}s前有数据")
                    else:
                        self.logger.debug(f"💓 WebSocket心跳正常: {silence_time:.1f}s前有数据")
                    
            except asyncio.CancelledError:
                self.logger.info("💓 Backpack心跳检测被取消")
                break
            except Exception as e:
                self.logger.error(f"❌ Backpack心跳检测错误: {type(e).__name__}: {e}")
                # 心跳检测出错，等待后继续
                await asyncio.sleep(10)  # 错误后等待10秒
                
        self.logger.info("💓 Backpack心跳检测循环已退出")
    
    async def _reconnect_websocket(self):
        """WebSocket自动重连 - 修复版本"""
        max_attempts = 10  # 增加重连次数
        base_delay = 2
        
        self.logger.info(f"🔄 [重连调试] 开始Backpack重连流程，当前尝试次数: {self._reconnect_attempts}")
        
        if self._reconnect_attempts >= max_attempts:
            self.logger.error(f"❌ Backpack达到最大重连次数({max_attempts})，停止重连")
            self._ws_connected = False
            # 停止心跳检测
            if hasattr(self, '_heartbeat_task') and self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
            return
        
        self._reconnect_attempts += 1
        delay = min(base_delay * (2 ** (self._reconnect_attempts - 1)), 60)  # 最大延迟60秒
        
        self.logger.info(f"🔄 [重连调试] 尝试重连Backpack WebSocket (第{self._reconnect_attempts}次，延迟{delay}s)...")
        
        try:
            # 步骤1: 停止旧的心跳任务
            self.logger.info("🔧 [重连调试] 步骤1: 停止旧的心跳任务...")
            if hasattr(self, '_heartbeat_task') and self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass
                self.logger.info("✅ [重连调试] 旧的心跳任务已停止")
            
            # 步骤2: 关闭旧的WebSocket连接
            self.logger.info("🔧 [重连调试] 步骤2: 关闭旧Backpack WebSocket连接...")
            if hasattr(self, '_ws_connection') and self._ws_connection:
                await self._ws_connection.close()
                self._ws_connection = None
                self.logger.info("✅ [重连调试] 旧Backpack WebSocket连接已关闭")
            
            # 步骤3: 关闭旧的session
            self.logger.info("🔧 [重连调试] 步骤3: 关闭旧Backpack session...")
            if hasattr(self, '_ws_session') and self._ws_session and not self._ws_session.closed:
                await self._ws_session.close()
                self.logger.info("✅ [重连调试] 旧Backpack session已关闭")
            
            # 步骤4: 停止旧的消息处理任务
            self.logger.info("🔧 [重连调试] 步骤4: 停止旧的消息处理任务...")
            if hasattr(self, '_ws_handler_task') and self._ws_handler_task and not self._ws_handler_task.done():
                self._ws_handler_task.cancel()
                try:
                    await self._ws_handler_task
                    self.logger.info("✅ [重连调试] 旧的消息处理任务已停止")
                except asyncio.CancelledError:
                    self.logger.info("✅ [重连调试] 旧的消息处理任务已取消")
                except Exception as e:
                    self.logger.warning(f"⚠️ [重连调试] 停止旧任务时出错: {e}")
            
            # 步骤5: 等待延迟
            self.logger.info(f"🔧 [重连调试] 步骤5: 等待{delay}秒后重连...")
            await asyncio.sleep(delay)
            self.logger.info("✅ [重连调试] 延迟等待完成")
            
            # 步骤6: 重新建立WebSocket连接
            self.logger.info("🔧 [重连调试] 步骤6: 重新建立Backpack WebSocket连接...")
            try:
                ws_url = "wss://ws.backpack.exchange/"
                self._ws_session = aiohttp.ClientSession()
                self.logger.info("✅ [重连调试] Backpack session已创建")
                
                self._ws_connection = await self._ws_session.ws_connect(ws_url)
                self.logger.info(f"✅ [重连调试] Backpack WebSocket连接已重新建立: {ws_url}")
                
                # 更新连接状态
                self._ws_connected = True
                self._last_heartbeat = time.time()
                
            except Exception as e:
                self.logger.error(f"❌ [重连调试] 步骤6失败 - 连接建立失败: {type(e).__name__}: {e}")
                self._ws_connected = False
                raise
            
            # 步骤7: 重新启动消息处理任务
            self.logger.info("🔧 [重连调试] 步骤7: 重新启动消息处理任务...")
            try:
                self._ws_handler_task = asyncio.create_task(self._websocket_message_handler())
                self.logger.info("✅ [重连调试] 消息处理任务已重新启动")
            except Exception as e:
                self.logger.error(f"❌ [重连调试] 步骤7失败 - 任务启动失败: {type(e).__name__}: {e}")
                self._ws_connected = False
                raise
            
            # 步骤8: 重新启动心跳检测任务
            self.logger.info("🔧 [重连调试] 步骤8: 重新启动心跳检测任务...")
            try:
                self._heartbeat_task = asyncio.create_task(self._websocket_heartbeat_loop())
                self.logger.info("✅ [重连调试] 心跳检测任务已重新启动")
            except Exception as e:
                self.logger.error(f"❌ [重连调试] 步骤8失败 - 心跳任务启动失败: {type(e).__name__}: {e}")
                # 心跳任务失败不影响数据接收，继续执行
            
            # 步骤9: 重新订阅所有交易对
            self.logger.info("🔧 [重连调试] 步骤9: 重新订阅所有交易对...")
            try:
                if hasattr(self, '_subscribed_symbols') and self._subscribed_symbols:
                    await self._resubscribe_all()
                    self.logger.info("✅ [重连调试] 所有交易对重新订阅完成")
                else:
                    self.logger.warning("⚠️ [重连调试] 没有找到订阅的交易对")
            except Exception as e:
                self.logger.error(f"❌ [重连调试] 步骤9失败 - 重新订阅失败: {type(e).__name__}: {e}")
                # 订阅失败不影响连接，标记为部分成功
            
            # 步骤10: 重置重连计数
            self.logger.info("🔧 [重连调试] 步骤10: 重置连接状态...")
            self._reconnect_attempts = 0  # 重置重连计数
            
            self.logger.info("🎉 [重连调试] Backpack WebSocket重连成功！")
            
        except asyncio.CancelledError:
            self.logger.warning("⚠️ [重连调试] Backpack重连被取消")
            self._ws_connected = False
            raise
        except Exception as e:
            self.logger.error(f"❌ [重连调试] Backpack重连失败: {type(e).__name__}: {e}")
            import traceback
            self.logger.error(f"[重连调试] 完整错误堆栈: {traceback.format_exc()}")
            # 重连失败，标记连接断开，但继续重连循环
            self._ws_connected = False
            # 等待一段时间后重新开始心跳检测，触发下一次重连
            await asyncio.sleep(30)
            if not self._ws_connected:  # 如果仍然未连接，重新启动心跳检测
                try:
                    self._ws_connected = True  # 临时设为True以启动心跳
                    self._heartbeat_task = asyncio.create_task(self._websocket_heartbeat_loop())
                    self.logger.info("🔄 重新启动心跳检测以继续重连...")
                except Exception as restart_e:
                    self.logger.error(f"重新启动心跳失败: {restart_e}")
    
    async def _resubscribe_all(self):
        """重新订阅所有交易对"""
        try:
            self.logger.info("🔄 [重订阅调试] 开始重新订阅Backpack所有交易对")
            
            if hasattr(self, '_subscribed_symbols') and self._subscribed_symbols:
                symbol_count = len(self._subscribed_symbols)
                self.logger.info(f"🔧 [重订阅调试] 待重新订阅的交易对数量: {symbol_count}")
                self.logger.info(f"🔧 [重订阅调试] 交易对列表: {list(self._subscribed_symbols)[:10]}...")  # 只显示前10个
                
                success_count = 0
                failed_count = 0
                
                for i, symbol in enumerate(self._subscribed_symbols):
                    try:
                        subscribe_msg = {
                            "method": "SUBSCRIBE",
                            "params": [f"ticker.{symbol}"],
                            "id": i + 1
                        }
                        
                        if self._ws_connection and not self._ws_connection.closed:
                            await self._ws_connection.send_str(json.dumps(subscribe_msg))
                            success_count += 1
                            if i < 5:  # 只记录前5个的详细信息
                                self.logger.info(f"✅ [重订阅调试] 重新订阅ticker: {symbol} (ID: {i+1})")
                            await asyncio.sleep(0.1)  # 小延迟
                        else:
                            self.logger.error(f"❌ [重订阅调试] WebSocket连接不可用，无法订阅: {symbol}")
                            failed_count += 1
                    except Exception as e:
                        self.logger.error(f"❌ [重订阅调试] 订阅{symbol}失败: {e}")
                        failed_count += 1
                        
                self.logger.info(f"✅ [重订阅调试] Backpack重新订阅完成: {success_count}个成功, {failed_count}个失败")
            else:
                self.logger.warning("⚠️ [重订阅调试] 没有找到订阅的交易对列表")
                
        except Exception as e:
            self.logger.error(f"❌ [重订阅调试] Backpack重新订阅失败: {type(e).__name__}: {e}")
            import traceback
            self.logger.error(f"[重订阅调试] 完整错误堆栈: {traceback.format_exc()}")
            raise

    async def _websocket_message_handler(self) -> None:
        """WebSocket消息处理器 - 添加心跳更新"""
        self.logger.info("🎯 WebSocket消息处理器开始运行...")
        try:
            async for msg in self._ws_connection:
                # 更新心跳时间
                self._last_heartbeat = time.time()
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._process_websocket_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    self.logger.error(f"WebSocket错误: {self._ws_connection.exception()}")
                    self._ws_connected = False
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    self.logger.warning("WebSocket连接已关闭")
                    self._ws_connected = False
                    break

        except Exception as e:
            self.logger.error(f"WebSocket消息处理失败: {e}")
            self._ws_connected = False
        finally:
            self.logger.info("WebSocket消息处理器已停止")

    async def _process_websocket_message(self, message: str) -> None:
        """处理WebSocket消息 - 根据Backpack官方文档修复"""
        try:
            import json
            data = json.loads(message)

            # 记录接收到的消息用于调试（减少日志量）
            if not hasattr(self, '_msg_count'):
                self._msg_count = 0
            self._msg_count += 1
            
            if self._msg_count <= 5:  # 只记录前5条消息
                self.logger.debug(f"收到WebSocket消息 #{self._msg_count}: {data}")

            # 处理订阅响应（可选，Backpack可能不发送）
            if 'result' in data and 'id' in data:
                if data['result'] is None:
                    self.logger.info(f"订阅确认: ID {data['id']}")
                else:
                    self.logger.warning(f"订阅可能失败: {data}")
                return

            # 处理错误消息
            if 'error' in data:
                error_info = data['error']
                error_code = error_info.get('code', 'unknown')
                error_message = error_info.get('message', 'unknown')
                
                # 记录详细的错误信息
                self.logger.error(f"WebSocket错误: {error_info}")
                
                # 如果是Invalid market错误，记录但不中断其他订阅
                if error_code == 4005 and 'Invalid market' in error_message:
                    error_id = data.get('id', 'unknown')
                    self.logger.warning(f"某个符号可能不支持WebSocket: 请求ID {error_id}")
                
                return

            # 🔧 修复：Backpack实际使用嵌套的stream/data格式！
            # 处理Backpack的stream/data格式消息
            if 'stream' in data and 'data' in data:
                stream_name = data['stream']
                stream_data = data['data']

                # Backpack格式：ticker.SOL_USDC_PERP, depth.SOL_USDC_PERP, trade.SOL_USDC_PERP
                if stream_name.startswith('ticker.'):
                    # 从stream名称提取符号：ticker.SOL_USDC_PERP -> SOL_USDC_PERP
                    symbol = stream_name.split('.', 1)[1] if '.' in stream_name else stream_name
                    await self._handle_backpack_ticker_update(symbol, stream_data)
                    
                elif stream_name.startswith('bookTicker.'):
                    # bookTicker也包含价格信息
                    symbol = stream_name.split('.', 1)[1] if '.' in stream_name else stream_name
                    await self._handle_backpack_ticker_update(symbol, stream_data)
                    
                elif stream_name.startswith('depth.'):
                    symbol = stream_name.split('.', 1)[1] if '.' in stream_name else stream_name
                    await self._handle_backpack_orderbook_update(symbol, stream_data)
                    
                elif stream_name.startswith('trade.'):
                    symbol = stream_name.split('.', 1)[1] if '.' in stream_name else stream_name
                    await self._handle_backpack_trade_update(symbol, stream_data)
                    
                elif 'userData' in stream_name:
                    await self._handle_user_data_update(stream_data)
                    
                else:
                    self.logger.debug(f"未知的流类型: {stream_name}")
            else:
                # 对于非标准格式的消息，记录但不报错
                if self._msg_count <= 5:
                    self.logger.debug(f"未知消息格式: {data}")

        except Exception as e:
            self.logger.error(f"处理WebSocket消息失败: {e}")
            self.logger.error(f"原始消息: {message}")

    async def _handle_backpack_ticker_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """处理Backpack ticker数据（stream_data格式）"""
        try:
            # 检查符号是否在我们的订阅列表中
            if hasattr(self, '_subscribed_symbols') and symbol not in self._subscribed_symbols:
                self.logger.debug(f"收到未订阅符号的数据: {symbol}")
                return

            # 解析交易所时间戳（Backpack使用微秒）
            exchange_timestamp = None
            current_time = datetime.now()
            
            # Backpack使用 'E' 字段表示事件时间（微秒）
            if 'E' in data:
                try:
                    timestamp_microseconds = int(data['E'])
                    exchange_timestamp = datetime.fromtimestamp(timestamp_microseconds / 1000000)
                except (ValueError, TypeError):
                    pass
            
            # 使用当前时间作为主时间戳（确保时效性）
            main_timestamp = current_time
            
            # 根据测试结果解析ticker数据（Binance兼容格式）
            ticker = TickerData(
                symbol=symbol,
                bid=None,  # ticker流中没有bid/ask信息
                ask=None,
                last=self._safe_decimal(data.get('c')),     # c = close/last price
                open=self._safe_decimal(data.get('o')),     # o = open price  
                high=self._safe_decimal(data.get('h')),     # h = high price
                low=self._safe_decimal(data.get('l')),      # l = low price
                close=self._safe_decimal(data.get('c')),    # c = close price
                volume=self._safe_decimal(data.get('v')),   # v = base asset volume
                quote_volume=self._safe_decimal(data.get('V')),  # V = quote asset volume
                change=None,  # 可以通过 open-close 计算
                percentage=None,  # 可以通过 (close-open)/open*100 计算
                timestamp=main_timestamp,
                exchange_timestamp=exchange_timestamp,
                raw_data=data
            )

            # 记录成功的ticker更新（限制日志量）
            if not hasattr(self, '_ticker_count'):
                self._ticker_count = {}
            if symbol not in self._ticker_count:
                self._ticker_count[symbol] = 0
                self.logger.info(f"✅ 首次收到Backpack ticker数据: {symbol} -> {ticker.last}")
            self._ticker_count[symbol] += 1

            # 调用相应的回调函数
            # 1. 检查批量订阅的回调
            if hasattr(self, 'ticker_callback') and self.ticker_callback:
                await self._safe_callback(self.ticker_callback, symbol, ticker)
            
            # 2. 检查单独订阅的回调
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'ticker' and sub_symbol == symbol:
                    await self._safe_callback(callback, ticker)

            # 发送事件到事件总线  
            if hasattr(super(), '_handle_ticker_update'):
                await super()._handle_ticker_update(ticker)

        except Exception as e:
            self.logger.error(f"处理Backpack ticker更新失败: {e}")
            self.logger.error(f"符号: {symbol}, 数据内容: {data}")

    async def _handle_backpack_orderbook_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """处理Backpack原生格式的订单簿更新"""
        try:
            # 解析交易所时间戳（微秒）
            exchange_timestamp = None
            if 'E' in data:
                try:
                    timestamp_microseconds = int(data['E'])
                    exchange_timestamp = datetime.fromtimestamp(timestamp_microseconds / 1000000)
                except (ValueError, TypeError):
                    pass

            # 解析买单和卖单
            bids = [
                OrderBookLevel(
                    price=self._safe_decimal(bid[0]),
                    size=self._safe_decimal(bid[1])
                )
                for bid in data.get('b', [])  # Backpack使用 'b' 表示bids
            ]

            asks = [
                OrderBookLevel(
                    price=self._safe_decimal(ask[0]),
                    size=self._safe_decimal(ask[1])
                )
                for ask in data.get('a', [])  # Backpack使用 'a' 表示asks
            ]

            main_timestamp = exchange_timestamp if exchange_timestamp else datetime.now()
            
            orderbook = OrderBookData(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=main_timestamp,
                nonce=data.get('u'),  # 使用更新ID作为nonce
                exchange_timestamp=exchange_timestamp,
                raw_data=data
            )

            # 调用相应的回调函数
            if hasattr(self, 'orderbook_callback') and self.orderbook_callback:
                await self._safe_callback(self.orderbook_callback, symbol, orderbook)
            
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'orderbook' and sub_symbol == symbol:
                    await self._safe_callback(callback, orderbook)

            # 发送事件到事件总线
            await super()._handle_orderbook_update(orderbook)

        except Exception as e:
            self.logger.error(f"处理Backpack订单簿更新失败: {e}")
            self.logger.error(f"符号: {symbol}, 数据内容: {data}")

    async def _handle_backpack_trade_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """处理Backpack原生格式的交易更新"""
        try:
            # 解析成交数据
            trade = TradeData(
                id=str(data.get('t', '')),  # t = trade ID
                symbol=symbol,
                side=OrderSide.BUY if data.get('m') == False else OrderSide.SELL,  # m = is maker
                amount=self._safe_decimal(data.get('q')),   # q = quantity
                price=self._safe_decimal(data.get('p')),    # p = price
                cost=self._safe_decimal(data.get('q', 0)) * self._safe_decimal(data.get('p', 0)),
                fee=None,
                timestamp=datetime.fromtimestamp(data.get('T', 0) / 1000000) if data.get('T') else datetime.now(),  # T = timestamp in microseconds
                order_id=None,
                raw_data=data
            )

            # 调用相应的回调函数
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'trades' and sub_symbol == symbol:
                    await self._safe_callback(callback, trade)

        except Exception as e:
            self.logger.error(f"处理Backpack交易更新失败: {e}")
            self.logger.error(f"符号: {symbol}, 数据内容: {data}")

    async def _handle_ticker_update(self, stream_name: str, data: Dict[str, Any]) -> None:
        """处理行情更新 - 适配完整符号格式"""
        try:
            # 从流名称提取交易对：ticker.SOL_USDC_PERP -> SOL_USDC_PERP
            if '.' in stream_name:
                symbol = stream_name.split('.')[1]  # 直接使用完整符号
            else:
                symbol = stream_name
            
            # 检查符号是否在我们的订阅列表中
            if hasattr(self, '_subscribed_symbols') and symbol not in self._subscribed_symbols:
                self.logger.debug(f"收到未订阅符号的数据: {stream_name} -> {symbol}")
                return

            # 解析交易所时间戳 - 修复版本
            exchange_timestamp = None
            current_time = datetime.now()  # 获取当前时间作为备用
            
            # 调试：偶尔打印数据格式
            if not hasattr(self, '_debug_count'):
                self._debug_count = 0
            self._debug_count += 1
            if self._debug_count % 100 == 1:  # 每100条消息打印一次调试信息
                self.logger.debug(f"🔍 Backpack数据格式示例 {symbol}: {list(data.keys())}")
                if 'timestamp' in data:
                    self.logger.debug(f"   timestamp字段: {data.get('timestamp')}")
                if 'E' in data:
                    self.logger.debug(f"   E字段: {data.get('E')}")
            
            # 尝试多种时间戳字段
            timestamp_candidates = [
                ('timestamp', 1000),        # 毫秒时间戳
                ('E', 1000),               # Binance格式事件时间（毫秒）
                ('eventTime', 1000),       # 事件时间
                ('T', 1000),               # 交易时间
                ('ts', 1000),              # 通用时间戳
                ('time', 1000),            # 时间字段
            ]
            
            for field, divisor in timestamp_candidates:
                if field in data and data[field]:
                    try:
                        timestamp_value = int(data[field])
                        # 检测时间戳精度（微秒 vs 毫秒）
                        if timestamp_value > 1e12:  # 微秒时间戳
                            exchange_timestamp = datetime.fromtimestamp(timestamp_value / 1000000)
                        elif timestamp_value > 1e9:  # 毫秒时间戳
                            exchange_timestamp = datetime.fromtimestamp(timestamp_value / 1000)
                        else:  # 秒时间戳
                            exchange_timestamp = datetime.fromtimestamp(timestamp_value)
                        
                        # 验证时间戳合理性（不能是未来时间，不能太旧）
                        time_diff = abs((current_time - exchange_timestamp).total_seconds())
                        if time_diff < 3600:  # 时间差小于1小时，认为是有效的
                            break
                        else:
                            exchange_timestamp = None
                    except (ValueError, TypeError, OSError):
                        continue

            # 使用当前时间作为主时间戳（确保时效性正确）
            # 注意：我们故意使用当前时间而不是交易所时间戳，因为我们关心的是数据的新鲜度
            main_timestamp = current_time
            
            ticker = TickerData(
                symbol=symbol,  # 直接使用完整符号
                bid=self._safe_decimal(data.get('bidPrice') or data.get('b')),
                ask=self._safe_decimal(data.get('askPrice') or data.get('a')),
                last=self._safe_decimal(data.get('lastPrice') or data.get('c')),
                open=self._safe_decimal(data.get('openPrice') or data.get('o')),
                high=self._safe_decimal(data.get('highPrice') or data.get('h')),
                low=self._safe_decimal(data.get('lowPrice') or data.get('l')),
                close=self._safe_decimal(data.get('lastPrice') or data.get('c')),
                volume=self._safe_decimal(data.get('volume') or data.get('v')),
                quote_volume=self._safe_decimal(data.get('quoteVolume') or data.get('V')),
                change=self._safe_decimal(data.get('priceChange') or data.get('P')),
                percentage=self._safe_decimal(data.get('priceChangePercent') or data.get('p')),
                timestamp=main_timestamp,
                exchange_timestamp=exchange_timestamp,
                raw_data=data
            )

            # 记录成功的ticker更新（限制日志量）
            if not hasattr(self, '_ticker_count'):
                self._ticker_count = {}
            if symbol not in self._ticker_count:
                self._ticker_count[symbol] = 0
                self.logger.info(f"✅ 首次收到ticker数据: {symbol} -> {ticker.last}")
            self._ticker_count[symbol] += 1

            # 调用相应的回调函数
            # 1. 检查批量订阅的回调
            if hasattr(self, 'ticker_callback') and self.ticker_callback:
                await self._safe_callback(self.ticker_callback, symbol, ticker)
            
            # 2. 检查单独订阅的回调
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'ticker' and sub_symbol == symbol:
                    await self._safe_callback(callback, ticker)

            # 发送事件到事件总线  
            if hasattr(super(), '_handle_ticker_update'):
                await super()._handle_ticker_update(ticker)

        except Exception as e:
            self.logger.error(f"处理行情更新失败: {e}")
            self.logger.error(f"流名称: {stream_name}, 数据内容: {data}")

    async def _handle_orderbook_update(self, stream_name: str, data: Dict[str, Any]) -> None:
        """处理订单簿更新"""
        try:
            # 从流名称提取交易对：depth.SOL_USDC -> SOL_USDC
            symbol_part = stream_name.split('.')[1] if '.' in stream_name else stream_name
            symbol = self._reverse_map_symbol(symbol_part)

            # 解析交易所时间戳
            exchange_timestamp = None
            if 'timestamp' in data:
                try:
                    timestamp_ms = int(data['timestamp'])
                    exchange_timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
                except (ValueError, TypeError):
                    pass
            elif 'E' in data:  # Binance格式的事件时间
                try:
                    timestamp_ms = int(data['E'])
                    exchange_timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
                except (ValueError, TypeError):
                    pass

            # 解析买单和卖单
            bids = [
                OrderBookLevel(
                    price=self._safe_decimal(bid[0]),
                    size=self._safe_decimal(bid[1])
                )
                for bid in data.get('bids', data.get('b', []))
            ]

            asks = [
                OrderBookLevel(
                    price=self._safe_decimal(ask[0]),
                    size=self._safe_decimal(ask[1])
                )
                for ask in data.get('asks', data.get('a', []))
            ]

            # 修复：使用exchange_timestamp作为主时间戳，如果没有则使用当前时间
            main_timestamp = exchange_timestamp if exchange_timestamp else datetime.now()
            
            orderbook = OrderBookData(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=main_timestamp,
                nonce=data.get('u'),  # 使用Backpack的更新ID作为nonce
                exchange_timestamp=exchange_timestamp,  # 设置交易所原始时间戳
                raw_data=data
            )

            # 调用相应的回调函数
            # 1. 检查批量订阅的回调
            if hasattr(self, 'orderbook_callback') and self.orderbook_callback:
                await self._safe_callback(self.orderbook_callback, symbol, orderbook)
            
            # 2. 检查单独订阅的回调
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'orderbook' and sub_symbol == symbol:
                    await self._safe_callback(callback, orderbook)

            # 发送事件到事件总线
            await super()._handle_orderbook_update(orderbook)

        except Exception as e:
            self.logger.error(f"处理订单簿更新失败: {e}")
            self.logger.error(f"数据内容: {data}")

    async def _handle_trade_update(self, stream_name: str, data: Dict[str, Any]) -> None:
        """处理成交更新"""
        try:
            # 从流名称提取交易对：trade.SOL_USDC -> SOL_USDC
            symbol_part = stream_name.split('.')[1] if '.' in stream_name else stream_name
            symbol = self._reverse_map_symbol(symbol_part)

            # 解析成交数据
            trade = TradeData(
                id=str(data.get('id', data.get('t', ''))),
                symbol=symbol,
                side=OrderSide.BUY if data.get('isBuyerMaker', data.get('m')) == False else OrderSide.SELL,
                amount=self._safe_decimal(data.get('quantity', data.get('q'))),
                price=self._safe_decimal(data.get('price', data.get('p'))),
                cost=self._safe_decimal(data.get('quantity', data.get('q', 0))) * self._safe_decimal(data.get('price', data.get('p', 0))),
                fee=None,
                timestamp=datetime.fromtimestamp((data.get('timestamp', data.get('T', 0))) / 1000),
                order_id=None,
                raw_data=data
            )

            # 调用相应的回调函数
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'trades' and sub_symbol == symbol:
                    await self._safe_callback(callback, trade)

        except Exception as e:
            self.logger.error(f"处理成交更新失败: {e}")
            self.logger.error(f"数据内容: {data}")

    async def _handle_user_data_update(self, data: Dict[str, Any]) -> None:
        """处理用户数据更新"""
        try:
            # 调用用户数据回调函数
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'user_data':
                    await self._safe_callback(callback, data)

        except Exception as e:
            self.logger.error(f"处理用户数据更新失败: {e}")
            self.logger.error(f"数据内容: {data}")

    async def _close_websocket(self) -> None:
        """关闭WebSocket连接 - 修复版本"""
        try:
            # 标记为断开状态
            self._ws_connected = False
            
            # 取消心跳任务
            if hasattr(self, '_heartbeat_task') and self._heartbeat_task:
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass
                self.logger.info("💓 WebSocket心跳任务已取消")
            
            # 取消消息处理任务
            if hasattr(self, '_ws_handler_task') and self._ws_handler_task:
                self._ws_handler_task.cancel()
                try:
                    await self._ws_handler_task
                except asyncio.CancelledError:
                    pass
                self.logger.info("WebSocket消息处理任务已取消")
            
            # 关闭WebSocket连接
            if hasattr(self, '_ws_connection') and self._ws_connection:
                await self._ws_connection.close()
                self.logger.info("WebSocket连接已关闭")
            
            # 关闭WebSocket session
            if hasattr(self, '_ws_session') and self._ws_session and not self._ws_session.closed:
                await self._ws_session.close()
                self.logger.info("WebSocket session已关闭")
                
        except Exception as e:
            self.logger.error(f"关闭WebSocket连接时出错: {e}")

    async def _safe_callback(self, callback: Callable, *args) -> None:
        """安全调用回调函数"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(*args)
            else:
                callback(*args)
        except Exception as e:
            self.logger.error(f"回调函数执行失败: {e}")
