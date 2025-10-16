"""
EdgeX交易所适配器

基于EdgeX交易所API实现的适配器
官方端点：
- HTTP: https://pro.edgex.exchange/
- WebSocket: wss://quote.edgex.exchange/

注意：由于EdgeX官方API文档不可用，此实现基于标准交易所API模式
"""

import asyncio
import time
import hmac
import hashlib
import json
import aiohttp
from typing import Dict, List, Optional, Any, Union, Callable
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

from ..adapter import ExchangeAdapter
from ..interface import ExchangeConfig
from ..models import (
    ExchangeType, OrderBookData, TradeData, TickerData, BalanceData, OrderData, OrderStatus,
    OrderSide, OrderType, PositionData, OrderBookLevel, ExchangeInfo, OHLCVData
)
from ....services.events import Event


class EdgeXOrderType(Enum):
    """EdgeX订单类型"""
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP_LIMIT = "STOP_LIMIT"
    STOP_MARKET = "STOP_MARKET"


class EdgeXOrderSide(Enum):
    """EdgeX订单方向"""
    BUY = "BUY"
    SELL = "SELL"


class EdgeXTimeInForce(Enum):
    """EdgeX订单时效"""
    GTC = "GTC"  # Good Till Canceled
    IOC = "IOC"  # Immediate Or Cancel
    FOK = "FOK"  # Fill Or Kill


@dataclass
class EdgeXSymbolInfo:
    """EdgeX交易对信息"""
    symbol: str
    base_asset: str
    quote_asset: str
    status: str
    base_precision: int
    quote_precision: int
    min_qty: Decimal
    max_qty: Decimal
    min_price: Decimal
    max_price: Decimal
    tick_size: Decimal
    min_notional: Decimal


class EdgeXAdapter(ExchangeAdapter):
    """EdgeX交易所适配器 - 基于MESA架构的统一接口实现"""

    DEFAULT_BASE_URL = "https://pro.edgex.exchange/"
    DEFAULT_WS_URL = "wss://quote.edgex.exchange/api/v1/public/ws"

    def __init__(self, config: ExchangeConfig, event_bus=None):
        """初始化EdgeX适配器"""
        super().__init__(config, event_bus)
        
        # EdgeX特有的属性
        self.session = None
        self.ws_connections = {}
        self.symbols_info = {}
        
        # 实际支持的交易对（将从API动态获取）
        self._supported_symbols = []
        self._contract_mappings = {}  # contract_id -> symbol
        self._symbol_contract_mappings = {}  # symbol -> contract_id
        
        # 符号映射（通用格式 -> EdgeX格式）
        self._default_symbol_mapping = {
            "BTC/USDC:PERP": "BTC_USDC",
            "ETH/USDC:PERP": "ETH_USDC", 
            "SOL/USDC:PERP": "SOL_USDC",
            "AVAX/USDC:PERP": "AVAX_USDC"
        }
        
        # 合并用户配置的符号映射
        if config.symbol_mapping:
            self._default_symbol_mapping.update(config.symbol_mapping)

        # 交易对信息将从API动态获取
        self.symbols_info = {}


    # === 生命周期管理实现 ===
    
    async def _do_connect(self) -> bool:
        """执行具体的连接逻辑"""
        try:
            # 创建HTTP会话
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={
                    'User-Agent': 'EdgeX-Adapter/1.0',
                    'Content-Type': 'application/json'
                }
            )

            self.logger.info("EdgeX连接成功")
            return True

        except Exception as e:
            self.logger.warning(f"EdgeX连接失败: {str(e)}")
            return False

    async def _do_disconnect(self) -> None:
        """执行具体的断开连接逻辑"""
        try:
            # 关闭WebSocket连接
            for ws in self.ws_connections.values():
                if not ws.closed:
                    await ws.close()
            self.ws_connections.clear()

            # 关闭HTTP会话
            if self.session:
                await self.session.close()
                self.session = None

        except Exception as e:
            self.logger.warning(f"断开EdgeX连接时出错: {e}")

    async def _do_authenticate(self) -> bool:
        """执行具体的认证逻辑"""
        try:
            # EdgeX认证逻辑
            return True
        except Exception as e:
            self.logger.warning(f"EdgeX认证失败: {str(e)}")
            return False

    async def _do_health_check(self) -> Dict[str, Any]:
        """执行具体的健康检查"""
        health_data = {
            'exchange_time': datetime.now(),
            'market_count': len(self.symbols_info),
            'api_accessible': True
        }

        try:
            return health_data
        except Exception as e:
            health_data['error'] = str(e)
            return health_data

    async def _do_heartbeat(self) -> None:
        """执行心跳检测"""
        pass

    # === HTTP请求方法 ===

    async def _fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """获取单个交易对行情数据"""
        url = f"{self.base_url}api/v1/ticker/24hr"
        params = {'symbol': symbol}
        
        async with self.session.get(url, params=params) as response:
            data = await response.json()
            if response.status != 200:
                raise Exception(f"EdgeX API错误: {data}")
            return data

    async def _fetch_orderbook(self, symbol: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """获取订单簿数据"""
        url = f"{self.base_url}api/v1/depth"
        params = {'symbol': symbol}
        if limit:
            params['limit'] = min(limit, 1000)
        
        async with self.session.get(url, params=params) as response:
            data = await response.json()
            if response.status != 200:
                raise Exception(f"EdgeX API错误: {data}")
            return data

    async def _fetch_trades(self, symbol: str, since: Optional[int] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取交易记录"""
        url = f"{self.base_url}api/v1/trades"
        params = {'symbol': symbol}
        if limit:
            params['limit'] = min(limit, 1000)
        if since:
            params['startTime'] = since
        
        async with self.session.get(url, params=params) as response:
            data = await response.json()
            if response.status != 200:
                raise Exception(f"EdgeX API错误: {data}")
                return data

    # === 数据解析方法 ===

    def _parse_ticker(self, data: Dict[str, Any], symbol: str) -> TickerData:
        """解析行情数据"""
            return TickerData(
                symbol=symbol,
            last=Decimal(data.get('lastPrice', '0')),
                bid=Decimal(data.get('bidPrice', '0')),
                ask=Decimal(data.get('askPrice', '0')),
            open=Decimal(data.get('openPrice', '0')),
                high=Decimal(data.get('highPrice', '0')),
                low=Decimal(data.get('lowPrice', '0')),
                close=Decimal(data.get('lastPrice', '0')),
                volume=Decimal(data.get('volume', '0')),
            quote_volume=Decimal(data.get('quoteVolume', '0')),
                change=Decimal(data.get('priceChange', '0')),
                percentage=Decimal(data.get('priceChangePercent', '0')),
            timestamp=datetime.fromtimestamp(int(data.get('closeTime', time.time() * 1000)) / 1000),
                raw_data=data
            )

    def _parse_orderbook(self, data: Dict[str, Any], symbol: str) -> OrderBookData:
        """解析订单簿数据"""
        bids = [
            OrderBookLevel(
                price=Decimal(bid[0]),
                quantity=Decimal(bid[1])
            )
            for bid in data.get('bids', [])
        ]

        asks = [
            OrderBookLevel(
                price=Decimal(ask[0]),
                quantity=Decimal(ask[1])
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

    def _parse_trade(self, data: Dict[str, Any], symbol: str) -> TradeData:
        """解析交易数据"""
        return TradeData(
            id=str(data.get('id', '')),
            symbol=symbol,
            side=OrderSide.BUY if data.get('isBuyerMaker', False) else OrderSide.SELL,
            amount=Decimal(data.get('qty', '0')),
            price=Decimal(data.get('price', '0')),
            cost=Decimal(data.get('quoteQty', '0')),
            fee=None,
            timestamp=datetime.fromtimestamp(int(data.get('time', time.time() * 1000)) / 1000),
            order_id=None,
            raw_data=data
        )

    # === 符号映射方法 ===

    def _map_symbol(self, symbol: str) -> str:
        """映射交易对符号"""
        return self._default_symbol_mapping.get(symbol, symbol)

    def _reverse_map_symbol(self, exchange_symbol: str) -> str:
        """反向映射交易对符号"""
        reverse_mapping = {v: k for k, v in self._default_symbol_mapping.items()}
        return reverse_mapping.get(exchange_symbol, exchange_symbol)

    # === 市场数据接口实现 ===

    async def get_exchange_info(self) -> ExchangeInfo:
        """获取交易所信息"""
        return ExchangeInfo(
            name="EdgeX",
            id="edgex",
            type=ExchangeType.PERPETUAL,
            supported_features=[
                "spot_trading", "perpetual_trading", "websocket",
                "orderbook", "ticker", "ohlcv", "user_stream"
            ],
            rate_limits=self.config.rate_limits,
            precision=self.config.precision,
            fees={},  # TODO: 获取实际费率
            markets={},
            status="operational",
            timestamp=datetime.now()
        )

    async def get_ticker(self, symbol: str) -> TickerData:
        """获取单个交易对行情数据"""
        mapped_symbol = self._map_symbol(symbol)

        ticker_data = await self._execute_with_retry(
            self._fetch_ticker,
            mapped_symbol,
            operation_name="get_ticker"
        )

        return self._parse_ticker(ticker_data, symbol)

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿数据"""
        mapped_symbol = self._map_symbol(symbol)

        orderbook_data = await self._execute_with_retry(
            self._fetch_orderbook,
            mapped_symbol,
            limit,
            operation_name="get_orderbook"
        )

        return self._parse_orderbook(orderbook_data, symbol)

    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """获取最近成交记录"""
        mapped_symbol = self._map_symbol(symbol)
        since_timestamp = int(since.timestamp() * 1000) if since else None

        trades_data = await self._execute_with_retry(
            self._fetch_trades,
            mapped_symbol,
            since_timestamp,
            limit,
            operation_name="get_trades"
        )

        return [
            self._parse_trade(trade, symbol)
            for trade in trades_data
        ]

    async def get_balances(self) -> List[BalanceData]:
        """获取账户余额"""
        balance_data = await self._execute_with_retry(
            self._fetch_balances,
            operation_name="get_balances"
        )

        return [
            self._parse_balance(balance)
            for balance in balance_data.get('balances', [])
            if Decimal(balance.get('free', '0')) > 0 or Decimal(balance.get('locked', '0')) > 0
        ]

    async def _fetch_balances(self) -> Dict[str, Any]:
        """获取账户余额数据"""
        url = f"{self.base_url}api/v1/account"
        headers = self._get_auth_headers()
        
        async with self.session.get(url, headers=headers) as response:
            data = await response.json()
            if response.status != 200:
                raise Exception(f"EdgeX API错误: {data}")
                return data

    def _parse_balance(self, data: Dict[str, Any]) -> BalanceData:
        """解析余额数据"""
        free = Decimal(data.get('free', '0'))
        locked = Decimal(data.get('locked', '0'))
        
        return BalanceData(
            currency=data.get('asset', ''),
                        free=free,
                        used=locked,
                        total=free + locked,
                        usd_value=None,
                        timestamp=datetime.now(),
            raw_data=data
        )

    def _get_auth_headers(self) -> Dict[str, str]:
        """获取认证请求头"""
        timestamp = str(int(time.time() * 1000))
        
        # 简单的API Key认证
        return {
            'X-API-Key': self.api_key,
            'X-Timestamp': timestamp,
            'Content-Type': 'application/json'
        }

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """
        获取多个交易对行情

        Args:
            symbols: 交易对符号列表，None表示获取所有

        Returns:
            List[TickerData]: 行情数据列表
        """
        try:
            if symbols is None:
                symbols = list(self.symbols_info.keys())

            # 并发获取所有ticker数据
            tasks = [self.get_ticker(symbol) for symbol in symbols]
            tickers = await asyncio.gather(*tasks, return_exceptions=True)

            # 过滤掉异常结果
            valid_tickers = [
                ticker for ticker in tickers if isinstance(ticker, TickerData)]

            return valid_tickers

        except Exception as e:
            self.logger.warning(f"获取多个行情数据失败: {e}")
            return []

    def _normalize_symbol(self, symbol: str) -> str:
        """标准化交易对符号"""
        # 应用符号映射
        if symbol in self.symbol_mapping:
            return self.symbol_mapping[symbol]

        # 标准格式转换为EdgeX格式
        # 例如: BTC/USDT -> BTC_USDC (EdgeX使用USDC)
        if '/' in symbol:
            base, quote = symbol.split('/')
            if quote == 'USDT':
                quote = 'USDC'  # EdgeX主要使用USDC
            symbol = f"{base}_{quote}"
        if ':' in symbol:
            # 处理期货合约格式
            symbol = symbol.replace(':', '_')

        return symbol.upper()

    async def get_supported_symbols(self) -> List[str]:
        """获取交易所实际支持的交易对列表"""
        if not self._supported_symbols:
            await self._fetch_supported_symbols()
        return self._supported_symbols.copy()

    async def _fetch_supported_symbols(self) -> None:
        """通过metadata频道获取支持的交易对"""
        try:
            self.logger.info("开始获取EdgeX支持的交易对列表...")
            
            # 创建临时WebSocket连接来获取metadata
            session = aiohttp.ClientSession()
            ws = None
            try:
                ws = await session.ws_connect(self.DEFAULT_WS_URL)
                
                # 订阅metadata频道
                subscribe_msg = {
                    "type": "subscribe",
                    "channel": "metadata"
                }
                await ws.send_str(json.dumps(subscribe_msg))
                
                # 等待并处理响应
                timeout = 10  # 10秒超时
                start_time = time.time()
                
                while time.time() - start_time < timeout:
                    try:
                        msg = await asyncio.wait_for(ws.receive(), timeout=2)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            
                            # 检查是否是metadata响应
                            if (data.get("type") == "quote-event" and 
                                data.get("channel") == "metadata"):
                                
                                self.logger.info("收到metadata quote-event消息")
                                await self._process_metadata_response(data)
                                break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            self.logger.warning(f"WebSocket错误: {ws.exception()}")
                            break
                            
                    except asyncio.TimeoutError:
                        continue
        except Exception as e:
                        self.logger.warning(f"处理metadata响应时出错: {e}")
                        break
                
                if not self._supported_symbols:
                    self.logger.warning("未能获取到支持的交易对")
                else:
                    self.logger.info(f"成功获取到 {len(self._supported_symbols)} 个交易对")
                    
            finally:
                if ws and not ws.closed:
                    await ws.close()
                await session.close()
                    
        except Exception as e:
            self.logger.warning(f"获取支持的交易对时出错: {e}")

    async def _process_metadata_response(self, data: Dict[str, Any]) -> None:
        """处理metadata响应数据"""
        try:
            self.logger.info(f"开始处理metadata响应: {json.dumps(data, indent=2)}")
            
            content = data.get("content", {})
            self.logger.info(f"metadata content: {json.dumps(content, indent=2)}")
            
            # 尝试多种数据结构
            contracts = []
            
            # 根据分析结果，合约数据位于: content.data[0].contractList
            metadata_data = content.get("data", [])
            self.logger.info(f"metadata_data类型: {type(metadata_data)}, 长度: {len(metadata_data) if isinstance(metadata_data, list) else 'N/A'}")
            
            if metadata_data and isinstance(metadata_data, list) and len(metadata_data) > 0:
                first_item = metadata_data[0]
                self.logger.info(f"第一个数据项的keys: {list(first_item.keys()) if isinstance(first_item, dict) else 'N/A'}")
                
                # EdgeX实际使用contractList字段
                contracts = first_item.get("contractList", [])
                if contracts:
                    self.logger.info(f"✅ 在data[0].contractList中找到 {len(contracts)} 个合约")
                else:
                    # 备用方案：尝试contract字段
                    contracts = first_item.get("contract", [])
                    if contracts:
                        self.logger.info(f"✅ 在data[0].contract中找到 {len(contracts)} 个合约")
                    else:
                        self.logger.warning("❌ 在data[0]中未找到contractList或contract字段")
            else:
                self.logger.warning("❌ metadata_data为空或格式不正确")
            
            if not contracts:
                self.logger.warning("未找到任何合约数据")
                return
                
            supported_symbols = []
            contract_mappings = {}
            symbol_contract_mappings = {}
            
            total_contracts = len(contracts)
            filtered_contracts = []
            
            self.logger.info(f"开始处理 {total_contracts} 个合约...")
            
            for contract in contracts:
                contract_id = contract.get("contractId")
                # EdgeX使用contractName字段，而不是symbol字段
                symbol = contract.get("contractName") or contract.get("symbol")
                # 只保留启用交易且启用显示的合约
                enable_trade = contract.get("enableTrade", False)
                enable_display = contract.get("enableDisplay", False)
                
                if contract_id and symbol:
                    contract_info = {
                        'symbol': symbol,
                        'contract_id': contract_id,
                        'enable_trade': enable_trade,
                        'enable_display': enable_display,
                        'included': enable_trade and enable_display
                    }
                    
                    if enable_trade and enable_display:
                        # 将symbol转换为标准格式
                        normalized_symbol = self._normalize_contract_symbol(symbol)
                        
                        supported_symbols.append(normalized_symbol)
                        contract_mappings[contract_id] = normalized_symbol
                        symbol_contract_mappings[normalized_symbol] = contract_id
                        
                        self.logger.info(f"✅ 包含交易对: {symbol} -> {normalized_symbol} (ID: {contract_id})")
                    else:
                        # 记录过滤原因
                        reasons = []
                        if not enable_trade:
                            reasons.append("未启用交易")
                        if not enable_display:
                            reasons.append("未启用显示")
                        
                        reason_str = "、".join(reasons)
                        self.logger.info(f"❌ 过滤交易对: {symbol} (ID: {contract_id}) - {reason_str}")
                    
                    filtered_contracts.append(contract_info)
                else:
                    self.logger.warning(f"⚠️  无效合约数据: contractId={contract_id}, symbol={symbol}")
            
            # 输出统计信息
            included_count = len(supported_symbols)
            excluded_count = total_contracts - included_count
            
            self.logger.info(f"📊 EdgeX交易对统计:")
            self.logger.info(f"   总合约数: {total_contracts}")
            self.logger.info(f"   包含的: {included_count}")
            self.logger.info(f"   过滤的: {excluded_count}")
            
            # 显示被过滤的交易对详情
            if excluded_count > 0:
                excluded_symbols = [c['symbol'] for c in filtered_contracts if not c['included']]
                self.logger.info(f"被过滤的交易对: {excluded_symbols}")
            
            self._supported_symbols = supported_symbols
            self._contract_mappings = contract_mappings
            self._symbol_contract_mappings = symbol_contract_mappings
            
            self.logger.info(f"✅ 成功解析metadata，最终获取到 {len(supported_symbols)} 个可用交易对")

        except Exception as e:
            self.logger.warning(f"处理metadata响应时出错: {e}")
            self.logger.warning(f"数据结构: {json.dumps(data, indent=2)}")

    def _normalize_contract_symbol(self, symbol: str) -> str:
        """将EdgeX合约symbol转换为标准格式"""
        # EdgeX返回类似 "BTCUSDT", "ETHUSDT", "SOLUSDT" 等格式
        # 标准化为 "BTC_USDT" 格式
        if "/" in symbol:
            return symbol.replace("/", "_")
        elif symbol.endswith("USDT"):
            # 处理BTCUSDT格式
            base = symbol[:-4]  # 移除USDT
            return f"{base}_USDT"
        elif symbol.endswith("USDC"):
            # 处理BTCUSDC格式
            base = symbol[:-4]  # 移除USDC
            return f"{base}_USDC"
        else:
            # 对于其他格式，保持原样
            return symbol

    async def batch_subscribe_tickers(self, symbols: Optional[List[str]] = None, 
                                     callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """批量订阅多个交易对的ticker数据"""
        try:
            # 如果未指定symbols，使用所有支持的交易对
            if symbols is None:
                symbols = await self.get_supported_symbols()
                
            self.logger.info(f"开始批量订阅 {len(symbols)} 个交易对的ticker数据")
            
            # 建立WebSocket连接
            await self._setup_websocket_connection()
            
            # 批量订阅
            for symbol in symbols:
                try:
                    contract_id = self._symbol_contract_mappings.get(symbol)
                    if not contract_id:
                        self.logger.warning(f"未找到交易对 {symbol} 的合约ID，跳过订阅")
                        continue
                    
                    # 订阅ticker
                    subscribe_msg = {
                        "type": "subscribe",
                        "channel": f"ticker.{contract_id}"
                    }
                    
                    if hasattr(self, '_ws_connection') and self._ws_connection:
                        await self._ws_connection.send_str(json.dumps(subscribe_msg))
                        self.logger.debug(f"已订阅 {symbol} (合约ID: {contract_id}) 的ticker")
                    
                    # 小延迟避免过于频繁的请求
                    await asyncio.sleep(0.1)

        except Exception as e:
                    self.logger.warning(f"订阅 {symbol} ticker时出错: {e}")
                    continue
                    
            # 如果提供了回调函数，保存它
            if callback:
                self.ticker_callback = callback
                
            self.logger.info(f"批量ticker订阅完成")
            
        except Exception as e:
            self.logger.warning(f"批量订阅ticker时出错: {e}")

    async def batch_subscribe_orderbooks(self, symbols: Optional[List[str]] = None,
                                        depth: int = 15,
                                        callback: Optional[Callable[[str, OrderBookData], None]] = None) -> None:
        """批量订阅多个交易对的订单簿数据"""
        try:
            # 如果未指定symbols，使用所有支持的交易对
            if symbols is None:
                symbols = await self.get_supported_symbols()
                
            self.logger.info(f"开始批量订阅 {len(symbols)} 个交易对的订单簿数据")
            
            # 建立WebSocket连接
            await self._setup_websocket_connection()
            
            # 批量订阅
            for symbol in symbols:
                try:
                    contract_id = self._symbol_contract_mappings.get(symbol)
                    if not contract_id:
                        self.logger.warning(f"未找到交易对 {symbol} 的合约ID，跳过订阅")
                        continue
                    
                    # 订阅orderbook - 确保depth参数为整数
                    subscribe_msg = {
                        "type": "subscribe",
                        "channel": f"depth.{contract_id}.{int(depth)}"
                    }
                    
                    if hasattr(self, '_ws_connection') and self._ws_connection:
                        await self._ws_connection.send_str(json.dumps(subscribe_msg))
                        self.logger.debug(f"已订阅 {symbol} (合约ID: {contract_id}) 的订单簿，深度: {depth}")
                    
                    # 小延迟避免过于频繁的请求
                    await asyncio.sleep(0.1)

                except Exception as e:
                    self.logger.warning(f"订阅 {symbol} 订单簿时出错: {e}")
                    continue
                    
            # 如果提供了回调函数，保存它
            if callback:
                self.orderbook_callback = callback
                
            self.logger.info(f"批量订单簿订阅完成")
            
        except Exception as e:
            self.logger.warning(f"批量订阅订单簿时出错: {e}")

    async def batch_subscribe_all_tickers(self, callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """订阅所有交易对的ticker数据（使用ticker.all频道）"""
        try:
            self.logger.info("开始订阅所有交易对的ticker数据")
            
            # 建立WebSocket连接
            await self._setup_websocket_connection()
            
            # 订阅所有ticker
            subscribe_msg = {
                "type": "subscribe",
                "channel": "ticker.all"
            }
            
            if hasattr(self, '_ws_connection') and self._ws_connection:
                await self._ws_connection.send_str(json.dumps(subscribe_msg))
                self.logger.info("已订阅所有交易对的ticker数据")
            
            # 如果提供了回调函数，保存它
            if callback:
                self.ticker_callback = callback

        except Exception as e:
            self.logger.warning(f"订阅所有ticker时出错: {e}")

    async def get_symbol_info(self, symbol: str) -> Optional[EdgeXSymbolInfo]:
        """获取交易对信息"""
        return self.symbols_info.get(symbol)

    async def get_exchange_status(self) -> Dict[str, Any]:
        """获取交易所状态"""
            return {
            'status': 'online' if self.connected else 'offline',
                'timestamp': int(time.time() * 1000)
            }

    async def authenticate(self) -> bool:
        """
        进行身份认证

        Returns:
            bool: 认证是否成功
        """
        # 实际认证逻辑应该在这里实现
        # 目前设为True以保持向后兼容
            self.is_authenticated = True
            return True

    async def health_check(self) -> Dict[str, Any]:
        """
        健康检查

        Returns:
            Dict: 健康状态信息
        """
                return {
            "status": "ok" if self.connected else "disconnected",
            "connection": "connected" if self.connected else "disconnected",
            "authentication": "enabled" if self.is_authenticated else "disabled",
            "symbols_count": len(self.symbols_info),
                    "timestamp": time.time()
                }

    # 订单相关方法（基本实现）
    async def place_order(self, symbol: str, side: OrderSide, order_type: OrderType,
                          quantity: Decimal, price: Decimal = None,
                          time_in_force: str = "GTC", client_order_id: str = None) -> OrderData:
        """下单功能暂未实现"""
        raise NotImplementedError("EdgeX下单功能暂未实现")

    async def cancel_order(self, symbol: str, order_id: str = None,
                           client_order_id: str = None) -> bool:
        """取消订单功能暂未实现"""
        raise NotImplementedError("EdgeX取消订单功能暂未实现")

    async def get_order_status(self, symbol: str, order_id: str = None,
                               client_order_id: str = None) -> OrderData:
        """查询订单状态功能暂未实现"""
        raise NotImplementedError("EdgeX查询订单功能暂未实现")

    # ======================
    # WebSocket连接和订阅 - 统一接口实现
    # ======================

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅行情数据流 - 统一接口"""
        await self._subscribe_websocket('ticker', symbol, callback)

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """订阅订单簿数据流 - 统一接口"""
        await self._subscribe_websocket('orderbook', symbol, callback)

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅成交数据流 - 统一接口"""
        await self._subscribe_websocket('trades', symbol, callback)

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """订阅用户数据流 - 统一接口"""
        await self._subscribe_websocket('user_data', None, callback)

    # 向后兼容的方法
    async def subscribe_order_book(self, symbol: str, callback, depth: int = 20):
        """订阅订单簿数据 - 向后兼容"""
        await self.subscribe_orderbook(symbol, callback)

    async def _subscribe_websocket(self, sub_type: str, symbol: Optional[str], callback: Callable) -> None:
        """WebSocket订阅通用方法 - 与Backpack保持一致"""
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
            self.logger.warning(f"EdgeX WebSocket订阅失败 {sub_type} {symbol}: {e}")

    async def _setup_websocket_connection(self) -> None:
        """建立WebSocket连接"""
        try:
            import aiohttp
            import json

            # 使用EdgeX WebSocket URL
            ws_url = self.DEFAULT_WS_URL

            if hasattr(self, 'session'):
                self._ws_connection = await self.session.ws_connect(ws_url)
            else:
                session = aiohttp.ClientSession()
                self._ws_connection = await session.ws_connect(ws_url)

            self.logger.info(f"EdgeX WebSocket连接已建立: {ws_url}")

            # 启动消息处理任务
            asyncio.create_task(self._websocket_message_handler())

        except Exception as e:
            self.logger.warning(f"建立EdgeX WebSocket连接失败: {e}")

    def _build_subscribe_message(self, sub_type: str, symbol: Optional[str]) -> str:
        """构建订阅消息 - 基于EdgeX实际API格式"""
        import json

        # 使用动态映射系统获取合约ID
        contract_id = self._symbol_contract_mappings.get(symbol, "10000001") if symbol else "10000001"
        
        if sub_type == 'ticker':
            # 24小时ticker统计
            return json.dumps({
                "type": "subscribe",
                "channel": f"ticker.{contract_id}"
            })
        elif sub_type == 'orderbook':
            # 实时订单簿深度
            return json.dumps({
                "type": "subscribe",
                "channel": f"depth.{contract_id}.15"
            })
        elif sub_type == 'trades':
            # 实时交易流
            return json.dumps({
                "type": "subscribe",
                "channel": f"trades.{contract_id}"
            })
        elif sub_type == 'user_data':
            # 用户数据流需要认证
            return json.dumps({
                "type": "subscribe",
                "channel": "userData"
            })
        else:
            return json.dumps({
                "type": "subscribe",
                "channel": f"ticker.{contract_id}"
            })

    async def _websocket_message_handler(self) -> None:
        """WebSocket消息处理器"""
        try:
            async for msg in self._ws_connection:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._process_websocket_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    self.logger.warning(f"EdgeX WebSocket错误: {self._ws_connection.exception()}")
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    self.logger.info("EdgeX WebSocket连接已关闭")
                    break

        except Exception as e:
            self.logger.warning(f"EdgeX WebSocket消息处理失败: {e}")

    async def _process_websocket_message(self, message: str) -> None:
        """处理WebSocket消息"""
        try:
            import json
            data = json.loads(message)

            # 处理连接确认消息
            if data.get('type') == 'connected':
                self.logger.info(f"EdgeX WebSocket连接确认: {data.get('sid')}")
                return

            # 处理订阅确认消息
            if data.get('type') == 'subscribed':
                self.logger.info(f"EdgeX订阅成功: {data.get('channel')}")
                return

            # 处理ping消息
            if data.get('type') == 'ping':
                pong_message = {
                    "type": "pong",
                    "time": data.get("time")
                }
                if hasattr(self, '_ws_connection') and self._ws_connection:
                    await self._ws_connection.send_str(json.dumps(pong_message))
                    self.logger.debug(f"发送pong响应: {data.get('time')}")
                return

            # 处理数据消息
            if data.get('type') == 'quote-event':
                channel = data.get('channel', '')
                content = data.get('content', {})
                
                if channel.startswith('ticker.'):
                    await self._handle_ticker_update(channel, content)
                elif channel.startswith('depth.'):
                    await self._handle_orderbook_update(channel, content)
                elif channel.startswith('trades.'):
                    await self._handle_trade_update(channel, content)
                else:
                    self.logger.debug(f"EdgeX未知的频道类型: {channel}")
                return

            # 处理错误消息
            if data.get('type') == 'error':
                self.logger.warning(f"EdgeX WebSocket错误: {data.get('content')}")
                return

            # 其他未识别的消息
            self.logger.debug(f"EdgeX未知消息格式: {data}")

        except Exception as e:
            self.logger.warning(f"处理EdgeX WebSocket消息失败: {e}")
            self.logger.debug(f"原始消息: {message}")

    async def _handle_ticker_update(self, channel: str, content: Dict[str, Any]) -> None:
        """处理行情更新"""
        try:
            # 从频道名称提取contractId
            contract_id = channel.split('.')[-1]  # ticker.10000001 -> 10000001
            symbol = self._contract_mappings.get(contract_id)
            
            if not symbol:
                self.logger.debug(f"未找到合约ID {contract_id} 对应的交易对")
                return

            # 解析EdgeX ticker数据格式
            data_list = content.get('data', [])
            if not data_list:
                return
                
            ticker_data = data_list[0]  # 取第一个数据

            # 解析交易所时间戳
            exchange_timestamp = None
            if 'timestamp' in ticker_data:
                try:
                    timestamp_ms = int(ticker_data['timestamp'])
                    exchange_timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
                except (ValueError, TypeError):
                    pass
            elif 'ts' in ticker_data:  # 可能的时间戳字段
                try:
                    timestamp_ms = int(ticker_data['ts'])
                    exchange_timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
                except (ValueError, TypeError):
                    pass

            # 根据EdgeX实际数据格式解析
            # 修复：使用exchange_timestamp作为主时间戳，如果没有则使用当前时间
            main_timestamp = exchange_timestamp if exchange_timestamp else datetime.now()
            
            ticker = TickerData(
                symbol=symbol,
                bid=self._safe_decimal(ticker_data.get('bestBidPrice')),
                ask=self._safe_decimal(ticker_data.get('bestAskPrice')),
                last=self._safe_decimal(ticker_data.get('lastPrice')),
                open=self._safe_decimal(ticker_data.get('open')),
                high=self._safe_decimal(ticker_data.get('high')),
                low=self._safe_decimal(ticker_data.get('low')),
                close=self._safe_decimal(ticker_data.get('close')),
                volume=self._safe_decimal(ticker_data.get('size')),
                quote_volume=self._safe_decimal(ticker_data.get('value')),
                change=self._safe_decimal(ticker_data.get('priceChange')),
                percentage=self._safe_decimal(ticker_data.get('priceChangePercent')),
                timestamp=main_timestamp,
                exchange_timestamp=exchange_timestamp,  # 设置交易所原始时间戳
                raw_data=ticker_data
            )

            # 调用相应的回调函数
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'ticker' and sub_symbol == symbol:
                    await self._safe_callback(callback, ticker)

            # 发送事件到事件总线
            await super()._handle_ticker_update(ticker)

        except Exception as e:
            self.logger.warning(f"处理EdgeX行情更新失败: {e}")
            self.logger.debug(f"频道: {channel}, 内容: {content}")

    async def _handle_orderbook_update(self, channel: str, content: Dict[str, Any]) -> None:
        """处理EdgeX订单簿更新"""
        try:
            # 从频道名称提取contractId
            contract_id = channel.split('.')[-2]  # depth.10000001.15 -> 10000001
            symbol = self._contract_mappings.get(contract_id)
            
            if not symbol:
                self.logger.debug(f"未找到合约ID {contract_id} 对应的交易对")
                return

            # 解析EdgeX订单簿数据格式
            data_list = content.get('data', [])
            if not data_list:
                return
                
            orderbook_data = data_list[0]  # 取第一个数据

            # 解析交易所时间戳
            exchange_timestamp = None
            if 'timestamp' in orderbook_data:
                try:
                    timestamp_ms = int(orderbook_data['timestamp'])
                    exchange_timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
                except (ValueError, TypeError):
                    pass
            elif 'ts' in orderbook_data:  # 可能的时间戳字段
                try:
                    timestamp_ms = int(orderbook_data['ts'])
                    exchange_timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
                except (ValueError, TypeError):
                    pass

            # 解析买单和卖单
            bids = []
            for bid in orderbook_data.get('bids', []):
                bids.append(OrderBookLevel(
                    price=self._safe_decimal(bid.get('price')),
                    size=self._safe_decimal(bid.get('size'))
                ))

            asks = []
            for ask in orderbook_data.get('asks', []):
                asks.append(OrderBookLevel(
                    price=self._safe_decimal(ask.get('price')),
                    size=self._safe_decimal(ask.get('size'))
                ))

            # 创建OrderBookData对象，包含nonce参数
            orderbook = OrderBookData(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=datetime.now(),
                nonce=orderbook_data.get('endVersion'),  # 使用EdgeX的版本号作为nonce
                exchange_timestamp=exchange_timestamp,  # 设置交易所原始时间戳
                raw_data=orderbook_data
            )

            # 调用相应的回调函数
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'orderbook' and sub_symbol == symbol:
                    await self._safe_callback(callback, orderbook)

            # 发送事件到事件总线
            await super()._handle_orderbook_update(orderbook)

        except Exception as e:
            self.logger.warning(f"处理EdgeX订单簿更新失败: {e}")
            self.logger.debug(f"频道: {channel}, 内容: {content}")

    async def _handle_trade_update(self, stream_name: str, data: Dict[str, Any]) -> None:
        """处理成交更新"""
        try:
            symbol_part = stream_name.split('@')[0]
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
            self.logger.warning(f"处理EdgeX成交更新失败: {e}")
            self.logger.debug(f"数据内容: {data}")

    async def _handle_user_data_update(self, data: Dict[str, Any]) -> None:
        """处理用户数据更新"""
        try:
            # 调用用户数据回调函数
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'user_data':
                    await self._safe_callback(callback, data)

        except Exception as e:
            self.logger.warning(f"处理EdgeX用户数据更新失败: {e}")
            self.logger.debug(f"数据内容: {data}")

    async def _close_websocket(self) -> None:
        """关闭WebSocket连接"""
        try:
            if hasattr(self, '_ws_connection') and self._ws_connection:
                await self._ws_connection.close()
                self._ws_connection = None
                self.logger.info("EdgeX WebSocket连接已关闭")
        except Exception as e:
            self.logger.warning(f"关闭EdgeX WebSocket连接失败: {e}")

    async def _safe_callback(self, callback: Callable, data: Any) -> None:
        """安全调用回调函数"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            self.logger.warning(f"EdgeX回调函数执行失败: {e}")

    # ======================
    # 工具方法
    # ======================

    def format_quantity(self, symbol: str, quantity: Decimal) -> Decimal:
        """格式化数量精度"""
        symbol_info = self.get_symbol_info(symbol)
        if symbol_info:
            # 根据交易对精度规则格式化数量
            precision = symbol_info.base_precision
            return quantity.quantize(Decimal('0.1') ** precision)
        return quantity

    def format_price(self, symbol: str, price: Decimal) -> Decimal:
        """格式化价格精度"""
        symbol_info = self.get_symbol_info(symbol)
        if symbol_info:
            # 根据交易对精度规则格式化价格
            precision = symbol_info.quote_precision
            return price.quantize(Decimal('0.1') ** precision)
        return price

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        获取K线数据

        Args:
            symbol: 交易对符号
            timeframe: 时间框架（如'1m', '5m', '1h', '1d'）
            since: 开始时间
            limit: 数据条数限制

        Returns:
            List[Dict]: K线数据列表
        """
        try:
            # 映射时间框架
            interval_map = {
                '1m': '1m',
                '5m': '5m',
                '15m': '15m',
                '30m': '30m',
                '1h': '1h',
                '4h': '4h',
                '1d': '1d'
            }

            interval = interval_map.get(timeframe, '1h')
            symbol = self._normalize_symbol(symbol)

            params = {
                'symbol': symbol,
                'interval': interval
            }

            if limit:
                params['limit'] = min(limit, 1000)  # 限制最大1000条

            if since:
                params['startTime'] = int(since.timestamp() * 1000)

            endpoint = "api/v1/klines"
            response = await self._request('GET', endpoint, params)

            # 转换数据格式
            klines = []
            for kline in response:
                if len(kline) >= 6:
                    klines.append({
                        'timestamp': kline[0],
                        'open': float(kline[1]),
                        'high': float(kline[2]),
                        'low': float(kline[3]),
                        'close': float(kline[4]),
                        'volume': float(kline[5])
                    })

            return klines

        except Exception as e:
            self.logger.warning(f"获取K线数据失败: {e}")
            return []

    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """
        获取最近成交记录（接口匹配方法）

        Args:
            symbol: 交易对符号
            since: 开始时间
            limit: 数据条数限制

        Returns:
            List[TradeData]: 成交数据列表
        """
        return await self.get_recent_trades(symbol, limit or 500)

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        获取持仓信息

        Args:
            symbols: 交易对符号列表

        Returns:
            List[Dict]: 持仓数据列表
        """
        try:
            if not self.is_authenticated:
                raise Exception("需要认证才能获取持仓信息")

            params = {}
            if symbols:
                params['symbols'] = ','.join(
                    [self._normalize_symbol(s) for s in symbols])

            endpoint = "api/v1/account/positions"
            response = await self._request('GET', endpoint, params, signed=True)

            positions = []
            for pos in response.get('positions', []):
                positions.append({
                    'symbol': pos.get('symbol', ''),
                    'size': Decimal(str(pos.get('positionAmt', '0'))),
                    'side': 'long' if float(pos.get('positionAmt', '0')) > 0 else 'short',
                    'entry_price': Decimal(str(pos.get('entryPrice', '0'))),
                    'mark_price': Decimal(str(pos.get('markPrice', '0'))),
                    'unrealized_pnl': Decimal(str(pos.get('unRealizedProfit', '0'))),
                    'percentage': float(pos.get('percentage', '0')),
                    'timestamp': datetime.now()
                })

            return positions

        except Exception as e:
            self.logger.warning(f"获取持仓信息失败: {e}")
            return []

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> OrderData:
        """
        创建订单（接口匹配方法）

        Args:
            symbol: 交易对符号
            side: 订单方向
            order_type: 订单类型
            amount: 数量
            price: 价格
            params: 额外参数

        Returns:
            OrderData: 订单数据
        """
        return await self.place_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=amount,
            price=price,
            time_in_force=params.get(
                'timeInForce', 'GTC') if params else 'GTC',
            client_order_id=params.get('clientOrderId') if params else None
        )

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """
        取消所有订单

        Args:
            symbol: 交易对符号，None表示取消所有

        Returns:
            List[OrderData]: 被取消的订单列表
        """
        try:
            if not self.is_authenticated:
                raise Exception("需要认证才能取消订单")

            params = {}
            if symbol:
                params['symbol'] = self._normalize_symbol(symbol)

            endpoint = "api/v1/openOrders"
            response = await self._request('DELETE', endpoint, params, signed=True)

            cancelled_orders = []
            for order in response:
                cancelled_orders.append(self._parse_order(order))

            return cancelled_orders

        except Exception as e:
            self.logger.warning(f"取消所有订单失败: {e}")
            return []

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """
        获取单个订单信息（接口匹配方法）

        Args:
            order_id: 订单ID
            symbol: 交易对符号

        Returns:
            OrderData: 订单数据
        """
        return await self.get_order_status(symbol, order_id)

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """
        获取开放订单

        Args:
            symbol: 交易对符号，None表示获取所有

        Returns:
            List[OrderData]: 开放订单列表
        """
        try:
            # EdgeX智能模拟模式 - 返回空订单列表
            return []
        except Exception as e:
            self.logger.warning(f"获取开放订单失败: {e}")
            return []

    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OrderData]:
        """
        获取订单历史

        Args:
            symbol: 交易对符号
            since: 开始时间
            limit: 数据条数限制

        Returns:
            List[OrderData]: 订单历史列表
        """
        try:
            if not self.is_authenticated:
                raise Exception("需要认证才能获取订单历史")

            params = {}
            if symbol:
                params['symbol'] = self._normalize_symbol(symbol)
            if since:
                params['startTime'] = int(since.timestamp() * 1000)
            if limit:
                params['limit'] = min(limit, 1000)

            endpoint = "api/v1/allOrders"
            response = await self._request('GET', endpoint, params, signed=True)

            orders = []
            for order in response:
                orders.append(self._parse_order(order))

            return orders

        except Exception as e:
            self.logger.warning(f"获取订单历史失败: {e}")
            return []

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """
        设置杠杆倍数

        Args:
            symbol: 交易对符号
            leverage: 杠杆倍数

        Returns:
            Dict: 设置结果
        """
        try:
            if not self.is_authenticated:
                raise Exception("需要认证才能设置杠杆")

            params = {
                'symbol': self._normalize_symbol(symbol),
                'leverage': leverage
            }

            endpoint = "api/v1/leverage"
            response = await self._request('POST', endpoint, params, signed=True)

            return {
                'symbol': symbol,
                'leverage': leverage,
                'result': response
            }

        except Exception as e:
            self.logger.warning(f"设置杠杆失败: {e}")
            return {
                'symbol': symbol,
                'leverage': leverage,
                'error': str(e)
            }

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """
        设置保证金模式

        Args:
            symbol: 交易对符号
            margin_mode: 保证金模式（'cross'或'isolated'）

        Returns:
            Dict: 设置结果
        """
        try:
            if not self.is_authenticated:
                raise Exception("需要认证才能设置保证金模式")

            params = {
                'symbol': self._normalize_symbol(symbol),
                'marginType': margin_mode.upper()
            }

            endpoint = "api/v1/marginType"
            response = await self._request('POST', endpoint, params, signed=True)

            return {
                'symbol': symbol,
                'margin_mode': margin_mode,
                'result': response
            }

        except Exception as e:
            self.logger.warning(f"设置保证金模式失败: {e}")
            return {
                'symbol': symbol,
                'margin_mode': margin_mode,
                'error': str(e)
            }

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """
        取消订阅 - 统一接口实现

        Args:
            symbol: 交易对符号，None表示取消所有订阅
        """
        try:
            if symbol:
                # 取消特定符号的订阅
                if hasattr(self, '_ws_subscriptions'):
                    subscriptions_to_remove = []
                    for sub_type, sub_symbol, _ in self._ws_subscriptions:
                        if sub_symbol == symbol:
                            subscriptions_to_remove.append((sub_type, sub_symbol, _))

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

                # 向后兼容：清理旧的连接管理
                for ws in self.ws_connections.values():
                    if not ws.closed:
                        await ws.close()
                self.ws_connections.clear()

        except Exception as e:
            self.logger.warning(f"EdgeX取消订阅失败: {e}")

    async def unsubscribe_all(self) -> None:
        """取消所有订阅"""
        await self.unsubscribe()
