"""
OKX交易所WebSocket模块 - 重构版

包含OKX交易所的WebSocket连接和数据流处理
支持行情数据、订单簿、成交数据和用户数据流
"""

import asyncio
import json
import websockets
import time
import gzip
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, Set
from decimal import Decimal
from enum import Enum

from .okx_base import OKXBase
from ..models import (
    TickerData, OrderBookData, TradeData, BalanceData, OrderData,
    OrderBookLevel, OrderSide
)


class OKXStreamType(Enum):
    """OKX数据流类型"""
    TICKER = "tickers"
    ORDERBOOK = "books"
    ORDERBOOK5 = "books5"
    TRADES = "trades"
    CANDLE = "candle"
    ACCOUNT = "account"
    POSITIONS = "positions"
    ORDERS = "orders"
    BALANCE_AND_POSITION = "balance_and_position"


class OKXWebSocket(OKXBase):
    """OKX WebSocket连接和数据流处理"""
    
    def __init__(self, config, logger=None):
        super().__init__(config)
        self.logger = logger
        
        # WebSocket连接
        self._public_websocket = None
        self._private_websocket = None
        self._public_connected = False
        self._private_connected = False
        
        # 订阅管理
        self._public_subscriptions = {}  # channel -> callback
        self._private_subscriptions = {}  # channel -> callback
        
        # 重连配置
        self.reconnect_interval = 5
        self.max_reconnect_attempts = 10
        self._public_reconnect_attempts = 0
        self._private_reconnect_attempts = 0
        
        # 心跳配置
        self.heartbeat_interval = 25  # OKX要求25秒心跳
        self._last_public_heartbeat = 0
        self._last_private_heartbeat = 0
        
        # 数据缓存
        self._ticker_cache = {}
        self._orderbook_cache = {}
        
        # 事件循环任务
        self._public_heartbeat_task = None
        self._private_heartbeat_task = None
        
        # 登录状态
        self._authenticated = False
        
    async def initialize(self) -> bool:
        """初始化WebSocket连接"""
        try:
            if self.logger:
                self.logger.info("🚀 初始化OKX WebSocket连接...")
            
            # 创建心跳任务
            self._public_heartbeat_task = asyncio.create_task(self._public_heartbeat_loop())
            self._private_heartbeat_task = asyncio.create_task(self._private_heartbeat_loop())
            
            if self.logger:
                self.logger.info("✅ OKX WebSocket初始化成功")
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ OKX WebSocket初始化失败: {str(e)}")
            return False
    
    async def close(self):
        """关闭WebSocket连接"""
        try:
            # 取消心跳任务
            if self._public_heartbeat_task:
                self._public_heartbeat_task.cancel()
                try:
                    await self._public_heartbeat_task
                except asyncio.CancelledError:
                    pass
            
            if self._private_heartbeat_task:
                self._private_heartbeat_task.cancel()
                try:
                    await self._private_heartbeat_task
                except asyncio.CancelledError:
                    pass
            
            # 关闭WebSocket连接
            if self._public_websocket:
                await self._public_websocket.close()
                self._public_websocket = None
                
            if self._private_websocket:
                await self._private_websocket.close()
                self._private_websocket = None
            
            self._public_connected = False
            self._private_connected = False
            
            if self.logger:
                self.logger.info("✅ OKX WebSocket连接已关闭")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 关闭WebSocket连接失败: {str(e)}")
    
    async def _connect_public_stream(self) -> bool:
        """连接公共数据流"""
        try:
            if self._public_websocket and not self._public_websocket.closed:
                return True
            
            if self.logger:
                self.logger.info(f"📡 连接OKX公共数据流: {self.ws_url}")
            
            self._public_websocket = await websockets.connect(self.ws_url)
            self._public_connected = True
            self._public_reconnect_attempts = 0
            
            # 启动消息处理任务
            asyncio.create_task(self._handle_public_messages())
            
            if self.logger:
                self.logger.info("✅ OKX公共数据流连接成功")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 连接公共数据流失败: {str(e)}")
            return False
    
    async def _connect_private_stream(self) -> bool:
        """连接私有数据流"""
        try:
            if not self.config or not getattr(self.config, 'api_key'):
                if self.logger:
                    self.logger.warning("⚠️ 未配置API密钥，跳过私有数据流连接")
                return False
            
            if self.logger:
                self.logger.info(f"📡 连接OKX私有数据流: {self.private_ws_url}")
            
            self._private_websocket = await websockets.connect(self.private_ws_url)
            self._private_connected = True
            
            # 启动消息处理任务
            asyncio.create_task(self._handle_private_messages())
            
            # 进行身份验证
            await self._authenticate()
            
            if self.logger:
                self.logger.info("✅ OKX私有数据流连接成功")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 连接私有数据流失败: {str(e)}")
            return False
    
    async def _authenticate(self) -> bool:
        """进行WebSocket身份验证"""
        try:
            import hmac
            import hashlib
            import base64
            
            timestamp = str(int(time.time()))
            method = 'GET'
            path = '/users/self/verify'
            
            # 创建签名
            message = timestamp + method + path
            signature = base64.b64encode(
                hmac.new(
                    self.config.api_secret.encode('utf-8'),
                    message.encode('utf-8'),
                    hashlib.sha256
                ).digest()
            ).decode('utf-8')
            
            # 发送登录消息
            login_msg = {
                "op": "login",
                "args": [{
                    "apiKey": self.config.api_key,
                    "passphrase": getattr(self.config, 'passphrase', ''),
                    "timestamp": timestamp,
                    "sign": signature
                }]
            }
            
            await self._private_websocket.send(json.dumps(login_msg))
            
            # 等待登录响应
            await asyncio.sleep(1)
            self._authenticated = True
            
            if self.logger:
                self.logger.info("✅ OKX WebSocket身份验证成功")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ WebSocket身份验证失败: {str(e)}")
            return False
    
    async def _handle_public_messages(self):
        """处理公共数据消息"""
        try:
            async for message in self._public_websocket:
                try:
                    # OKX可能发送压缩数据
                    if isinstance(message, bytes):
                        message = gzip.decompress(message).decode('utf-8')
                    
                    data = json.loads(message)
                    await self._process_public_message(data)
                except json.JSONDecodeError:
                    if self.logger:
                        self.logger.warning(f"⚠️ 无法解析公共WebSocket消息: {message}")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ 处理公共消息失败: {str(e)}")
                        
        except websockets.exceptions.ConnectionClosed:
            if self.logger:
                self.logger.warning("⚠️ 公共数据流连接断开，尝试重连")
            self._public_connected = False
            await self._reconnect_public_stream()
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 公共消息处理异常: {str(e)}")
    
    async def _handle_private_messages(self):
        """处理私有数据消息"""
        try:
            async for message in self._private_websocket:
                try:
                    # OKX可能发送压缩数据
                    if isinstance(message, bytes):
                        message = gzip.decompress(message).decode('utf-8')
                    
                    data = json.loads(message)
                    await self._process_private_message(data)
                except json.JSONDecodeError:
                    if self.logger:
                        self.logger.warning(f"⚠️ 无法解析私有数据消息: {message}")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ 处理私有消息失败: {str(e)}")
                        
        except websockets.exceptions.ConnectionClosed:
            if self.logger:
                self.logger.warning("⚠️ 私有数据流连接断开，尝试重连")
            self._private_connected = False
            await self._reconnect_private_stream()
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 私有消息处理异常: {str(e)}")
    
    async def _process_public_message(self, data: Dict[str, Any]):
        """处理公共数据消息"""
        try:
            # 处理心跳响应
            if data.get('event') == 'pong':
                self._last_public_heartbeat = time.time()
                return
            
            # 处理订阅确认
            if data.get('event') == 'subscribe':
                if self.logger:
                    self.logger.info(f"✅ 订阅成功: {data.get('arg')}")
                return
            
            # 处理数据推送
            if 'data' in data and 'arg' in data:
                arg = data['arg']
                channel = arg.get('channel')
                inst_id = arg.get('instId', '')
                
                # 根据频道类型处理数据
                if channel == 'tickers':
                    await self._handle_ticker_message(data['data'], inst_id)
                elif channel in ['books', 'books5']:
                    await self._handle_orderbook_message(data['data'], inst_id)
                elif channel == 'trades':
                    await self._handle_trade_message(data['data'], inst_id)
                elif channel.startswith('candle'):
                    await self._handle_candle_message(data['data'], inst_id)
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理公共消息失败: {str(e)}")
    
    async def _process_private_message(self, data: Dict[str, Any]):
        """处理私有数据消息"""
        try:
            # 处理登录响应
            if data.get('event') == 'login':
                if data.get('code') == '0':
                    self._authenticated = True
                    if self.logger:
                        self.logger.info("✅ WebSocket登录成功")
                else:
                    if self.logger:
                        self.logger.error(f"❌ WebSocket登录失败: {data}")
                return
            
            # 处理心跳响应
            if data.get('event') == 'pong':
                self._last_private_heartbeat = time.time()
                return
            
            # 处理数据推送
            if 'data' in data and 'arg' in data:
                arg = data['arg']
                channel = arg.get('channel')
                
                # 根据频道类型处理数据
                if channel == 'account':
                    await self._handle_account_message(data['data'])
                elif channel == 'positions':
                    await self._handle_positions_message(data['data'])
                elif channel == 'orders':
                    await self._handle_orders_message(data['data'])
                elif channel == 'balance_and_position':
                    await self._handle_balance_position_message(data['data'])
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理私有消息失败: {str(e)}")
    
    async def _handle_ticker_message(self, data_list: List[Dict[str, Any]], inst_id: str):
        """处理行情数据"""
        try:
            for data in data_list:
                symbol = self.map_symbol_from_okx(inst_id)
                
                ticker = TickerData(
                    symbol=symbol,
                    bid=self._safe_decimal(data.get('bidPx')),
                    ask=self._safe_decimal(data.get('askPx')),
                    last=self._safe_decimal(data.get('last')),
                    open=self._safe_decimal(data.get('open24h')),
                    high=self._safe_decimal(data.get('high24h')),
                    low=self._safe_decimal(data.get('low24h')),
                    close=self._safe_decimal(data.get('last')),
                    volume=self._safe_decimal(data.get('vol24h')),
                    quote_volume=self._safe_decimal(data.get('volCcy24h')),
                    change=None,  # 需要计算
                    percentage=None,  # 需要计算
                    timestamp=datetime.fromtimestamp(int(data.get('ts', 0)) / 1000),
                    raw_data=data
                )
                
                # 缓存数据
                self._ticker_cache[symbol] = ticker
                
                # 调用回调函数
                channel_key = f"tickers:{inst_id}"
                if channel_key in self._public_subscriptions:
                    callback = self._public_subscriptions[channel_key]
                    await self._safe_callback(callback, ticker)
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理行情数据失败: {str(e)}")
    
    async def _handle_orderbook_message(self, data_list: List[Dict[str, Any]], inst_id: str):
        """处理订单簿数据"""
        try:
            for data in data_list:
                symbol = self.map_symbol_from_okx(inst_id)
                
                # 解析买卖盘
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
                
                orderbook = OrderBookData(
                    symbol=symbol,
                    bids=bids,
                    asks=asks,
                    timestamp=datetime.fromtimestamp(int(data.get('ts', 0)) / 1000),
                    nonce=None,
                    raw_data=data
                )
                
                # 缓存数据
                self._orderbook_cache[symbol] = orderbook
                
                # 调用回调函数
                channel_key = f"books:{inst_id}"
                if channel_key in self._public_subscriptions:
                    callback = self._public_subscriptions[channel_key]
                    await self._safe_callback(callback, orderbook)
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理订单簿数据失败: {str(e)}")
    
    async def _handle_trade_message(self, data_list: List[Dict[str, Any]], inst_id: str):
        """处理成交数据"""
        try:
            for data in data_list:
                symbol = self.map_symbol_from_okx(inst_id)
                
                trade = TradeData(
                    id=str(data.get('tradeId', '')),
                    symbol=symbol,
                    side=OrderSide.BUY if data.get('side') == 'buy' else OrderSide.SELL,
                    amount=self._safe_decimal(data.get('sz')),
                    price=self._safe_decimal(data.get('px')),
                    cost=self._safe_decimal(float(data.get('px', 0)) * float(data.get('sz', 0))),
                    fee=None,
                    timestamp=datetime.fromtimestamp(int(data.get('ts', 0)) / 1000),
                    order_id=None,
                    raw_data=data
                )
                
                # 调用回调函数
                channel_key = f"trades:{inst_id}"
                if channel_key in self._public_subscriptions:
                    callback = self._public_subscriptions[channel_key]
                    await self._safe_callback(callback, trade)
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理成交数据失败: {str(e)}")
    
    async def _handle_candle_message(self, data_list: List[Dict[str, Any]], inst_id: str):
        """处理K线数据"""
        try:
            # K线数据处理逻辑
            for data in data_list:
                # 这里可以根据需要实现K线数据处理
                pass
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理K线数据失败: {str(e)}")
    
    async def _handle_account_message(self, data_list: List[Dict[str, Any]]):
        """处理账户数据"""
        try:
            # 调用用户数据回调
            if 'account' in self._private_subscriptions:
                callback = self._private_subscriptions['account']
                await self._safe_callback(callback, {'type': 'account', 'data': data_list})
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理账户数据失败: {str(e)}")
    
    async def _handle_positions_message(self, data_list: List[Dict[str, Any]]):
        """处理持仓数据"""
        try:
            # 调用用户数据回调
            if 'positions' in self._private_subscriptions:
                callback = self._private_subscriptions['positions']
                await self._safe_callback(callback, {'type': 'positions', 'data': data_list})
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理持仓数据失败: {str(e)}")
    
    async def _handle_orders_message(self, data_list: List[Dict[str, Any]]):
        """处理订单数据"""
        try:
            # 调用用户数据回调
            if 'orders' in self._private_subscriptions:
                callback = self._private_subscriptions['orders']
                await self._safe_callback(callback, {'type': 'orders', 'data': data_list})
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理订单数据失败: {str(e)}")
    
    async def _handle_balance_position_message(self, data_list: List[Dict[str, Any]]):
        """处理余额和持仓数据"""
        try:
            # 调用用户数据回调
            if 'balance_and_position' in self._private_subscriptions:
                callback = self._private_subscriptions['balance_and_position']
                await self._safe_callback(callback, {'type': 'balance_and_position', 'data': data_list})
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理余额和持仓数据失败: {str(e)}")
    
    async def _safe_callback(self, callback: Callable, data: Any):
        """安全调用回调函数"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 回调函数执行失败: {str(e)}")
    
    async def _public_heartbeat_loop(self):
        """公共数据流心跳循环"""
        try:
            while True:
                await asyncio.sleep(self.heartbeat_interval)
                
                # 检查连接状态并发送心跳
                if self._public_connected and self._public_websocket:
                    try:
                        ping_msg = {"op": "ping"}
                        await self._public_websocket.send(json.dumps(ping_msg))
                    except Exception:
                        self._public_connected = False
                        await self._reconnect_public_stream()
                        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 公共心跳循环异常: {str(e)}")
    
    async def _private_heartbeat_loop(self):
        """私有数据流心跳循环"""
        try:
            while True:
                await asyncio.sleep(self.heartbeat_interval)
                
                # 检查连接状态并发送心跳
                if self._private_connected and self._private_websocket:
                    try:
                        ping_msg = {"op": "ping"}
                        await self._private_websocket.send(json.dumps(ping_msg))
                    except Exception:
                        self._private_connected = False
                        await self._reconnect_private_stream()
                        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 私有心跳循环异常: {str(e)}")
    
    async def _reconnect_public_stream(self):
        """重连公共数据流"""
        if self._public_reconnect_attempts >= self.max_reconnect_attempts:
            if self.logger:
                self.logger.error(f"❌ 公共数据流重连次数超限: {self._public_reconnect_attempts}")
            return
        
        self._public_reconnect_attempts += 1
        
        if self.logger:
            self.logger.info(f"🔄 重连公共数据流 (尝试 {self._public_reconnect_attempts}/{self.max_reconnect_attempts})")
        
        await asyncio.sleep(self.reconnect_interval)
        await self._connect_public_stream()
    
    async def _reconnect_private_stream(self):
        """重连私有数据流"""
        if self.logger:
            self.logger.info("🔄 重连私有数据流")
        
        await asyncio.sleep(self.reconnect_interval)
        await self._connect_private_stream()
    
    # ==================== 公共接口 ====================
    
    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]):
        """订阅行情数据"""
        try:
            # 确保连接
            if not self._public_connected:
                await self._connect_public_stream()
            
            # 构建OKX符号
            okx_symbol = self.map_symbol_to_okx(symbol)
            
            # 注册回调
            channel_key = f"tickers:{okx_symbol}"
            self._public_subscriptions[channel_key] = callback
            
            # 发送订阅消息
            subscribe_msg = {
                "op": "subscribe",
                "args": [{
                    "channel": "tickers",
                    "instId": okx_symbol
                }]
            }
            
            if self._public_websocket:
                await self._public_websocket.send(json.dumps(subscribe_msg))
            
            if self.logger:
                self.logger.info(f"📈 订阅行情数据: {symbol}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅行情失败 {symbol}: {str(e)}")
            raise
    
    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]):
        """订阅订单簿数据"""
        try:
            # 确保连接
            if not self._public_connected:
                await self._connect_public_stream()
            
            # 构建OKX符号
            okx_symbol = self.map_symbol_to_okx(symbol)
            
            # 注册回调
            channel_key = f"books:{okx_symbol}"
            self._public_subscriptions[channel_key] = callback
            
            # 发送订阅消息
            subscribe_msg = {
                "op": "subscribe",
                "args": [{
                    "channel": "books",
                    "instId": okx_symbol
                }]
            }
            
            if self._public_websocket:
                await self._public_websocket.send(json.dumps(subscribe_msg))
            
            if self.logger:
                self.logger.info(f"📊 订阅订单簿数据: {symbol}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅订单簿失败 {symbol}: {str(e)}")
            raise
    
    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]):
        """订阅成交数据"""
        try:
            # 确保连接
            if not self._public_connected:
                await self._connect_public_stream()
            
            # 构建OKX符号
            okx_symbol = self.map_symbol_to_okx(symbol)
            
            # 注册回调
            channel_key = f"trades:{okx_symbol}"
            self._public_subscriptions[channel_key] = callback
            
            # 发送订阅消息
            subscribe_msg = {
                "op": "subscribe",
                "args": [{
                    "channel": "trades",
                    "instId": okx_symbol
                }]
            }
            
            if self._public_websocket:
                await self._public_websocket.send(json.dumps(subscribe_msg))
            
            if self.logger:
                self.logger.info(f"💱 订阅成交数据: {symbol}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅成交失败 {symbol}: {str(e)}")
            raise
    
    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]):
        """订阅用户数据"""
        try:
            # 确保连接
            if not self._private_connected:
                await self._connect_private_stream()
            
            # 注册回调
            self._private_subscriptions['account'] = callback
            self._private_subscriptions['positions'] = callback
            self._private_subscriptions['orders'] = callback
            self._private_subscriptions['balance_and_position'] = callback
            
            # 发送订阅消息
            subscribe_msgs = [
                {
                    "op": "subscribe",
                    "args": [{"channel": "account"}]
                },
                {
                    "op": "subscribe", 
                    "args": [{"channel": "positions", "instType": "SWAP"}]
                },
                {
                    "op": "subscribe",
                    "args": [{"channel": "orders", "instType": "SWAP"}]
                }
            ]
            
            if self._private_websocket:
                for msg in subscribe_msgs:
                    await self._private_websocket.send(json.dumps(msg))
            
            if self.logger:
                self.logger.info("👤 订阅用户数据流")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅用户数据失败: {str(e)}")
            raise
    
    async def unsubscribe(self, symbol: Optional[str] = None):
        """取消订阅"""
        try:
            if symbol:
                # 取消指定符号的订阅
                okx_symbol = self.map_symbol_to_okx(symbol)
                channels_to_remove = [
                    key for key in self._public_subscriptions.keys()
                    if okx_symbol in key
                ]
                
                for channel_key in channels_to_remove:
                    del self._public_subscriptions[channel_key]
                    
                    # 发送取消订阅消息
                    channel_type = channel_key.split(':')[0]
                    unsubscribe_msg = {
                        "op": "unsubscribe",
                        "args": [{
                            "channel": channel_type,
                            "instId": okx_symbol
                        }]
                    }
                    
                    if self._public_websocket:
                        await self._public_websocket.send(json.dumps(unsubscribe_msg))
                
                if self.logger:
                    self.logger.info(f"🚫 取消订阅: {symbol}")
            else:
                # 取消所有订阅
                self._public_subscriptions.clear()
                self._private_subscriptions.clear()
                
                if self.logger:
                    self.logger.info("🚫 取消所有订阅")
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 取消订阅失败: {str(e)}")
    
    def get_cached_ticker(self, symbol: str) -> Optional[TickerData]:
        """获取缓存的行情数据"""
        return self._ticker_cache.get(symbol)
    
    def get_cached_orderbook(self, symbol: str) -> Optional[OrderBookData]:
        """获取缓存的订单簿数据"""
        return self._orderbook_cache.get(symbol)
    
    @property
    def is_connected(self) -> bool:
        """检查公共数据流连接状态"""
        return self._public_connected and self._public_websocket and not self._public_websocket.closed
    
    @property
    def is_private_connected(self) -> bool:
        """检查私有数据流连接状态"""
        return self._private_connected and self._private_websocket and not self._private_websocket.closed and self._authenticated
