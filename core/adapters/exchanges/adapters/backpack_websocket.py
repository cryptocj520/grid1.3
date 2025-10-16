"""
Backpack WebSocket模块

包含WebSocket连接管理、数据订阅、消息处理、实时数据解析等功能
应用了EdgeX的重连机制修复
"""

import asyncio
import time
import json
import aiohttp
import logging
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal
from datetime import datetime

from .backpack_base import BackpackBase
from ..models import TickerData, OrderBookData, TradeData, OrderBookLevel, OrderSide


class BackpackWebSocket(BackpackBase):
    """Backpack WebSocket接口"""

    def __init__(self, config=None, logger=None):
        super().__init__(config)
        # 🔥 如果没有传入logger，创建一个专门的logger
        if logger is None:
            self.logger = logging.getLogger('ExchangeAdapter.backpack')
            self.logger.setLevel(logging.INFO)
        else:
            self.logger = logger

        if config and hasattr(config, 'ws_url') and config.ws_url:
            self.ws_url = config.ws_url
        else:
            self.ws_url = self.DEFAULT_WS_URL
        self._ws_connection = None
        self._session = None
        self._ws_subscriptions = []
        self.ticker_callback = None
        self.orderbook_callback = None
        self.trades_callback = None
        self.user_data_callback = None

        # 初始化状态变量
        self._ws_connected = False
        self._last_heartbeat = 0
        self._reconnect_attempts = 0
        self._reconnecting = False
        self._heartbeat_should_stop = False  # 🔧 修复：心跳停止标志

        # 🔥 初始化持仓监控相关
        # 持仓缓存: {symbol: {size, entry_price, unrealized_pnl, side, timestamp}}
        self._position_cache = {}
        self._position_callbacks = []  # 持仓更新回调函数列表

        # 🔥 新增：混合心跳模式参数 (参考Hyperliquid)
        self._last_ping_time = 0      # 上次发送ping的时间
        self._last_pong_time = 0      # 上次收到pong的时间
        self._ping_interval = 25      # 25秒ping间隔 (更积极)
        self._pong_timeout = 50       # 50秒无pong响应则重连
        self._message_timeout = 70    # 70秒无任何消息则重连 (更积极)

        # 缓存相关
        self._latest_orderbooks: Dict[str, Dict[str, Any]] = {}
        self._orderbook_cache_timeout = 30  # 30秒缓存超时

        # === 新增：orderbook数据缓存 ===
        # {symbol: {bids: [...], asks: [...], timestamp: ...}}
        self._latest_orderbooks = {}
        self._orderbook_cache_timeout = 30  # orderbook缓存超时时间（秒）

    async def _check_network_connectivity(self) -> bool:
        """检查网络连通性"""
        try:
            # 测试DNS解析和基本HTTP连通性
            test_url = "https://httpbin.org/status/200"  # 简单的测试端点
            timeout = aiohttp.ClientTimeout(total=5)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(test_url) as response:
                    return response.status == 200

        except Exception as e:
            if self.logger:
                self.logger.warning(f"🌐 网络连通性检查失败: {e}")
            return False

    async def _check_exchange_connectivity(self) -> bool:
        """检查交易所服务器连通性"""
        try:
            # 检查Backpack的REST API是否可达
            api_url = "https://api.backpack.exchange/api/v1/status"  # 尝试status端点
            timeout = aiohttp.ClientTimeout(total=8)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(api_url) as response:
                    return response.status in [200, 404]  # 404也说明服务器可达

        except Exception as e:
            if self.logger:
                self.logger.warning(f"🏢 Backpack服务器连通性检查失败: {e}")
            return False

    def _is_connection_usable(self) -> bool:
        """检查WebSocket连接是否可用"""
        return (
            hasattr(self, '_ws_connection') and
            self._ws_connection is not None and
            not self._ws_connection.closed and
            getattr(self, '_ws_connected', False)
        )

    async def _safe_send_message(self, message: str) -> bool:
        """安全发送WebSocket消息"""
        try:
            if not self._is_connection_usable():
                if self.logger:
                    self.logger.warning("⚠️ WebSocket连接不可用，无法发送消息")
                return False

            await self._ws_connection.send_str(message)
            return True
        except Exception as e:
            if self.logger:
                self.logger.warning(f"发送WebSocket消息失败: {e}")
            return False

    async def connect(self) -> bool:
        """建立WebSocket连接"""
        try:
            # 使用aiohttp建立WebSocket连接
            if not hasattr(self, '_session') or (hasattr(self, '_session') and self._session.closed):
                self._session = aiohttp.ClientSession()
            self._ws_connection = await self._session.ws_connect(self.ws_url)

            if self.logger:
                self.logger.info(f"Backpack WebSocket连接已建立: {self.ws_url}")

            # 初始化状态
            self._ws_connected = True
            self._last_heartbeat = time.time()
            self._reconnect_attempts = 0
            self._reconnecting = False
            self._heartbeat_should_stop = False  # 🔧 修复：重置心跳停止标志

            # 🔥 新增：初始化ping/pong时间戳
            current_time = time.time()
            self._last_ping_time = current_time
            self._last_pong_time = current_time

            # 启动消息处理任务
            self._ws_handler_task = asyncio.create_task(
                self._websocket_message_handler())

            # 启动心跳检测
            self._heartbeat_task = asyncio.create_task(
                self._websocket_heartbeat_loop())
            if self.logger:
                self.logger.info("💓 Backpack心跳检测已启动")

            return True

        except Exception as e:
            if self.logger:
                self.logger.warning(f"建立Backpack WebSocket连接失败: {e}")
            self._ws_connected = False
            return False

    async def disconnect(self) -> None:
        """断开WebSocket连接（应用EdgeX修复）"""
        if self.logger:
            self.logger.info("🔄 开始断开Backpack WebSocket连接...")

        try:
            # 1. 标记为断开状态，停止新的操作
            self._ws_connected = False

            # 🔧 修复：停止心跳检测循环
            if hasattr(self, '_heartbeat_should_stop'):
                self._heartbeat_should_stop = True

            # 2. 取消心跳任务
            if hasattr(self, '_heartbeat_task') and self._heartbeat_task and not self._heartbeat_task.done():
                if self.logger:
                    self.logger.info("🛑 取消Backpack心跳任务...")
                self._heartbeat_task.cancel()
                try:
                    await asyncio.wait_for(self._heartbeat_task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                if self.logger:
                    self.logger.info("✅ Backpack心跳任务已停止")

            # 3. 取消消息处理任务
            if hasattr(self, '_ws_handler_task') and self._ws_handler_task and not self._ws_handler_task.done():
                if self.logger:
                    self.logger.info("🛑 取消Backpack消息处理任务...")
                self._ws_handler_task.cancel()
                try:
                    await asyncio.wait_for(self._ws_handler_task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                if self.logger:
                    self.logger.info("✅ Backpack消息处理任务已停止")

            # 4. 关闭WebSocket连接
            if hasattr(self, '_ws_connection') and self._ws_connection and not self._ws_connection.closed:
                if self.logger:
                    self.logger.info("🛑 关闭Backpack WebSocket连接...")
                try:
                    await asyncio.wait_for(self._ws_connection.close(), timeout=3.0)
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ WebSocket关闭超时，强制设置为None")
                self._ws_connection = None
                if self.logger:
                    self.logger.info("✅ Backpack WebSocket连接已关闭")

            # 5. 关闭session
            if hasattr(self, '_session') and self._session and not self._session.closed:
                if self.logger:
                    self.logger.info("🛑 关闭Backpack session...")
                try:
                    await asyncio.wait_for(self._session.close(), timeout=3.0)
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ Session关闭超时")
                if self.logger:
                    self.logger.info("✅ Backpack session已关闭")

            # 6. 清理状态变量
            self._last_heartbeat = 0
            self._reconnect_attempts = 0

            if self.logger:
                self.logger.info("🎉 Backpack WebSocket连接断开完成")

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 关闭Backpack WebSocket连接时出错: {e}")
                import traceback
                self.logger.error(f"断开连接错误堆栈: {traceback.format_exc()}")

            # 强制清理状态
            self._ws_connected = False
            self._ws_connection = None

    async def _send_ping(self) -> None:
        """发送标准WebSocket ping消息"""
        try:
            if self._ws_connection and self._ws_connected and not self._ws_connection.closed:
                await self._ws_connection.ping()
                if self.logger:
                    self.logger.debug("🏓 发送WebSocket ping")
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 发送ping失败: {str(e)}")

    async def _websocket_heartbeat_loop(self):
        """WebSocket混合心跳检测循环 - 主动ping + 被动检测 (参考Hyperliquid)"""

        if self.logger:
            self.logger.info("💓 Backpack混合心跳检测循环启动 (数据流优先模式)")

        try:
            # 🔧 修复：使用独立的停止标志，不依赖连接状态
            self._heartbeat_should_stop = False

            while not self._heartbeat_should_stop:
                try:
                    # 等待10秒后进行下一次检测
                    await asyncio.wait_for(
                        asyncio.sleep(10),
                        timeout=15
                    )

                    # 检查是否应该停止心跳检测
                    if self._heartbeat_should_stop:
                        if self.logger:
                            self.logger.info("💓 [心跳调试] 心跳检测被停止")
                        break

                    current_time = time.time()

                    # 🔧 修复：检查连接状态，如果断开则立即重连
                    if not self._ws_connected:
                        if self.logger:
                            self.logger.warning("⚠️ 检测到连接断开，立即触发重连...")
                        await self._trigger_reconnection("连接断开")
                        continue

                    # === 🔥 主动ping检测机制 ===
                    if current_time - self._last_ping_time >= self._ping_interval:
                        await self._send_ping()
                        self._last_ping_time = current_time
                        if self.logger:
                            self.logger.debug(f"🏓 主动ping检测: 已发送ping")

                    # === 💌 主要检测：数据流心跳（优先级最高）===
                    message_silence = current_time - self._last_heartbeat
                    if message_silence >= self._message_timeout:
                        if self.logger:
                            self.logger.warning(
                                f"⚠️ Backpack WebSocket消息超时: "
                                f"{message_silence:.1f}s无任何消息，触发重连..."
                            )
                        await self._trigger_reconnection("消息超时")
                        continue

                    # === 📡 辅助检测：ping/pong超时（更宽松的超时时间）===
                    # 🔧 修复：调整ping/pong超时逻辑，只有在数据流也异常时才重连
                    pong_silence = current_time - self._last_pong_time if self._last_pong_time > 0 else 0
                    if (self._last_pong_time > 0 and
                        pong_silence >= self._pong_timeout * 2 and  # 💡 延长至120秒
                            message_silence >= 30):  # 💡 且数据流也静默30秒以上
                        if self.logger:
                            self.logger.warning(
                                f"⚠️ Backpack WebSocket ping/pong异常: "
                                f"{pong_silence:.1f}s无pong响应，且{message_silence:.1f}s无消息，触发重连..."
                            )
                        await self._trigger_reconnection("ping/pong异常")
                        continue

                    # === ✅ 正常状态日志 ===
                    if self.logger:
                        ping_status = f"{current_time - self._last_ping_time:.1f}s前ping"
                        if self._last_pong_time > 0:
                            pong_status = f"{pong_silence:.1f}s前pong"
                        else:
                            pong_status = "无pong"
                        message_status = f"{message_silence:.1f}s前消息"

                        # 💡 区分正常状态和轻微异常
                        if pong_silence > self._pong_timeout and message_silence < 30:
                            # ping/pong异常但数据流正常
                            self.logger.debug(
                                f"💓 Backpack心跳状态: {ping_status}, {pong_status}(异常但数据流正常), {message_status}"
                            )
                        else:
                            # 一切正常
                            self.logger.debug(
                                f"💓 Backpack心跳正常: {ping_status}, {pong_status}, {message_status}"
                            )

                except asyncio.CancelledError:
                    if self.logger:
                        self.logger.info("💓 [心跳调试] 心跳检测被取消")
                    break
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ [心跳调试] 心跳检测超时")
                    continue
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ Backpack混合心跳检测错误: {e}")
                    # 错误后等待较短时间再继续
                    try:
                        await asyncio.wait_for(asyncio.sleep(5), timeout=10)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        break

        except asyncio.CancelledError:
            if self.logger:
                self.logger.info("💓 [心跳调试] 心跳循环被正常取消")
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Backpack混合心跳循环异常退出: {e}")
        finally:
            if self.logger:
                self.logger.info("💓 Backpack混合心跳检测循环已退出")
            # 清理重连状态
            self._reconnecting = False

    async def _trigger_reconnection(self, reason: str) -> None:
        """触发重连的统一入口"""
        # 检查是否已经在重连中
        if hasattr(self, '_reconnecting') and self._reconnecting:
            if self.logger:
                self.logger.info(f"🔄 [心跳调试] 已有重连在进行中，跳过{reason}重连")
            return

        # 标记重连状态
        self._reconnecting = True

        try:
            if self.logger:
                self.logger.info(f"🔄 [心跳调试] 开始执行重连 (原因: {reason})...")
            await self._reconnect_websocket()
            if self.logger:
                self.logger.info("✅ [心跳调试] 重连完成")
        except asyncio.CancelledError:
            if self.logger:
                self.logger.warning("⚠️ [心跳调试] 重连被取消")
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [心跳调试] 重连失败: {type(e).__name__}: {e}")
        finally:
            # 清除重连状态标记
            self._reconnecting = False

    async def _reconnect_websocket(self):
        """WebSocket自动重连 - 无限重试 + 指数退避 + 网络诊断"""
        base_delay = 2
        max_delay = 300  # 最大延迟5分钟

        # 无限重试，移除次数限制
        self._reconnect_attempts += 1

        # 改进的指数退避：限制最大延迟
        delay = min(
            base_delay * (2 ** min(self._reconnect_attempts - 1, 8)), max_delay)

        if self.logger:
            self.logger.info(
                f"🔄 [重连调试] Backpack重连尝试 #{self._reconnect_attempts}，延迟{delay}s")

        reconnect_success = False

        try:
            # 步骤1: 网络诊断
            if self.logger:
                self.logger.info("🔧 [重连调试] 步骤1: 网络连通性诊断...")

            # 检查基本网络连通性
            network_ok = await self._check_network_connectivity()
            if not network_ok:
                if self.logger:
                    self.logger.warning("⚠️ 基本网络连通性检查失败，跳过本次重连")
                return  # 网络不通，跳过本次重连

            # 检查交易所服务器连通性
            exchange_ok = await self._check_exchange_connectivity()
            if self.logger:
                status = "✅ 可达" if exchange_ok else "⚠️ 不可达"
                self.logger.info(f"🏢 Backpack服务器连通性: {status}")

            # 步骤2: 彻底清理旧连接
            if self.logger:
                self.logger.info("🔧 [重连调试] 步骤2: 彻底清理旧连接...")
            await self._cleanup_old_connections()

            # 步骤3: 等待延迟
            if self.logger:
                self.logger.info(f"🔧 [重连调试] 步骤3: 等待{delay}秒后重连...")
            await asyncio.sleep(delay)

            # 步骤4: 重新建立连接
            if self.logger:
                self.logger.info("🔧 [重连调试] 步骤4: 重新建立Backpack WebSocket连接...")

            # 使用现有的connect方法，它已经包含了完整的连接逻辑
            reconnect_success = await self.connect()

            if reconnect_success:
                # 步骤5: 重新订阅所有频道
                if self.logger:
                    self.logger.info("🔧 [重连调试] 步骤5: 重新订阅所有频道...")
                await self._resubscribe_all()

                # 步骤6: 重置状态 - 重连成功，重置计数
                self._reconnect_attempts = 0
                self._last_heartbeat = time.time()

                # 🔥 新增：重置ping/pong时间戳
                current_time = time.time()
                self._last_ping_time = current_time
                self._last_pong_time = current_time

                if self.logger:
                    self.logger.info("🎉 [重连调试] Backpack WebSocket重连成功！")
            else:
                raise Exception("连接建立失败")

        except asyncio.CancelledError:
            if self.logger:
                self.logger.warning("⚠️ [重连调试] Backpack重连被取消")
            self._ws_connected = False
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"❌ [重连调试] Backpack重连失败: {type(e).__name__}: {e}")
                import traceback
                self.logger.error(f"[重连调试] 完整错误堆栈: {traceback.format_exc()}")

            # 重连失败处理 - 无限重试模式
            reconnect_success = False

        # 无限重试：重连失败也不停止，继续让心跳检测工作
        if not reconnect_success:
            if self.logger:
                self.logger.warning(
                    f"⚠️ Backpack重连失败，将在下次心跳检测时继续重试 (已尝试{self._reconnect_attempts}次)")
            # 保持连接状态为True，让心跳检测继续工作
            # 不停止心跳任务，实现真正的无限重试

    async def _cleanup_old_connections(self):
        """彻底清理旧的连接和任务（应用EdgeX修复）"""
        try:
            # 1. 停止消息处理任务
            if hasattr(self, '_ws_handler_task') and self._ws_handler_task and not self._ws_handler_task.done():
                self._ws_handler_task.cancel()
                try:
                    await asyncio.wait_for(self._ws_handler_task, timeout=1.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

            # 2. 关闭WebSocket连接
            if hasattr(self, '_ws_connection') and self._ws_connection and not self._ws_connection.closed:
                try:
                    await asyncio.wait_for(self._ws_connection.close(), timeout=2.0)
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ [清理调试] WebSocket关闭超时")
                self._ws_connection = None

            # 3. 关闭session
            if hasattr(self, '_session') and self._session and not self._session.closed:
                try:
                    await asyncio.wait_for(self._session.close(), timeout=2.0)
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ [清理调试] Session关闭超时")

            if self.logger:
                self.logger.info("✅ [清理调试] 旧连接清理完成")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"⚠️ [清理调试] 清理旧连接时出错: {e}")

    async def _resubscribe_all(self):
        """重新订阅所有频道（Backpack版本）"""
        try:
            if self.logger:
                self.logger.info("🔄 [重订阅调试] 开始重新订阅Backpack所有频道")

            # 🔥 步骤1: 重新订阅用户数据流（订单更新）
            if hasattr(self, 'user_data_callback') and self.user_data_callback:
                if self.logger:
                    self.logger.info("🔄 [重订阅调试] 重新订阅用户数据流（订单更新）...")
                try:
                    # 生成签名
                    timestamp = int(time.time() * 1000)
                    window = 5000
                    sign_string = f"instruction=subscribe&timestamp={timestamp}&window={window}"
                    signature = self._sign_message_for_subscription(
                        sign_string)

                    # 重新订阅
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": ["account.orderUpdate"],
                        "signature": [self.config.api_key, signature, str(timestamp), str(window)]
                    }

                    if await self._safe_send_message(json.dumps(subscribe_msg)):
                        if self.logger:
                            self.logger.info("✅ [重订阅调试] 用户数据流重新订阅成功")
                    else:
                        if self.logger:
                            self.logger.error("❌ [重订阅调试] 用户数据流重新订阅失败")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ [重订阅调试] 用户数据流重新订阅出错: {e}")

            # 🔥 步骤2: 重新订阅ticker数据
            if hasattr(self, '_subscribed_symbols') and self._subscribed_symbols:
                # 应用黑名单过滤
                original_symbols = list(self._subscribed_symbols)
                filtered_symbols = self.filter_websocket_symbols(
                    original_symbols)

                symbol_count = len(filtered_symbols)
                filtered_count = len(original_symbols) - len(filtered_symbols)

                if self.logger:
                    self.logger.info(f"🔧 [重订阅调试] 待重新订阅的交易对数量: {symbol_count}")
                    if filtered_count > 0:
                        self.logger.info(
                            f"🚫 [重订阅调试] 已过滤黑名单交易对: {filtered_count} 个")
                    self.logger.info(
                        f"🔧 [重订阅调试] 交易对列表: {filtered_symbols[:10]}...")  # 只显示前10个

                success_count = 0
                failed_count = 0

                for i, symbol in enumerate(filtered_symbols):
                    try:
                        subscribe_msg = {
                            "method": "SUBSCRIBE",
                            "params": [f"ticker.{symbol}"],
                            "id": i + 1
                        }

                        if await self._safe_send_message(json.dumps(subscribe_msg)):
                            success_count += 1
                            if i < 5:  # 只记录前5个的详细信息
                                if self.logger:
                                    self.logger.info(
                                        f"✅ [重订阅调试] 重新订阅ticker: {symbol} (ID: {i+1})")
                            await asyncio.sleep(0.1)  # 小延迟
                        else:
                            if self.logger:
                                self.logger.error(
                                    f"❌ [重订阅调试] WebSocket连接不可用，无法订阅: {symbol}")
                            failed_count += 1
                    except Exception as e:
                        if self.logger:
                            self.logger.error(f"❌ [重订阅调试] 订阅{symbol}失败: {e}")
                        failed_count += 1

                # 更新订阅列表为过滤后的列表
                self._subscribed_symbols = set(filtered_symbols)

                if self.logger:
                    self.logger.info(
                        f"✅ [重订阅调试] Backpack重新订阅完成: {success_count}个成功, {failed_count}个失败")
            else:
                if self.logger:
                    self.logger.warning("⚠️ [重订阅调试] 没有找到订阅的交易对列表")

        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"❌ [重订阅调试] Backpack重新订阅失败: {type(e).__name__}: {e}")
                import traceback
                self.logger.error(f"[重订阅调试] 完整错误堆栈: {traceback.format_exc()}")
            raise

    async def _websocket_message_handler(self) -> None:
        """处理WebSocket消息（使用aiohttp的消息类型）"""
        try:
            async for msg in self._ws_connection:
                # 🔥 新增：更新心跳时间戳（收到任何消息）
                self._last_heartbeat = time.time()

                if msg.type == aiohttp.WSMsgType.TEXT:
                    message = msg.data
                    await self._process_websocket_message(message)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    if self.logger:
                        self.logger.error(
                            f"Backpack WebSocket错误: {self._ws_connection.exception()}")
                    break
                elif msg.type == aiohttp.WSMsgType.PONG:
                    # 🔥 新增：处理pong响应
                    self._last_pong_time = time.time()
                    if self.logger:
                        self.logger.debug("🏓 收到WebSocket pong响应")
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    if self.logger:
                        self.logger.warning("Backpack WebSocket连接已关闭")
                    self._ws_connected = False
                    break
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Backpack WebSocket消息处理失败: {e}")
            self._ws_connected = False

    async def _process_websocket_message(self, message: str) -> None:
        """处理WebSocket消息 - 根据Backpack官方文档修复"""
        try:
            data = json.loads(message)

            # 记录接收到的消息用于调试（减少日志量）
            if not hasattr(self, '_msg_count'):
                self._msg_count = 0
            self._msg_count += 1

            if self._msg_count <= 5:  # 只记录前5条消息
                if self.logger:
                    self.logger.debug(
                        f"收到WebSocket消息 #{self._msg_count}: {data}")

            # 处理订阅响应（可选，Backpack可能不发送）
            if 'result' in data and 'id' in data:
                if data['result'] is None:
                    if self.logger:
                        self.logger.info(f"订阅确认: ID {data['id']}")
                else:
                    if self.logger:
                        self.logger.warning(f"订阅可能失败: {data}")
                return

            # 处理错误消息
            if 'error' in data:
                error_info = data['error']
                error_code = error_info.get('code', 'unknown')
                error_message = error_info.get('message', 'unknown')

                # 记录详细的错误信息
                if self.logger:
                    self.logger.error(f"WebSocket错误: {error_info}")

                # 如果是Invalid market错误，记录但不中断其他订阅
                if error_code == 4005 and 'Invalid market' in error_message:
                    error_id = data.get('id', 'unknown')
                    if self.logger:
                        self.logger.warning(
                            f"某个符号可能不支持WebSocket: 请求ID {error_id}")

                return

            # 🔧 修复：Backpack实际使用嵌套的stream/data格式！
            # 处理Backpack的stream/data格式消息
            if 'stream' in data and 'data' in data:
                stream_name = data['stream']
                stream_data = data['data']

                # Backpack格式：ticker.SOL_USDC_PERP, depth.SOL_USDC_PERP, trade.SOL_USDC_PERP
                if stream_name.startswith('ticker.'):
                    # 从stream名称提取符号：ticker.SOL_USDC_PERP -> SOL_USDC_PERP
                    symbol = stream_name.split(
                        '.', 1)[1] if '.' in stream_name else stream_name
                    await self._handle_backpack_ticker_update(symbol, stream_data)

                elif stream_name.startswith('bookTicker.'):
                    # bookTicker也包含价格信息
                    symbol = stream_name.split(
                        '.', 1)[1] if '.' in stream_name else stream_name
                    await self._handle_backpack_ticker_update(symbol, stream_data)

                elif stream_name.startswith('depth.'):
                    symbol = stream_name.split(
                        '.', 1)[1] if '.' in stream_name else stream_name
                    await self._handle_backpack_orderbook_update(symbol, stream_data)

                elif stream_name.startswith('trade.'):
                    symbol = stream_name.split(
                        '.', 1)[1] if '.' in stream_name else stream_name
                    await self._handle_backpack_trade_update(symbol, stream_data)

                elif stream_name == 'account.orderUpdate':
                    # 订单更新流 - 传入完整data（包含stream字段）
                    await self._handle_user_data_update(data)

                elif stream_name == 'account.positionUpdate':
                    # 持仓更新流 - 传入完整data（包含stream字段）
                    await self._handle_user_data_update(data)

                elif 'userData' in stream_name or 'account.' in stream_name:
                    # 兼容旧格式和其他账户流 - 传入完整data
                    await self._handle_user_data_update(data)

                else:
                    if self.logger:
                        self.logger.debug(f"未知的流类型: {stream_name}")
            else:
                # 对于非标准格式的消息，记录但不报错
                if self._msg_count <= 5:
                    if self.logger:
                        self.logger.debug(f"未知消息格式: {data}")

        except Exception as e:
            if self.logger:
                self.logger.error(f"处理WebSocket消息失败: {e}")
                self.logger.error(f"原始消息: {message}")

    # ⚠️ 第一个定义已删除，真正的实现在第985行

    async def _safe_callback(self, callback: Callable, data: Any) -> None:
        """安全调用回调函数"""
        try:
            if callback:
                if asyncio.iscoroutinefunction(callback):
                    await callback(data)
                else:
                    callback(data)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Backpack回调函数执行失败: {e}")

    async def _handle_backpack_ticker_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """处理Backpack原生格式的ticker更新"""
        try:
            # 解析交易所时间戳（微秒）
            exchange_timestamp = None
            if 'E' in data:
                try:
                    timestamp_microseconds = int(data['E'])
                    exchange_timestamp = datetime.fromtimestamp(
                        timestamp_microseconds / 1000000)
                except (ValueError, TypeError):
                    pass

            # 使用当前时间作为主时间戳（确保时效性）
            current_time = datetime.now()
            main_timestamp = current_time

            # === 优先从ticker数据中获取bid/ask，然后从orderbook缓存获取 ===
            # 首先尝试从ticker数据中获取bid/ask（某些Backpack数据可能包含）
            bid_price = self._safe_decimal(data.get('b'))  # bid price
            ask_price = self._safe_decimal(data.get('a'))  # ask price
            bid_size = self._safe_decimal(data.get('B'))   # bid size
            ask_size = self._safe_decimal(data.get('A'))   # ask size

            # 如果ticker数据中没有bid/ask，从orderbook缓存获取
            if bid_price is None or ask_price is None:
                cached_bid, cached_ask, cached_bid_size, cached_ask_size = self._get_best_bid_ask_from_cache(
                    symbol)
                bid_price = bid_price or cached_bid
                ask_price = ask_price or cached_ask
                bid_size = bid_size or cached_bid_size
                ask_size = ask_size or cached_ask_size

            # 根据测试结果解析ticker数据（Binance兼容格式）
            ticker = TickerData(
                symbol=symbol,
                bid=bid_price,  # 最佳买价
                ask=ask_price,  # 最佳卖价
                bid_size=bid_size,  # 最佳买单数量
                ask_size=ask_size,  # 最佳卖单数量
                # c = close/last price
                last=self._safe_decimal(data.get('c')),
                open=self._safe_decimal(data.get('o')),     # o = open price
                high=self._safe_decimal(data.get('h')),     # h = high price
                low=self._safe_decimal(data.get('l')),      # l = low price
                close=self._safe_decimal(data.get('c')),    # c = close price
                # v = base asset volume
                volume=self._safe_decimal(data.get('v')),
                quote_volume=self._safe_decimal(
                    data.get('V')),  # V = quote asset volume
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
                if self.logger:
                    # 首次ticker数据，显示完整信息
                    if bid_price and bid_size:
                        bid_info = f"买价: {bid_price:.2f} (数量: {bid_size:.4f})"
                    elif bid_price:
                        bid_info = f"买价: {bid_price:.2f}"
                    else:
                        bid_info = "买价: N/A"

                    if ask_price and ask_size:
                        ask_info = f"卖价: {ask_price:.2f} (数量: {ask_size:.4f})"
                    elif ask_price:
                        ask_info = f"卖价: {ask_price:.2f}"
                    else:
                        ask_info = "卖价: N/A"

                    # ✅ 改为debug级别，避免终端刷屏
                    self.logger.debug(
                        f"✅ 首次收到Backpack完整ticker数据: {symbol} -> {ticker.last} | {bid_info} | {ask_info}")
                self._ticker_count[symbol] += 1

            # 调用相应的回调函数
            # 1. 检查批量订阅的回调（需要两个参数：symbol, ticker）
            if hasattr(self, 'ticker_callback') and self.ticker_callback:
                await self._safe_callback_with_symbol(self.ticker_callback, symbol, ticker)

            # 2. 检查单独订阅的回调（只需要一个参数：ticker）
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'ticker' and sub_symbol == symbol:
                    await self._safe_callback(callback, ticker)

        except Exception as e:
            if self.logger:
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
                    exchange_timestamp = datetime.fromtimestamp(
                        timestamp_microseconds / 1000000)
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

            # === 新增：缓存最新的orderbook数据供ticker使用 ===
            self._cache_orderbook_data(symbol, bids, asks, main_timestamp)

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
            # 1. 检查批量订阅的回调（需要两个参数：symbol, orderbook）
            if hasattr(self, 'orderbook_callback') and self.orderbook_callback:
                await self._safe_callback_with_symbol(self.orderbook_callback, symbol, orderbook)

            # 2. 检查单独订阅的回调（只需要一个参数：orderbook）
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'orderbook' and sub_symbol == symbol:
                    await self._safe_callback(callback, orderbook)

        except Exception as e:
            if self.logger:
                self.logger.error(f"处理Backpack订单簿更新失败: {e}")
                self.logger.error(f"符号: {symbol}, 数据内容: {data}")

    async def _handle_backpack_trade_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """处理Backpack原生格式的交易更新"""
        try:
            # 解析成交数据
            trade = TradeData(
                id=str(data.get('t', '')),  # t = trade ID
                symbol=symbol,
                side=OrderSide.BUY if data.get(
                    'm') is False else OrderSide.SELL,  # m = is maker
                amount=self._safe_decimal(data.get('q')),   # q = quantity
                price=self._safe_decimal(data.get('p')),    # p = price
                cost=self._safe_decimal(
                    data.get('q', 0)) * self._safe_decimal(data.get('p', 0)),
                fee=None,
                timestamp=datetime.fromtimestamp(data.get(
                    'T', 0) / 1000000) if data.get('T') else datetime.now(),  # T = timestamp in microseconds
                order_id=None,
                raw_data=data
            )

            # 调用相应的回调函数
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'trades' and sub_symbol == symbol:
                    await self._safe_callback(callback, trade)

        except Exception as e:
            if self.logger:
                self.logger.error(f"处理Backpack交易更新失败: {e}")
                self.logger.error(f"符号: {symbol}, 数据内容: {data}")

    async def _handle_user_data_update(self, data: Dict[str, Any]) -> None:
        """处理用户数据更新（订单更新 + 持仓更新）"""
        try:
            subscriptions = getattr(self, '_ws_subscriptions', [])

            # 根据stream字段区分订单更新和持仓更新
            stream = data.get('stream', '')
            event_type = data.get('e', '')  # 兼容旧格式

            # 检查是否是持仓更新
            if 'position' in stream.lower() or ('position' in event_type.lower()):
                # 持仓更新 - 单独处理并缓存
                await self._handle_position_update(data)

            # 调用用户数据回调函数（兼容旧逻辑）
            for sub_type, sub_symbol, callback in subscriptions:
                if sub_type == 'user_data':
                    await self._safe_callback(callback, data)

        except Exception as e:
            if self.logger:
                self.logger.error(f"处理用户数据更新失败: {e}")
                self.logger.error(f"数据内容: {data}")

    async def _handle_position_update(self, data: Dict[str, Any]) -> None:
        """
        处理持仓更新（WebSocket推送）

        根据实际测试，Backpack持仓更新格式：
        {
          "stream": "account.positionUpdate",
          "data": {
            "s": "HYPE_USDC_PERP",       # 交易对
            "Q": "0.20",                 # 持仓数量（绝对值，总是正数）❌ 不可用于判断方向
            "q": "0.20" / "-0.20",       # 持仓数量（带符号）✅ 正数=多仓，负数=空仓
            "B": "43.107",               # 入场价格/成本价格
            "b": "43.11821",             # 标记价格或清算价格
            "P": "-0.003504",            # 未实现盈亏
            "p": "0",                    # 已实现盈亏
            "M": "43.08947989",          # 标记价格
            "n": "8.617895978",          # 名义价值
            ...
          }
        }

        🔥 关键发现：必须使用小写 'q' 字段来判断持仓方向！
        """
        try:
            if self.logger:
                self.logger.info(f"📊 收到持仓更新: {data}")
                # 🔥 记录原始数据以便调试持仓方向问题
                self.logger.info(
                    f"   原始数据字段: {list(data.get('data', data).keys())}")

            # 🔥 从data字段中提取持仓信息
            pos_data = data.get('data', {})
            if not pos_data:
                # 兼容旧格式
                pos_data = data

            # 解析持仓数据
            symbol = pos_data.get('s', '')

            # 🔥 关键修复：使用小写 'q' 字段（带符号的持仓数量）
            # 测试发现：
            # - 'Q' (大写) = 绝对值，总是 >= 0，无法判断方向
            # - 'q' (小写) = 带符号的数量，正数=多仓，负数=空仓
            quantity_str = pos_data.get('q', '0')  # 只使用小写q
            entry_price_str = pos_data.get('B', pos_data.get('b', '0'))
            unrealized_pnl_str = pos_data.get('P', pos_data.get('p', '0'))

            # 转换为Decimal
            quantity = self._safe_decimal(quantity_str) or Decimal('0')
            entry_price = self._safe_decimal(entry_price_str) or Decimal('0')
            unrealized_pnl = self._safe_decimal(
                unrealized_pnl_str) or Decimal('0')

            # 🔥 根据 q 字段的符号判断持仓方向
            # q > 0 → 多仓
            # q < 0 → 空仓
            # q = 0 → 无持仓
            if quantity > 0:
                side = 'Long'
            elif quantity < 0:
                side = 'Short'
            else:
                side = 'None'

            # 🔥 缓存持仓信息（供剥头皮模式使用）
            if not hasattr(self, '_position_cache'):
                self._position_cache = {}

            self._position_cache[symbol] = {
                'size': quantity,
                'entry_price': entry_price,
                'unrealized_pnl': unrealized_pnl,
                'side': side,
                'timestamp': datetime.now()
            }

            if self.logger:
                self.logger.info(
                    f"💰 持仓缓存已更新: {symbol} {side} "
                    f"数量={quantity}, 成本=${entry_price}, "
                    f"未实现盈亏=${unrealized_pnl}"
                )

            # 🔥 触发持仓更新回调（如果有）
            if hasattr(self, '_position_callbacks') and self._position_callbacks:
                position_info = {
                    'symbol': symbol,
                    'size': quantity,
                    'entry_price': entry_price,
                    'unrealized_pnl': unrealized_pnl,
                    'side': side
                }
                for callback in self._position_callbacks:
                    await self._safe_callback(callback, position_info)

        except Exception as e:
            if self.logger:
                self.logger.error(f"处理持仓更新失败: {e}")
                self.logger.error(f"原始数据: {data}")
                import traceback
                self.logger.error(traceback.format_exc())

    async def _safe_callback(self, callback: Callable, data: Any) -> None:
        """安全调用回调函数"""
        try:
            if callback:
                if asyncio.iscoroutinefunction(callback):
                    await callback(data)
                else:
                    callback(data)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Backpack回调函数执行失败: {e}")

    async def _safe_callback_with_symbol(self, callback: Callable, symbol: str, data: Any) -> None:
        """安全调用需要symbol参数的回调函数"""
        try:
            if callback:
                if asyncio.iscoroutinefunction(callback):
                    await callback(symbol, data)
                else:
                    callback(symbol, data)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Backpack回调函数执行失败: {e}")

    # === 订阅接口 ===

    async def subscribe_position_updates(self, symbol: str, callback: Callable) -> None:
        """
        订阅持仓更新流（异步回调）

        注意：account.positionUpdate 流已经在 subscribe_user_data 中订阅
        这个方法只是注册回调，确保持仓更新能触发回调

        Args:
            symbol: 交易对
            callback: 持仓更新回调函数，接收参数：
                {
                    'symbol': str,
                    'size': Decimal,  # 带符号，正数=多仓，负数=空仓
                    'entry_price': Decimal,
                    'unrealized_pnl': Decimal,
                    'side': str  # 'Long' or 'Short'
                }
        """
        if not hasattr(self, '_position_callbacks'):
            self._position_callbacks = []

        self._position_callbacks.append(callback)

        if self.logger:
            self.logger.info(
                f"✅ 持仓更新回调已注册: {symbol}\n"
                f"   注意：持仓更新流已在subscribe_user_data中订阅"
            )

        # 🔥 如果缓存中已有数据，立即触发一次回调（同步初始状态）
        if hasattr(self, '_position_cache') and symbol in self._position_cache:
            cached_pos = self._position_cache[symbol]
            try:
                await callback({
                    'symbol': symbol,
                    'size': cached_pos['size'],
                    'entry_price': cached_pos['entry_price'],
                    'unrealized_pnl': cached_pos.get('unrealized_pnl', Decimal('0')),
                    'side': cached_pos.get('side', 'None')
                })
                if self.logger:
                    self.logger.info(
                        f"📊 从缓存立即同步初始持仓: {symbol} "
                        f"数量={cached_pos['size']}, 成本=${cached_pos['entry_price']}"
                    )
            except Exception as e:
                if self.logger:
                    self.logger.error(f"❌ 立即同步缓存持仓失败: {e}")
        else:
            if self.logger:
                self.logger.info(
                    f"📊 持仓缓存暂无数据: {symbol}\n"
                    f"   将在收到WebSocket更新后自动同步"
                )

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅行情数据流"""
        try:
            # 检查是否为黑名单交易对
            if self.is_websocket_blacklisted(symbol):
                if self.logger:
                    self.logger.warning(f"🚫 跳过黑名单交易对: {symbol}")
                return

            self._ws_subscriptions.append(('ticker', symbol, callback))

            # 修复：单独订阅时也要添加到_subscribed_symbols
            if not hasattr(self, '_subscribed_symbols'):
                self._subscribed_symbols = set()
            self._subscribed_symbols.add(symbol)

            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [f"ticker.{symbol}"],
                "id": len(self._ws_subscriptions)
            }

            if await self._safe_send_message(json.dumps(subscribe_msg)):
                if self.logger:
                    self.logger.debug(f"已订阅 {symbol} 的ticker (单独订阅)")
            else:
                if self.logger:
                    self.logger.warning(f"发送 {symbol} ticker订阅消息失败")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅ticker失败: {e}")

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """订阅订单簿数据流"""
        try:
            # 检查是否为黑名单交易对
            if self.is_websocket_blacklisted(symbol):
                if self.logger:
                    self.logger.warning(f"🚫 跳过黑名单交易对: {symbol}")
                return

            self._ws_subscriptions.append(('orderbook', symbol, callback))

            # 修复：单独订阅时也要添加到_subscribed_symbols
            if not hasattr(self, '_subscribed_symbols'):
                self._subscribed_symbols = set()
            self._subscribed_symbols.add(symbol)

            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [f"depth.{symbol}"],
                "id": len(self._ws_subscriptions)
            }

            if await self._safe_send_message(json.dumps(subscribe_msg)):
                if self.logger:
                    self.logger.debug(f"已订阅 {symbol} 的orderbook (单独订阅)")
            else:
                if self.logger:
                    self.logger.warning(f"发送 {symbol} orderbook订阅消息失败")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅orderbook失败: {e}")

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅成交数据流"""
        try:
            # 检查是否为黑名单交易对
            if self.is_websocket_blacklisted(symbol):
                if self.logger:
                    self.logger.warning(f"🚫 跳过黑名单交易对: {symbol}")
                return

            self._ws_subscriptions.append(('trades', symbol, callback))

            # 修复：单独订阅时也要添加到_subscribed_symbols
            if not hasattr(self, '_subscribed_symbols'):
                self._subscribed_symbols = set()
            self._subscribed_symbols.add(symbol)

            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [f"trade.{symbol}"],
                "id": len(self._ws_subscriptions)
            }

            if await self._safe_send_message(json.dumps(subscribe_msg)):
                if self.logger:
                    self.logger.debug(f"已订阅 {symbol} 的trades (单独订阅)")
            else:
                if self.logger:
                    self.logger.warning(f"发送 {symbol} trades订阅消息失败")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅trades失败: {e}")

    def _sign_message_for_subscription(self, message: str) -> str:
        """
        为WebSocket订阅生成ED25519签名

        Args:
            message: 要签名的消息字符串

        Returns:
            Base64编码的签名
        """
        import base64
        from nacl.signing import SigningKey
        from nacl.encoding import Base64Encoder

        # 从config获取私钥
        if not self.config or not hasattr(self.config, 'api_secret') or not self.config.api_secret:
            raise ValueError("API私钥未配置")

        # 解码私钥
        private_key_bytes = base64.b64decode(self.config.api_secret)
        signing_key = SigningKey(private_key_bytes)

        # 签名
        signed = signing_key.sign(message.encode(), encoder=Base64Encoder)
        return signed.signature.decode()

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        订阅用户数据流（订单更新 + 持仓更新）

        参考: 
        - https://docs.backpack.exchange/#tag/Streams/Private/Order-update
        - https://docs.backpack.exchange/#tag/Streams/Private/Position-update
        """
        try:
            self._ws_subscriptions.append(('user_data', None, callback))
            self.user_data_callback = callback

            # 生成签名
            timestamp = int(time.time() * 1000)
            window = 5000

            # 签名字符串: instruction=subscribe&timestamp=<timestamp>&window=<window>
            sign_string = f"instruction=subscribe&timestamp={timestamp}&window={window}"
            signature = self._sign_message_for_subscription(sign_string)

            # 获取API Key
            if not self.config or not hasattr(self.config, 'api_key') or not self.config.api_key:
                raise ValueError("API Key未配置")

            # 🔥 订阅订单更新流
            subscribe_order_msg = {
                "method": "SUBSCRIBE",
                "params": ["account.orderUpdate"],  # 订单更新
                "signature": [self.config.api_key, signature, str(timestamp), str(window)]
            }

            if self.logger:
                self.logger.info(
                    f"订阅订单更新流: account.orderUpdate "
                    f"(timestamp={timestamp})"
                )

            if await self._safe_send_message(json.dumps(subscribe_order_msg)):
                if self.logger:
                    self.logger.info("✅ 订单更新流订阅请求已发送")
            else:
                if self.logger:
                    self.logger.warning("发送订单更新订阅消息失败")

            # 🔥 订阅持仓更新流（重要！剥头皮模式需要实时持仓）
            # 需要重新生成签名（时间戳可能变化）
            timestamp2 = int(time.time() * 1000)
            sign_string2 = f"instruction=subscribe&timestamp={timestamp2}&window={window}"
            signature2 = self._sign_message_for_subscription(sign_string2)

            subscribe_position_msg = {
                "method": "SUBSCRIBE",
                "params": ["account.positionUpdate"],  # 持仓更新
                "signature": [self.config.api_key, signature2, str(timestamp2), str(window)]
            }

            if self.logger:
                self.logger.info(
                    f"订阅持仓更新流: account.positionUpdate "
                    f"(timestamp={timestamp2})"
                )

            if await self._safe_send_message(json.dumps(subscribe_position_msg)):
                if self.logger:
                    self.logger.info("✅ 持仓更新流订阅请求已发送")
            else:
                if self.logger:
                    self.logger.warning("发送持仓更新订阅消息失败")

        except Exception as e:
            if self.logger:
                self.logger.error(f"订阅用户数据流失败: {e}")
                import traceback
                self.logger.error(traceback.format_exc())

    async def batch_subscribe_tickers(self, symbols: Optional[List[str]] = None, callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """批量订阅多个交易对的ticker数据 - 使用完整符号格式"""
        try:
            # 如果未指定symbols，使用所有支持的交易对
            if symbols is None:
                symbols = await self.get_supported_symbols()

            # 过滤掉黑名单中的交易对
            original_count = len(symbols)
            symbols = self.filter_websocket_symbols(symbols)

            if self.logger:
                if original_count > len(symbols):
                    self.logger.info(
                        f"开始批量订阅 {len(symbols)} 个交易对的ticker数据 (已过滤 {original_count - len(symbols)} 个黑名单交易对)")
                else:
                    self.logger.info(
                        f"开始批量订阅 {len(symbols)} 个交易对的ticker数据 (使用完整符号格式)")

            # 记录订阅的符号（用于数据映射）
            self._subscribed_symbols = set(symbols)

            # 逐个发送订阅消息（使用完整符号格式）
            successful_subscriptions = 0
            for i, symbol in enumerate(symbols):
                try:
                    # 修复：直接使用完整符号，不进行映射
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        # 使用完整符号：ticker.SOL_USDC_PERP
                        "params": [f"ticker.{symbol}"],
                        "id": i + 1
                    }

                    if await self._safe_send_message(json.dumps(subscribe_msg)):
                        if self.logger:
                            self.logger.debug(f"✅ 已订阅: ticker.{symbol}")
                        successful_subscriptions += 1

                        # 小延迟避免过快
                        await asyncio.sleep(0.1)

                except Exception as e:
                    if self.logger:
                        self.logger.error(f"订阅 {symbol} 时出错: {e}")
                    continue

            if self.logger:
                self.logger.info(
                    f"🎯 已发送 {successful_subscriptions}/{len(symbols)} 个订阅消息 (完整符号格式)")
                self.logger.info("🎯 开始监听数据流（Backpack无订阅确认）")

            # 如果提供了回调函数，保存它
            if callback:
                self.ticker_callback = callback

            if self.logger:
                self.logger.info(f"✅ 批量ticker订阅完成")

        except Exception as e:
            if self.logger:
                self.logger.error(f"批量订阅ticker时出错: {e}")

    async def batch_subscribe_orderbooks(self, symbols: Optional[List[str]] = None,
                                         callback: Optional[Callable[[str, OrderBookData], None]] = None) -> None:
        """批量订阅多个交易对的订单簿数据"""
        try:
            # 如果未指定symbols，使用所有支持的交易对
            if symbols is None:
                symbols = await self.get_supported_symbols()

            # 过滤掉黑名单中的交易对
            original_count = len(symbols)
            symbols = self.filter_websocket_symbols(symbols)

            if self.logger:
                if original_count > len(symbols):
                    self.logger.info(
                        f"开始批量订阅 {len(symbols)} 个交易对的订单簿数据 (已过滤 {original_count - len(symbols)} 个黑名单交易对)")
                else:
                    self.logger.info(f"开始批量订阅 {len(symbols)} 个交易对的订单簿数据")

            # 批量订阅订单簿
            for symbol in symbols:
                try:
                    # 订阅orderbook数据
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": [f"depth.{symbol}"],
                        "id": len(self._ws_subscriptions) + 1
                    }

                    if await self._safe_send_message(json.dumps(subscribe_msg)):
                        if self.logger:
                            self.logger.debug(f"已订阅 {symbol} 的订单簿")

                    # 小延迟避免过于频繁的请求
                    await asyncio.sleep(0.1)

                except Exception as e:
                    if self.logger:
                        self.logger.error(f"订阅 {symbol} 订单簿时出错: {e}")
                    continue

            # 如果提供了回调函数，保存它
            if callback:
                self.orderbook_callback = callback

            if self.logger:
                self.logger.info(f"批量订单簿订阅完成")

        except Exception as e:
            if self.logger:
                self.logger.error(f"批量订阅订单簿时出错: {e}")

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        try:
            if symbol:
                # 取消特定符号的订阅
                subscriptions_to_remove = []
                for sub_type, sub_symbol, callback in self._ws_subscriptions:
                    if sub_symbol == symbol:
                        subscriptions_to_remove.append(
                            (sub_type, sub_symbol, callback))

                for sub in subscriptions_to_remove:
                    self._ws_subscriptions.remove(sub)
            else:
                # 取消所有订阅
                self._ws_subscriptions.clear()

        except Exception as e:
            if self.logger:
                self.logger.warning(f"取消订阅失败: {e}")

    async def get_supported_symbols(self) -> List[str]:
        """获取支持的交易对列表"""
        if not self._supported_symbols:
            await self._use_default_symbols()
        return self._supported_symbols.copy()

    # === 向后兼容方法 ===

    async def batch_subscribe_all_tickers(self, callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """批量订阅所有支持交易对的ticker数据"""
        try:
            # 获取所有支持的交易对
            symbols = await self.get_supported_symbols()
            if self.logger:
                self.logger.info(f"开始批量订阅所有 {len(symbols)} 个交易对的ticker数据")

            # 使用batch_subscribe_tickers方法
            await self.batch_subscribe_tickers(symbols, callback)

            if self.logger:
                self.logger.info(f"✅ 已成功批量订阅所有ticker数据")

        except Exception as e:
            if self.logger:
                self.logger.error(f"批量订阅所有ticker数据失败: {e}")
            raise

    async def unsubscribe_all(self) -> None:
        """取消所有订阅"""
        try:
            # 清空所有订阅
            self._ws_subscriptions.clear()
            self._subscribed_symbols.clear()

            if self.logger:
                self.logger.info("已取消所有Backpack订阅")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"取消所有Backpack订阅失败: {e}")

    async def fetch_supported_symbols(self) -> None:
        """通过API获取支持的交易对 - 🔥 修改：只获取永续合约"""
        try:
            if self.logger:
                self.logger.info("开始获取Backpack支持的交易对列表...")

            # 调用市场API获取所有交易对
            if hasattr(self, '_session') and self._session:
                async with self._session.get(f"{self.base_url}api/v1/markets") as response:
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
                                # 🔥 修改：只获取永续合约，排除现货
                                if symbol.endswith('_PERP'):
                                    # 永续合约
                                    normalized_symbol = self._normalize_backpack_symbol(
                                        symbol)
                                    supported_symbols.append(normalized_symbol)
                                    market_info[normalized_symbol] = market
                                    perpetual_count += 1

                                    if self.logger:
                                        self.logger.debug(
                                            f"添加永续合约: {normalized_symbol}")
                                else:
                                    # 现货交易对 - 跳过
                                    spot_count += 1
                                    if self.logger:
                                        self.logger.debug(f"跳过现货交易对: {symbol}")

                        self._supported_symbols = supported_symbols
                        self._market_info = market_info

                        if self.logger:
                            self.logger.info(f"✅ Backpack WebSocket市场数据统计:")
                            self.logger.info(f"  - 总市场数量: {total_markets}")
                            self.logger.info(f"  - 永续合约: {perpetual_count}")
                            self.logger.info(f"  - 现货交易对: {spot_count} (已跳过)")
                            self.logger.info(
                                f"  - 最终可用: {len(supported_symbols)} 个永续合约")

                    else:
                        if self.logger:
                            self.logger.error(f"获取市场数据失败: {response.status}")
                        await self._use_default_symbols()

        except Exception as e:
            if self.logger:
                self.logger.error(f"获取支持的交易对时出错: {e}")
            await self._use_default_symbols()

    def _cache_orderbook_data(self, symbol: str, bids: List[OrderBookLevel], asks: List[OrderBookLevel], timestamp: datetime) -> None:
        """缓存最新的orderbook数据供ticker使用"""
        try:
            # 只保留前5档买卖盘数据，减少内存占用
            best_bids = bids[:5] if bids else []
            best_asks = asks[:5] if asks else []

            self._latest_orderbooks[symbol] = {
                'bids': best_bids,
                'asks': best_asks,
                'timestamp': timestamp,
                'cache_time': time.time()
            }

            # 定期清理过期缓存
            self._cleanup_expired_orderbook_cache()

        except Exception as e:
            if self.logger:
                self.logger.warning(f"缓存orderbook数据失败: {e}")

    def _cleanup_expired_orderbook_cache(self) -> None:
        """清理过期的orderbook缓存"""
        try:
            current_time = time.time()
            expired_symbols = []

            for symbol, cache_data in self._latest_orderbooks.items():
                if current_time - cache_data.get('cache_time', 0) > self._orderbook_cache_timeout:
                    expired_symbols.append(symbol)

            for symbol in expired_symbols:
                del self._latest_orderbooks[symbol]

        except Exception as e:
            if self.logger:
                self.logger.warning(f"清理过期orderbook缓存失败: {e}")

    def _get_best_bid_ask_from_cache(self, symbol: str) -> tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        """从缓存的orderbook数据中获取最佳买卖价格和数量

        Returns:
            tuple: (bid_price, ask_price, bid_size, ask_size)
        """
        try:
            if symbol not in self._latest_orderbooks:
                return None, None, None, None

            cache_data = self._latest_orderbooks[symbol]

            # 检查缓存是否过期
            if time.time() - cache_data.get('cache_time', 0) > self._orderbook_cache_timeout:
                return None, None, None, None

            bid_price = bid_size = ask_price = ask_size = None

            # 获取最佳买价和数量
            bids = cache_data.get('bids', [])
            if bids:
                # 找到第一个有效的买单（数量大于0）
                for bid in bids:
                    if bid.size > 0:
                        bid_price = bid.price
                        bid_size = bid.size
                        break

            # 获取最佳卖价和数量
            asks = cache_data.get('asks', [])
            if asks:
                # 找到第一个有效的卖单（数量大于0）
                for ask in asks:
                    if ask.size > 0:
                        ask_price = ask.price
                        ask_size = ask.size
                        break

            return bid_price, ask_price, bid_size, ask_size

        except Exception as e:
            if self.logger:
                self.logger.warning(f"从orderbook缓存获取最佳价格失败: {e}")
            return None, None, None, None
