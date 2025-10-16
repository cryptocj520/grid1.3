"""
EdgeX WebSocket模块

包含WebSocket连接管理、数据订阅、消息处理、实时数据解析等功能
"""

import asyncio
import time
import json
import aiohttp
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal
from datetime import datetime

from .edgex_base import EdgeXBase
from ..models import TickerData, OrderBookData, TradeData, OrderBookLevel


class EdgeXWebSocket(EdgeXBase):
    """EdgeX WebSocket接口"""

    def __init__(self, config=None, logger=None):
        super().__init__(config)
        self.logger = logger
        if config and hasattr(config, 'ws_url') and config.ws_url:
            self.ws_url = config.ws_url
        else:
            self.ws_url = self.DEFAULT_WS_URL
        self._ws_connection = None
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
        
        # 🔥 新增：失败计数器，避免立即重连
        self._ping_failure_count = 0
        self._connection_issue_count = 0

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
            # 检查EdgeX的REST API是否可达 - 使用正确的官方端点
            api_url = "https://pro.edgex.exchange/"  # 正确的EdgeX官方端点
            timeout = aiohttp.ClientTimeout(total=8)
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(api_url) as response:
                    # 检查HTTP状态码，2xx和3xx都表示服务器可达
                    return response.status < 500  # 500以下状态码说明服务器可达
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"🏢 EdgeX服务器连通性检查失败: {e}")
            return False

    def _is_connection_usable(self) -> bool:
        """检查WebSocket连接是否可用 - 简化版本，只检查连接对象状态"""
        # 基础检查
        if not (hasattr(self, '_ws_connection') and 
                self._ws_connection is not None and 
                not self._ws_connection.closed and
                getattr(self, '_ws_connected', False)):
            return False
        
        # 简单的异常检查
        try:
            if hasattr(self._ws_connection, 'exception') and self._ws_connection.exception():
                return False
        except Exception:
            pass
        
        return True
    
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

    async def _send_websocket_ping(self) -> None:
        """发送WebSocket ping消息保持连接活跃"""
        try:
            if self._is_connection_usable():
                # 使用aiohttp的内置ping方法
                await self._ws_connection.ping()
                # 🔥 ping成功，重置失败计数器
                self._ping_failure_count = 0
                if self.logger:
                    self.logger.debug("🏓 EdgeX发送WebSocket ping")
            else:
                # 🔥 ping检查失败，增加失败计数
                self._ping_failure_count += 1
                if self.logger:
                    self.logger.warning(f"⚠️ 无法发送ping，WebSocket连接不可用 (失败次数: {self._ping_failure_count})")
                # 🔥 多次失败后才触发重连 (改为2次失败)
                if self._ping_failure_count >= 2:
                    self._ws_connected = False
                    self._last_heartbeat = time.time() - 180  # 超过120秒阈值
                    if self.logger:
                        self.logger.info(f"🔄 连续{self._ping_failure_count}次ping失败，触发重连")
        except Exception as e:
            # 🔥 ping异常，增加失败计数
            self._ping_failure_count += 1
            if self.logger:
                self.logger.error(f"❌ EdgeX发送ping失败: {str(e)} (失败次数: {self._ping_failure_count})")
            # 🔥 多次失败后才触发重连
            if self._ping_failure_count >= 2:
                self._ws_connected = False
                self._last_heartbeat = time.time() - 180  # 超过120秒阈值
                if self.logger:
                    self.logger.info(f"🔄 连续{self._ping_failure_count}次ping异常，触发重连")

    async def connect(self) -> bool:
        """建立WebSocket连接"""
        try:
            # 使用aiohttp建立WebSocket连接
            if not hasattr(self, '_session') or (hasattr(self, '_session') and self._session.closed):
                self._session = aiohttp.ClientSession()
            self._ws_connection = await self._session.ws_connect(self.ws_url)
            
            if self.logger:
                self.logger.info(f"EdgeX WebSocket连接已建立: {self.ws_url}")
            
            # 初始化状态
            self._ws_connected = True
            current_time = time.time()
            self._last_heartbeat = current_time
            self._reconnect_attempts = 0
            self._reconnecting = False
            
            # 🔥 新增：初始化ping/pong时间戳
            self._last_ping_time = current_time
            self._last_pong_time = current_time
            
            # 🔥 重置失败计数器
            self._ping_failure_count = 0
            self._connection_issue_count = 0
            
            # 启动消息处理任务
            self._ws_handler_task = asyncio.create_task(self._websocket_message_handler())
            
            # 启动心跳检测
            self._heartbeat_task = asyncio.create_task(self._websocket_heartbeat_loop())
            if self.logger:
                self.logger.info("💓 EdgeX心跳检测已启动")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.warning(f"建立EdgeX WebSocket连接失败: {e}")
            self._ws_connected = False
            return False

    async def disconnect(self) -> None:
        """断开WebSocket连接"""
        if self.logger:
            self.logger.info("🔄 开始断开EdgeX WebSocket连接...")
        
        try:
            # 1. 标记为断开状态，停止新的操作
            self._ws_connected = False
            
            # 2. 取消心跳任务
            if hasattr(self, '_heartbeat_task') and self._heartbeat_task and not self._heartbeat_task.done():
                if self.logger:
                    self.logger.info("🛑 取消EdgeX心跳任务...")
                self._heartbeat_task.cancel()
                try:
                    await asyncio.wait_for(self._heartbeat_task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                if self.logger:
                    self.logger.info("✅ EdgeX心跳任务已停止")
            
            # 3. 取消消息处理任务
            if hasattr(self, '_ws_handler_task') and self._ws_handler_task and not self._ws_handler_task.done():
                if self.logger:
                    self.logger.info("🛑 取消EdgeX消息处理任务...")
                self._ws_handler_task.cancel()
                try:
                    await asyncio.wait_for(self._ws_handler_task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                if self.logger:
                    self.logger.info("✅ EdgeX消息处理任务已停止")
            
            # 4. 关闭WebSocket连接
            if hasattr(self, '_ws_connection') and self._ws_connection and not self._ws_connection.closed:
                if self.logger:
                    self.logger.info("🛑 关闭EdgeX WebSocket连接...")
                try:
                    await asyncio.wait_for(self._ws_connection.close(), timeout=3.0)
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ WebSocket关闭超时，强制设置为None")
                self._ws_connection = None
                if self.logger:
                    self.logger.info("✅ EdgeX WebSocket连接已关闭")
            
            # 5. 关闭session
            if hasattr(self, '_session') and self._session and not self._session.closed:
                if self.logger:
                    self.logger.info("🛑 关闭EdgeX session...")
                try:
                    await asyncio.wait_for(self._session.close(), timeout=3.0)
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ Session关闭超时")
                if self.logger:
                    self.logger.info("✅ EdgeX session已关闭")
            
            # 6. 清理状态变量
            self._last_heartbeat = 0
            self._reconnect_attempts = 0
            
            if self.logger:
                self.logger.info("🎉 EdgeX WebSocket连接断开完成")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 关闭EdgeX WebSocket连接时出错: {e}")
                import traceback
                self.logger.error(f"断开连接错误堆栈: {traceback.format_exc()}")
            
            # 强制清理状态
            self._ws_connected = False
            self._ws_connection = None

    async def _websocket_heartbeat_loop(self):
        """WebSocket主动心跳检测循环 - 优化EdgeX连接稳定性"""
        # 🔥 优化：平衡重连敏感度和稳定性
        heartbeat_interval = 15  # 15秒检测一次 (加快检测频率)
        ping_interval = 30       # 30秒发送一次ping (加快ping频率)
        max_silence = 120        # 120秒(2分钟)无消息则重连 (适中)
        
        # 初始化ping相关时间戳
        self._last_ping_time = time.time()
        self._last_pong_time = time.time()
        
        if self.logger:
            self.logger.info("💓 EdgeX优化心跳检测循环启动 (快速重连模式)")
            self.logger.info(f"💓 EdgeX心跳参数: 检测间隔={heartbeat_interval}s, ping间隔={ping_interval}s, 最大静默={max_silence}s, 失败容忍=2次")
        
        try:
            while self._ws_connected:
                try:
                    # 使用更短的等待时间，加快检测响应
                    await asyncio.wait_for(
                        asyncio.sleep(heartbeat_interval), 
                        timeout=heartbeat_interval + 5
                    )
                    
                    # 再次检查连接状态
                    if not self._ws_connected:
                        if self.logger:
                            self.logger.info("💓 [EdgeX心跳] 连接已断开，退出心跳循环")
                        break
                    
                    current_time = time.time()
                    
                    # === 🔥 优化：加快ping频率 ===
                    if current_time - self._last_ping_time >= ping_interval:
                        await self._send_websocket_ping()
                        self._last_ping_time = current_time
                        if self.logger:
                            self.logger.debug(f"🏓 EdgeX心跳ping: 保持连接活跃")
                    
                    # === 💌 优化：综合判断重连条件 ===
                    silence_time = current_time - self._last_heartbeat
                    
                    # 🔥 综合判断：数据静默时间 OR 连续ping失败
                    should_reconnect = (
                        silence_time > max_silence or 
                        self._ping_failure_count >= 2
                    )
                    
                    if should_reconnect:
                        reason = []
                        if silence_time > max_silence:
                            reason.append(f"长时间静默: {silence_time:.1f}s")
                        if self._ping_failure_count >= 2:
                            reason.append(f"连续ping失败: {self._ping_failure_count}次")
                        
                        if self.logger:
                            self.logger.warning(f"⚠️ EdgeX WebSocket准备重连: {', '.join(reason)}")
                        
                        # 检查是否已经在重连中
                        if hasattr(self, '_reconnecting') and self._reconnecting:
                            if self.logger:
                                self.logger.info("🔄 [EdgeX心跳] 已有重连在进行中，跳过此次检测")
                            continue
                        
                        # 标记重连状态
                        self._reconnecting = True
                        
                        try:
                            if self.logger:
                                self.logger.info("🔄 [EdgeX心跳] 开始执行重连...")
                            await self._reconnect_websocket()
                            if self.logger:
                                self.logger.info("✅ [EdgeX心跳] 重连完成")
                            # 🔥 重连成功后重置计数器
                            self._ping_failure_count = 0
                        except asyncio.CancelledError:
                            if self.logger:
                                self.logger.warning("⚠️ [EdgeX心跳] 重连被取消")
                            raise
                        except Exception as e:
                            if self.logger:
                                self.logger.error(f"❌ [EdgeX心跳] 重连失败: {type(e).__name__}: {e}")
                        finally:
                            # 清除重连状态标记
                            self._reconnecting = False
                    else:
                        # 🔥 优化：减少正常状态的日志输出频率
                        if silence_time > 30:  # 只在超过30秒时输出警告
                            if self.logger:
                                self.logger.debug(f"💓 EdgeX WebSocket心跳: {silence_time:.1f}s前有数据")
                        else:
                            if self.logger:
                                self.logger.debug(f"💓 EdgeX WebSocket心跳正常: {silence_time:.1f}s前有数据")
                    
                except asyncio.CancelledError:
                    if self.logger:
                        self.logger.info("💓 [EdgeX心跳] 心跳检测被取消")
                    break
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ [EdgeX心跳] 心跳检测超时")
                    continue
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ EdgeX心跳检测错误: {e}")
                    # 错误后等待较短时间再继续
                    try:
                        await asyncio.wait_for(asyncio.sleep(5), timeout=10)  # 减少错误后的等待时间
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        break
                        
        except asyncio.CancelledError:
            if self.logger:
                self.logger.info("💓 [EdgeX心跳] 心跳循环被正常取消")
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ EdgeX心跳循环异常退出: {e}")
        finally:
            if self.logger:
                self.logger.info("💓 EdgeX心跳检测循环已退出")
            # 清理重连状态
            self._reconnecting = False
    
    async def _reconnect_websocket(self):
        """WebSocket自动重连 - 优化EdgeX重连稳定性"""
        # 🔥 优化：调整重连参数，提高EdgeX重连响应速度
        base_delay = 2   # 基础延迟改为2秒 (原来5秒，加快重连速度)
        max_delay = 300  # 最大延迟保持5分钟
        
        # 无限重试，移除次数限制
        self._reconnect_attempts += 1
        
        # 🔥 优化：更快的指数退避策略
        if self._reconnect_attempts <= 3:
            # 前3次使用较短的固定延迟
            delay = base_delay * self._reconnect_attempts  # 2s, 4s, 6s
        else:
            # 后续使用指数退避，但限制最大延迟
            delay = min(base_delay * (2 ** min(self._reconnect_attempts - 3, 7)), max_delay)
        
        if self.logger:
            self.logger.info(f"🔄 [EdgeX重连] 重连尝试 #{self._reconnect_attempts}，延迟{delay}s")
        
        reconnect_success = False
        
        try:
            # 步骤1: 网络诊断
            if self.logger:
                self.logger.info("🔧 [EdgeX重连] 步骤1: 网络连通性诊断...")
            
            # 🔥 优化：更快的网络检测，减少超时时间
            network_ok = await self._check_network_connectivity()
            if not network_ok:
                if self.logger:
                    self.logger.warning("⚠️ 基本网络连通性检查失败，跳过本次重连")
                return  # 网络不通，跳过本次重连
                
            # 检查交易所服务器连通性
            exchange_ok = await self._check_exchange_connectivity()
            if self.logger:
                status = "✅ 可达" if exchange_ok else "⚠️ 不可达"
                self.logger.info(f"🏢 EdgeX服务器连通性: {status}")
            
            # 🔥 优化：如果服务器不可达，增加延迟但不加倍
            if not exchange_ok:
                if self.logger:
                    self.logger.warning("⚠️ EdgeX服务器不可达，延迟重连")
                delay = delay + 3  # 只增加3秒，而不是加倍
            
            # 步骤2: 彻底清理旧连接
            if self.logger:
                self.logger.info("🔧 [EdgeX重连] 步骤2: 彻底清理旧连接...")
            await self._cleanup_old_connections()
            
            # 步骤3: 等待延迟
            if self.logger:
                self.logger.info(f"🔧 [EdgeX重连] 步骤3: 等待{delay}s后重连...")
            await asyncio.sleep(delay)
            
            # 步骤4: 重新建立连接
            if self.logger:
                self.logger.info("🔧 [EdgeX重连] 步骤4: 重新建立EdgeX WebSocket连接...")
            
            # 使用现有的connect方法，它已经包含了完整的连接逻辑
            reconnect_success = await self.connect()
            
            if reconnect_success:
                # 步骤5: 重新订阅所有频道
                if self.logger:
                    self.logger.info("🔧 [EdgeX重连] 步骤5: 重新订阅所有频道...")
                await self._resubscribe_all()
                
                # 步骤6: 重置状态 - 重连成功，重置计数
                self._reconnect_attempts = 0
                current_time = time.time()
                self._last_heartbeat = current_time
                
                # 🔥 新增：重置ping/pong时间戳
                self._last_ping_time = current_time
                self._last_pong_time = current_time
                
                if self.logger:
                    self.logger.info("🎉 [EdgeX重连] EdgeX WebSocket重连成功！")
            else:
                raise Exception("连接建立失败")
                
        except asyncio.CancelledError:
            if self.logger:
                self.logger.warning("⚠️ [EdgeX重连] EdgeX重连被取消")
            self._ws_connected = False
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [EdgeX重连] EdgeX重连失败: {type(e).__name__}: {e}")
                import traceback
                self.logger.error(f"[EdgeX重连] 完整错误堆栈: {traceback.format_exc()}")
            
            # 重连失败处理 - 无限重试模式
            reconnect_success = False
        
        # 🔥 优化：重连失败后的处理
        if not reconnect_success:
            if self.logger:
                self.logger.warning(f"⚠️ EdgeX重连失败，将在下次心跳检测时继续重试 (已尝试{self._reconnect_attempts}次)")
            
            # 🔥 优化：重连失败后，适当降低心跳检测的敏感度
            if self._reconnect_attempts > 5:
                # 多次重连失败后，临时调整心跳时间戳，避免立即再次重连
                self._last_heartbeat = time.time() - 200  # 给200秒的缓冲时间
                if self.logger:
                    self.logger.info("🔧 [EdgeX重连] 多次重连失败，临时降低心跳检测敏感度")
            
            # 保持连接状态为True，让心跳检测继续工作
            # 不停止心跳任务，实现真正的无限重试
    
    async def _cleanup_old_connections(self):
        """彻底清理旧的连接和任务"""
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
        """重新订阅所有频道"""
        try:
            if self.logger:
                self.logger.info("🔄 [重订阅调试] 开始重新订阅EdgeX所有频道")
            
            # 1. 重新订阅metadata (关键！)
            if self.logger:
                self.logger.info("🔧 [重订阅调试] 步骤1: 重新订阅metadata频道...")
            try:
                await self.subscribe_metadata()
                if self.logger:
                    self.logger.info("✅ [重订阅调试] 已重新订阅metadata频道")
            except Exception as e:
                if self.logger:
                    self.logger.error(f"❌ [重订阅调试] metadata订阅失败: {e}")
                raise
            
            # 2. 等待metadata解析完成
            if self.logger:
                self.logger.info("🔧 [重订阅调试] 步骤2: 等待metadata解析完成...")
            await asyncio.sleep(1)
            if self.logger:
                self.logger.info("✅ [重订阅调试] metadata等待完成")
            
            # 3. 检查合约映射状态
            if self.logger:
                mapping_count = len(self._symbol_contract_mappings) if hasattr(self, '_symbol_contract_mappings') else 0
                self.logger.info(f"🔧 [重订阅调试] 步骤3: 当前合约映射数量: {mapping_count}")
                if mapping_count > 0:
                    sample_mappings = dict(list(self._symbol_contract_mappings.items())[:3])
                    self.logger.info(f"🔧 [重订阅调试] 合约映射示例: {sample_mappings}")
            
            # 4. 重新订阅所有ticker
            if self.logger:
                self.logger.info("🔧 [重订阅调试] 步骤4: 重新订阅所有ticker...")
            ticker_count = 0
            failed_count = 0
            
            subscription_count = len(self._ws_subscriptions) if hasattr(self, '_ws_subscriptions') else 0
            if self.logger:
                self.logger.info(f"🔧 [重订阅调试] 待处理订阅数量: {subscription_count}")
            
            for sub_type, symbol, callback in self._ws_subscriptions:
                if sub_type == 'ticker' and symbol:
                    # 获取合约ID
                    contract_id = self._symbol_contract_mappings.get(symbol)
                    if contract_id:
                        try:
                            subscribe_msg = {
                                "type": "subscribe",
                                "channel": f"ticker.{contract_id}"
                            }
                            
                            if await self._safe_send_message(json.dumps(subscribe_msg)):
                                ticker_count += 1
                                if self.logger:
                                    self.logger.debug(f"✅ [重订阅调试] 重新订阅ticker: {symbol} (合约ID: {contract_id})")
                            else:
                                if self.logger:
                                    self.logger.error(f"❌ [重订阅调试] WebSocket连接不可用: {symbol}")
                                failed_count += 1
                            
                            await asyncio.sleep(0.1)  # 小延迟
                        except Exception as e:
                            if self.logger:
                                self.logger.error(f"❌ [重订阅调试] 订阅{symbol}失败: {e}")
                            failed_count += 1
                    else:
                        if self.logger:
                            self.logger.warning(f"⚠️ [重订阅调试] 未找到符号 {symbol} 的合约ID，跳过重新订阅")
                        failed_count += 1
                            
            # 5. 重新订阅其他类型的频道（直接发送消息，避免重复添加到订阅列表）
            if self.logger:
                self.logger.info("🔧 [重订阅调试] 步骤5: 重新订阅其他类型频道...")
            other_count = 0
            for sub_type, symbol, callback in self._ws_subscriptions:
                try:
                    if sub_type == 'orderbook' and symbol:
                        # 直接发送订阅消息，避免重复添加到_ws_subscriptions
                        contract_id = self._symbol_contract_mappings.get(symbol)
                        if contract_id:
                            subscribe_msg = {
                                "type": "subscribe",
                                "channel": f"depth.{contract_id}.15"
                            }
                            if await self._safe_send_message(json.dumps(subscribe_msg)):
                                other_count += 1
                                if self.logger:
                                    self.logger.debug(f"✅ [重订阅调试] 重新订阅orderbook: {symbol}")
                            await asyncio.sleep(0.1)
                    elif sub_type == 'trades' and symbol:
                        # 直接发送订阅消息，避免重复添加到_ws_subscriptions
                        contract_id = self._symbol_contract_mappings.get(symbol)
                        if contract_id:
                            subscribe_msg = {
                                "type": "subscribe",
                                "channel": f"trades.{contract_id}"
                            }
                            if await self._safe_send_message(json.dumps(subscribe_msg)):
                                other_count += 1
                                if self.logger:
                                    self.logger.debug(f"✅ [重订阅调试] 重新订阅trades: {symbol}")
                            await asyncio.sleep(0.1)
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ [重订阅调试] 订阅{sub_type}:{symbol}失败: {e}")
                    
            if self.logger:
                self.logger.info(f"✅ [重订阅调试] EdgeX重连订阅完成: {ticker_count}个ticker + {other_count}个其他 + metadata (失败: {failed_count})")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [重订阅调试] EdgeX重新订阅失败: {type(e).__name__}: {e}")
                import traceback
                self.logger.error(f"[重订阅调试] 完整错误堆栈: {traceback.format_exc()}")
            raise

    async def _websocket_message_handler(self) -> None:
        """WebSocket消息处理器"""
        try:
            async for msg in self._ws_connection:
                # 更新心跳时间
                self._last_heartbeat = time.time()
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._process_websocket_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.PONG:
                    # 🔥 新增：处理pong响应
                    if hasattr(self, '_last_pong_time'):
                        self._last_pong_time = time.time()
                    if self.logger:
                        self.logger.debug("🏓 EdgeX收到WebSocket pong响应")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    if self.logger:
                        self.logger.warning(f"EdgeX WebSocket错误: {self._ws_connection.exception()}")
                    self._ws_connected = False
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    if self.logger:
                        self.logger.warning("EdgeX WebSocket连接已关闭")
                    self._ws_connected = False
                    break
        except Exception as e:
            if self.logger:
                self.logger.warning(f"EdgeX WebSocket消息处理失败: {e}")
            self._ws_connected = False

    async def _process_websocket_message(self, message: str) -> None:
        """处理WebSocket消息"""
        try:
            data = json.loads(message)

            # 处理连接确认消息
            if data.get('type') == 'connected':
                if self.logger:
                    self.logger.info(f"EdgeX WebSocket连接确认: {data.get('sid')}")
                return

            # 处理订阅确认消息
            if data.get('type') == 'subscribed':
                if self.logger:
                    self.logger.debug(f"EdgeX订阅成功: {data.get('channel')}")
                return

            # 处理ping消息
            if data.get('type') == 'ping':
                pong_message = {
                    "type": "pong",
                    "time": data.get("time")
                }
                if await self._safe_send_message(json.dumps(pong_message)):
                    if self.logger:
                        self.logger.debug(f"发送pong响应: {data.get('time')}")
                else:
                    if self.logger:
                        self.logger.warning("发送pong响应失败")
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
                elif channel == 'metadata':
                    await self._handle_metadata_update(content)
                else:
                    if self.logger:
                        self.logger.debug(f"EdgeX未知的频道类型: {channel}")
                return

            # 处理错误消息
            if data.get('type') == 'error':
                if self.logger:
                    self.logger.warning(f"EdgeX WebSocket错误: {data.get('content')}")
                return

            # 其他未识别的消息
            if self.logger:
                self.logger.debug(f"EdgeX未知消息格式: {data}")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"处理EdgeX WebSocket消息失败: {e}")
                self.logger.debug(f"原始消息: {message}")

    async def _handle_ticker_update(self, channel: str, content: Dict[str, Any]) -> None:
        """处理行情更新"""
        try:
            # 从频道名称提取contractId
            contract_id = channel.split('.')[-1]  # ticker.10000001 -> 10000001
            symbol = self._contract_mappings.get(contract_id)
            
            if not symbol:
                if self.logger:
                    self.logger.debug(f"未找到合约ID {contract_id} 对应的交易对")
                return

            # 解析EdgeX ticker数据格式
            data_list = content.get('data', [])
            if not data_list:
                return
                
            ticker_data = data_list[0]  # 取第一个数据

            # 解析交易所时间戳
            exchange_timestamp = None
            current_time = datetime.now()  # 获取当前时间作为备用
            
            timestamp_candidates = [
                ('timestamp', 1000),        # 毫秒时间戳
                ('ts', 1000),              # 通用时间戳
                ('eventTime', 1000),       # 事件时间
                ('time', 1000),            # 时间字段
            ]
            
            for field, divisor in timestamp_candidates:
                if field in ticker_data and ticker_data[field]:
                    try:
                        timestamp_value = int(ticker_data[field])
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
            
            # === 完整解析EdgeX ticker数据的所有字段 ===
            ticker = TickerData(
                symbol=symbol,
                
                # === 基础价格信息 ===
                # 注意：EdgeX的ticker数据中没有bid/ask，需要从orderbook获取
                bid=self._safe_decimal(ticker_data.get('bestBidPrice')),  # EdgeX可能没有此字段
                ask=self._safe_decimal(ticker_data.get('bestAskPrice')),  # EdgeX可能没有此字段
                bid_size=None,  # EdgeX ticker中不提供，需要从orderbook获取
                ask_size=None,  # EdgeX ticker中不提供，需要从orderbook获取
                last=self._safe_decimal(ticker_data.get('lastPrice')),   # EdgeX: lastPrice
                open=self._safe_decimal(ticker_data.get('open')),        # EdgeX: open
                high=self._safe_decimal(ticker_data.get('high')),        # EdgeX: high
                low=self._safe_decimal(ticker_data.get('low')),          # EdgeX: low
                close=self._safe_decimal(ticker_data.get('close')),      # EdgeX: close
                
                # === 成交量信息 ===
                volume=self._safe_decimal(ticker_data.get('size')),      # EdgeX: size (基础资产交易量)
                quote_volume=self._safe_decimal(ticker_data.get('value')), # EdgeX: value (计价资产交易额)
                trades_count=self._safe_int(ticker_data.get('trades')),  # EdgeX: trades (成交笔数)
                
                # === 价格变化信息 ===
                change=self._safe_decimal(ticker_data.get('priceChange')),        # EdgeX: priceChange
                percentage=self._safe_decimal(ticker_data.get('priceChangePercent')), # EdgeX: priceChangePercent
                
                # === 合约特有信息（期货/永续合约） ===
                funding_rate=self._safe_decimal(ticker_data.get('fundingRate')),  # EdgeX: fundingRate (当前资金费率)
                predicted_funding_rate=None,  # EdgeX不提供预测资金费率
                funding_time=ticker_data.get('fundingTime'),     # EdgeX: fundingTime (当前资金费率时间)
                next_funding_time=ticker_data.get('nextFundingTime'), # EdgeX: nextFundingTime (下次资金费率时间)
                funding_interval=None,  # EdgeX不直接提供，可从两次时间戳推算
                
                # === 价格参考信息 ===
                index_price=self._safe_decimal(ticker_data.get('indexPrice')),   # EdgeX: indexPrice (指数价格)
                mark_price=None,  # EdgeX不提供标记价格，使用indexPrice或lastPrice
                oracle_price=self._safe_decimal(ticker_data.get('oraclePrice')), # EdgeX: oraclePrice (预言机价格)
                
                # === 持仓和合约信息 ===
                open_interest=self._safe_decimal(ticker_data.get('openInterest')), # EdgeX: openInterest (未平仓合约数量)
                open_interest_value=None,  # EdgeX不直接提供，需要用openInterest * price计算
                delivery_date=None,  # EdgeX永续合约无交割日期
                
                # === 时间相关信息 ===
                high_time=ticker_data.get('highTime'),          # EdgeX: highTime (最高价时间)
                low_time=ticker_data.get('lowTime'),            # EdgeX: lowTime (最低价时间)
                start_time=ticker_data.get('startTime'),        # EdgeX: startTime (统计开始时间)
                end_time=ticker_data.get('endTime'),            # EdgeX: endTime (统计结束时间)
                
                # === 合约标识信息 ===
                contract_id=ticker_data.get('contractId'),      # EdgeX: contractId (合约ID)
                contract_name=ticker_data.get('contractName'),  # EdgeX: contractName (合约名称)
                base_currency=None,  # 需要从symbol解析，如BTC_USDT -> BTC
                quote_currency=None, # 需要从symbol解析，如BTC_USDT -> USDT
                contract_size=None,  # EdgeX不提供，通常为1
                tick_size=None,      # EdgeX不在ticker中提供
                lot_size=None,       # EdgeX不在ticker中提供
                
                # === 时间戳链条 ===
                timestamp=main_timestamp,
                exchange_timestamp=exchange_timestamp,
                received_timestamp=current_time,  # 数据接收时间
                processed_timestamp=None,         # 将在处理完成后设置
                sent_timestamp=None,              # 将在发送给回调时设置
                
                # === 原始数据保留 ===
                raw_data=ticker_data
            )
            
            # 设置处理完成时间戳
            ticker.processed_timestamp = datetime.now()
            
            # 解析基础货币和计价货币（从symbol中提取）
            if symbol and '_' in symbol:
                parts = symbol.split('_')
                if len(parts) >= 2:
                    ticker.base_currency = parts[0]    # 如BTC_USDT -> BTC
                    ticker.quote_currency = parts[1]   # 如BTC_USDT -> USDT
            
            # 计算未平仓合约价值（如果有足够数据）
            if ticker.open_interest is not None and ticker.last is not None:
                ticker.open_interest_value = ticker.open_interest * ticker.last
            
            # 计算资金费率收取间隔（如果有两个时间戳）
            if ticker.funding_time is not None and ticker.next_funding_time is not None:
                try:
                    funding_time_dt = ticker.funding_time if isinstance(ticker.funding_time, datetime) else datetime.fromtimestamp(int(ticker.funding_time) / 1000)
                    next_funding_time_dt = ticker.next_funding_time if isinstance(ticker.next_funding_time, datetime) else datetime.fromtimestamp(int(ticker.next_funding_time) / 1000)
                    interval_seconds = int((next_funding_time_dt - funding_time_dt).total_seconds())
                    ticker.funding_interval = interval_seconds
                except (ValueError, TypeError):
                    pass
            
            # 如果没有mark_price，使用index_price或last作为替代
            if ticker.mark_price is None:
                if ticker.index_price is not None:
                    ticker.mark_price = ticker.index_price
                elif ticker.last is not None:
                    ticker.mark_price = ticker.last
            
            # 设置发送时间戳
            ticker.sent_timestamp = datetime.now()

            # 调用回调函数
            if self.ticker_callback:
                await self._safe_callback_with_symbol(self.ticker_callback, symbol, ticker)
            
            # 调用特定的订阅回调
            for sub_type, sub_symbol, callback in self._ws_subscriptions:
                if sub_type == 'ticker' and sub_symbol == symbol:
                    # 🔥 修复：批量订阅的回调函数需要两个参数 (symbol, ticker_data)
                    if callback:
                        await self._safe_callback_with_symbol(callback, symbol, ticker)

        except Exception as e:
            if self.logger:
                self.logger.warning(f"处理EdgeX行情更新失败: {e}")
                self.logger.debug(f"频道: {channel}, 内容: {content}")

    async def _handle_orderbook_update(self, channel: str, content: Dict[str, Any]) -> None:
        """处理EdgeX订单簿更新"""
        try:
            # 从频道名称提取contractId
            parts = channel.split('.')
            if len(parts) >= 2:
                contract_id = parts[1]  # depth.10000001.15 -> 10000001
            else:
                return
                
            symbol = self._contract_mappings.get(contract_id)
            
            if not symbol:
                if self.logger:
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
            elif 'ts' in orderbook_data:
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

            # 创建OrderBookData对象
            # 修复：使用exchange_timestamp作为主时间戳，如果没有则使用当前时间
            main_timestamp = exchange_timestamp if exchange_timestamp else datetime.now()
            
            orderbook = OrderBookData(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=main_timestamp,
                nonce=orderbook_data.get('endVersion'),
                exchange_timestamp=exchange_timestamp,
                raw_data=orderbook_data
            )

            # 调用回调函数
            if self.orderbook_callback:
                await self._safe_callback_with_symbol(self.orderbook_callback, symbol, orderbook)
            
            # 调用特定的订阅回调
            for sub_type, sub_symbol, callback in self._ws_subscriptions:
                if sub_type == 'orderbook' and sub_symbol == symbol:
                    await self._safe_callback(callback, orderbook)

        except Exception as e:
            if self.logger:
                self.logger.warning(f"处理EdgeX订单簿更新失败: {e}")
                self.logger.debug(f"频道: {channel}, 内容: {content}")

    async def _handle_trade_update(self, channel: str, content: Dict[str, Any]) -> None:
        """处理成交更新"""
        try:
            # 从频道名称提取contractId
            contract_id = channel.split('.')[-1]  # trades.10000001 -> 10000001
            symbol = self._contract_mappings.get(contract_id)
            
            if not symbol:
                if self.logger:
                    self.logger.debug(f"未找到合约ID {contract_id} 对应的交易对")
                return

            # 解析成交数据
            data_list = content.get('data', [])
            for trade_data in data_list:
                # 解析交易时间戳
                trade_timestamp = None
                if 'timestamp' in trade_data:
                    try:
                        timestamp_ms = int(trade_data['timestamp'])
                        trade_timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
                    except (ValueError, TypeError):
                        trade_timestamp = datetime.now()
                else:
                    trade_timestamp = datetime.now()
                
                trade = TradeData(
                    id=str(trade_data.get('tradeId', '')),
                    symbol=symbol,
                    side=trade_data.get('side', ''),
                    amount=self._safe_decimal(trade_data.get('size')),
                    price=self._safe_decimal(trade_data.get('price')),
                    cost=self._safe_decimal(trade_data.get('size', 0)) * self._safe_decimal(trade_data.get('price', 0)),
                    fee=None,
                    timestamp=trade_timestamp,
                    order_id=None,
                    raw_data=trade_data
                )

                # 调用回调函数
                await self._safe_callback(self.trades_callback, trade)
                
                # 调用特定的订阅回调
                for sub_type, sub_symbol, callback in self._ws_subscriptions:
                    if sub_type == 'trades' and sub_symbol == symbol:
                        await self._safe_callback(callback, trade)

        except Exception as e:
            if self.logger:
                self.logger.warning(f"处理EdgeX成交更新失败: {e}")
                self.logger.debug(f"频道: {channel}, 内容: {content}")

    async def _handle_metadata_update(self, content: Dict[str, Any]) -> None:
        """处理metadata更新"""
        try:
            if self.logger:
                self.logger.info("收到metadata更新")
            await self._process_metadata_response({'content': content})
        except Exception as e:
            if self.logger:
                self.logger.warning(f"处理metadata更新失败: {e}")

    async def _process_metadata_response(self, data: Dict[str, Any]) -> None:
        """处理metadata响应数据"""
        try:
            if self.logger:
                self.logger.info(f"开始处理metadata响应")
            
            content = data.get("content", {})
            
            # 根据分析结果，合约数据位于: content.data[0].contractList
            metadata_data = content.get("data", [])
            
            if metadata_data and isinstance(metadata_data, list) and len(metadata_data) > 0:
                first_item = metadata_data[0]
                
                # EdgeX实际使用contractList字段
                contracts = first_item.get("contractList", [])
                if not contracts:
                    contracts = first_item.get("contract", [])
                
                if not contracts:
                    if self.logger:
                        self.logger.warning("❌ 未找到任何合约数据")
                    return
                    
                supported_symbols = []
                contract_mappings = {}
                symbol_contract_mappings = {}
                
                total_contracts = len(contracts)
                
                if self.logger:
                    self.logger.info(f"开始处理 {total_contracts} 个合约...")
                
                for contract in contracts:
                    contract_id = contract.get("contractId")
                    symbol = contract.get("contractName") or contract.get("symbol")
                    enable_trade = contract.get("enableTrade", False)
                    enable_display = contract.get("enableDisplay", False)
                    
                    if contract_id and symbol and enable_trade and enable_display:
                        # 将symbol转换为标准格式
                        normalized_symbol = self._normalize_contract_symbol(symbol)
                        
                        supported_symbols.append(normalized_symbol)
                        contract_mappings[contract_id] = normalized_symbol
                        symbol_contract_mappings[normalized_symbol] = contract_id
                        
                        if self.logger:
                            self.logger.info(f"✅ 包含交易对: {symbol} -> {normalized_symbol} (ID: {contract_id})")
                
                # 更新实例变量
                self._supported_symbols = supported_symbols
                self._contract_mappings = contract_mappings
                self._symbol_contract_mappings = symbol_contract_mappings
                
                if self.logger:
                    self.logger.info(f"✅ 成功解析metadata，最终获取到 {len(supported_symbols)} 个可用交易对")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"处理metadata响应时出错: {e}")

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
                self.logger.warning(f"EdgeX回调函数执行失败: {e}")

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
                self.logger.warning(f"EdgeX回调函数执行失败: {e}")

    # === 订阅接口 ===

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅行情数据流"""
        try:
            self._ws_subscriptions.append(('ticker', symbol, callback))
            
            contract_id = self._symbol_contract_mappings.get(symbol)
            if not contract_id:
                if self.logger:
                    self.logger.warning(f"未找到交易对 {symbol} 的合约ID")
                return
            
            subscribe_msg = {
                "type": "subscribe",
                "channel": f"ticker.{contract_id}"
            }
            
            if await self._safe_send_message(json.dumps(subscribe_msg)):
                if self.logger:
                    self.logger.debug(f"已订阅 {symbol} 的ticker")
            else:
                if self.logger:
                    self.logger.warning(f"发送 {symbol} ticker订阅消息失败")
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅ticker失败: {e}")

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None], depth: int = 15) -> None:
        """订阅订单簿数据流"""
        try:
            self._ws_subscriptions.append(('orderbook', symbol, callback))
            
            contract_id = self._symbol_contract_mappings.get(symbol)
            if not contract_id:
                if self.logger:
                    self.logger.warning(f"未找到交易对 {symbol} 的合约ID")
                return
            
            subscribe_msg = {
                "type": "subscribe",
                "channel": f"depth.{contract_id}.{depth}"
            }
            
            if await self._safe_send_message(json.dumps(subscribe_msg)):
                if self.logger:
                    self.logger.debug(f"已订阅 {symbol} 的orderbook")
            else:
                if self.logger:
                    self.logger.warning(f"发送 {symbol} orderbook订阅消息失败")
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅orderbook失败: {e}")

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅成交数据流"""
        try:
            self._ws_subscriptions.append(('trades', symbol, callback))
            
            contract_id = self._symbol_contract_mappings.get(symbol)
            if not contract_id:
                if self.logger:
                    self.logger.warning(f"未找到交易对 {symbol} 的合约ID")
                return
            
            subscribe_msg = {
                "type": "subscribe",
                "channel": f"trades.{contract_id}"
            }
            
            if await self._safe_send_message(json.dumps(subscribe_msg)):
                if self.logger:
                    self.logger.debug(f"已订阅 {symbol} 的trades")
            else:
                if self.logger:
                    self.logger.warning(f"发送 {symbol} trades订阅消息失败")
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅trades失败: {e}")

    async def subscribe_metadata(self) -> None:
        """订阅metadata频道获取支持的交易对"""
        try:
            subscribe_msg = {
                "type": "subscribe",
                "channel": "metadata"
            }
            
            if await self._safe_send_message(json.dumps(subscribe_msg)):
                if self.logger:
                    self.logger.debug("已订阅metadata频道")
            else:
                if self.logger:
                    self.logger.warning("发送metadata订阅消息失败")
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅metadata失败: {e}")

    async def batch_subscribe_tickers(self, symbols: Optional[List[str]] = None, callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """批量订阅多个交易对的ticker数据"""
        try:
            if symbols is None:
                symbols = self._supported_symbols
                
            if self.logger:
                self.logger.info(f"开始批量订阅 {len(symbols)} 个交易对的ticker数据")
            
            # 设置全局回调
            if callback:
                self.ticker_callback = callback
            
            # 🔥 修复：保存订阅信息到重连列表
            for symbol in symbols:
                # 检查是否已经存在相同的订阅
                existing_sub = None
                for i, (sub_type, sub_symbol, sub_callback) in enumerate(self._ws_subscriptions):
                    if sub_type == 'ticker' and sub_symbol == symbol:
                        existing_sub = i
                        break
                
                if existing_sub is not None:
                    # 更新现有订阅的回调
                    self._ws_subscriptions[existing_sub] = ('ticker', symbol, callback)
                else:
                    # 添加新的订阅
                    self._ws_subscriptions.append(('ticker', symbol, callback))
            
            # 批量订阅
            for symbol in symbols:
                try:
                    contract_id = self._symbol_contract_mappings.get(symbol)
                    if not contract_id:
                        if self.logger:
                            self.logger.warning(f"未找到交易对 {symbol} 的合约ID，跳过订阅")
                        continue
                    
                    subscribe_msg = {
                        "type": "subscribe",
                        "channel": f"ticker.{contract_id}"
                    }
                    
                    if await self._safe_send_message(json.dumps(subscribe_msg)):
                        if self.logger:
                            self.logger.debug(f"已订阅 {symbol} (合约ID: {contract_id}) 的ticker")
                    else:
                        if self.logger:
                            self.logger.warning(f"发送 {symbol} ticker订阅消息失败")
                    
                    # 小延迟避免过于频繁的请求
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    if self.logger:
                        self.logger.warning(f"订阅 {symbol} ticker时出错: {e}")
                    continue
                    
            if self.logger:
                self.logger.info(f"批量ticker订阅完成")
            
        except Exception as e:
            if self.logger:
                self.logger.warning(f"批量订阅ticker时出错: {e}")

    async def batch_subscribe_orderbooks(self, symbols: Optional[List[str]] = None, depth: int = 15, callback: Optional[Callable[[str, OrderBookData], None]] = None) -> None:
        """批量订阅多个交易对的orderbook数据"""
        try:
            if symbols is None:
                symbols = self._supported_symbols
                
            if self.logger:
                self.logger.info(f"开始批量订阅 {len(symbols)} 个交易对的orderbook数据")
            
            # 设置全局回调
            if callback:
                self.orderbook_callback = callback
            
            # 🔥 修复：保存订阅信息到重连列表
            for symbol in symbols:
                # 检查是否已经存在相同的订阅
                existing_sub = None
                for i, (sub_type, sub_symbol, sub_callback) in enumerate(self._ws_subscriptions):
                    if sub_type == 'orderbook' and sub_symbol == symbol:
                        existing_sub = i
                        break
                
                if existing_sub is not None:
                    # 更新现有订阅的回调
                    self._ws_subscriptions[existing_sub] = ('orderbook', symbol, callback)
                else:
                    # 添加新的订阅
                    self._ws_subscriptions.append(('orderbook', symbol, callback))
            
            # 批量订阅
            for symbol in symbols:
                try:
                    contract_id = self._symbol_contract_mappings.get(symbol)
                    if not contract_id:
                        if self.logger:
                            self.logger.warning(f"未找到交易对 {symbol} 的合约ID，跳过订阅")
                        continue
                    
                    subscribe_msg = {
                        "type": "subscribe",
                        "channel": f"depth.{contract_id}.{depth}"
                    }
                    
                    if await self._safe_send_message(json.dumps(subscribe_msg)):
                        if self.logger:
                            self.logger.debug(f"已订阅 {symbol} (合约ID: {contract_id}) 的orderbook")
                    else:
                        if self.logger:
                            self.logger.warning(f"发送 {symbol} orderbook订阅消息失败")
                    
                    # 小延迟避免过于频繁的请求
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    if self.logger:
                        self.logger.warning(f"订阅 {symbol} orderbook时出错: {e}")
                    continue
                    
            if self.logger:
                self.logger.info(f"批量orderbook订阅完成")
            
        except Exception as e:
            if self.logger:
                self.logger.warning(f"批量订阅orderbook时出错: {e}")

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        try:
            if symbol:
                # 取消特定符号的订阅
                subscriptions_to_remove = []
                for sub_type, sub_symbol, callback in self._ws_subscriptions:
                    if sub_symbol == symbol:
                        subscriptions_to_remove.append((sub_type, sub_symbol, callback))
                
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
        return self._supported_symbols.copy()

    async def fetch_supported_symbols(self) -> None:
        """通过WebSocket获取支持的交易对"""
        try:
            if self.logger:
                self.logger.info("开始获取EdgeX支持的交易对列表...")
            
            # 如果还没有连接，先连接
            if not self._ws_connection:
                await self.connect()
            
            # 订阅metadata频道
            await self.subscribe_metadata()
            
            # 等待metadata响应
            timeout = 10
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                if self._supported_symbols:
                    break
                await asyncio.sleep(0.5)
            
            if not self._supported_symbols:
                if self.logger:
                    self.logger.warning("未能获取到支持的交易对")
            else:
                if self.logger:
                    self.logger.info(f"成功获取到 {len(self._supported_symbols)} 个交易对")
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取支持的交易对时出错: {e}")

    # 向后兼容方法
    async def subscribe_order_book(self, symbol: str, callback, depth: int = 20):
        """订阅订单簿数据 - 向后兼容"""
        await self.subscribe_orderbook(symbol, callback, depth)

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """订阅用户数据流"""
        try:
            self._ws_subscriptions.append(('user_data', None, callback))
            self.user_data_callback = callback
            
            subscribe_msg = {
                "type": "subscribe",
                "channel": "userData"
            }
            
            if self._ws_connection:
                await self._ws_connection.send_str(json.dumps(subscribe_msg))
                if self.logger:
                    self.logger.debug("已订阅用户数据流")
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅用户数据流失败: {e}")

    async def unsubscribe_all(self) -> None:
        """取消所有订阅"""
        try:
            if self._ws_connection:
                unsubscribe_message = {
                    "type": "unsubscribe_all"
                }
                await self._ws_connection.send_str(json.dumps(unsubscribe_message))
                self.logger.info("已取消所有EdgeX订阅")
                
                # 清空所有订阅
                self._ws_subscriptions.clear()
                self._ticker_callbacks.clear()
                self._orderbook_callbacks.clear()
                self._trade_callbacks.clear()
                self._user_data_callbacks.clear()
                
        except Exception as e:
            self.logger.warning(f"取消所有EdgeX订阅失败: {e}")

    # === 兼容性方法 - 保持与原始实现的兼容性 ===

    async def _subscribe_websocket(self, sub_type: str, symbol: Optional[str], callback: Callable) -> None:
        """WebSocket订阅通用方法 - 与原始实现保持一致"""
        try:
            # 初始化订阅列表
            if not hasattr(self, '_ws_subscriptions'):
                self._ws_subscriptions = []

            # 添加订阅
            self._ws_subscriptions.append((sub_type, symbol, callback))

            # 如果还没有WebSocket连接，创建一个
            if not self._ws_connection:
                await self._setup_websocket_connection()

            # 发送订阅消息
            if self._ws_connection:
                subscribe_msg = self._build_subscribe_message(sub_type, symbol)
                await self._ws_connection.send_str(subscribe_msg)

        except Exception as e:
            self.logger.warning(f"EdgeX WebSocket订阅失败 {sub_type} {symbol}: {e}")

    async def _setup_websocket_connection(self) -> None:
        """建立WebSocket连接 - 与原始实现保持一致"""
        try:
            # 使用现有的connect方法
            await self.connect()
            
            self.logger.info(f"EdgeX WebSocket连接已建立: {self.ws_url}")

        except Exception as e:
            self.logger.warning(f"建立EdgeX WebSocket连接失败: {e}")

    def _build_subscribe_message(self, sub_type: str, symbol: Optional[str]) -> str:
        """构建订阅消息 - 基于EdgeX实际API格式"""
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

    async def _handle_user_data_update(self, data: Dict[str, Any]) -> None:
        """处理用户数据更新"""
        try:
            # 调用用户数据回调函数
            for callback in self._user_data_callbacks:
                await self._safe_callback(callback, data)

        except Exception as e:
            self.logger.warning(f"处理EdgeX用户数据更新失败: {e}")
            self.logger.debug(f"数据内容: {data}")

    # === 属性访问方法 ===

    @property
    def ws_connection(self):
        """WebSocket连接属性"""
        return self._ws_connection

    @property
    def ws_subscriptions(self):
        """WebSocket订阅列表属性"""
        return getattr(self, '_ws_subscriptions', []) 