#!/usr/bin/env python3
"""
刷量交易主程序

通过双向挂单快速刷交易量
"""

from core.services.volume_maker.terminal_ui import VolumeMakerTerminalUI
from core.services.volume_maker.models.volume_maker_config import VolumeMakerConfig
from core.services.volume_maker.implementations.volume_maker_service_impl import VolumeMakerServiceImpl
from core.adapters.exchanges.interface import ExchangeConfig, ExchangeType
from core.adapters.exchanges.factory import get_exchange_factory
import asyncio
import signal
import sys
from pathlib import Path
from typing import Optional
import yaml

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))


class VolumeMakerApp:
    """刷量交易应用"""

    def __init__(self, config_file: str):
        """
        初始化应用

        Args:
            config_file: 配置文件路径
        """
        self.config_file = config_file
        self.config: Optional[VolumeMakerConfig] = None
        self.service: Optional[VolumeMakerServiceImpl] = None
        self.ui: Optional[VolumeMakerTerminalUI] = None
        self.adapter = None
        self._stop_requested = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _setup_signal_handlers(self):
        """设置信号处理器（必须在事件循环中调用）"""
        def signal_handler():
            """信号处理器"""
            print("\n\n🛑 检测到停止信号，正在安全退出...")
            self._stop_requested = True

            # 🔥 关键修复：停止 UI（这会让 UI 循环退出）
            if self.ui:
                self.ui.stop()

            # 停止服务
            if self.service and self.service.is_running():
                # 在事件循环中调度停止任务
                asyncio.create_task(self._safe_stop())

        # 注册信号处理（仅在Unix系统上）
        try:
            if self._loop and hasattr(self._loop, 'add_signal_handler'):
                for sig in (signal.SIGINT, signal.SIGTERM):
                    self._loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Windows不支持add_signal_handler，依赖KeyboardInterrupt
            pass

    async def _safe_stop(self):
        """安全停止服务"""
        try:
            print("⏸️  正在停止服务...")
            if self.service:
                await self.service.stop()
            print("✅ 服务已停止")
        except Exception as e:
            print(f"⚠️  停止服务时出错: {e}")

    def load_config(self) -> bool:
        """加载配置"""
        try:
            config_path = Path(self.config_file)
            if not config_path.exists():
                print(f"❌ 配置文件不存在: {config_path}")
                return False

            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)

            self.config = VolumeMakerConfig.from_dict(config_data)
            print(f"✅ 配置文件加载成功: {config_path}")
            return True

        except Exception as e:
            print(f"❌ 加载配置文件失败: {e}")
            return False

    def load_exchange_config(self) -> Optional[ExchangeConfig]:
        """加载交易所配置"""
        try:
            # 根据交易所名称加载对应配置
            exchange_config_file = Path(
                "config/exchanges") / f"{self.config.exchange}_config.yaml"

            if not exchange_config_file.exists():
                print(f"❌ 交易所配置文件不存在: {exchange_config_file}")
                return None

            with open(exchange_config_file, 'r', encoding='utf-8') as f:
                exchange_data = yaml.safe_load(f)

            # 获取交易所配置
            exchange_conf = exchange_data.get(self.config.exchange, {})

            # 获取认证配置（支持不同格式）
            auth_conf = exchange_conf.get('authentication', {})
            api_conf = exchange_conf.get('api', {})

            # Backpack使用 private_key，其他交易所使用 api_secret
            api_secret = (
                auth_conf.get('private_key') or  # Backpack格式
                exchange_conf.get('api_secret') or  # 直接配置
                auth_conf.get('api_secret') or  # 认证块中
                ''
            )

            api_key = (
                auth_conf.get('api_key') or  # 认证块中
                exchange_conf.get('api_key') or  # 直接配置
                ''
            )

            # 创建ExchangeConfig
            config = ExchangeConfig(
                exchange_id=self.config.exchange,
                name=exchange_conf.get('name', self.config.exchange),
                exchange_type=ExchangeType(exchange_conf.get('type', 'spot')),
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=exchange_conf.get(
                    'api_passphrase') or auth_conf.get('api_passphrase'),
                testnet=exchange_conf.get('testnet', False) or exchange_conf.get(
                    'development', {}).get('sandbox', False),
                base_url=api_conf.get(
                    'base_url') or exchange_conf.get('base_url'),
                ws_url=api_conf.get('ws_url') or exchange_conf.get('ws_url'),
                default_leverage=exchange_conf.get('default_leverage', 1),
                default_margin_mode=exchange_conf.get(
                    'default_margin_mode', 'cross')
            )

            # 验证API密钥已加载
            if api_key and api_secret:
                # 显示部分密钥用于确认（安全起见只显示前后几个字符）
                masked_key = f"{api_key[:8]}...{api_key[-4:]}" if len(
                    api_key) > 12 else "***"
                masked_secret = f"{api_secret[:8]}...{api_secret[-4:]}" if len(
                    api_secret) > 12 else "***"
                print(f"✅ 交易所配置加载成功: {self.config.exchange}")
                print(f"   API Key: {masked_key}")
                print(f"   API Secret: {masked_secret}")
            else:
                print(f"⚠️  警告: API密钥未配置或配置不完整")
                if not api_key:
                    print(f"   缺少 API Key")
                if not api_secret:
                    print(f"   缺少 API Secret (或 private_key)")

            return config

        except Exception as e:
            print(f"❌ 加载交易所配置失败: {e}")
            return None

    async def initialize(self) -> bool:
        """初始化"""
        try:
            # 加载配置
            if not self.load_config():
                return False

            # 加载交易所配置
            exchange_config = self.load_exchange_config()
            if not exchange_config:
                return False

            # 创建交易所适配器
            print(f"🔧 创建 {self.config.exchange} 适配器...")
            factory = get_exchange_factory()
            self.adapter = factory.create_adapter(
                exchange_id=self.config.exchange,
                config=exchange_config
            )

            # 创建刷量服务
            print("🔧 创建刷量服务...")
            self.service = VolumeMakerServiceImpl(self.adapter)

            # 初始化服务
            print("🔧 初始化刷量服务...")
            if not await self.service.initialize(self.config):
                return False

            # 创建终端UI
            if self.config.ui.enabled:
                print("🔧 创建终端UI...")
                self.ui = VolumeMakerTerminalUI(self.service)

            print("✅ 初始化完成")
            return True

        except Exception as e:
            print(f"❌ 初始化失败: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def run(self) -> None:
        """运行应用"""
        try:
            # 获取当前事件循环并设置信号处理器
            self._loop = asyncio.get_running_loop()
            self._setup_signal_handlers()

            # 启动服务
            print("🚀 启动刷量服务...")
            await self.service.start()

            # 如果启用UI，运行UI
            if self.ui:
                print("🎨 启动终端UI...")
                # 在一个任务中运行UI，以便可以响应停止信号
                ui_task = asyncio.create_task(self.ui.run())

                # 等待UI完成或停止请求
                while not self._stop_requested and self.service.is_running():
                    await asyncio.sleep(0.5)
                    if ui_task.done():
                        break

                # 如果是停止请求，取消UI任务
                if not ui_task.done():
                    ui_task.cancel()
                    try:
                        await ui_task
                    except asyncio.CancelledError:
                        pass
            else:
                # 否则等待服务完成或停止请求
                while not self._stop_requested and self.service.is_running():
                    await asyncio.sleep(0.5)

        except KeyboardInterrupt:
            print("\n\n🛑 检测到 Ctrl+C，正在停止...")
        except Exception as e:
            print(f"❌ 运行出错: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await self.cleanup()

    async def cleanup(self) -> None:
        """清理资源"""
        try:
            print("\n🧹 清理资源...")

            # 停止UI
            if self.ui:
                try:
                    self.ui.stop()
                    print("  ✅ UI已停止")
                except Exception as e:
                    print(f"  ⚠️  停止UI失败: {e}")

            # 停止服务（带超时保护）
            if self.service and self.service.is_running():
                try:
                    print("  ⏸️  正在停止刷量服务...")
                    await asyncio.wait_for(self.service.stop(), timeout=10.0)
                    print("  ✅ 刷量服务已停止")
                except asyncio.TimeoutError:
                    print("  ⚠️  停止服务超时（10秒）")
                except Exception as e:
                    print(f"  ⚠️  停止服务失败: {e}")

            # 断开交易所连接（带超时保护）
            if self.adapter and hasattr(self.adapter, 'is_connected'):
                try:
                    if self.adapter.is_connected():
                        print("  ⏸️  正在断开交易所连接...")
                        await asyncio.wait_for(self.adapter.disconnect(), timeout=5.0)
                        print("  ✅ 交易所连接已断开")
                except asyncio.TimeoutError:
                    print("  ⚠️  断开连接超时（5秒）")
                except Exception as e:
                    print(f"  ⚠️  断开连接失败: {e}")

            print("\n✅ 清理完成\n")

        except Exception as e:
            print(f"\n⚠️  清理过程出错: {e}\n")


async def main():
    """主函数"""
    # 默认配置文件
    config_file = "config/volume_maker/backpack_btc_volume_maker.yaml"

    # 从命令行参数获取配置文件
    if len(sys.argv) > 1:
        config_file = sys.argv[1]

    print("=" * 60)
    print("🎯 刷量交易系统 v1.0")
    print("=" * 60)
    print(f"配置文件: {config_file}")
    print()

    # 创建应用
    app = VolumeMakerApp(config_file)

    # 初始化
    if not await app.initialize():
        print("❌ 初始化失败，退出程序")
        return

    # 运行
    await app.run()

    print()
    print("=" * 60)
    print("👋 程序已退出")
    print("=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序异常退出: {e}")
        import traceback
        traceback.print_exc()
