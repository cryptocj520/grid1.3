#!/usr/bin/env python3
"""
网格交易系统启动脚本

独立启动网格交易系统
"""

from core.adapters.exchanges.models import ExchangeType
from core.adapters.exchanges import ExchangeFactory, ExchangeConfig
from core.services.grid.terminal_ui import GridTerminalUI
from core.services.grid.coordinator import GridCoordinator
from core.services.grid.implementations import (
    GridStrategyImpl,
    GridEngineImpl,
    PositionTrackerImpl
)
from core.services.grid.models import GridConfig, GridType, GridState
from core.logging import get_system_logger
import sys
import asyncio
import yaml
from pathlib import Path
from decimal import Decimal

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


# 导入交易所适配器


async def load_config(config_path: str) -> dict:
    """
    加载配置文件

    Args:
        config_path: 配置文件路径

    Returns:
        配置字典
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        print(f"❌ 加载配置文件失败: {e}")
        raise


def create_grid_config(config_data: dict) -> GridConfig:
    """
    创建网格配置对象

    Args:
        config_data: 配置数据

    Returns:
        网格配置对象
    """
    grid_config = config_data['grid_system']
    grid_type = GridType(grid_config['grid_type'])

    # 基础参数
    params = {
        'exchange': grid_config['exchange'],
        'symbol': grid_config['symbol'],
        'grid_type': grid_type,
        'grid_interval': Decimal(str(grid_config['grid_interval'])),
        'order_amount': Decimal(str(grid_config['order_amount'])),
        'max_position': Decimal(str(grid_config.get('max_position'))) if grid_config.get('max_position') else None,
        'enable_notifications': grid_config.get('enable_notifications', False),
        'order_health_check_interval': grid_config.get('order_health_check_interval', 600),
        # 默认万分之1
        'fee_rate': Decimal(str(grid_config.get('fee_rate', '0.0001'))),
    }

    # 🔥 价格移动网格：使用 follow_grid_count
    if grid_type in [GridType.FOLLOW_LONG, GridType.FOLLOW_SHORT]:
        params['follow_grid_count'] = grid_config['follow_grid_count']
        params['follow_timeout'] = grid_config.get('follow_timeout', 300)
        params['follow_distance'] = grid_config.get('follow_distance', 1)
        # lower_price 和 upper_price 保持默认值 None
    else:
        # 普通网格和马丁网格：从 price_range 读取
        params['lower_price'] = Decimal(
            str(grid_config['price_range']['lower_price']))
        params['upper_price'] = Decimal(
            str(grid_config['price_range']['upper_price']))

    # 🔥 马丁网格：添加 martingale_increment
    if 'martingale_increment' in grid_config:
        params['martingale_increment'] = Decimal(
            str(grid_config['martingale_increment']))

    # 🔥 剥头皮模式：添加剥头皮参数
    if 'scalping_enabled' in grid_config:
        params['scalping_enabled'] = grid_config['scalping_enabled']
    if 'scalping_trigger_percent' in grid_config:
        params['scalping_trigger_percent'] = grid_config['scalping_trigger_percent']
    if 'scalping_take_profit_grids' in grid_config:
        params['scalping_take_profit_grids'] = grid_config['scalping_take_profit_grids']

    # 🛡️ 本金保护模式：添加本金保护参数
    if 'capital_protection_enabled' in grid_config:
        params['capital_protection_enabled'] = grid_config['capital_protection_enabled']
    if 'capital_protection_trigger_percent' in grid_config:
        params['capital_protection_trigger_percent'] = grid_config['capital_protection_trigger_percent']

    # 💰 止盈模式：添加止盈参数
    if 'take_profit_enabled' in grid_config:
        params['take_profit_enabled'] = grid_config['take_profit_enabled']
    if 'take_profit_percentage' in grid_config:
        params['take_profit_percentage'] = Decimal(
            str(grid_config['take_profit_percentage']))

    # 🔒 价格锁定模式：添加价格锁定参数
    if 'price_lock_enabled' in grid_config:
        params['price_lock_enabled'] = grid_config['price_lock_enabled']
    if 'price_lock_threshold' in grid_config:
        params['price_lock_threshold'] = Decimal(
            str(grid_config['price_lock_threshold']))
    if 'price_lock_start_at_threshold' in grid_config:
        params['price_lock_start_at_threshold'] = grid_config['price_lock_start_at_threshold']

    # 🎯 反手挂单参数：添加反手挂单格子距离参数
    if 'reverse_order_grid_distance' in grid_config:
        params['reverse_order_grid_distance'] = int(
            grid_config['reverse_order_grid_distance'])

    return GridConfig(**params)


async def create_exchange_adapter(config_data: dict):
    """
    创建交易所适配器

    Args:
        config_data: 配置数据

    Returns:
        交易所适配器
    """
    import os

    grid_config = config_data['grid_system']
    exchange_name = grid_config['exchange'].lower()

    # 优先级：环境变量 > 交易所配置文件 > 空字符串
    api_key = os.getenv(f"{exchange_name.upper()}_API_KEY")
    api_secret = os.getenv(f"{exchange_name.upper()}_API_SECRET")

    # 如果环境变量没有设置，尝试从交易所配置文件读取
    if not api_key or not api_secret:
        try:
            exchange_config_path = Path(
                f"config/exchanges/{exchange_name}_config.yaml")
            if exchange_config_path.exists():
                with open(exchange_config_path, 'r', encoding='utf-8') as f:
                    exchange_config_data = yaml.safe_load(f)

                auth_config = exchange_config_data.get(
                    exchange_name, {}).get('authentication', {})
                api_key = api_key or auth_config.get('api_key', "")
                api_secret = api_secret or auth_config.get(
                    'private_key', "") or auth_config.get('api_secret', "")

                if api_key and api_secret:
                    print(f"   ✓ 从配置文件读取API密钥: {exchange_config_path}")
        except Exception as e:
            print(f"   ⚠️  无法读取交易所配置文件: {e}")

    # 如果仍然没有密钥，给出警告
    if not api_key or not api_secret:
        print(f"   ⚠️  警告：未找到API密钥配置")
        print(
            f"   提示：请设置环境变量或在 config/exchanges/{exchange_name}_config.yaml 中配置")

    # 创建交易所配置
    exchange_config = ExchangeConfig(
        exchange_id=exchange_name,
        name=exchange_name.capitalize(),
        exchange_type=ExchangeType.PERPETUAL,  # 默认使用永续合约
        api_key=api_key or "",
        api_secret=api_secret or "",
        testnet=False,
        enable_websocket=True,
        enable_auto_reconnect=True
    )

    # 使用工厂创建适配器
    factory = ExchangeFactory()
    adapter = factory.create_adapter(
        exchange_id=exchange_name,
        config=exchange_config
    )

    # 连接交易所
    await adapter.connect()

    return adapter


async def main(config_path: str = "config/grid/default_grid.yaml"):
    """
    主函数

    Args:
        config_path: 配置文件路径
    """
    logger = get_system_logger()

    try:
        print("=" * 70)
        print("🎯 网格交易系统启动")
        print("=" * 70)

        # 1. 加载配置
        print("\n📋 步骤 1/6: 加载配置文件...")
        config_data = await load_config(config_path)
        grid_config = create_grid_config(config_data)
        print(f"✅ 配置加载成功")
        print(f"   - 交易所: {grid_config.exchange}")
        print(f"   - 交易对: {grid_config.symbol}")
        print(f"   - 网格类型: {grid_config.grid_type.value}")

        # 🔥 价格移动网格：价格区间在运行时动态设置
        if grid_config.is_follow_mode():
            print(f"   - 价格区间: 动态跟随（运行时根据当前价格设置）")
        else:
            print(
                f"   - 价格区间: ${grid_config.lower_price:,.2f} - ${grid_config.upper_price:,.2f}")

        print(f"   - 网格间隔: ${grid_config.grid_interval}")
        print(f"   - 网格数量: {grid_config.grid_count}个")
        print(f"   - 订单数量: {grid_config.order_amount}")

        # 🔥 显示特殊模式参数
        if grid_config.is_martingale_mode():
            print(f"   - 马丁递增: {grid_config.martingale_increment} (每格递增)")
        if grid_config.is_follow_mode():
            print(f"   - 脱离超时: {grid_config.follow_timeout}秒")
            print(f"   - 脱离距离: {grid_config.follow_distance}格")

        # 2. 创建交易所适配器
        print("\n🔌 步骤 2/6: 连接交易所...")
        exchange_adapter = await create_exchange_adapter(config_data)
        print(f"✅ 交易所连接成功: {grid_config.exchange}")

        # 3. 创建核心组件
        print("\n⚙️  步骤 3/6: 初始化核心组件...")

        # 创建策略
        strategy = GridStrategyImpl()
        print("   ✓ 网格策略已创建")

        # 创建执行引擎
        engine = GridEngineImpl(exchange_adapter)
        print("   ✓ 执行引擎已创建")

        # 创建网格状态
        grid_state = GridState()

        # 创建持仓跟踪器
        tracker = PositionTrackerImpl(grid_config, grid_state)
        print("   ✓ 持仓跟踪器已创建")

        # 4. 创建协调器
        print("\n🎮 步骤 4/6: 创建系统协调器...")
        coordinator = GridCoordinator(
            config=grid_config,
            strategy=strategy,
            engine=engine,
            tracker=tracker,
            grid_state=grid_state
        )
        print("✅ 协调器创建成功")

        # 5. 初始化并启动网格系统
        print("\n🚀 步骤 5/6: 启动网格系统...")
        print(f"   - 准备批量挂单：{grid_config.grid_count}个订单")

        # 🔥 价格移动网格：价格区间在启动后才设置
        if not grid_config.is_follow_mode():
            print(
                f"   - 覆盖价格区间：${grid_config.lower_price:,.2f} - ${grid_config.upper_price:,.2f}")
        else:
            print(f"   - 价格区间：动态跟随（将根据当前价格设置）")

        await coordinator.start()
        print("✅ 网格系统已启动")
        print(f"   - 已成功挂出{grid_config.grid_count}个订单")

        # 🔥 价格移动网格：显示实际设置的价格区间
        if grid_config.is_follow_mode():
            print(
                f"   - 实际价格区间：${grid_config.lower_price:,.2f} - ${grid_config.upper_price:,.2f}")

        print(f"   - 所有网格已就位，等待成交...")

        # 6. 启动终端界面
        print("\n🖥️  步骤 6/6: 启动监控界面...")
        terminal_ui = GridTerminalUI(coordinator)

        print("=" * 70)
        print("✅ 网格交易系统完全启动")
        print("=" * 70)
        print()

        # 运行终端界面
        await terminal_ui.run()

    except KeyboardInterrupt:
        print("\n\n⚠️  收到退出信号，正在停止系统...")

    except Exception as e:
        logger.error(f"❌ 系统错误: {e}", exc_info=True)
        print(f"\n❌ 系统错误: {e}")

    finally:
        # 清理资源
        print("\n🧹 清理资源...")
        try:
            if 'coordinator' in locals():
                await coordinator.stop()
                print("   ✓ 网格系统已停止")

            if 'exchange_adapter' in locals():
                await exchange_adapter.disconnect()
                print("   ✓ 交易所已断开")

            print("\n✅ 系统已安全退出")

        except Exception as e:
            print(f"⚠️  清理过程出错: {e}")


def print_usage():
    """打印使用说明"""
    print("""
使用方法:
    python3 run_grid_trading.py [配置文件路径]

示例:
    # 使用默认配置
    python3 run_grid_trading.py
    
    # 使用做多网格配置
    python3 run_grid_trading.py config/grid/backpack_btc_long.yaml
    
    # 使用做空网格配置
    python3 run_grid_trading.py config/grid/backpack_btc_short.yaml

配置文件:
    - config/grid/default_grid.yaml          默认配置
    - config/grid/backpack_btc_long.yaml     BTC做多网格
    - config/grid/backpack_btc_short.yaml    BTC做空网格

注意事项:
    1. 确保API密钥已正确配置
    2. 确保有足够的资金用于网格交易
    3. 建议先用小额资金测试
    4. 网格系统会永久运行，除非手动停止
    5. 使用 Ctrl+C 或 Q 键安全退出系统
    """)


if __name__ == "__main__":
    # 检查命令行参数
    config_path = "config/grid/default_grid.yaml"

    if len(sys.argv) > 1:
        if sys.argv[1] in ['-h', '--help', 'help']:
            print_usage()
            sys.exit(0)

        # 支持 --config 格式
        if sys.argv[1] == '--config' or sys.argv[1] == '-c':
            if len(sys.argv) > 2:
                config_path = sys.argv[2]
            else:
                print("❌ --config 参数需要指定配置文件路径")
                print("\n使用 -h 或 --help 查看使用说明")
                sys.exit(1)
        else:
            # 直接传入配置文件路径
            config_path = sys.argv[1]

    # 检查配置文件是否存在
    if not Path(config_path).exists():
        print(f"❌ 配置文件不存在: {config_path}")
        print("\n使用 -h 或 --help 查看使用说明")
        sys.exit(1)

    try:
        # 运行主程序
        asyncio.run(main(config_path))
    except KeyboardInterrupt:
        print("\n👋 程序已退出")
    except Exception as e:
        print(f"\n❌ 启动失败: {e}")
        sys.exit(1)
