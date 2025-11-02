"""
刷量交易配置模型
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional


@dataclass
class LoggingConfig:
    """日志配置"""
    enabled: bool = True
    log_to_file: bool = True
    log_to_console: bool = False
    log_level: str = "INFO"
    log_dir: str = "logs"
    log_file: str = "volume_maker.log"

    # 日志轮转配置
    max_bytes: int = 5242880  # 5MB
    backup_count: int = 3
    encoding: str = "utf-8"


@dataclass
class StatisticsConfig:
    """统计配置"""
    enabled: bool = True
    track_total_volume: bool = True
    track_success_rate: bool = True
    track_pnl: bool = True
    save_interval: int = 60


@dataclass
class UIConfig:
    """终端UI配置"""
    enabled: bool = True
    refresh_rate: int = 1
    show_orderbook: bool = True
    show_recent_trades: bool = True
    recent_trades_limit: int = 10


@dataclass
class AdvancedConfig:
    """高级配置"""
    use_post_only: bool = False
    cancel_on_timeout: bool = True
    retry_on_rate_limit: bool = True
    rate_limit_cooldown: int = 1


@dataclass
class VolumeMakerConfig:
    """刷量交易配置"""

    # 基础配置
    exchange: str = "backpack"
    symbol: str = "BTC_USDC_PERP"

    # 🔥 双交易所符号配置（用于Lighter刷量系统）
    # 信号源交易所符号（如 Backpack: "BTC_USDC_PERP"）
    signal_symbol: Optional[str] = None
    execution_symbol: Optional[str] = None   # 执行端交易所符号（如 Lighter: "BTC"）

    # 订单参数
    order_mode: str = "limit"  # 订单模式: "limit"(限价开仓) 或 "market"(市价开仓)
    order_size: Decimal = Decimal("0.001")
    min_size: Decimal = Decimal("0.0001")
    max_size: Decimal = Decimal("0.01")
    quantity_precision: int = 5
    market_order_interval_ms: int = 0  # 市价模式下两个订单之间的间隔（毫秒），0表示无间隔
    market_wait_price_change: bool = False  # 市价模式：是否等待价格变化后再平仓（启用后interval_ms失效）
    market_close_on_quantity_reversal: bool = False  # 市价模式：平仓时检测订单簿数量反转（仅市价模式有效）
    market_price_change_count: int = 1  # 市价模式：价格变化多少次后触发平仓（默认1次）
    market_wait_timeout: float = 30.0  # 市价模式：等待价格变化的最大超时时间（秒，默认30秒）

    # Lighter 链上交易配置
    chain_confirmation_wait: int = 30  # 链上交易确认等待时间（秒）
    websocket_fill_timeout: int = 15  # WebSocket成交确认超时时间（秒）

    # 成交价格获取方式
    # 成交价格获取方式: "rest"(REST API查询) 或 "websocket"(WebSocket订阅)
    fill_price_method: str = "rest"

    # 订单簿获取方式
    # 订单簿获取方式: "rest"(REST API轮询) 或 "websocket"(WebSocket订阅)
    orderbook_method: str = "rest"

    # 价格稳定检测参数
    stability_check_duration: int = 3
    price_tolerance: Decimal = Decimal("0.0")
    check_interval: float = 0.1
    check_orderbook_reversal: bool = False  # 是否检测买卖单数量对比反转
    orderbook_reversal_trigger: int = 3  # 订单簿反转多少次后重置倒计时（默认3次）
    orderbook_quantity_ratio: float = 0.0   # 买卖单数量比例阈值（百分比），0表示不启用
    orderbook_min_quantity: float = 0.0  # 订单簿最小数量要求（检查数量多的那一方，仅市价模式，0表示不启用）

    # 交易方向策略
    reverse_trading: bool = False  # 反向交易模式：true=反向，false=正常（默认false）

    # 风控参数
    max_cycles: int = 1000
    max_position: Decimal = Decimal("0.1")
    max_slippage: Decimal = Decimal("0.001")
    order_timeout: int = 60
    max_consecutive_fails: int = 5
    min_balance: Optional[Decimal] = None  # 最小余额阈值（低于此值停止交易），None表示不检查

    # 执行控制
    cycle_interval: int = 0
    emergency_stop: bool = True
    auto_restart_on_error: bool = False

    # 子配置
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    statistics: StatisticsConfig = field(default_factory=StatisticsConfig)
    ui: UIConfig = field(default_factory=UIConfig)
    advanced: AdvancedConfig = field(default_factory=AdvancedConfig)

    @classmethod
    def from_dict(cls, data: dict) -> 'VolumeMakerConfig':
        """从字典创建配置"""
        vm_data = data.get('volume_maker', {})

        # 基础配置
        config = cls(
            exchange=vm_data.get('exchange', 'backpack'),
            symbol=vm_data.get('symbol', 'BTC_USDC_PERP'),
            signal_symbol=vm_data.get('signal_symbol'),
            execution_symbol=vm_data.get('execution_symbol'),
            order_mode=vm_data.get('order_mode', 'limit'),
            order_size=Decimal(str(vm_data.get('order_size', 0.001))),
            min_size=Decimal(str(vm_data.get('min_size', 0.0001))),
            max_size=Decimal(str(vm_data.get('max_size', 0.01))),
            quantity_precision=vm_data.get('quantity_precision', 5),
            market_order_interval_ms=vm_data.get(
                'market_order_interval_ms', 0),
            market_wait_price_change=vm_data.get(
                'market_wait_price_change', False),
            market_close_on_quantity_reversal=vm_data.get(
                'market_close_on_quantity_reversal', False),
            market_price_change_count=vm_data.get(
                'market_price_change_count', 1),
            market_wait_timeout=float(vm_data.get(
                'market_wait_timeout', 30.0)),
            chain_confirmation_wait=vm_data.get(
                'chain_confirmation_wait', 30),
            websocket_fill_timeout=vm_data.get(
                'websocket_fill_timeout', 15),
            fill_price_method=vm_data.get(
                'fill_price_method', 'rest'),
            orderbook_method=vm_data.get(
                'orderbook_method', 'rest'),
            stability_check_duration=vm_data.get(
                'stability_check_duration', 3),
            price_tolerance=Decimal(str(vm_data.get('price_tolerance', 0.0))),
            check_interval=vm_data.get('check_interval', 0.1),
            check_orderbook_reversal=vm_data.get(
                'check_orderbook_reversal', False),
            orderbook_reversal_trigger=vm_data.get(
                'orderbook_reversal_trigger', 3),
            orderbook_quantity_ratio=float(vm_data.get(
                'orderbook_quantity_ratio', 0.0)),
            orderbook_min_quantity=float(vm_data.get(
                'orderbook_min_quantity', 0.0)),
            reverse_trading=vm_data.get('reverse_trading', False),
            max_cycles=vm_data.get('max_cycles', 1000),
            max_position=Decimal(str(vm_data.get('max_position', 0.1))),
            max_slippage=Decimal(str(vm_data.get('max_slippage', 0.001))),
            order_timeout=vm_data.get('order_timeout', 60),
            max_consecutive_fails=vm_data.get('max_consecutive_fails', 5),
            min_balance=Decimal(str(vm_data['min_balance'])) if vm_data.get(
                'min_balance') else None,
            cycle_interval=vm_data.get('cycle_interval', 0),
            emergency_stop=vm_data.get('emergency_stop', True),
            auto_restart_on_error=vm_data.get('auto_restart_on_error', False)
        )

        # 日志配置
        log_data = vm_data.get('logging', {})
        rotation_data = log_data.get('rotation', {})
        config.logging = LoggingConfig(
            enabled=log_data.get('enabled', True),
            log_to_file=log_data.get('log_to_file', True),
            log_to_console=log_data.get('log_to_console', False),
            log_level=log_data.get('log_level', 'INFO'),
            log_dir=log_data.get('log_dir', 'logs'),
            log_file=log_data.get('log_file', 'volume_maker.log'),
            max_bytes=rotation_data.get('max_bytes', 5242880),
            backup_count=rotation_data.get('backup_count', 3),
            encoding=rotation_data.get('encoding', 'utf-8')
        )

        # 统计配置
        stats_data = vm_data.get('statistics', {})
        config.statistics = StatisticsConfig(
            enabled=stats_data.get('enabled', True),
            track_total_volume=stats_data.get('track_total_volume', True),
            track_success_rate=stats_data.get('track_success_rate', True),
            track_pnl=stats_data.get('track_pnl', True),
            save_interval=stats_data.get('save_interval', 60)
        )

        # UI配置
        ui_data = vm_data.get('ui', {})
        config.ui = UIConfig(
            enabled=ui_data.get('enabled', True),
            refresh_rate=ui_data.get('refresh_rate', 1),
            show_orderbook=ui_data.get('show_orderbook', True),
            show_recent_trades=ui_data.get('show_recent_trades', True),
            recent_trades_limit=ui_data.get('recent_trades_limit', 10)
        )

        # 高级配置
        adv_data = vm_data.get('advanced', {})
        config.advanced = AdvancedConfig(
            use_post_only=adv_data.get('use_post_only', False),
            cancel_on_timeout=adv_data.get('cancel_on_timeout', True),
            retry_on_rate_limit=adv_data.get('retry_on_rate_limit', True),
            rate_limit_cooldown=adv_data.get('rate_limit_cooldown', 1)
        )

        return config
