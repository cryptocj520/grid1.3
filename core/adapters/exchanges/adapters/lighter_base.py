"""
Lighter交易所适配器 - 基础模块

提供Lighter交易所的基础配置、工具方法和数据解析功能
"""

from typing import Dict, Any, Optional, List
from decimal import Decimal
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class LighterBase:
    """Lighter交易所基础类"""

    # Lighter API端点
    MAINNET_URL = "https://mainnet.zklighter.elliot.ai"
    TESTNET_URL = "https://testnet.zklighter.elliot.ai"
    MAINNET_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
    TESTNET_WS_URL = "wss://testnet.zklighter.elliot.ai/stream"

    # 交易类型常量（来自 SignerClient）
    TX_TYPE_CREATE_ORDER = 14
    TX_TYPE_CANCEL_ORDER = 15
    TX_TYPE_CANCEL_ALL_ORDERS = 16
    TX_TYPE_MODIFY_ORDER = 17
    TX_TYPE_WITHDRAW = 13
    TX_TYPE_TRANSFER = 12

    # 订单类型常量
    ORDER_TYPE_LIMIT = 0
    ORDER_TYPE_MARKET = 1
    ORDER_TYPE_STOP_LOSS = 2
    ORDER_TYPE_STOP_LOSS_LIMIT = 3
    ORDER_TYPE_TAKE_PROFIT = 4
    ORDER_TYPE_TAKE_PROFIT_LIMIT = 5
    ORDER_TYPE_TWAP = 6

    # 时间生效常量
    ORDER_TIME_IN_FORCE_IOC = 0  # Immediate or Cancel
    ORDER_TIME_IN_FORCE_GTT = 1  # Good Till Time
    ORDER_TIME_IN_FORCE_POST_ONLY = 2

    # 保证金模式
    CROSS_MARGIN_MODE = 0
    ISOLATED_MARGIN_MODE = 1

    # 订单状态映射
    ORDER_STATUS_MAP = {
        "pending": "open",
        "active": "open",
        "filled": "filled",
        "canceled": "canceled",
        "expired": "expired",
        "rejected": "rejected",
    }

    # 订单方向映射
    ORDER_SIDE_MAP = {
        True: "sell",   # is_ask=True -> sell
        False: "buy",   # is_ask=False -> buy
    }

    def __init__(self, config: Dict[str, Any]):
        """
        初始化Lighter基础类

        Args:
            config: 配置字典，包含API密钥、URL等信息
        """
        self.config = config
        self.testnet = config.get("testnet", False)

        # API 配置
        self.api_key_private_key = config.get("api_key_private_key", "")
        self.account_index = config.get("account_index", 0)
        self.api_key_index = config.get("api_key_index", 0)

        # URL配置
        self.base_url = self.TESTNET_URL if self.testnet else self.MAINNET_URL
        self.ws_url = self.TESTNET_WS_URL if self.testnet else self.MAINNET_WS_URL

        # 覆盖URL（如果配置中提供）
        if "api_url" in config:
            self.base_url = config["api_url"]
        if "ws_url" in config:
            self.ws_url = config["ws_url"]

        # 🔥 确保ws_url不为None
        if not self.ws_url:
            default_ws = self.TESTNET_WS_URL if self.testnet else self.MAINNET_WS_URL
            logger.warning(f"⚠️ ws_url为空，使用默认值: {default_ws}")
            self.ws_url = default_ws

        # 市场信息缓存
        self._markets_cache: Dict[int, Dict[str, Any]] = {}
        self._symbol_to_market_index: Dict[str, int] = {}

        # 初始化SignerClient（用于认证和签名）
        self.signer_client = None
        if self.api_key_private_key:
            try:
                from lighter import SignerClient
                self.signer_client = SignerClient(
                    url=self.base_url,
                    private_key=self.api_key_private_key,
                    account_index=self.account_index,
                    api_key_index=self.api_key_index,
                )
                logger.info("✅ SignerClient初始化成功")
            except Exception as e:
                logger.warning(f"⚠️ SignerClient初始化失败: {e}")

        logger.info(
            f"Lighter基础配置初始化完成 - URL: {self.base_url}, 测试网: {self.testnet}")

    def get_base_url(self) -> str:
        """获取REST API基础URL"""
        return self.base_url

    def get_ws_url(self) -> str:
        """获取WebSocket URL"""
        return self.ws_url

    # ============= 符号转换方法 =============

    def normalize_symbol(self, symbol: str) -> str:
        """
        标准化交易对符号

        Args:
            symbol: 原始符号，如 "BTC-USD" 或 "BTCUSD"

        Returns:
            标准化后的符号，如 "BTC-USD"
        """
        # Lighter使用 BTC-USD 格式
        symbol = symbol.upper().replace("_", "-")

        if "-" not in symbol and len(symbol) > 3:
            # 尝试分割，如 BTCUSD -> BTC-USD
            if symbol.endswith("USD"):
                base = symbol[:-3]
                quote = "USD"
                symbol = f"{base}-{quote}"
            elif symbol.endswith("USDT"):
                base = symbol[:-4]
                quote = "USDT"
                symbol = f"{base}-{quote}"

        return symbol

    def get_market_index(self, symbol: str) -> Optional[int]:
        """
        获取交易对的市场索引

        Args:
            symbol: 交易对符号

        Returns:
            市场索引，如果不存在返回None
        """
        normalized = self.normalize_symbol(symbol)
        return self._symbol_to_market_index.get(normalized)

    def update_markets_cache(self, markets: List[Dict[str, Any]]):
        """
        更新市场信息缓存

        Args:
            markets: 市场信息列表
        """
        for market in markets:
            market_index = market.get(
                "market_index") or market.get("market_id")
            if market_index is not None:
                self._markets_cache[market_index] = market
                # 构建符号到索引的映射
                symbol = market.get("symbol", "")
                if symbol:
                    self._symbol_to_market_index[symbol] = market_index

        logger.info(f"更新市场缓存完成: {len(self._markets_cache)} 个市场")

    # ============= 数据解析辅助方法 =============

    @staticmethod
    def _safe_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
        """
        安全转换为Decimal类型

        Args:
            value: 待转换的值
            default: 默认值

        Returns:
            Decimal类型的值
        """
        if value is None or value == "":
            return default
        try:
            return Decimal(str(value))
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        """
        安全转换为float类型

        Args:
            value: 待转换的值
            default: 默认值

        Returns:
            float类型的值
        """
        if value is None or value == "":
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        """
        安全转换为int类型

        Args:
            value: 待转换的值
            default: 默认值

        Returns:
            int类型的值
        """
        if value is None or value == "":
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _parse_timestamp(timestamp: Any) -> Optional[int]:
        """
        解析时间戳

        Args:
            timestamp: 时间戳（秒或毫秒）

        Returns:
            毫秒级时间戳
        """
        if timestamp is None:
            return None

        try:
            ts = int(timestamp)
            # 如果是秒级时间戳，转换为毫秒
            if ts < 10000000000:
                ts = ts * 1000
            return ts
        except (ValueError, TypeError):
            return None

    def _parse_order_side(self, is_ask: bool) -> str:
        """
        解析订单方向

        Args:
            is_ask: Lighter的is_ask字段

        Returns:
            标准化的订单方向 ("buy" 或 "sell")
        """
        return self.ORDER_SIDE_MAP.get(is_ask, "buy")

    def _parse_order_status(self, status: str) -> str:
        """
        解析订单状态

        Args:
            status: Lighter的订单状态

        Returns:
            标准化的订单状态
        """
        status_lower = status.lower() if status else "unknown"
        return self.ORDER_STATUS_MAP.get(status_lower, status_lower)

    def _parse_order_type(self, order_type: int) -> str:
        """
        解析订单类型

        Args:
            order_type: Lighter的订单类型常量

        Returns:
            标准化的订单类型字符串
        """
        type_map = {
            self.ORDER_TYPE_LIMIT: "limit",
            self.ORDER_TYPE_MARKET: "market",
            self.ORDER_TYPE_STOP_LOSS: "stop_loss",
            self.ORDER_TYPE_STOP_LOSS_LIMIT: "stop_loss_limit",
            self.ORDER_TYPE_TAKE_PROFIT: "take_profit",
            self.ORDER_TYPE_TAKE_PROFIT_LIMIT: "take_profit_limit",
            self.ORDER_TYPE_TWAP: "twap",
        }
        return type_map.get(order_type, "unknown")

    def _parse_time_in_force(self, tif: int) -> str:
        """
        解析时间生效类型

        Args:
            tif: Lighter的时间生效常量

        Returns:
            标准化的时间生效类型字符串
        """
        tif_map = {
            self.ORDER_TIME_IN_FORCE_IOC: "IOC",
            self.ORDER_TIME_IN_FORCE_GTT: "GTT",
            self.ORDER_TIME_IN_FORCE_POST_ONLY: "POST_ONLY",
        }
        return tif_map.get(tif, "GTT")

    # ============= 精度和数量处理 =============

    def format_quantity(self, quantity: Decimal, symbol: str) -> str:
        """
        格式化数量

        Args:
            quantity: 数量
            symbol: 交易对符号

        Returns:
            格式化后的数量字符串
        """
        # Lighter使用精度自适应，这里保留8位小数
        return f"{quantity:.8f}".rstrip('0').rstrip('.')

    def format_price(self, price: Decimal, symbol: str) -> str:
        """
        格式化价格

        Args:
            price: 价格
            symbol: 交易对符号

        Returns:
            格式化后的价格字符串
        """
        # Lighter使用精度自适应，这里保留8位小数
        return f"{price:.8f}".rstrip('0').rstrip('.')

    # ============= 错误处理 =============

    def parse_error(self, error: Any) -> str:
        """
        解析错误信息

        Args:
            error: 错误对象

        Returns:
            错误信息字符串
        """
        if error is None:
            return ""

        if isinstance(error, str):
            return error

        if isinstance(error, Exception):
            return str(error).strip().split("\n")[-1]

        return str(error)

    def __repr__(self) -> str:
        """字符串表示"""
        return f"LighterBase(testnet={self.testnet}, url={self.base_url})"
