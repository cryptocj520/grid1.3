"""
现货预留BTC管理器

职责：
1. 追踪预留BTC消耗（买入手续费）
2. 计算补充数量（遵守精度规则）
3. 执行自动购买（市价单）
4. 提供状态查询

仅对现货网格生效，永续合约不使用此模块。
"""

from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

from ....logging import get_logger


class SpotReserveManager:
    """现货预留BTC管理器（仅现货使用）"""

    def __init__(
        self,
        reserve_config: Dict[str, Any],
        exchange_adapter,
        symbol: str,
        quantity_precision: int
    ):
        """
        初始化现货预留管理器

        Args:
            reserve_config: spot_reserve配置字典
            exchange_adapter: 交易所适配器
            symbol: 交易对符号 (例如: UBTC/USDC)
            quantity_precision: 数量精度（例如: 5）
        """
        self.config = reserve_config
        self.exchange = exchange_adapter
        self.symbol = symbol
        self.quantity_precision = quantity_precision
        self.logger = get_logger(self.__class__.__name__)

        # 解析币种
        symbol_parts = symbol.split('/')
        self.base_currency = symbol_parts[0]  # UBTC
        self.quote_currency = symbol_parts[1].split(
            ':')[0] if ':' in symbol_parts[1] else symbol_parts[1]  # USDC

        # 基础配置
        self.reserve_amount = Decimal(str(reserve_config['reserve_amount']))
        self.spot_buy_fee_rate = Decimal(
            str(reserve_config['spot_buy_fee_rate']))

        # 自动补充配置
        auto_config = reserve_config.get('auto_replenish', {})
        self.replenish_enabled = auto_config.get('enabled', True)
        self.trigger_percent = Decimal(
            str(auto_config.get('trigger_percent', 0.5)))
        self.target_percent = Decimal(
            str(auto_config.get('target_percent', 1.0)))
        self.max_replenish_per_day = auto_config.get(
            'max_replenish_per_day', 10)
        self.min_replenish_interval = auto_config.get(
            'min_replenish_interval', 300)

        # 状态追踪
        self.total_fee_consumed = Decimal('0')
        self.fee_history: List[Dict[str, Any]] = []
        self.replenish_history: List[Dict[str, Any]] = []
        self.last_replenish_time: Optional[datetime] = None

        self.logger.info(
            f"✅ 现货预留管理器初始化: "
            f"预留={self.reserve_amount} {self.base_currency}, "
            f"手续费率={self.spot_buy_fee_rate}, "
            f"自动补充={'启用' if self.replenish_enabled else '禁用'}"
        )

    # ========== 核心方法 ==========

    def update_reserve_amount(self, new_reserve: Decimal):
        """
        更新预留基数

        用途：启动时如果账户余额 >= 配置的预留，将账户余额作为实际预留

        Args:
            new_reserve: 新的预留基数
        """
        old_reserve = self.reserve_amount
        self.reserve_amount = new_reserve
        self.logger.info(
            f"🔄 预留基数已更新: {old_reserve} → {new_reserve} {self.base_currency}"
        )

    def record_buy_fee(self, buy_amount: Decimal) -> Decimal:
        """
        记录买入手续费消耗

        Args:
            buy_amount: 买入数量

        Returns:
            手续费数量
        """
        fee = buy_amount * self.spot_buy_fee_rate
        self.total_fee_consumed += fee

        self.fee_history.append({
            'time': datetime.now(),
            'buy_amount': float(buy_amount),
            'fee': float(fee),
            'total_consumed': float(self.total_fee_consumed),
            'health_after': float(self.get_reserve_health_percent())
        })

        self.logger.debug(
            f"📊 记录买入手续费: {fee} {self.base_currency}, "
            f"累计消耗: {self.total_fee_consumed}"
        )

        return fee

    def get_current_reserve(self) -> Decimal:
        """获取当前预留数量"""
        return self.reserve_amount - self.total_fee_consumed

    def get_reserve_health_percent(self) -> Decimal:
        """获取预留健康度百分比 (0-100)"""
        current = self.get_current_reserve()
        if self.reserve_amount == 0:
            return Decimal('0')
        return (current / self.reserve_amount) * 100

    def get_trading_position(self, total_balance: Decimal) -> Decimal:
        """
        计算交易持仓（排除预留）

        Args:
            total_balance: 账户总余额

        Returns:
            交易持仓 = 总余额 - 当前预留
        """
        current_reserve = self.get_current_reserve()
        return total_balance - current_reserve

    # ========== 补充逻辑 ==========

    def need_replenish(self) -> bool:
        """
        检查是否需要补充

        Returns:
            bool: True=需要补充
        """
        if not self.replenish_enabled:
            return False

        # 检查健康度
        health_percent = self.get_reserve_health_percent() / 100
        if health_percent >= self.trigger_percent:
            return False

        # 检查补充间隔
        if self.last_replenish_time:
            elapsed = (datetime.now() -
                       self.last_replenish_time).total_seconds()
            if elapsed < self.min_replenish_interval:
                self.logger.debug(
                    f"⏳ 补充间隔不足: "
                    f"已过 {elapsed:.0f}秒/{self.min_replenish_interval}秒"
                )
                return False

        # 检查今日补充次数
        today_count = self._get_today_replenish_count()
        if today_count >= self.max_replenish_per_day:
            self.logger.warning(
                f"⚠️ 今日补充次数已达上限: "
                f"{today_count}/{self.max_replenish_per_day}"
            )
            return False

        return True

    def calculate_replenish_amount(self) -> Decimal:
        """
        计算需要补充的数量（已处理精度）

        Returns:
            补充数量
        """
        current = self.get_current_reserve()
        target = self.reserve_amount * self.target_percent
        shortage = target - current

        if shortage <= 0:
            return Decimal('0')

        # 🔥 处理精度：确保符合quantity_precision
        return self._round_to_precision(shortage, round_up=True)

    async def execute_replenish(
        self,
        replenish_amount: Decimal,
        current_price: Optional[Decimal] = None
    ) -> bool:
        """
        执行补充购买

        Args:
            replenish_amount: 补充数量
            current_price: 当前价格（可选）

        Returns:
            bool: 是否成功
        """
        try:
            # 🔥 修复导入路径：从正确的位置导入 OrderType 和 OrderSide
            from ....adapters.exchanges.models import OrderType, OrderSide

            # 🔥 检查补充金额是否达到交易所最小要求
            # 从配置读取最小订单价值，默认为 12 USDC（Hyperliquid 最小 10 USDC + 容错）
            auto_replenish_config = self.config.get('auto_replenish', {})
            min_order_value = Decimal(
                str(auto_replenish_config.get('min_order_value_usdc', 12.0)))

            if current_price is None or current_price <= 0:
                self.logger.error("❌ 无法补充：当前价格无效")
                return False

            replenish_value = replenish_amount * current_price
            if replenish_value < min_order_value:
                self.logger.warning(
                    f"⚠️ 补充金额过小，跳过补充: "
                    f"{replenish_amount} {self.base_currency} = ${replenish_value:.2f} USDC "
                    f"(最小要求: ${min_order_value} USDC)"
                )
                return False

            self.logger.info(
                f"🔄 执行预留补充: {replenish_amount} {self.base_currency} "
                f"(价值: ${replenish_value:.2f} USDC)"
            )

            # 使用市价单购买
            order = await self.exchange.create_order(
                symbol=self.symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                amount=replenish_amount,
                price=current_price
            )

            # 🔥 安全获取订单信息（处理 None 值）
            order_id = getattr(order, 'order_id', None) if order else None

            # 获取成交价格（市价单可能返回 None 或 0）
            order_price = getattr(order, 'price', None) if order and hasattr(
                order, 'price') else None
            if order_price is None or order_price == 0:
                # 如果订单没有价格，使用当前市场价格
                order_price = current_price if current_price else Decimal('0')

            # 记录补充历史
            self.replenish_history.append({
                'time': datetime.now(),
                'amount': float(replenish_amount),
                'order_id': order_id,
                'price': float(order_price),
                'health_before': float(self.get_reserve_health_percent()),
            })

            self.last_replenish_time = datetime.now()

            # 🔥 预留基数保持为配置值（固定值，不累加）
            # 补充购买只是增加账户余额，不修改 reserve_amount

            self.logger.info(
                f"✅ 补充成功: {replenish_amount} {self.base_currency}, "
                f"预留基数: {self.reserve_amount}（固定）"
            )

            return True

        except Exception as e:
            self.logger.error(f"❌ 补充失败: {e}", exc_info=True)
            return False

    # ========== 状态查询 ==========

    def get_status(self) -> Dict[str, Any]:
        """获取完整状态"""
        health_percent = self.get_reserve_health_percent()
        current_reserve = self.get_current_reserve()

        # 判断状态
        if health_percent < 30:
            status = 'critical'
            emoji = '🔴'
        elif health_percent < 50:
            status = 'warning'
            emoji = '🟡'
        else:
            status = 'healthy'
            emoji = '🟢'

        return {
            'status': status,
            'emoji': emoji,
            'health_percent': float(health_percent),
            'reserve_amount': float(self.reserve_amount),
            'current_reserve': float(current_reserve),
            'total_consumed': float(self.total_fee_consumed),
            'need_replenish': self.need_replenish(),
            'trades_count': len(self.fee_history),
            'replenish_count': len(self.replenish_history),
            'replenish_today': self._get_today_replenish_count(),
            'last_replenish': self.last_replenish_time.isoformat() if self.last_replenish_time else None
        }

    # ========== 私有方法 ==========

    def _round_to_precision(self, amount: Decimal, round_up: bool = False) -> Decimal:
        """
        根据quantity_precision处理精度

        Args:
            amount: 原始数量
            round_up: 是否向上取整

        Returns:
            处理后的数量
        """
        # 创建精度字符串，例如 "0.00001" for precision=5
        precision_str = f"0.{'0' * self.quantity_precision}"
        precision_decimal = Decimal(precision_str)

        if round_up:
            # 向上取整
            quantized = amount.quantize(precision_decimal, rounding=ROUND_DOWN)
            if quantized < amount:
                # 如果向下取整后小于原值，加一个最小单位
                quantized += Decimal(10) ** (-self.quantity_precision)
            return quantized
        else:
            # 标准四舍五入
            return amount.quantize(precision_decimal)

    def _get_today_replenish_count(self) -> int:
        """获取今天的补充次数"""
        today = datetime.now().date()
        return sum(
            1 for record in self.replenish_history
            if record['time'].date() == today
        )
