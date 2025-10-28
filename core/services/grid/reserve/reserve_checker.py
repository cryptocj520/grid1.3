"""
启动检查器

职责：
1. 启动前检查预留BTC是否充足
2. 自动购买不足的预留
3. 提供用户交互和提示

在系统启动前调用，仅对现货生效。
"""

from decimal import Decimal
from typing import Optional

from ....adapters.exchanges.interface import ExchangeType
from ....logging import get_logger

logger = get_logger(__name__)


async def check_spot_reserve_on_startup(
    config,
    exchange_adapter,
    reserve_manager
) -> bool:
    """
    启动时检查预留（在挂单前调用）

    Args:
        config: GridConfig配置
        exchange_adapter: 交易所适配器
        reserve_manager: SpotReserveManager实例（如果是现货）

    Returns:
        bool: True=检查通过, False=需终止启动
    """
    # 仅对现货生效
    if exchange_adapter.config.exchange_type != ExchangeType.SPOT:
        return True

    if reserve_manager is None:
        logger.warning("⚠️ 现货模式未启用预留管理")
        return True

    logger.info("="*80)
    logger.info("🔍 启动前预留BTC检查")
    logger.info("="*80)

    # 获取当前余额
    current_balance = await _get_base_currency_balance(
        exchange_adapter,
        reserve_manager.base_currency
    )

    required_reserve = reserve_manager.reserve_amount

    logger.info(f"  币种: {reserve_manager.base_currency}")
    logger.info(f"  当前余额: {current_balance}")
    logger.info(f"  需要预留: {required_reserve}")

    if current_balance >= required_reserve:
        logger.info("✅ 预留充足，检查通过")
        # 🔥 将账户余额作为实际预留基数（而不是配置值）
        # 这样可以充分利用账户的残留BTC，无需补充
        if current_balance > required_reserve:
            reserve_manager.update_reserve_amount(current_balance)
            logger.info(
                f"   预留基数已更新: {required_reserve} → {current_balance} {reserve_manager.base_currency}"
            )
            logger.info(
                f"   原因: 账户余额大于配置预留，将余额作为实际预留，无需补充"
            )
        else:
            logger.info(
                f"   预留基数: {reserve_manager.reserve_amount} {reserve_manager.base_currency}"
            )
        logger.info("="*80)
        return True

    # 预留不足
    shortage = required_reserve - current_balance
    logger.warning(f"⚠️ 预留不足: 缺少 {shortage} {reserve_manager.base_currency}")

    # 🔥 容错机制：如果差额小于一个基础网格数量，视为在手续费误差范围内
    tolerance_amount = config.order_amount  # 基础网格数量作为容错阈值
    if shortage < tolerance_amount:
        logger.warning(
            f"⚠️ 预留不足 {shortage} {reserve_manager.base_currency}，"
            f"但小于一个基础网格数量（{tolerance_amount}），视为手续费误差范围内"
        )
        logger.info("✅ 容错检查通过，继续启动")
        logger.info(
            f"   预留基数: {reserve_manager.reserve_amount} {reserve_manager.base_currency}（固定）")
        logger.info("="*80)
        return True

    # 检查是否启用自动购买
    startup_config = reserve_manager.config.get('startup_check', {})
    auto_buy = startup_config.get('auto_buy_on_startup', True)

    if not auto_buy:
        logger.error("❌ 自动购买已禁用")
        logger.info(f"💡 请手动购买至少 {shortage} {reserve_manager.base_currency}")
        logger.info("="*80)
        return False

    # 执行自动购买
    success = await auto_buy_reserve_if_needed(
        exchange_adapter,
        reserve_manager,
        shortage
    )

    # 检查失败时的处理
    if not success:
        continue_on_failure = startup_config.get('continue_on_failure', False)
        if continue_on_failure:
            logger.warning("⚠️ 购买失败，但将继续启动（可能会出现交易错误）")
            logger.info("="*80)
            return True
        else:
            logger.error("🛑 购买失败，启动终止")
            logger.info("="*80)
            return False

    logger.info("="*80)
    return True


async def auto_buy_reserve_if_needed(
    exchange_adapter,
    reserve_manager,
    shortage: Decimal
) -> bool:
    """
    自动购买不足的预留BTC

    Args:
        exchange_adapter: 交易所适配器
        reserve_manager: SpotReserveManager实例
        shortage: 缺少的数量

    Returns:
        bool: 是否购买成功
    """
    try:
        logger.info("🔄 开始自动购买预留BTC...")

        # 获取当前市场价格
        ticker = await exchange_adapter.get_ticker(reserve_manager.symbol)
        current_price = Decimal(str(ticker.last))

        # 计算购买数量（确保足够）
        buy_amount = reserve_manager._round_to_precision(
            shortage, round_up=True)
        estimated_cost = buy_amount * current_price

        logger.info(f"   当前价格: ${current_price}")
        logger.info(f"   购买数量: {buy_amount} {reserve_manager.base_currency}")
        logger.info(
            f"   预估成本: ${estimated_cost} {reserve_manager.quote_currency}")

        # 执行购买
        success = await reserve_manager.execute_replenish(buy_amount, current_price)

        if success:
            logger.info("✅ 自动购买成功")
            return True
        else:
            logger.error("❌ 自动购买失败")
            return False

    except Exception as e:
        logger.error(f"❌ 购买失败: {e}", exc_info=True)
        return False


async def _get_base_currency_balance(exchange_adapter, currency: str) -> Decimal:
    """
    获取指定币种余额（总余额）

    🔥 重要：启动检查时应该查询总余额(total)，而不是可用余额(free)
    因为：
    - 预留BTC 在账户总余额中
    - 持仓BTC 也在账户总余额中
    - 可用余额(free) 会被挂单冻结，不能准确反映账户实际持有的BTC
    """
    try:
        balances = await exchange_adapter.get_balances()

        for balance in balances:
            if balance.currency.upper() == currency.upper():
                # 🔥 修复：返回总余额，不是可用余额
                return balance.total

        return Decimal('0')
    except Exception as e:
        logger.error(f"获取余额失败: {e}")
        return Decimal('0')
