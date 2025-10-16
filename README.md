# 马丁网格交易系统完整指南

> **版本**: 2.0  
> **更新日期**: 2025-01-16  
> **适用交易所**: Backpack

---

## 📖 目录

- [系统概述](#系统概述)
- [核心功能](#核心功能)
- [网格模式](#网格模式)
- [保护机制](#保护机制)
- [配置指南](#配置指南)
- [快速开始](#快速开始)
- [工具集](#工具集)
- [常见问题](#常见问题)

---

## 系统概述

### 什么是马丁网格？

马丁网格是一种**订单金额递增**的网格交易策略，相比普通网格的固定金额，马丁网格会根据价格偏离程度逐步增加订单金额，适合有明确趋势预期的交易场景。

### 核心特点

| 特性 | 说明 |
|------|------|
| 🎯 **金额递增** | 订单金额随网格递增，摊平成本更快 |
| 🛡️ **四重保护** | 剥头皮、本金保护、止盈、价格锁定 |
| 📊 **健康检查** | 自动修复订单和持仓异常 |
| 🔄 **自动重置** | 触发保护后自动重置网格 |
| 💻 **实时监控** | 终端UI实时显示运行状态 |

### 与普通网格的区别

```
普通网格：
Grid 1: 0.01 BNB
Grid 2: 0.01 BNB    ← 固定金额
Grid 3: 0.01 BNB
...

马丁网格：
Grid 1: 0.010 BNB   ← 基础金额
Grid 2: 0.015 BNB   ← 递增 0.005
Grid 3: 0.020 BNB   ← 递增 0.005
...
Grid 100: 0.505 BNB ← 金额显著增加
```

---

## 核心功能

### 1. 智能订单管理

- **自动下单**：根据价格区间自动创建网格订单
- **反手挂单**：订单成交后自动反向挂单（可配置间隔）
- **订单追踪**：实时跟踪订单状态和成交情况

### 2. 订单健康检查

每隔一定时间（默认5分钟）自动检查：

| 检查项 | 说明 | 修复方式 |
|--------|------|----------|
| 订单数量 | 是否等于网格数量 | 补齐缺失订单，取消多余订单 |
| 订单价格 | 是否与网格价格匹配 | 取消错误订单，重新下单 |
| 持仓准确性 | 实际持仓是否与预期一致 | 根据差额下单调整 |

**持仓计算方式（马丁网格）**：
```python
# 对于已成交的网格，考虑交易所精度格式化
expected_position = Σ(格式化后的订单金额)

# 格式化示例（quantity_precision=3）：
Grid 1: 0.010 → 0.010 (无需调整)
Grid 2: 0.0145 → 0.015 (四舍五入到3位小数)
Grid 3: 0.019 → 0.019 (无需调整)
```

### 3. 仓位跟踪

- **实时更新**：通过WebSocket实时同步持仓变化
- **成本计算**：自动计算平均持仓成本
- **盈亏统计**：实时显示未实现盈亏

---

## 网格模式

### 1. 价格移动网格（推荐）

**类型**: `follow_long` / `follow_short`

**特点**：
- ✅ 网格随价格移动自动调整
- ✅ 适合趋势行情
- ✅ 可配置脱离检测
- ✅ 支持价格锁定功能

**配置参数**：
```yaml
grid_type: "follow_long"
follow_grid_count: 200      # 网格数量
follow_timeout: 300         # 脱离超时(秒)
follow_distance: 2          # 脱离距离(格)
```

**工作原理**：
```
初始：价格 $1000, 网格范围 $920-$1180 (200格×$1.3)
   ↓
价格上涨到 $1050
   ↓
网格自动调整：$970-$1230 (跟随价格上移)
```

### 2. 固定范围网格

**类型**: `long` / `short`

**特点**：
- ✅ 网格范围固定不变
- ✅ 适合震荡行情
- ⚠️ 价格脱离范围后停止工作

**配置参数**：
```yaml
grid_type: "long"
lower_price: 900            # 下限价格
upper_price: 1100           # 上限价格
grid_interval: 1.3          # 网格间隔
```

### 3. 马丁模式

**类型**: `martingale_long` / `martingale_short`

**特点**：
- 🔥 订单金额递增
- ⚠️ 风险较高，需充足资金
- ✅ 可与固定或移动网格结合

**配置参数**：
```yaml
grid_type: "martingale_long"
order_amount: 0.01           # 基础金额
martingale_increment: 0.0005 # 递增金额
```

**金额计算**：
```
Grid N 的订单金额 = order_amount + (N-1) × martingale_increment

示例（order_amount=0.01, increment=0.0005）：
Grid 1:   0.010 BNB
Grid 10:  0.0145 BNB
Grid 50:  0.0345 BNB
Grid 100: 0.0595 BNB
Grid 200: 0.1095 BNB  ← 是第1格的 10.95 倍
```

---

## 保护机制

系统提供**四重保护机制**，按触发条件自动激活：

### 1️⃣ 剥头皮模式 (Scalping Mode)

**目标**: 快速止盈，锁定利润

**触发条件**：
```
价格下跌到触发点 = 网格顶部 - (网格总数 × 触发百分比)

示例（200格，触发10%）：
触发点 = Grid 200 - (200 × 10%) = Grid 180
```

**执行动作**：
1. 取消所有反向挂单（做多时取消卖单）
2. 保留主方向挂单继续建仓
3. 计算止盈价格（基于已实现盈亏）
4. 挂出止盈订单
5. 止盈成交后重置网格

**止盈价格计算**（新算法）：
```python
# 1. 计算已实现盈亏
realized_pnl = current_collateral - initial_capital

# 2. 计算回本所需价格变动
required_move = -realized_pnl / current_position

# 3. 计算回本价格
breakeven_price = current_price + required_move

# 4. 止盈价格 = 回本价格 + N个网格
take_profit_price = breakeven_price + (N × grid_interval)
```

**配置示例**：
```yaml
scalping_enabled: true
scalping_trigger_percent: 10      # 触发点：下跌10%
scalping_take_profit_grids: 2     # 止盈：回本价+2格
```

### 2️⃣ 本金保护模式 (Capital Protection)

**目标**: 保护本金，避免亏损扩大

**触发条件**：
```
价格下跌到触发点 = 网格顶部 - (网格总数 × 触发百分比)

示例（200格，触发40%）：
触发点 = Grid 200 - (200 × 40%) = Grid 120
```

**执行动作**：
1. 暂停新订单创建
2. 等待抵押品余额回本
3. 回本后重置网格并重启

**配置示例**：
```yaml
capital_protection_enabled: true
capital_protection_trigger_percent: 40  # 触发点：下跌40%
```

### 3️⃣ 止盈模式 (Take Profit)

**目标**: 盈利达标后锁定收益

**触发条件**：
```
盈利比例 = (当前抵押品 - 初始本金) / 初始本金
触发：盈利比例 >= take_profit_percentage
```

**执行动作**：
1. 平掉所有持仓
2. 取消所有挂单
3. 重置网格并重启

**配置示例**：
```yaml
take_profit_enabled: true
take_profit_percentage: 0.002  # 盈利0.2%时止盈
```

### 4️⃣ 价格锁定模式 (Price Lock)

**目标**: 价格过高时冻结网格，等待回归

**触发条件**：
```
做多：价格向上脱离网格范围 且 价格 >= 锁定阈值
做空：价格向下脱离网格范围 且 价格 <= 锁定阈值
```

**执行动作**：
1. 冻结网格（不平仓）
2. 保留订单和持仓
3. 等待价格回到网格范围
4. 价格回归后自动解锁

**配置示例**：
```yaml
price_lock_enabled: true
price_lock_threshold: 1200           # 锁定价格阈值
price_lock_start_at_threshold: false # 启动时是否使用阈值
```

### 保护机制触发优先级

多个保护机制可能同时启用，但**只会触发最先满足条件的那个**：

```
价格下跌过程（做多）：
    ↓
条件1: 本金保护 (40%)  → 先触发 ✅
    ↓
条件2: 剥头皮 (10%)    → 不会触发 ❌

⚠️ 配置时需注意：
- 本金保护触发点应 > 剥头皮触发点
- 建议：本金保护 40-50%，剥头皮 10-20%
```

---

## 配置指南

### 必需配置

```yaml
grid_system:
  # 基础配置
  exchange: "backpack"
  symbol: "BNB_USDC_PERP"
  
  # 网格类型（三选一）
  grid_type: "follow_long"
  
  # 价格移动网格参数
  follow_grid_count: 200
  grid_interval: 1.3
  order_amount: 0.01
  
  # 交易精度（重要！）
  quantity_precision: 3
```

### 可选配置

```yaml
  # 马丁模式
  martingale_increment: 0.0005
  
  # 剥头皮
  scalping_enabled: true
  scalping_trigger_percent: 10
  scalping_take_profit_grids: 2
  
  # 本金保护
  capital_protection_enabled: true
  capital_protection_trigger_percent: 40
  
  # 止盈
  take_profit_enabled: true
  take_profit_percentage: 0.002
  
  # 价格锁定
  price_lock_enabled: true
  price_lock_threshold: 1200
  
  # 高级配置
  reverse_order_grid_distance: 1
  order_health_check_interval: 300
```

### 配置模板

系统提供多个预配置模板：

| 文件 | 说明 |
|------|------|
| `backpack_capital_protection_long.yaml` | 价格移动做多 + 四重保护 |
| `backpack_fixed_range_long.yaml` | 固定范围做多 |
| `backpack_simple_follow_long.yaml` | 简单价格移动做多 |

### 参数调优建议

#### 1. 网格间隔 (grid_interval)

```yaml
grid_interval: 1.3  # 每格 $1.3

# 选择建议：
# - 小间隔 (0.5-1): 成交频繁，利润薄，适合震荡
# - 中间隔 (1-3):   平衡成交和利润，适合多数情况
# - 大间隔 (3-10):  成交少，利润厚，适合趋势
```

#### 2. 订单金额 (order_amount)

```yaml
order_amount: 0.01

# 选择建议：
# - 根据总资金量确定
# - 确保能承受所有网格成交
# - 使用工具 tools/martin_grid_calculator.py 计算
```

#### 3. 马丁递增 (martingale_increment)

```yaml
martingale_increment: 0.0005

# 选择建议：
# - 递增越大，后期资金压力越大
# - 建议倍数关系：5-10倍（Grid 200 / Grid 1）
# - 使用计算器工具找到合适参数
```

#### 4. 数量精度 (quantity_precision)

```yaml
quantity_precision: 3

# ⚠️ 必须与交易所实际精度一致！
# 常见代币精度：
# - BTC: 8位 (0.00000001)
# - ETH: 6位 (0.000001)
# - BNB: 3位 (0.001)
# - SOL: 4位 (0.0001)
```

---

## 快速开始

### 1. 环境准备

```bash
# 克隆项目
git clone <repository>
cd trading_strategy_sys_hype_xpl2_hype_bnb

# 安装依赖
pip install -r requirements.txt
```

### 2. 配置交易所API

编辑 `config/exchanges/backpack.yaml`:

```yaml
api_credentials:
  api_key: "YOUR_API_KEY"
  api_secret: "YOUR_API_SECRET"
```

### 3. 配置网格策略

复制并编辑配置文件：

```bash
cp config/grid/backpack_capital_protection_long.yaml config/grid/my_grid.yaml
```

编辑 `my_grid.yaml`，调整参数：
- 交易对 (`symbol`)
- 网格间隔 (`grid_interval`)
- 订单金额 (`order_amount`)
- 保护机制参数

### 4. 启动系统

```bash
# 方式1：使用启动脚本
python run_grid_trading.py

# 方式2：指定配置文件
python run_grid_trading.py --config config/grid/my_grid.yaml
```

### 5. 监控运行

系统启动后会显示实时终端UI：

```
╔═══════════════════════════════════════════════════╗
║            网格交易系统状态监控                    ║
╠═══════════════════════════════════════════════════╣
║ 交易对: BNB_USDC_PERP                             ║
║ 模式: 价格移动网格 (做多)                         ║
║ 网格数量: 200                                     ║
║ 网格间隔: $1.3                                    ║
╠═══════════════════════════════════════════════════╣
║ 当前价格: $1,052.30                               ║
║ 网格范围: $922.31 ~ $1,182.31                     ║
║ 持仓数量: 2.450 BNB                               ║
║ 持仓成本: $1,045.20                               ║
║ 未实现盈亏: +$17.40 (+0.6%)                       ║
╠═══════════════════════════════════════════════════╣
║ 马丁模式: ✅ 启用                                 ║
║ 剥头皮模式: 🔴 未触发 (触发点: Grid 180)          ║
║ 本金保护: 🔴 未触发 (触发点: Grid 120)            ║
║ 止盈模式: 🔴 未触发 (目标: +0.2%)                 ║
║ 价格锁定: 🔴 未触发 (阈值: $1,200)                ║
╚═══════════════════════════════════════════════════╝
```

### 6. 停止系统

```bash
# Ctrl+C 优雅停止
# 系统会自动：
# 1. 停止接收新订单
# 2. 保存当前状态
# 3. 关闭连接
```

---

## 工具集

### 1. 马丁网格计算器

**用途**: 计算马丁网格的资金需求和分布

**位置**: `tools/martin_grid_calculator.py`

**使用方式**：

```bash
# 交互式模式（推荐）
python tools/martin_grid_calculator.py

# 命令行模式
python tools/martin_grid_calculator.py 0.01 0.0005
```

**输出示例**：

```
📊 马丁网格计算结果
================================================================================

【核心数据】
💰 总投入金额:        $      12,000.50
📊 平均每格金额:      $         60.002
🎯 Grid 1 金额:       $          0.010
🎯 Grid 200 金额:     $          0.110
📈 倍数关系:                     11.00x

【资金消耗进度】
💵 用完 50% 资金:     Grid 138 (还剩 62 个网格)
💵 用完 75% 资金:     Grid 173 (还剩 27 个网格)
💵 用完 90% 资金:     Grid 189 (还剩 11 个网格)

【后期网格资金占比】
🔸 最后 10 个网格:    占总资金的   9.1%
🔸 最后 20 个网格:    占总资金的  17.8%
🔸 最后 50 个网格:    占总资金的  41.2%
```

**使用技巧**：

1. **确定目标**：
   - 总资金: $10,000
   - 倍数关系: 10x

2. **调整参数**：
   - 先确定 `martingale_increment / order_amount` 的比值
   - 再调整 `order_amount` 使总金额达到目标

3. **评估风险**：
   - 查看"资金消耗进度"
   - 确保能承受价格跌到 Grid 100 左右

### 2. 日志查看

**位置**: `logs/`

```bash
# 实时查看主日志
tail -f logs/core.services.grid.coordinator.grid_coordinator.log

# 查看健康检查日志
tail -f logs/core.services.grid.implementations.order_health_checker.log

# 查看剥头皮日志
tail -f logs/core.services.grid.scalping.scalping_manager.log
```

### 3. 配置验证

```bash
# 验证配置文件语法
python -c "import yaml; yaml.safe_load(open('config/grid/my_grid.yaml'))"
```

---

## 常见问题

### Q1: 健康检查报告持仓异常，但实际持仓是对的？

**原因**: `quantity_precision` 配置不正确

**解决方案**：
```yaml
# 确保 quantity_precision 与交易所实际精度一致
quantity_precision: 3  # BNB 是 3 位小数
```

### Q2: 订单金额不符合交易所要求？

**原因**: 订单金额小于交易所最小交易量或精度不对

**解决方案**：
1. 增加 `order_amount`
2. 确保格式化后的金额 >= 交易所最小值
3. 使用计算器工具验证

### Q3: 剥头皮模式没有触发？

**可能原因**：
1. 本金保护先触发了（触发点更高）
2. 价格还没跌到触发点

**解决方案**：
```yaml
# 调整触发顺序
scalping_trigger_percent: 10        # 剥头皮：10%
capital_protection_trigger_percent: 50  # 本金保护：50%
# 确保本金保护触发点 > 剥头皮触发点
```

### Q4: 重置后订单重复，触发交易所限制？

**原因**: 旧订单未完全取消就创建新订单

**解决方案**: 
系统已修复此问题，重置时会：
1. 取消所有订单
2. 验证订单已取消（最多重试3次）
3. 确认无订单后才创建新订单

### Q5: 剥头皮止盈价格不准确？

**原因**: 旧版本使用 `average_cost_price`，未考虑已实现盈亏

**解决方案**: 
系统已更新为新算法，基于 `initial_capital` 和 `current_collateral` 计算回本价格，更准确地反映总体盈亏。

### Q6: 如何计算总资金需求？

**方法**：

```bash
# 使用计算器工具
python tools/martin_grid_calculator.py 0.01 0.0005

# 查看 "总投入金额"，即为所需资金
```

### Q7: 网格不跟随价格移动？

**检查项**：
```yaml
# 确保使用价格移动网格
grid_type: "follow_long"  # 或 "follow_short"

# 而不是固定范围网格
# grid_type: "long"  ← 这个不会移动
```

### Q8: 如何调整利润？

**方法1: 增加反手挂单间隔**
```yaml
reverse_order_grid_distance: 2  # 从1格改为2格
# 买单@$1000成交 → 卖单@$1002.6 (增加$2.6利润)
```

**方法2: 增加网格间隔**
```yaml
grid_interval: 2.0  # 从1.3改为2.0
# 每次成交利润更高，但成交次数减少
```

---

## 附录

### A. 术语表

| 术语 | 英文 | 说明 |
|------|------|------|
| 网格 | Grid | 价格区间内的订单点位 |
| 反手挂单 | Reverse Order | 订单成交后反向挂单 |
| 剥头皮 | Scalping | 快速建仓后止盈的策略 |
| 本金保护 | Capital Protection | 防止本金亏损的机制 |
| 价格锁定 | Price Lock | 价格过高时冻结网格 |
| 健康检查 | Health Check | 定期检查并修复异常 |

### B. 配置文件位置

```
project_root/
├── config/
│   ├── exchanges/
│   │   └── backpack.yaml          # 交易所配置
│   └── grid/
│       ├── backpack_capital_protection_long.yaml
│       ├── backpack_fixed_range_long.yaml
│       └── backpack_simple_follow_long.yaml
├── tools/
│   └── martin_grid_calculator.py  # 计算器工具
└── logs/
    └── *.log                      # 日志文件
```

### C. 日志级别

```python
# 日志配置位置: config/logging.yaml

# 级别说明：
DEBUG:   详细调试信息
INFO:    常规运行信息（推荐）
WARNING: 警告信息
ERROR:   错误信息
```

### D. 联系和支持

- **文档位置**: `docs/`
- **工具脚本**: `tools/`
- **配置示例**: `config/grid/`
- **测试脚本**: `test/`

---

**最后更新**: 2025-01-16  
**文档版本**: v2.0  
**系统版本**: v2.5.0

