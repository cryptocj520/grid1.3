# 网格交易配置文件使用指南

## 📁 配置文件清单

本目录仅包含2个完整的配置文件，涵盖所有网格交易功能：

| 文件名 | 说明 | 默认模式 |
|--------|------|---------|
| `backpack_capital_protection_long.yaml` | 做多完整配置 | 价格移动网格 + 四重保护 |
| `backpack_capital_protection_short.yaml` | 做空完整配置 | 价格移动网格 + 四重保护 |

---

## 🎯 快速启动

### 做多网格
```bash
python run_grid_trading.py config/grid/backpack_capital_protection_long.yaml
```

### 做空网格
```bash
python run_grid_trading.py config/grid/backpack_capital_protection_short.yaml
```

---

## 🔧 配置文件功能说明

每个配置文件都包含：

### ✅ 当前启用的功能
- **价格移动网格**：自动跟随价格调整网格范围
- **剥头皮模式**：90%触发，快速止盈
- **本金保护模式**：40%触发，等待回本
- **止盈模式**：盈利0.5%自动止盈
- **价格锁定模式**：价格大幅脱离时冻结网格

### 📝 通过注释可启用的功能
- **固定范围网格**：手动指定价格上下限
- **马丁模式**：订单金额递增策略
- **参数调整示例**：各种优化建议

---

## 📚 如何切换网格模式

### 方式1：切换到固定范围网格

**编辑配置文件：**
```yaml
# 1. 注释掉价格移动网格类型
# grid_type: "follow_long"         # ❌ 注释掉

# 2. 启用固定范围网格类型
grid_type: "long"                  # ✅ 取消注释

# 3. 注释掉价格移动网格参数
# follow_grid_count: 200           # ❌ 注释掉
# follow_timeout: 300              # ❌ 注释掉
# follow_distance: 2               # ❌ 注释掉

# 4. 启用固定范围网格参数
lower_price: 1.90                  # ✅ 取消注释
upper_price: 2.30                  # ✅ 取消注释
```

---

### 方式2：启用马丁模式

**编辑配置文件：**
```yaml
# 1. 切换网格类型为马丁
grid_type: "martingale_long"       # ✅ 做多马丁
# grid_type: "martingale_short"    # ✅ 做空马丁

# 2. 启用马丁参数
martingale_increment: 0.5          # ✅ 取消注释，设置递增量
```

**马丁模式说明：**
- 第1格订单：5.0 ASTER
- 第2格订单：5.5 ASTER（5.0 + 0.5）
- 第3格订单：6.0 ASTER（5.5 + 0.5）
- 依此类推...

---

## 🎨 参数调整建议

### 降低仓位积累速度
```yaml
order_amount: 2.0           # 从 5.0 降到 2.0
grid_interval: 0.005        # 从 0.002 增大到 0.005
follow_grid_count: 100      # 从 200 减少到 100
```

### 提前触发保护机制
```yaml
scalping_trigger_percent: 80       # 从 90% 降到 80%
capital_protection_trigger_percent: 50  # 从 40% 提高到 50%
```

### 调整止盈策略
```yaml
take_profit_percentage: 0.01       # 从 0.005(0.5%) 提高到 0.01(1%)
scalping_take_profit_grids: 3      # 从 2格 增加到 3格
```

### 调整价格锁定阈值
```yaml
# 做多：应该设置在网格上限之上
price_lock_threshold: 2.5          # 假设网格上限 2.30

# 做空：应该设置在网格下限之下
price_lock_threshold: 1.8          # 假设网格下限 1.90
```

---

## 📊 功能组合方案

### 方案1：保守型（价格移动网格 + 全部保护）
```yaml
grid_type: "follow_long"
scalping_enabled: true
capital_protection_enabled: true
take_profit_enabled: true
price_lock_enabled: true
```
**适用场景：**新手、不确定行情方向、希望最大化保护

---

### 方案2：激进型（固定范围网格 + 马丁 + 剥头皮）
```yaml
grid_type: "martingale_long"
lower_price: 1.90
upper_price: 2.30
martingale_increment: 0.5
scalping_enabled: true
```
**适用场景：**震荡行情、有明确支撑阻力位、追求高收益

---

### 方案3：极简型（固定范围网格）
```yaml
grid_type: "long"
lower_price: 1.90
upper_price: 2.30
scalping_enabled: false           # 关闭所有保护
capital_protection_enabled: false
take_profit_enabled: false
price_lock_enabled: false
```
**适用场景：**老手、对行情有把握、追求纯网格收益

---

## 🛡️ 四重保护机制说明

### 1. 剥头皮模式（第一道防线）
- **触发条件：**买单/卖单成交90%
- **操作：**取消反向订单，挂止盈单
- **结果：**止盈成交后重置网格

### 2. 本金保护模式（第二道防线）
- **触发条件：**价格继续不利方向（40%）
- **操作：**等待抵押品回本
- **结果：**回本后平仓重置

### 3. 止盈模式（盈利保护）
- **触发条件：**盈利超过本金0.5%
- **操作：**平仓所有持仓
- **结果：**锁定利润，重置网格

### 4. 价格锁定模式（智能冻结）
- **触发条件：**价格大幅有利方向脱离
- **操作：**冻结网格，保留订单和持仓
- **结果：**价格回归后解除锁定

---

## ⚠️ 重要提示

1. **修改配置后必须重启程序才能生效**
2. **价格移动网格不需要设置 lower_price 和 upper_price**
3. **固定范围网格不需要设置 follow_grid_count 等参数**
4. **马丁模式会快速增加持仓，谨慎使用**
5. **价格锁定阈值应该设置在网格范围之外**

---

## 📖 更多信息

详细的功能说明、场景举例、参数计算请参考配置文件内的注释说明。

配置文件内包含了：
- ✅ 完整的场景举例（5个场景）
- ✅ 做多 vs 做空对比表
- ✅ 使用建议和优化方案
- ✅ 所有参数的详细说明

---

## 🎓 学习路径

**第1步：**使用默认配置，了解基本运行流程  
**第2步：**调整参数（order_amount、grid_interval等）  
**第3步：**尝试不同保护模式的开关组合  
**第4步：**切换网格类型（价格移动 vs 固定范围）  
**第5步：**高级功能（马丁模式、价格锁定）  

---

**祝交易顺利！** 🚀

