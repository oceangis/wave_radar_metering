# 两遍波浪分析（Iterative Refinement）技术文档

## 1. 背景与动机

### 1.1 问题

Work模式的SWAP质量控制（4σ/4δ）是纯数据驱动的，存在鸡生蛋问题：

- 统计量σ和δ本身会被噪声污染 → 阈值偏松 → 残留更多噪声 → H1/3偏高
- 不知道波高就无法设物理约束；但要算波高又需要先滤波

相比之下，meter模式利用已知的安装高度、紧凑IQR、固定跳变阈值等**先验知识**，实现了更高精度。

### 1.2 核心思路

两遍分析打破鸡生蛋循环：

1. **第一遍**（粗估）：用宽松SWAP 4σ/4δ滤波 → 快速过零法 → 得到粗估 H1/3、T1/3
2. **第二遍**（精算）：基于第一遍的H1/3、T1/3作为物理约束，**从原始数据重新滤波** → 完整分析

关键：第二遍回到原始数据重新开始，不在第一遍结果上叠加。

### 1.3 安全性分析

噪声只会使H1/3偏高（加能量），不会偏低。因此第一遍的H1/3是安全上界：
- 过估 → 约束稍松但仍远优于无约束 → 安全
- 不可能低估 → 不会误切真实波浪信号

## 2. 实现架构

### 2.1 处理流程

```
原始距离数据 (distances)
        │
        ├──── 第一遍 (_prepare_wave_data, pass2_constraints=None) ────┐
        │     │                                                        │
        │     │  [有先验知识 prior_knowledge.enabled=true]             │
        │     ├─ 绝对范围: array_height ± (潮差/2 + 最大波高/2)      │
        │     │  中心 = 安装高度（已知常量，不受噪声影响）              │
        │     │                                                        │
        │     │  [无先验知识]                                          │
        │     ├─ 绝对范围: median ± 3m（数据自适应）                   │
        │     │                                                        │
        │     ├─ 0-sigma: 恒定值检测                                   │
        │     ├─ 4-sigma: 幅度异常值                                   │
        │     ├─ 4-delta: 跳变异常值                                   │
        │     └─ → η → 快速过零法 (_quick_zero_crossing)              │
        │                                                               │
        │         粗估: H1/3, T1/3, median_dist ◄──────────────────────┘
        │              │
        │         质量检查:
        │         ├─ H1/3 > min_h13 (0.05m)?
        │         ├─ T1/3 > 0?
        │         └─ spike_ratio < 30%?
        │              │
        │         ┌────┴────┐
        │       通过      不通过
        │         │         │
        ├──── 第二遍 ─┘    使用第一遍结果（退回保守模式）
        │     (pass2_constraints={H13, T13, median_dist})
        │     ├─ 绝对范围: median ± 3×H1/3（收紧）+ 先验硬上限
        │     ├─ 0-sigma: 恒定值检测（不变）
        │     ├─ 3-sigma: 更紧的幅度阈值
        │     ├─ 波陡跳变: π·H/T × safety_factor（物理约束）
        │     └─ → η → 完整分析
        │
        └──→ 最终结果
```

### 2.2 三层知识叠加

两遍分析 + 先验知识形成三层约束递进：

| 层级 | 知识来源 | 约束内容 | 何时生效 |
|------|---------|---------|---------|
| 第一层：部署先验 | system_config | array_height为锚点，潮差+最大波高为范围 | Pass1绝对范围 |
| 第二层：粗估波浪参数 | Pass1过零法 | H1/3→收紧绝对范围，T1/3→波陡跳变阈值 | Pass2 |
| 第三层：SWAP统计 | 数据驱动 | 3σ/3δ在更干净数据上更准确 | Pass2 σ/δ测试 |

先验知识使Pass1就更干净 → Pass1的H1/3更准确 → Pass2的物理约束更紧 → 形成正向级联。

### 2.2 物理约束详解

#### 绝对范围（距离域）

| 模式 | 范围 | 计算方式 |
|------|------|---------|
| Pass1 work | median ± 3.0m | 宽松固定值 |
| Pass2 work | median ± 3×H1/3 | 基于波高自适应 |
| Meter | [0.3, array_height+0.3] | 基于已知安装高度 |

Pass2示例：H1/3=0.3m → ±0.9m（比Pass1的±3m收紧3.3倍）

#### 跳变阈值（波陡极限）

线性波理论：波面最大垂直速度 = π·H/T

```
max_velocity = π × H1/3 / T1/3
delta_threshold = steepness_factor × max_velocity / sample_rate
```

示例：H1/3=0.3m, T1/3=4s, factor=1.5
- max_velocity = π × 0.3 / 4 = 0.236 m/s
- threshold = 1.5 × 0.236 / 6 = 0.059 m/sample = 59mm

vs Pass1的4δ（统计值，被噪声污染时可能远大于此）

#### σ/δ乘数

| 参数 | Pass1 | Pass2 |
|------|-------|-------|
| σ乘数 | 4.0 | 3.0 |
| δ乘数 | 4.0 | 3.0（或被波陡约束替代） |

### 2.3 先验知识配置

`system_config.yaml` 新增 `prior_knowledge` 块：

```yaml
analysis:
  prior_knowledge:
    enabled: true
    max_wave_height: 6.0    # 部署区域历史最大波高(m)
    tidal_range: 3.0        # 潮差(m)
    min_wave_period: 1.0    # 最小合理波浪周期(s)
    max_wave_period: 25.0   # 最大合理波浪周期(s)
```

先验知识叠加到两遍分析中：
- Pass1：用潮差+最大波高替代固定3m余量
- Pass2：叠加物理硬上限 `array_height ± tidal_range/2 ± max_wave_height/2`

## 3. 配置参数

### 3.1 两遍分析参数

```yaml
analysis:
  two_pass:
    enabled: true                   # work模式启用两遍分析
    abs_range_multiplier: 3.0       # 绝对范围 = median ± multiplier × H1/3
    jump_use_steepness: true        # 用波陡极限替代统计δ
    jump_steepness_factor: 1.5      # 波陡安全系数
    r1_ref_multiplier: 1.0          # R1参考阈值 = multiplier × H1/3
    sigma_multiplier: 3.0           # 第二遍σ乘数
    delta_multiplier: 3.0           # 第二遍δ乘数
    min_h13: 0.05                   # H1/3下限保护(m)
    max_spike_ratio_pass1: 0.30     # 第一遍异常比例上限
```

### 3.2 参数选择依据

| 参数 | 默认值 | 依据 |
|------|-------|------|
| abs_range_multiplier=3.0 | 波面振幅≈H1/3/2，×3留6倍余量 |
| jump_steepness_factor=1.5 | 线性波理论上限的1.5倍，覆盖非线性效应 |
| sigma_multiplier=3.0 | 从4σ收紧到3σ，更贴近正态分布 |
| min_h13=0.05 | 50mm以下H1/3不可靠（接近雷达噪声底 |
| max_spike_ratio_pass1=0.30 | 超过30%异常说明数据质量太差 |

## 4. 边界情况处理

### 4.1 极平静海况（H1/3 < 0.05m）

第一遍H1/3低于`min_h13`，不启用第二遍收紧。退回标准SWAP 4σ/4δ。

### 4.2 第一遍数据质量差（spike_ratio > 30%）

说明原始数据存在系统性问题（设备故障、严重干扰等），不收紧，退回保守模式。

### 4.3 突发干扰（暴雨、鸟类等）

第一遍H1/3被高估 → 第二遍约束偏松，但仍优于无约束的Pass1。不会比不用两遍更差。

### 4.4 Meter模式

不启用两遍分析。Meter模式已有严格先验知识（安装高度、IQR×1.5、固定跳变阈值等），两遍分析是为了让work模式获得类似的物理约束能力。

## 5. 预期收益

### 5.1 收益最大场景

小波（H1/3 < 0.5m）：噪声幅度与信号量级可比，4σ的σ被噪声撑大，阈值失效。两遍后用物理约束替代被污染的统计量。

### 5.2 收益最小场景

大波（H1/3 > 1m）：信噪比本身就高，SWAP 4σ/4δ已足够。两遍分析不会让结果变差，但改善幅度有限。

### 5.3 量化对比（示例）

| 约束项 | Pass1 (H1/3=0.3m) | Pass2 | 收紧倍数 |
|-------|-------------------|-------|---------|
| 绝对范围 | ±3000mm | ±900mm | 3.3× |
| 跳变阈值 | 4δ≈200mm(被污染) | 59mm(波陡) | 3.4× |
| σ阈值 | 4σ | 3σ | 1.3× |

## 6. 计算开销

- 第一遍：只需基础SWAP QC + 快速过零法（无Welch谱、无DIWASP）
- 数据量：6Hz × 1200s = 7200点，内存~56KB
- 两遍总耗时增加 < 500ms（相比20分钟采集窗口可忽略）
- 适用于树莓派等嵌入式平台

## 7. 相关文件

| 文件 | 修改内容 |
|------|---------|
| `services/mqtt_analyzer.py` | `_quick_zero_crossing()`新方法、`_prepare_wave_data()`增加pass2_constraints参数、`analyze_window()`三种雷达模式均支持两遍 |
| `config/system_config.yaml` | 新增`two_pass`和`prior_knowledge`配置块 |

## 8. 参考

- SWAP: Mathematical description of the Standard Wave Analysis Package (Rijkswaterstaat, 1994)
- Iterative sigma-clipping: 天文学标准异常值剔除方法
- 波陡极限：线性波理论 max(dη/dt) = π·H/T
