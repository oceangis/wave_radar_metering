# 串口通信协议文档 V2.0

## 1. 物理层参数

| 参数 | 值 |
|------|-----|
| 接口 | RS232 |
| 波特率 | 115200 bps |
| 数据位 | 8 |
| 停止位 | 1 |
| 校验位 | 无 (None) |
| 流控 | 无 |
| 编码 | UTF-8 |
| 行结束符 | `\r\n` |

---

## 2. 通信概览

```
Pi 上电
  │
  ├─→ [自动] 发送当前系统配置（type: "config"）
  │
  ├─→ [持续] 实时数据流（6Hz，type: "stream"）
  │
  └─→ [响应] PC 发 JSON 命令 → Pi 返回 JSON 结果
```

- Pi 上电后**自动**向串口推送一次当前配置，PC 无需主动查询
- 实时数据流持续输出；执行分析命令期间自动暂停，结果返回后恢复
- 双方均使用 **JSON 格式**，每条消息单独一行，以 `\r\n` 结尾

---

## 3. 数据类型说明

### 3.1 PC → Pi（发送）

所有命令均为 **JSON 对象**，以 `\r\n` 结尾，UTF-8 编码。

```json
{"cmd": "命令名", ...附加参数}
```

### 3.2 Pi → PC（接收）

所有输出均为 **JSON 对象**，以 `\r\n` 结尾，UTF-8 编码。通过 `type` 字段区分消息类型：

| `type` 值 | 触发条件 |
|-----------|----------|
| `"stream"` | 持续 6Hz，雷达测距实时数据 |
| `"config"` | 上电自动 / 响应 `CONFIG_GET` |
| `"tide"` | 响应 `METER` / `WORK`，潮位先到先发 |
| `"wave"` | 响应 `METER` / `WORK`，波浪分析完成后发 |
| `"status"` | 响应 `STATUS` |
| `"ack"` | 响应 `CONFIG_SET` 成功 |
| `"error"` | 命令出错 / 分析超时 |

---

## 4. PC → Pi 命令

### 4.1 分析命令

#### METER — 计量模式波浪+潮位分析

```json
{"cmd": "METER", "repeat": false}
```

触发计量模式分析（使用 `meter_window` 配置的窗口时长），同时启动波浪分析和潮位分析。
**返回两条独立 JSON**：潮位结果（`type: "tide"`）先到先发，波浪结果（`type: "wave"`）随后发送。
最长等待时间：`meter_window + 60` 秒。

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `repeat` | bool | `false` | `false`=单次分析；`true`=连续滑动窗口，每 `meter_window` 秒重复，直到 `STOP` |

#### WORK — 工作模式波浪+潮位分析

```json
{"cmd": "WORK", "repeat": false}
```

触发工作模式分析（使用 `work_window` 配置的窗口时长），同时启动波浪分析和潮位分析。
**返回两条独立 JSON**：潮位结果（`type: "tide"`）先到先发，波浪结果（`type: "wave"`）随后发送。
最长等待时间：`work_window + 60` 秒。

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `repeat` | bool | `false` | `false`=单次分析；`true`=连续滑动窗口，每 `work_window` 秒重复，直到 `STOP` |

#### STOP — 停止分析

```json
{"cmd": "STOP"}
```

立即停止当前待执行或正在等待的波浪分析和潮位分析，清除调度计划，进度条归零。

#### STATUS — 查询系统状态

```json
{"cmd": "STATUS"}
```

即时返回当前在线雷达数量。

### 4.2 配置命令

#### CONFIG_GET — 查询当前配置

```json
{"cmd": "CONFIG_GET"}
```

即时返回当前所有可配置参数。

#### CONFIG_SET — 批量设置配置

```json
{
  "cmd": "CONFIG_SET",
  "height": 10.5,
  "heading": 243.0,
  "meter_window": 300,
  "work_window": 1200,
  "interval": 300
}
```

支持一次发送多个参数，只需包含需要修改的字段，未包含的字段保持不变。所有修改即时生效并永久保存到配置文件。

**可配置参数：**

| 字段名 | 范围 | 单位 | 说明 |
|--------|------|------|------|
| `height` | 0.5 ~ 200.0 | m | 雷达阵列距基准面的安装高度 |
| `heading` | 0.0 ~ 359.9 | deg | 阵列正方向相对正北的角度（顺时针）|
| `meter_window` | 60 ~ 3600 | s | 计量模式（METER 命令）数据窗口时长 |
| `work_window` | 60 ~ 7200 | s | 工作模式（WORK 命令）数据窗口时长 |
| `interval` | 30 ~ 3600 | s | 自动分析滚动窗口间隔 |

---

## 5. Pi → PC 输出

### 5.1 上电配置推送（type: "config"）

系统启动后自动发送一次，也是 `CONFIG_GET` 和 `CONFIG_SET` 成功后的响应。

```json
{
  "type": "config",
  "height": 10.0,
  "heading": 62.0,
  "sample_rate": 6,
  "meter_window": 300,
  "work_window": 1200,
  "interval": 300,
  "tide_window": 300
}
```

| 字段 | 单位 | 说明 |
|------|------|------|
| `height` | m | 雷达阵列安装高度 |
| `heading` | deg | 阵列安装方向（正北=0°，顺时针）|
| `sample_rate` | Hz | 采样率（只读，需从 Web 界面修改）|
| `meter_window` | s | 计量模式窗口时长 |
| `work_window` | s | 工作模式窗口时长 |
| `interval` | s | 自动分析滚动窗口间隔 |
| `tide_window` | s | 潮位分析时长（只读，需从 Web 界面修改）|

### 5.2 实时数据流（type: "stream"，6Hz 持续）

```json
{"type": "stream", "time": "12:30:25.166", "r1": 5.123, "r2": 5.234, "r3": 5.345, "online": 3}
{"type": "stream", "time": "12:30:25.333", "r1": 5.124, "r2": null,  "r3": 5.341, "online": 2}
```

| 字段 | 说明 |
|------|------|
| `time` | 本地时间（HH:MM:SS.mmm，精确到毫秒）|
| `r1` / `r2` / `r3` | 雷达测距值（米）；雷达离线时为 `null` |
| `online` | 当前在线雷达数量（0 ~ 3）|

> 测距值为雷达到海面的原始距离，**非水位**。执行分析命令期间数据流暂停，结果输出后自动恢复。

### 5.3 潮位结果（type: "tide"）

响应 `METER` 或 `WORK` 命令，潮位分析完成后**立即发送**（通常几秒内），不等待波浪分析。

```json
{"type": "tide", "mode": "work", "time": "2026-03-02 12:15:05", "tide_level": 2.345}
```

| 字段 | 单位 | 说明 |
|------|------|------|
| `mode` | — | 触发模式（`"meter"` 或 `"work"`）|
| `time` | — | 潮位分析完成时间（本地时间）|
| `tide_level` | m | 窗口内均值水位（已去除波浪分量）；分析失败时为 `null` |

### 5.4 波浪分析结果（type: "wave"）

响应 `METER` 或 `WORK` 命令，波浪分析完成后发送（耗时取决于窗口时长）。

```json
{
  "type": "wave",
  "mode": "meter",
  "window": 300,
  "time": "2026-03-02 12:35:01",
  "Hm0": 1.234,  "Tp": 8.50,  "Tz": 6.20,  "Te": 7.10,  "Tm01": 6.80,
  "peak_frequency": 0.1176, "fm": 0.1176, "fz": 0.1613, "fe": 0.1408,
  "df": 0.0033, "f_min": 0.04, "f_max": 1.0, "Nf": 291, "epsilon_0": 0.6512,
  "m_minus1": 0.012345, "m0": 0.009521, "m1": 0.001234, "m2": 0.000456, "m4": 0.000012,
  "Hmax": 2.103, "Hs": 1.156, "H1_10": 1.876, "Hmean": 0.823,
  "Tmax": 10.20, "T1_10": 9.50, "Ts": 8.30, "Tmean": 6.20,
  "wave_count": 35,
  "direction": 243.0, "mean_direction": 238.5, "directional_spread": 28.3, "direction_at_peak": 241.0,
  "Hm0_r1": 1.234, "Hm0_r2": 1.198, "Hm0_r3": 1.251,
  "radar_count": 3
}
```

**基本信息**

| 字段 | 单位 | 说明 |
|------|------|------|
| `mode` | — | 分析模式（`"meter"` 或 `"work"`）|
| `window` | s | 本次使用的数据窗口时长 |
| `time` | — | 分析完成时间（本地时间）|

**频域参数（谱分析）**

| 字段 | 单位 | 说明 |
|------|------|------|
| `Hm0` | m | 有效波高（谱矩法，$4\sqrt{m_0}$）|
| `Tp` | s | 峰值周期（谱峰对应周期）|
| `Tz` | s | 零交叉平均周期（$\sqrt{m_0/m_2}$）|
| `Te` | s | 能量周期（$m_{-1}/m_0$）|
| `Tm01` | s | 平均周期（$m_0/m_1$）|
| `peak_frequency` | Hz | 谱峰频率 |
| `fm` | Hz | 峰值频率 |
| `fz` | Hz | 零交叉频率 |
| `fe` | Hz | 能量频率 |
| `df` | Hz | 频率分辨率 |
| `f_min` | Hz | 分析频率下限 |
| `f_max` | Hz | 分析频率上限 |
| `Nf` | 个 | 频率点数 |
| `epsilon_0` | — | 谱宽参数 |

**谱矩**

| 字段 | 单位 | 说明 |
|------|------|------|
| `m_minus1` | m²·s | 负一阶谱矩 |
| `m0` | m² | 零阶谱矩（方差）|
| `m1` | m²/s | 一阶谱矩 |
| `m2` | m²/s² | 二阶谱矩 |
| `m4` | m²/s⁴ | 四阶谱矩 |

**时域参数（零交叉法）**

| 字段 | 单位 | 说明 |
|------|------|------|
| `Hmax` | m | 最大波高 |
| `Hs` | m | 1/3 大波高 |
| `H1_10` | m | 1/10 大波高 |
| `Hmean` | m | 平均波高 |
| `Tmax` | s | 最大波周期 |
| `T1_10` | s | 1/10 大波周期 |
| `Ts` | s | 1/3 大波周期 |
| `Tmean` | s | 平均周期 |
| `wave_count` | 个 | 统计窗口内识别的波浪个数 |

**方向参数**

| 字段 | 单位 | 说明 |
|------|------|------|
| `direction` | deg | 峰值波向（正北=0°，顺时针）；单/双雷达时为 `null` |
| `mean_direction` | deg | 平均波向；单/双雷达时为 `null` |
| `directional_spread` | deg | 方向扩展度；单/双雷达时为 `null` |
| `direction_at_peak` | deg | 谱峰处波向；单/双雷达时为 `null` |

**各雷达波高**

| 字段 | 单位 | 说明 |
|------|------|------|
| `Hm0_r1` | m | 雷达1 有效波高；离线时为 `null` |
| `Hm0_r2` | m | 雷达2 有效波高；离线时为 `null` |
| `Hm0_r3` | m | 雷达3 有效波高；离线时为 `null` |
| `radar_count` | 个 | 参与本次分析的雷达数量（1/2/3）|

### 5.5 系统状态（type: "status"）

响应 `STATUS` 命令，即时返回。

```json
{"type": "status", "time": "2026-03-02 12:35:01", "online": 3}
```

| 字段 | 说明 |
|------|------|
| `time` | 当前本地时间 |
| `online` | 在线雷达数量（0 ~ 3）|

### 5.6 操作确认（type: "ack"）

响应 `CONFIG_SET` 成功时返回。

```json
{"type": "ack", "cmd": "CONFIG_SET", "success": true, "message": "已更新: height=10.5, heading=243.0"}
```

### 5.7 错误响应（type: "error"）

| 情形 | 示例 |
|------|------|
| 分析超时/数据不足 | `{"type": "error", "cmd": "METER", "message": "超时: 360s内未完成分析（数据不足或系统繁忙）"}` |
| 参数值超出范围 | `{"type": "error", "cmd": "CONFIG_SET", "message": "height 须在 0.5 ~ 200.0 m 范围内"}` |
| 参数值非数字 | `{"type": "error", "cmd": "CONFIG_SET", "message": "height 值无效，须为数字"}` |
| 未包含有效配置项 | `{"type": "error", "cmd": "CONFIG_SET", "message": "未包含任何有效配置项"}` |
| JSON 解析失败 | `{"type": "error", "cmd": "", "message": "JSON解析失败: <原始内容>"}` |
| 未知命令 | `{"type": "error", "cmd": "XXX", "message": "未知命令: XXX，支持: METER, WORK, STOP, STATUS, CONFIG_GET, CONFIG_SET"}` |

---

## 6. 完整交互示例

### 6.1 查询配置

PC 发送：
```json
{"cmd": "CONFIG_GET"}
```

Pi 响应：
```json
{"type": "config", "height": 10.0, "heading": 62.0, "sample_rate": 6, "meter_window": 300, "work_window": 1200, "interval": 300, "tide_window": 300}
```

### 6.2 批量修改配置

PC 发送：
```json
{"cmd": "CONFIG_SET", "height": 10.5, "heading": 243.0, "meter_window": 600}
```

Pi 响应（先发更新后的完整配置，再发 ack）：
```json
{"type": "config", "height": 10.5, "heading": 243.0, "sample_rate": 6, "meter_window": 600, "work_window": 1200, "interval": 300, "tide_window": 300}
{"type": "ack", "cmd": "CONFIG_SET", "success": true, "message": "已更新: height=10.5, heading=243.0, meter_window=600"}
```

### 6.3 单次计量模式分析

PC 发送：
```json
{"cmd": "METER", "repeat": false}
```

Pi 响应（潮位先到先发，波浪随后）：
```json
{"type": "ack", "cmd": "METER", "message": "将在300s后开始分析"}
{"type": "tide", "mode": "meter", "time": "2026-03-02 12:30:05", "tide_level": 2.345}
{"type": "wave", "mode": "meter", "window": 300, "time": "2026-03-02 12:35:01", "Hm0": 1.234, "Hmax": 2.103, "Hs": 1.156, "H1_10": 1.876, "Tp": 8.50, "Tz": 6.20, "Te": 7.10, "direction": 243.0, "wave_count": 35, "radar_count": 3}
```

### 6.4 连续滑动窗口分析

PC 发送：
```json
{"cmd": "METER", "repeat": true}
```

Pi 响应（每 `meter_window` 秒重复一次，直到收到 STOP）：
```json
{"type": "ack", "cmd": "METER", "message": "将在300s后开始分析，连续模式"}
{"type": "tide", ...}
{"type": "wave", ...}
{"type": "ack", "cmd": "METER", "message": "将在300s后开始分析，连续模式"}
{"type": "tide", ...}
{"type": "wave", ...}
...
```

停止连续分析：
```json
{"cmd": "STOP"}
```
