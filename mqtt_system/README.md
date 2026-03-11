# 🌊 波浪监测系统 V2.0.0

一个完整的海洋波浪实时监测与分析系统，基于雷达阵列技术。

## ✨ 核心功能

### 1. 数据采集
- **3雷达阵列**: 实时采集海面高度数据
- **6 Hz采样率**: 满足波浪频谱分析要求
- **微秒级同步**: 三个雷达时间差 < 1ms
- **自动重连**: 设备断开后自动重试连接

### 2. 波浪分析
- **频谱分析**: FFT计算能量谱，提取波浪参数
  - 有效波高 (Hm0, Hs)
  - 峰值周期 (Tp)
  - 平均周期 (Tm01, Tz, Te)
  - 谱矩参数 (m-1, m0, m1, m2, m4)

- **方向谱分析**: DIWASP算法
  - 波向 (θ)
  - 方向分散度
  - 峰值频率方向

- **零交叉法**: 时域统计
  - 最大波高 (Hmax)
  - 1/10大波高 (H1/10)
  - 波浪个数统计

- **潮汐分析**: UTide算法
  - 潮汐调和分析
  - 潮汐预报

### 3. 实时可视化
- **仪表盘**: 关键参数大字展示
- **实时图表**: 能量谱、时间序列、趋势图
- **方向谱图**: 极坐标方向分布
- **历史查询**: 任意时间段数据回溯
- **系统监控**: 设备状态、采样率、存储统计

## 🚀 快速开始

### 系统要求
- **硬件**: 树莓派5 (推荐) 或 4核ARM64/x86_64
- **内存**: 4GB+ RAM
- **存储**: 32GB+ SD卡/SSD
- **设备**: 3个VEGA雷达（Modbus/RS485）

### 一键部署
```bash
# 1. 克隆项目
git clone <repo-url> mqtt_system
cd mqtt_system

# 2. 安装系统依赖
sudo apt install -y python3 python3-pip python3-venv postgresql mosquitto

# 3. 创建虚拟环境
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 4. 配置数据库（使用管道避免权限问题）
cat database/setup_database.sql | sudo -u postgres psql

# 5. 配置系统
cp config/system_config.yaml.example config/system_config.yaml
# 编辑 config/system_config.yaml，修改密码和站点信息

# 6. 安装服务
sudo cp systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now wave-*

# 7. 访问Web界面
# 打开浏览器: http://<树莓派IP>:8080
```

详细部署指南请参考 [DEPLOYMENT.md](DEPLOYMENT.md)

## 📊 系统架构

```
  Radars → Collector → Storage → Database
                ↓
            Analyzer → MQTT
                ↓
           Web Server ← Browser
```

**核心组件:**
- `mqtt_collector.py` - 雷达数据采集服务
- `mqtt_storage.py` - 数据存储服务
- `mqtt_analyzer.py` - 波浪频谱分析服务
- `mqtt_tide_analyzer.py` - 潮汐分析服务
- `web_server.py` - Web服务器 (Flask + WebSocket)

## 📈 技术指标

| 参数 | 指标 |
|------|------|
| 采样率 | 6 Hz (可配置) |
| 分析窗口 | 512秒 (8.5分钟) |
| 频率范围 | 0.04 - 0.5 Hz (2-25秒周期) |
| 雷达同步 | < 1 ms |
| 数据延迟 | < 2秒 (采集到显示) |
| 历史数据 | 原始30天，分析365天 |

## 🛠️ 主要依赖

- **Python 3.11+**
- **PostgreSQL 17** - 时序数据存储
- **Mosquitto** - MQTT消息代理
- **Flask** - Web框架
- **NumPy/SciPy** - 科学计算
- **PySerial** - 串口通信
- **Chart.js** - 前端图表

完整依赖列表见 `requirements.txt`

## 📁 项目结构

```
mqtt_system/
├── config/
│   ├── system_config.yaml         # 主配置文件
│   └── system_config.yaml.example # 配置模板
├── services/
│   ├── mqtt_collector.py          # 数据采集
│   ├── mqtt_storage.py            # 数据存储
│   ├── mqtt_analyzer.py           # 波浪分析
│   ├── mqtt_tide_analyzer.py      # 潮汐分析
│   └── web_server.py              # Web服务
├── database/
│   └── create_tide_tables.sql     # 数据库初始化
├── systemd/
│   ├── wave-collector.service     # 采集服务
│   ├── wave-storage.service       # 存储服务
│   ├── wave-analyzer.service      # 分析服务
│   ├── wave-tide-analyzer.service # 潮汐服务
│   └── wave-web.service           # Web服务
├── web/
│   ├── templates/                 # HTML模板
│   └── static/                    # CSS/JS静态文件
├── logs/                          # 日志目录
├── DEPLOYMENT.md                  # 部署指南
├── README.md                      # 本文件
└── requirements.txt               # Python依赖
```

## 🔧 配置说明

主配置文件 `config/system_config.yaml`:

```yaml
# 采集配置
collection:
  sample_rate: 6              # 采样率 (Hz)
  radar_retry_interval: 30    # 雷达重连间隔（秒）

# 分析配置
analysis:
  window_duration: 512        # 分析窗口（秒）
  filter_band: [0.04, 0.5]    # 波浪频率范围
  diwasp_method: IMLM         # 方向谱算法

# 数据保留
storage:
  retention:
    raw_data_days: 30         # 原始数据保留天数
    analysis_data_days: 365   # 分析数据保留天数
```

## 📊 API接口

### REST API
- `GET /api/latest` - 获取最新数据
- `GET /api/history/raw?hours=1` - 原始数据历史
- `GET /api/history/analysis?days=7` - 分析数据历史
- `GET /api/config` - 获取系统配置
- `POST /api/config` - 更新系统配置

### WebSocket
- `/ws` - 实时数据推送
  - `raw` - 原始雷达数据
  - `analyzed` - 分析结果
  - `status` - 系统状态

## 🔍 监控与调试

### 查看服务状态
```bash
systemctl status wave-*
```

### 查看日志
```bash
# 应用日志
tail -f /home/obsis/radar/mqtt_system/logs/*.log

# 系统日志
journalctl -u wave-collector -f
```

### 测试MQTT连接
```bash
# 订阅原始数据
mosquitto_sub -h localhost -t "radar/raw" -u wave_user -P wave2025

# 订阅分析结果
mosquitto_sub -h localhost -t "radar/analyzed" -u wave_user -P wave2025
```

### 分析采样性能
```bash
python3 analyze_radar_sync.py
```

## 🐛 故障排查

### 雷达无法连接
```bash
# 检查设备
ls -l /dev/radar*

# 检查服务日志
journalctl -u wave-collector -n 50
```

### Web界面无数据
```bash
# 检查服务是否运行
systemctl status wave-analyzer wave-web

# 检查MQTT连接
mosquitto_sub -h localhost -t "radar/analyzed" -u wave_user -P wave2025
```

更多故障排查请参考 [DEPLOYMENT.md](DEPLOYMENT.md#故障排查)

## 📝 开发说明

### 运行测试
```bash
# 数据模拟器（无雷达情况下）
python3 services/data_simulator.py

# 雷达连接测试
python3 test_radar_connection.py

# 同步性能分析
python3 analyze_radar_sync.py
```

### 代码规范
- Python 3.11+ 类型注解
- Docstrings (Google风格)
- 日志级别: DEBUG/INFO/WARNING/ERROR

## 🔒 安全建议

1. **修改默认密码** (database, mqtt)
2. **配置防火墙** (限制端口访问)
3. **启用HTTPS** (生产环境)
4. **定期备份** (数据库和配置)

详见 [DEPLOYMENT.md](DEPLOYMENT.md#安全建议)

## 📦 版本历史

### V2.0.0 (2026-01-12)
- ✅ 雷达连接重试机制
- ✅ 实时波浪频谱分析
- ✅ 方向谱分析 (DIWASP)
- ✅ 潮汐分析 (UTide)
- ✅ Web实时监控界面
- ✅ 历史数据查询
- ✅ 系统配置管理
- ✅ 6Hz采样率，微秒级同步

## 📄 许可证

Proprietary - 版权所有

## 🤝 贡献

欢迎提交Issue和Pull Request

## 📧 联系方式

- 项目仓库: <repo-url>
- 问题反馈: GitHub Issues

---

**Built with ❤️ for ocean monitoring**
