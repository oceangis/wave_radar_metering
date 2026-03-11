# 波浪监测系统 - 部署说明

## 系统要求

- **硬件**: Raspberry Pi 4/5 (4GB+ RAM) 或 x86_64 Linux 主机
- **系统**: Debian 12 / Ubuntu 22.04+ / Raspberry Pi OS
- **存储**: 16GB+
- **网络**: 以太网或WiFi

## 快速安装

### 1. 复制项目到目标机器

```bash
# 在当前机器上打包 (不含venv和logs)
cd ~/radar
tar --exclude='mqtt_system/venv' \
    --exclude='mqtt_system/logs' \
    --exclude='mqtt_system/cache' \
    --exclude='__pycache__' \
    -czf /tmp/wave_monitor.tar.gz mqtt_system/

# 复制到目标机器
scp /tmp/wave_monitor.tar.gz <用户>@<目标IP>:/tmp/
```

### 2. 在目标机器上解压

```bash
ssh <用户>@<目标IP>
mkdir -p ~/radar
cd ~/radar
tar -xzf /tmp/wave_monitor.tar.gz
```

### 3. 执行安装

```bash
sudo bash ~/radar/mqtt_system/deployment/install.sh
```

安装脚本自动完成:
- 安装 mosquitto, postgresql, python3, socat
- 配置 MQTT Broker (用户: wave_user, 端口: 1883)
- 创建数据库 wave_monitoring 并初始化表结构
- 创建 Python 虚拟环境并安装依赖
- 注册 systemd 服务 (自动适配当前用户和路径)

### 4. 修改配置

```bash
nano ~/radar/mqtt_system/config/system_config.yaml
```

根据实际情况修改:
- `radar.ports` - 雷达串口路径
- `radar.array_heading` - 阵列朝向 (度)
- `radar.array_height` - 阵列安装高度 (m)
- `site.latitude/longitude` - 站点坐标
- `thingsboard.*` - ThingsBoard 服务器 (可选)

### 5. 配置串口 udev 规则

```bash
# 查看USB转RS485序列号
for port in /dev/ttyUSB*; do
    echo "$port: $(udevadm info -q all -n $port | grep ID_SERIAL_SHORT | cut -d= -f2)"
done

# 编辑规则，替换序列号
sudo cp ~/radar/mqtt_system/config/99-radar-ports.rules.example /etc/udev/rules.d/99-radar-ports.rules
sudo nano /etc/udev/rules.d/99-radar-ports.rules
sudo udevadm control --reload-rules && sudo udevadm trigger
```

### 6. 启动服务

```bash
~/radar/mqtt_system/scripts/wavectl.sh start
~/radar/mqtt_system/scripts/wavectl.sh enable   # 开机自启
~/radar/mqtt_system/scripts/wavectl.sh status    # 查看状态
```

### 7. 访问 Web 界面

浏览器打开: `http://<设备IP>:8080`

---

## 服务列表

| 服务 | 说明 | 资源限制 |
|------|------|----------|
| wave-collector | 雷达数据采集 (Modbus/RS485, 6Hz) | 512M / 50% CPU |
| wave-storage | 数据存储到 PostgreSQL | 512M / 50% CPU |
| wave-analyzer | 波浪频谱+方向分析 (DIWASP) | 1G / 75% CPU |
| wave-tide-analyzer | 潮汐调和分析 (UTide) | - |
| wave-web | Web 界面 (Flask, 端口 8080) | 512M / 50% CPU |
| wave-thingsboard | ThingsBoard 数据上报 (可选) | - |
| wave-ec800-thingsboard | 4G 模块上报 (可选) | - |

## 常用命令

```bash
# 服务管理
wavectl.sh start|stop|restart|status|enable|disable

# 查看日志
wavectl.sh logs wave-analyzer
sudo journalctl -u wave-collector -f

# 数据库
PGPASSWORD=wave2025 psql -U wave_user -d wave_monitoring -h localhost

# MQTT 调试
mosquitto_sub -h localhost -u wave_user -P wave2025 -t "radar/#" -v
```

## 卸载

```bash
sudo bash ~/radar/mqtt_system/deployment/uninstall.sh
```

## 部署包文件

```
deployment/
├── install.sh           # 一键安装脚本
├── uninstall.sh         # 卸载脚本
├── database_schema.sql  # 数据库完整建表脚本
└── README.md            # 本文件
```
