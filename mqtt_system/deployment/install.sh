#!/bin/bash
# =====================================================
# 波浪监测系统 - 一键安装脚本
# =====================================================
# 用法: sudo bash install.sh
#
# 支持系统: Debian/Ubuntu/Raspberry Pi OS (arm64/amd64)
# 安装内容:
#   1. 系统依赖 (mosquitto, postgresql, python3)
#   2. Python虚拟环境及依赖
#   3. 数据库初始化
#   4. MQTT Broker配置
#   5. systemd服务注册
#   6. udev串口规则 (可选)
# =====================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_step()  { echo -e "\n${CYAN}========== $* ==========${NC}"; }

# ==================== 检查运行环境 ====================
if [ "$EUID" -ne 0 ]; then
    log_error "请使用 sudo 运行此脚本"
    echo "用法: sudo bash $0"
    exit 1
fi

# 检测运行用户
get_run_user() {
    if [ -n "$SUDO_USER" ] && [ "$SUDO_USER" != "root" ]; then
        echo "$SUDO_USER"; return
    fi
    local logname_user
    logname_user=$(logname 2>/dev/null) || true
    if [ -n "$logname_user" ] && [ "$logname_user" != "root" ]; then
        echo "$logname_user"; return
    fi
    for home_dir in /home/*; do
        if [ -d "$home_dir" ]; then
            local user=$(basename "$home_dir")
            if id "$user" &>/dev/null; then
                echo "$user"; return
            fi
        fi
    done
    echo "root"
}

REAL_USER=$(get_run_user)
REAL_GROUP=$(id -gn "$REAL_USER" 2>/dev/null || echo "$REAL_USER")
REAL_HOME=$(getent passwd "$REAL_USER" | cut -d: -f6)
PROJECT_DIR="$REAL_HOME/radar/mqtt_system"
VENV_DIR="$PROJECT_DIR/venv"

echo "=================================================="
echo "  波浪监测系统 - 安装程序"
echo "=================================================="
echo ""
echo "  运行用户:   $REAL_USER:$REAL_GROUP"
echo "  用户主目录: $REAL_HOME"
echo "  项目目录:   $PROJECT_DIR"
echo "  Python虚拟环境: $VENV_DIR"
echo ""

# 检查项目目录
if [ ! -d "$PROJECT_DIR/services" ]; then
    log_error "项目目录不完整: $PROJECT_DIR/services 不存在"
    echo "请确保已将 mqtt_system 目录复制到 $REAL_HOME/radar/ 下"
    exit 1
fi

# ==================== 步骤 1: 系统依赖 ====================
log_step "步骤 1/7: 安装系统依赖"

apt-get update -qq

# 基础工具
apt-get install -y -qq python3 python3-pip python3-venv python3-dev socat > /dev/null 2>&1
log_info "Python3 + socat 已安装"

# Mosquitto MQTT Broker
if ! command -v mosquitto &> /dev/null; then
    apt-get install -y -qq mosquitto mosquitto-clients > /dev/null 2>&1
    log_info "Mosquitto MQTT Broker 已安装"
else
    log_info "Mosquitto 已存在，跳过"
fi

# PostgreSQL
if ! command -v psql &> /dev/null; then
    apt-get install -y -qq postgresql postgresql-contrib > /dev/null 2>&1
    log_info "PostgreSQL 已安装"
else
    log_info "PostgreSQL 已存在，跳过"
fi

# 编译依赖 (psycopg2等可能需要)
apt-get install -y -qq libpq-dev gcc > /dev/null 2>&1

log_info "系统依赖安装完成"

# ==================== 步骤 2: 配置 Mosquitto ====================
log_step "步骤 2/7: 配置 Mosquitto MQTT Broker"

MOSQUITTO_CONF="/etc/mosquitto/conf.d/wave_monitoring.conf"

cat > "$MOSQUITTO_CONF" <<EOF
# Wave Monitoring System MQTT Configuration
listener 1883
allow_anonymous false
password_file /etc/mosquitto/passwd
EOF

# 创建/更新MQTT用户
mosquitto_passwd -b -c /etc/mosquitto/passwd wave_user wave2025 2>/dev/null || \
    mosquitto_passwd -b /etc/mosquitto/passwd wave_user wave2025

systemctl restart mosquitto
systemctl enable mosquitto > /dev/null 2>&1

log_info "Mosquitto 配置完成 (用户: wave_user, 端口: 1883)"

# ==================== 步骤 3: 配置 PostgreSQL ====================
log_step "步骤 3/7: 配置 PostgreSQL 数据库"

systemctl start postgresql
systemctl enable postgresql > /dev/null 2>&1

# 创建用户和数据库
sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname = 'wave_user'" | grep -q 1 || \
    sudo -u postgres psql -c "CREATE USER wave_user WITH ENCRYPTED PASSWORD 'wave2025';" > /dev/null 2>&1
log_info "数据库用户 wave_user 已就绪"

sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = 'wave_monitoring'" | grep -q 1 || \
    sudo -u postgres psql -c "CREATE DATABASE wave_monitoring OWNER wave_user;" > /dev/null 2>&1
log_info "数据库 wave_monitoring 已就绪"

# 授权
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE wave_monitoring TO wave_user;" > /dev/null 2>&1

# 初始化表结构
SCHEMA_FILE="$SCRIPT_DIR/database_schema.sql"
if [ -f "$SCHEMA_FILE" ]; then
    cat "$SCHEMA_FILE" | sudo -u postgres psql wave_monitoring > /dev/null 2>&1
    log_info "数据库表结构已初始化"
else
    log_warn "database_schema.sql 未找到，跳过表结构初始化"
fi

# ==================== 步骤 4: Python 虚拟环境 ====================
log_step "步骤 4/7: 创建 Python 虚拟环境并安装依赖"

if [ ! -d "$VENV_DIR" ]; then
    sudo -u "$REAL_USER" python3 -m venv "$VENV_DIR"
    log_info "虚拟环境已创建: $VENV_DIR"
else
    log_info "虚拟环境已存在，跳过创建"
fi

# 升级pip
sudo -u "$REAL_USER" "$VENV_DIR/bin/pip" install --upgrade pip > /dev/null 2>&1

# 安装依赖
REQ_FILE="$PROJECT_DIR/requirements.txt"
if [ -f "$REQ_FILE" ]; then
    sudo -u "$REAL_USER" "$VENV_DIR/bin/pip" install -r "$REQ_FILE" 2>&1 | tail -1
    log_info "Python 依赖安装完成"
else
    log_warn "requirements.txt 未找到，安装核心依赖..."
    sudo -u "$REAL_USER" "$VENV_DIR/bin/pip" install \
        paho-mqtt PyYAML psycopg2-binary numpy scipy pandas \
        Flask flask-cors flask-sock pyserial python-dateutil utide > /dev/null 2>&1
    log_info "核心 Python 依赖已安装"
fi

# ==================== 步骤 5: systemd 服务 ====================
log_step "步骤 5/7: 配置 systemd 服务"

SYSTEMD_SRC="$PROJECT_DIR/systemd"
if [ ! -d "$SYSTEMD_SRC" ]; then
    SYSTEMD_SRC="$PROJECT_DIR/scripts"
fi

# 服务文件列表
SERVICE_FILES=(
    "wave-collector.service"
    "wave-storage.service"
    "wave-analyzer.service"
    "wave-tide-analyzer.service"
    "wave-web.service"
    "wave-thingsboard.service"
    "wave-ec800-thingsboard.service"
    "wave-monitor.target"
)

for svc_file in "${SERVICE_FILES[@]}"; do
    src="$SYSTEMD_SRC/$svc_file"
    dst="/etc/systemd/system/$svc_file"
    if [ -f "$src" ]; then
        cp "$src" "$dst"
        # 替换用户和路径
        sed -i "s|User=obsis|User=$REAL_USER|g" "$dst"
        sed -i "s|Group=obsis|Group=$REAL_GROUP|g" "$dst"
        sed -i "s|/home/obsis|$REAL_HOME|g" "$dst"
        chmod 644 "$dst"
        log_info "  $svc_file 已安装"
    fi
done

systemctl daemon-reload

# 启用核心服务开机自启
for svc in wave-collector wave-storage wave-analyzer wave-tide-analyzer wave-web; do
    systemctl enable "$svc" > /dev/null 2>&1 || true
done

log_info "systemd 服务配置完成"

# ==================== 步骤 6: 串口和权限 ====================
log_step "步骤 6/7: 配置串口权限"

# 添加用户到 dialout 组 (串口访问)
if [ "$REAL_USER" != "root" ]; then
    usermod -a -G dialout "$REAL_USER" 2>/dev/null || true
    log_info "用户 $REAL_USER 已添加到 dialout 组"
fi

# udev 规则
UDEV_EXAMPLE="$PROJECT_DIR/config/99-radar-ports.rules.example"
UDEV_DST="/etc/udev/rules.d/99-radar-ports.rules"
if [ -f "$UDEV_EXAMPLE" ] && [ ! -f "$UDEV_DST" ]; then
    log_warn "发现 udev 规则示例文件"
    echo "  请根据实际USB转RS485序列号修改后安装:"
    echo "  sudo cp $UDEV_EXAMPLE $UDEV_DST"
    echo "  sudo udevadm control --reload-rules && sudo udevadm trigger"
fi

# ==================== 步骤 7: 目录和权限 ====================
log_step "步骤 7/7: 设置目录和权限"

# 创建日志目录
mkdir -p "$PROJECT_DIR/logs"
mkdir -p "$PROJECT_DIR/cache"

# 设置文件权限
chown -R "$REAL_USER:$REAL_GROUP" "$PROJECT_DIR"
chmod +x "$PROJECT_DIR"/services/*.py 2>/dev/null || true
chmod +x "$PROJECT_DIR"/scripts/*.sh 2>/dev/null || true

log_info "目录权限设置完成"

# ==================== 安装完成 ====================
echo ""
echo "=================================================="
echo -e "  ${GREEN}安装完成!${NC}"
echo "=================================================="
echo ""
echo "  后续步骤:"
echo ""
echo "  1. 修改配置文件 (雷达串口、GPS坐标、ThingsBoard等):"
echo "     nano $PROJECT_DIR/config/system_config.yaml"
echo ""
echo "  2. 配置串口 udev 规则 (根据实际设备序列号):"
echo "     sudo cp $PROJECT_DIR/config/99-radar-ports.rules.example /etc/udev/rules.d/99-radar-ports.rules"
echo "     # 编辑规则中的序列号，然后:"
echo "     sudo udevadm control --reload-rules && sudo udevadm trigger"
echo ""
echo "  3. 启动所有服务:"
echo "     $PROJECT_DIR/scripts/wavectl.sh start"
echo ""
echo "  4. 查看服务状态:"
echo "     $PROJECT_DIR/scripts/wavectl.sh status"
echo ""
echo "  5. 设置开机自启:"
echo "     $PROJECT_DIR/scripts/wavectl.sh enable"
echo ""
echo "  6. 访问 Web 界面:"
echo "     http://localhost:8080"
echo ""
echo "=================================================="
