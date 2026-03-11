#!/bin/bash
# =====================================================
# 波浪监测系统 - 部署包打包脚本
# =====================================================
# 用法: bash pack.sh
# 输出: ~/radar/deployment/wave_monitor_<日期>.zip
# =====================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RADAR_DIR="$(dirname "$PROJECT_DIR")"
DATE=$(date +%Y%m%d)
PACK_NAME="wave_monitor_${DATE}"
WORK_DIR="/tmp/${PACK_NAME}"
TAR_OUTPUT="/tmp/${PACK_NAME}.tar.gz"
ZIP_OUTPUT="$SCRIPT_DIR/${PACK_NAME}.zip"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=================================================="
echo "  波浪监测系统 - 打包部署包"
echo "  打包日期: $DATE"
echo "=================================================="
echo ""

# 清理旧的临时目录
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR/mqtt_system"

echo -e "${CYAN}[1/8]${NC} 复制 Python 服务代码..."
mkdir -p "$WORK_DIR/mqtt_system/services"
cp "$PROJECT_DIR"/services/*.py "$WORK_DIR/mqtt_system/services/"
# 递归复制 pydiwasp（包含 private/ 子目录）
cp -r "$PROJECT_DIR/services/pydiwasp" "$WORK_DIR/mqtt_system/services/"
# 清除 __pycache__
find "$WORK_DIR/mqtt_system/services" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
echo -e "  服务文件: $(find "$WORK_DIR/mqtt_system/services" -name "*.py" | wc -l) 个 .py 文件"

echo -e "${CYAN}[2/8]${NC} 复制 Web 前端文件..."
mkdir -p "$WORK_DIR/mqtt_system/web/templates"
mkdir -p "$WORK_DIR/mqtt_system/web/static/css"
mkdir -p "$WORK_DIR/mqtt_system/web/static/js"
for f in index.html config.html history.html spectrum.html debug.html test.html; do
    [ -f "$PROJECT_DIR/web/templates/$f" ] && cp "$PROJECT_DIR/web/templates/$f" "$WORK_DIR/mqtt_system/web/templates/"
done
cp "$PROJECT_DIR"/web/static/css/*.css "$WORK_DIR/mqtt_system/web/static/css/"
cp "$PROJECT_DIR"/web/static/js/*.js "$WORK_DIR/mqtt_system/web/static/js/"

echo -e "${CYAN}[3/8]${NC} 复制配置文件..."
mkdir -p "$WORK_DIR/mqtt_system/config"
cp "$PROJECT_DIR/config/system_config.yaml" "$WORK_DIR/mqtt_system/config/system_config.yaml.example"
[ -f "$PROJECT_DIR/config/preprocessing_config.yaml" ] && cp "$PROJECT_DIR/config/preprocessing_config.yaml" "$WORK_DIR/mqtt_system/config/"
[ -f "$PROJECT_DIR/config/mosquitto.conf" ]             && cp "$PROJECT_DIR/config/mosquitto.conf" "$WORK_DIR/mqtt_system/config/"
[ -f "$PROJECT_DIR/config/99-radar-ports.rules.example" ] && cp "$PROJECT_DIR/config/99-radar-ports.rules.example" "$WORK_DIR/mqtt_system/config/"
echo -e "  ${YELLOW}注意: system_config.yaml 仅以 .example 形式打包，安装后需手动配置${NC}"

echo -e "${CYAN}[4/8]${NC} 复制 systemd 服务文件..."
mkdir -p "$WORK_DIR/mqtt_system/systemd"
cp "$PROJECT_DIR"/systemd/*.service "$WORK_DIR/mqtt_system/systemd/" 2>/dev/null || true
cp "$PROJECT_DIR"/systemd/*.target  "$WORK_DIR/mqtt_system/systemd/" 2>/dev/null || true

echo -e "${CYAN}[5/8]${NC} 复制管理脚本..."
mkdir -p "$WORK_DIR/mqtt_system/scripts"
cp "$PROJECT_DIR/scripts/wavectl.sh"   "$WORK_DIR/mqtt_system/scripts/"
[ -f "$PROJECT_DIR/scripts/start_all.sh" ] && cp "$PROJECT_DIR/scripts/start_all.sh" "$WORK_DIR/mqtt_system/scripts/"
[ -f "$PROJECT_DIR/scripts/stop_all.sh"  ] && cp "$PROJECT_DIR/scripts/stop_all.sh"  "$WORK_DIR/mqtt_system/scripts/"
[ -f "$PROJECT_DIR/scripts/status.sh"    ] && cp "$PROJECT_DIR/scripts/status.sh"    "$WORK_DIR/mqtt_system/scripts/"
chmod +x "$WORK_DIR"/mqtt_system/scripts/*.sh

echo -e "${CYAN}[6/8]${NC} 复制部署和数据库文件..."
mkdir -p "$WORK_DIR/mqtt_system/deployment"
cp "$SCRIPT_DIR/install.sh"         "$WORK_DIR/mqtt_system/deployment/"
cp "$SCRIPT_DIR/uninstall.sh"       "$WORK_DIR/mqtt_system/deployment/"
cp "$SCRIPT_DIR/database_schema.sql" "$WORK_DIR/mqtt_system/deployment/"
[ -f "$SCRIPT_DIR/README.md" ] && cp "$SCRIPT_DIR/README.md" "$WORK_DIR/mqtt_system/deployment/"
chmod +x "$WORK_DIR/mqtt_system/deployment/install.sh"
chmod +x "$WORK_DIR/mqtt_system/deployment/uninstall.sh"

mkdir -p "$WORK_DIR/mqtt_system/database"
[ -f "$PROJECT_DIR/database/create_tide_tables.sql" ] && cp "$PROJECT_DIR/database/create_tide_tables.sql" "$WORK_DIR/mqtt_system/database/"

echo -e "${CYAN}[7/8]${NC} 复制 requirements.txt 和文档..."
cp "$PROJECT_DIR/requirements.txt" "$WORK_DIR/mqtt_system/"
[ -f "$PROJECT_DIR/README.md"    ] && cp "$PROJECT_DIR/README.md"    "$WORK_DIR/mqtt_system/"
[ -f "$PROJECT_DIR/CHANGELOG.md" ] && cp "$PROJECT_DIR/CHANGELOG.md" "$WORK_DIR/mqtt_system/"

# 波浪模拟器 (可选)
if [ -d "$RADAR_DIR/wavesim" ]; then
    echo -e "${CYAN}[7.5/8]${NC} 复制波浪模拟器..."
    mkdir -p "$WORK_DIR/wavesim"
    [ -f "$RADAR_DIR/wavesim/wave_simulator.py" ] && cp "$RADAR_DIR/wavesim/wave_simulator.py" "$WORK_DIR/wavesim/"
    [ -f "$RADAR_DIR/wavesim/scenarios.yaml"    ] && cp "$RADAR_DIR/wavesim/scenarios.yaml"    "$WORK_DIR/wavesim/"
    [ -f "$RADAR_DIR/wavesim/start_sim.sh"      ] && cp "$RADAR_DIR/wavesim/start_sim.sh"      "$WORK_DIR/wavesim/"
fi

echo -e "${CYAN}[8/8]${NC} 打包压缩..."
cd /tmp
tar -czf "$TAR_OUTPUT" "${PACK_NAME}/"

# 打成 zip（tar.gz 放入 zip）
cd /tmp
rm -f "$ZIP_OUTPUT"
zip -j "$ZIP_OUTPUT" \
    "$TAR_OUTPUT" \
    "$SCRIPT_DIR/install.sh" \
    "$SCRIPT_DIR/uninstall.sh" \
    "$SCRIPT_DIR/database_schema.sql" \
    "$SCRIPT_DIR/pack.sh"
[ -f "$SCRIPT_DIR/README.md" ] && zip -j "$ZIP_OUTPUT" "$SCRIPT_DIR/README.md"

# 统计
FILE_COUNT=$(find "$WORK_DIR" -type f | wc -l)
TAR_SIZE=$(du -h "$TAR_OUTPUT" | cut -f1)
ZIP_SIZE=$(du -h "$ZIP_OUTPUT" | cut -f1)

# 清理
rm -rf "$WORK_DIR"
rm -f  "$TAR_OUTPUT"

echo ""
echo "=================================================="
echo -e "  ${GREEN}打包完成!${NC}"
echo "=================================================="
echo ""
echo "  输出文件: $ZIP_OUTPUT"
echo "  文件数量: $FILE_COUNT"
echo "  ZIP 大小: $ZIP_SIZE"
echo ""
echo "  传输到新机器:"
echo "    scp $ZIP_OUTPUT 用户@目标IP:/tmp/"
echo ""
echo "  目标机器上安装:"
echo "    cd ~ && mkdir -p radar"
echo "    cd /tmp && unzip ${PACK_NAME}.zip"
echo "    tar -xzf ${PACK_NAME}.tar.gz -C ~/radar/"
echo "    sudo bash ~/radar/mqtt_system/deployment/install.sh"
echo ""
echo "  安装后修改配置:"
echo "    nano ~/radar/mqtt_system/config/system_config.yaml"
echo ""
echo "=================================================="
