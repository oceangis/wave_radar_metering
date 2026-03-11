#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VEGA雷达液位计模拟器
====================

模拟3个VEGA雷达的Modbus RTU串口设备，使用JONSWAP谱生成真实波浪数据。
每个雷达在独立线程中监听虚拟串口，响应Modbus请求。

使用方式:
    python wave_simulator.py --ports /dev/pts/X /dev/pts/Y /dev/pts/Z
    python wave_simulator.py --ports /dev/pts/X /dev/pts/Y /dev/pts/Z --scenario storm
"""

import argparse
import logging
import math
import os
import signal
import struct
import sys
import threading
import time
from pathlib import Path

import serial
import yaml

# JONSWAP谱参数
NUM_FREQ_COMPONENTS = 50
GRAVITY = 9.81


def crc16(data: bytes) -> int:
    """Modbus CRC16 — 与 mqtt_collector 完全一致"""
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


class JONSWAPSpectrum:
    """JONSWAP频谱生成器"""

    def __init__(self, hs, tp, gamma=3.3):
        self.hs = hs
        self.tp = tp
        self.fp = 1.0 / tp
        self.gamma = gamma
        self.alpha = self._calc_alpha()

    def _calc_alpha(self):
        """从Hs反算alpha (Phillips常数)"""
        # 近似: Hs = 4 * sqrt(m0), 通过数值积分调整alpha
        # 先用alpha=1计算m0，然后缩放
        alpha_test = 1.0
        m0 = self._moment0(alpha_test)
        if m0 <= 0:
            return 0.0081  # 默认值
        hs_test = 4.0 * math.sqrt(m0)
        return alpha_test * (self.hs / hs_test) ** 2

    def _moment0(self, alpha):
        """计算谱的零阶矩"""
        fmin = 0.03
        fmax = 0.5
        df = (fmax - fmin) / 1000
        m0 = 0.0
        for i in range(1000):
            f = fmin + (i + 0.5) * df
            m0 += self._spectrum_value(f, alpha) * df
        return m0

    def _spectrum_value(self, f, alpha=None):
        """计算单一频率的JONSWAP谱值"""
        if alpha is None:
            alpha = self.alpha
        if f <= 0:
            return 0.0

        sigma = 0.07 if f <= self.fp else 0.09
        r = math.exp(-0.5 * ((f - self.fp) / (sigma * self.fp)) ** 2)

        s = (alpha * GRAVITY ** 2 / ((2 * math.pi) ** 4 * f ** 5)
             * math.exp(-1.25 * (self.fp / f) ** 4)
             * self.gamma ** r)
        return s

    def generate_components(self, n=NUM_FREQ_COMPONENTS):
        """生成N个频率分量 (频率, 振幅, 随机相位)"""
        import random
        fmin = 0.03
        fmax = 0.5
        df = (fmax - fmin) / n

        components = []
        for i in range(n):
            f = fmin + (i + 0.5) * df
            s = self._spectrum_value(f)
            amp = math.sqrt(2.0 * s * df)
            phase = random.uniform(0, 2 * math.pi)
            components.append((f, amp, phase))

        return components


class WaveField:
    """波浪场：计算各雷达位置的水面升高"""

    def __init__(self, scenario_cfg, radar_cfg, array_heading=0.0):
        self.radar_cfg = radar_cfg
        self.positions = radar_cfg['positions']
        self.tilt_angles = radar_cfg['tilt_angles']
        self.array_height = radar_cfg['array_height']
        self.array_heading = array_heading

        # 生成波浪分量（支持混合海况）
        self.wave_components = []  # [(freq, amp, phase, direction_rad), ...]
        self._build_components(scenario_cfg)

        self.start_time = time.time()
        logging.info(f"WaveField initialized with {len(self.wave_components)} total components")

    def _build_components(self, scenario_cfg):
        """从海况配置构建波浪分量"""
        import random

        if 'components' in scenario_cfg:
            # 混合海况：多个独立波浪系统
            for comp in scenario_cfg['components']:
                hs = comp['hs']
                tp = comp['tp']
                direction = comp['direction']
                gamma = comp.get('gamma', 3.3)
                self._add_spectral_components(hs, tp, direction, gamma)
        else:
            hs = scenario_cfg['hs']
            tp = scenario_cfg['tp']
            direction = scenario_cfg['direction']
            gamma = scenario_cfg.get('gamma', 3.3)
            self._add_spectral_components(hs, tp, direction, gamma)

    def _add_spectral_components(self, hs, tp, direction_compass, gamma):
        """从JONSWAP谱添加分量"""
        spectrum = JONSWAPSpectrum(hs, tp, gamma)
        freq_components = spectrum.generate_components()

        # 波向：compass来向 → 传播去向(弧度)
        # compass来向 N=0, 顺时针; 传播去向 = 来向 + 180
        propagation_deg = (direction_compass + 180.0) % 360.0
        # 转为数学坐标系弧度 (从x轴逆时针)
        # 数学角 = 90 - compass角
        # 关键修正：雷达位置在局部坐标系中，波浪传播方向需要从绝对坐标系
        # 旋转到局部坐标系。局部x轴指向compass (array_heading+90)°,
        # 因此需要加上 array_heading 旋转补偿。
        propagation_rad = math.radians(90.0 - propagation_deg + self.array_heading)

        for f, amp, phase in freq_components:
            self.wave_components.append((f, amp, phase, propagation_rad))

    def get_surface_elevation(self, x, y, t):
        """计算位置(x,y)在时刻t的水面升高 η"""
        eta = 0.0
        for f, amp, phase, theta in self.wave_components:
            omega = 2 * math.pi * f
            # 深水色散关系: k = omega^2 / g
            k = omega ** 2 / GRAVITY
            kx = k * math.cos(theta)
            ky = k * math.sin(theta)
            eta += amp * math.cos(omega * t - kx * x - ky * y + phase)
        return eta

    def get_radar_distance(self, radar_name, t):
        """获取指定雷达在时刻t的测距值

        倾斜雷达(R2/R3)的光束打到水面的位置偏离安装点正下方：
            offset = array_height × tan(tilt)
        需要在该投影点计算波面高度，再算斜距。
        """
        pos = self.positions[radar_name]
        x, y = pos[0], pos[1]

        tilt_deg = self.tilt_angles.get(radar_name, 0.0)
        if tilt_deg > 0:
            # 倾斜雷达：光束投射到水面的位置
            # azimuth=0 → 沿+Y（前方）倾斜
            offset = self.array_height * math.tan(math.radians(tilt_deg))
            x_water = x
            y_water = y + offset
            eta = self.get_surface_elevation(x_water, y_water, t)
            # 斜距 = 垂直距离 / cos(tilt)
            distance = (self.array_height - eta) / math.cos(math.radians(tilt_deg))
        else:
            eta = self.get_surface_elevation(x, y, t)
            distance = self.array_height - eta

        return distance


class AnomalyInjector:
    """异常注入器"""

    def __init__(self, anomaly_cfg, start_time):
        self.cfg = anomaly_cfg or {}
        self.start_time = start_time

        self.spike_cfg = self.cfg.get('spike', {})
        self.flat_line_cfg = self.cfg.get('flat_line', {})
        self.dropout_cfg = self.cfg.get('dropout', {})
        self.oor_cfg = self.cfg.get('out_of_range', {})

        self._flat_line_active = False
        self._flat_line_value = None

    def process(self, radar_idx, distance, current_time):
        """
        处理异常注入。

        返回:
            (distance, should_respond)
            distance: 可能被修改的测距值
            should_respond: False表示不响应(模拟dropout)
        """
        import random

        radar_num = radar_idx + 1  # 1-based
        elapsed = current_time - self.start_time

        # Dropout: 不响应
        if self.dropout_cfg.get('enabled', False):
            if self.dropout_cfg.get('radar', 0) == radar_num:
                if random.random() < self.dropout_cfg.get('probability', 0.05):
                    return distance, False

        # Flat line: 返回固定值
        if self.flat_line_cfg.get('enabled', False):
            if self.flat_line_cfg.get('radar', 0) == radar_num:
                start_after = self.flat_line_cfg.get('start_after', 120)
                duration = self.flat_line_cfg.get('duration', 30)
                if start_after <= elapsed < start_after + duration:
                    if not self._flat_line_active:
                        self._flat_line_active = True
                        self._flat_line_value = distance
                    return self._flat_line_value, True
                else:
                    self._flat_line_active = False

        # Spike: 随机极端值
        if self.spike_cfg.get('enabled', False):
            if random.random() < self.spike_cfg.get('probability', 0.001):
                magnitude = self.spike_cfg.get('magnitude', 3.0)
                spike_val = distance + random.choice([-1, 1]) * magnitude
                return spike_val, True

        # Out of range: 超量程
        if self.oor_cfg.get('enabled', False):
            if random.random() < self.oor_cfg.get('probability', 0.002):
                oor_val = random.choice([-1.0, 25.0])
                return oor_val, True

        return distance, True


class ModbusResponder(threading.Thread):
    """单个雷达的Modbus RTU应答线程"""

    def __init__(self, radar_idx, port_path, wave_field, anomaly_injector, modbus_address, baudrate):
        super().__init__(daemon=True)
        self.radar_idx = radar_idx
        self.radar_name = f"R{radar_idx + 1}"
        self.port_path = port_path
        self.wave_field = wave_field
        self.anomaly_injector = anomaly_injector
        self.modbus_address = modbus_address
        self.baudrate = baudrate
        self.running = True
        self.ser = None
        self.request_count = 0

    def run(self):
        logging.info(f"[{self.radar_name}] Starting Modbus responder on {self.port_path}")
        try:
            self.ser = serial.Serial(
                port=self.port_path,
                baudrate=self.baudrate,
                bytesize=8,
                parity='N',
                stopbits=1,
                timeout=0.01
            )
            logging.info(f"[{self.radar_name}] Serial port opened")
        except Exception as e:
            logging.error(f"[{self.radar_name}] Failed to open {self.port_path}: {e}")
            return

        while self.running:
            try:
                # 读取请求 (8字节: addr(1) + func(1) + reg(2) + count(2) + crc(2))
                data = self.ser.read(8)
                if len(data) < 8:
                    continue

                # 验证CRC
                calc_crc = crc16(data[:6])
                recv_crc = struct.unpack('<H', data[6:8])[0]
                if calc_crc != recv_crc:
                    logging.debug(f"[{self.radar_name}] CRC mismatch")
                    continue

                # 解析请求
                addr, func, reg, count = struct.unpack('>BBHH', data[:6])

                # 验证：地址匹配 + 功能码04 + 寄存器2004 + 数量2
                if addr != self.modbus_address:
                    continue
                if func != 0x04:
                    logging.debug(f"[{self.radar_name}] Unsupported function code: {func}")
                    continue
                if reg != 2004 or count != 2:
                    logging.debug(f"[{self.radar_name}] Unexpected register {reg} count {count}")
                    continue

                self.request_count += 1

                # 计算当前测距值
                t = time.time() - self.wave_field.start_time
                distance = self.wave_field.get_radar_distance(self.radar_name, t)

                # 异常注入
                distance, should_respond = self.anomaly_injector.process(
                    self.radar_idx, distance, time.time()
                )

                if not should_respond:
                    # 模拟dropout：不回复，让collector超时
                    continue

                # 构建Modbus响应
                # addr(1) + func(1) + byte_count=4(1) + float32_be(4) + crc_le(2) = 9字节
                resp_data = struct.pack('>BBB', self.modbus_address, 0x04, 4)
                resp_data += struct.pack('>f', distance)
                resp_crc = crc16(resp_data)
                resp_data += struct.pack('<H', resp_crc)

                self.ser.write(resp_data)

                if self.request_count % 100 == 0:
                    logging.info(f"[{self.radar_name}] Served {self.request_count} requests, "
                                 f"last distance={distance:.4f}m")

            except serial.SerialException as e:
                if self.running:
                    logging.error(f"[{self.radar_name}] Serial error: {e}")
                break
            except Exception as e:
                if self.running:
                    logging.debug(f"[{self.radar_name}] Error: {e}")

        if self.ser and self.ser.is_open:
            self.ser.close()
        logging.info(f"[{self.radar_name}] Responder stopped (served {self.request_count} requests)")

    def stop(self):
        self.running = False
        if self.ser and self.ser.is_open:
            self.ser.close()


def load_config(config_path, scenario_name):
    """加载场景配置"""
    with open(config_path, 'r') as f:
        cfg = yaml.safe_load(f)

    # 确定使用哪个场景
    if scenario_name is None:
        scenario_name = cfg.get('scenario', 'moderate_sea')

    if scenario_name not in cfg:
        logging.error(f"Scenario '{scenario_name}' not found in {config_path}")
        logging.info(f"Available scenarios: {[k for k in cfg if k not in ('scenario', 'anomalies', 'radar')]}")
        sys.exit(1)

    scenario_cfg = cfg[scenario_name]
    anomaly_cfg = cfg.get('anomalies', {})
    radar_cfg = cfg.get('radar', {})

    logging.info(f"Loaded scenario: {scenario_name}")
    if 'components' in scenario_cfg:
        for i, comp in enumerate(scenario_cfg['components']):
            logging.info(f"  Component {i+1}: Hs={comp['hs']}m, Tp={comp['tp']}s, Dir={comp['direction']}°")
    else:
        logging.info(f"  Hs={scenario_cfg['hs']}m, Tp={scenario_cfg['tp']}s, "
                     f"Dir={scenario_cfg['direction']}°, γ={scenario_cfg.get('gamma', 3.3)}")

    return scenario_cfg, anomaly_cfg, radar_cfg


def main():
    parser = argparse.ArgumentParser(description='VEGA Radar Simulator (Modbus RTU)')
    parser.add_argument('--ports', nargs=3, required=True,
                        help='Three virtual serial port paths (simulator side)')
    parser.add_argument('--scenario', default=None,
                        help='Scenario name (default: from scenarios.yaml)')
    parser.add_argument('--config', default=None,
                        help='Path to scenarios.yaml')
    args = parser.parse_args()

    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )

    # 配置文件路径
    if args.config:
        config_path = args.config
    else:
        config_path = str(Path(__file__).parent / 'scenarios.yaml')

    if not os.path.exists(config_path):
        logging.error(f"Config not found: {config_path}")
        sys.exit(1)

    # 加载配置
    scenario_cfg, anomaly_cfg, radar_cfg = load_config(config_path, args.scenario)

    # 从 system_config.yaml 读取 array_heading
    sys_config_path = Path(__file__).parent.parent / 'mqtt_system' / 'config' / 'system_config.yaml'
    array_heading = 0.0
    if sys_config_path.exists():
        with open(sys_config_path, 'r') as f:
            sys_cfg = yaml.safe_load(f)
        array_heading = sys_cfg.get('radar', {}).get('array_heading', 0.0)
        logging.info(f"Loaded array_heading={array_heading}° from {sys_config_path}")
    else:
        logging.warning(f"system_config.yaml not found at {sys_config_path}, using array_heading=0°")

    # 创建波浪场
    wave_field = WaveField(scenario_cfg, radar_cfg, array_heading=array_heading)

    # 创建异常注入器
    anomaly_injector = AnomalyInjector(anomaly_cfg, wave_field.start_time)

    modbus_address = radar_cfg.get('modbus_address', 246)
    baudrate = radar_cfg.get('baudrate', 9600)

    # 创建并启动3个Modbus应答线程
    responders = []
    for i, port_path in enumerate(args.ports):
        resp = ModbusResponder(i, port_path, wave_field, anomaly_injector,
                               modbus_address, baudrate)
        resp.start()
        responders.append(resp)

    logging.info("=" * 50)
    logging.info("Wave Simulator running. Press Ctrl+C to stop.")
    logging.info("=" * 50)

    # 等待退出信号
    stop_event = threading.Event()

    def signal_handler(signum, frame):
        logging.info(f"Received signal {signum}, shutting down...")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while not stop_event.is_set():
            stop_event.wait(timeout=1.0)
    except KeyboardInterrupt:
        pass

    # 停止所有应答线程
    logging.info("Stopping responders...")
    for resp in responders:
        resp.stop()
    for resp in responders:
        resp.join(timeout=3)

    logging.info("Simulator stopped.")


if __name__ == '__main__':
    main()
