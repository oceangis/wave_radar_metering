#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EC800 ThingsBoard MQTT Bridge - 通过4G模块发送数据
==================================================

功能：
1. 订阅本地 MQTT 的波浪分析结果
2. 通过 EC800 4G 模块的 AT 命令发送到 ThingsBoard

EC800 MQTT AT 命令：
- AT+QMTCFG - 配置 MQTT
- AT+QMTOPEN - 打开网络连接
- AT+QMTCONN - 连接 MQTT broker
- AT+QMTPUB - 发布消息

Author: Wave Monitoring System
Date: 2025-01-09
"""

import json
import logging
import signal
import sys
import yaml
import time
import serial
import re
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Thread, Lock
from queue import Queue

import paho.mqtt.client as mqtt


class EC800MQTTClient:
    """EC800 4G模块 MQTT 客户端"""

    def __init__(self, port: str = '/dev/ttyUSB0', baudrate: int = 115200):
        self.port = port
        self.baudrate = baudrate
        self.ser = None
        self.connected = False
        self.mqtt_connected = False
        self.lock = Lock()

    def connect(self) -> bool:
        """连接串口"""
        try:
            self.ser = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=1
            )
            time.sleep(0.5)
            self.ser.reset_input_buffer()

            # 测试 AT 命令
            if self._send_at("AT", "OK", timeout=2):
                logging.info(f"EC800 connected on {self.port}")
                self.connected = True
                return True
            else:
                logging.error("EC800 not responding to AT command")
                return False
        except Exception as e:
            logging.error(f"Failed to connect EC800: {e}")
            return False

    def disconnect(self):
        """断开连接"""
        if self.ser and self.ser.is_open:
            self.mqtt_disconnect()
            self.ser.close()
            self.connected = False
            logging.info("EC800 disconnected")

    def _send_at(self, cmd: str, expected: str = "OK", timeout: float = 5) -> bool:
        """发送 AT 命令并等待响应"""
        with self.lock:
            try:
                self.ser.reset_input_buffer()
                self.ser.write((cmd + "\r\n").encode())

                start_time = time.time()
                response = ""

                while time.time() - start_time < timeout:
                    if self.ser.in_waiting:
                        data = self.ser.read(self.ser.in_waiting).decode('utf-8', errors='ignore')
                        response += data
                        if expected in response:
                            logging.debug(f"AT CMD: {cmd} -> OK")
                            return True
                        if "ERROR" in response:
                            logging.warning(f"AT CMD: {cmd} -> ERROR: {response}")
                            return False
                    time.sleep(0.1)

                logging.warning(f"AT CMD: {cmd} -> Timeout, response: {response}")
                return False
            except Exception as e:
                logging.error(f"AT command error: {e}")
                return False

    def _send_at_get_response(self, cmd: str, timeout: float = 5) -> str:
        """发送 AT 命令并获取响应"""
        with self.lock:
            try:
                self.ser.reset_input_buffer()
                self.ser.write((cmd + "\r\n").encode())

                start_time = time.time()
                response = ""

                while time.time() - start_time < timeout:
                    if self.ser.in_waiting:
                        data = self.ser.read(self.ser.in_waiting).decode('utf-8', errors='ignore')
                        response += data
                        if "OK" in response or "ERROR" in response:
                            break
                    time.sleep(0.1)

                return response.strip()
            except Exception as e:
                logging.error(f"AT command error: {e}")
                return ""

    def check_network(self) -> bool:
        """检查网络注册状态"""
        # 检查 SIM 卡
        if not self._send_at("AT+CPIN?", "READY", timeout=3):
            logging.error("SIM card not ready")
            return False

        # 检查网络注册
        response = self._send_at_get_response("AT+CEREG?", timeout=3)
        if "+CEREG: 0,1" in response or "+CEREG: 0,5" in response:
            logging.info("Network registered")
        else:
            logging.warning(f"Network not registered: {response}")
            # 尝试等待网络注册
            for _ in range(10):
                time.sleep(2)
                response = self._send_at_get_response("AT+CEREG?", timeout=3)
                if "+CEREG: 0,1" in response or "+CEREG: 0,5" in response:
                    logging.info("Network registered")
                    break
            else:
                logging.error("Failed to register network")
                return False

        # 检查信号强度
        response = self._send_at_get_response("AT+CSQ", timeout=2)
        logging.info(f"Signal: {response}")

        return True

    def mqtt_open(self, host: str, port: int = 1883) -> bool:
        """打开 MQTT 网络连接"""
        # 先关闭可能存在的连接
        self._send_at("AT+QMTCLOSE=0", "OK", timeout=3)
        time.sleep(0.5)

        # 配置 MQTT 版本为 3.1.1
        self._send_at('AT+QMTCFG="version",0,4', "OK", timeout=2)

        # 打开网络连接
        cmd = f'AT+QMTOPEN=0,"{host}",{port}'
        self.ser.reset_input_buffer()
        self.ser.write((cmd + "\r\n").encode())

        # 等待 +QMTOPEN: 0,0 响应
        start_time = time.time()
        while time.time() - start_time < 30:
            if self.ser.in_waiting:
                response = self.ser.read(self.ser.in_waiting).decode('utf-8', errors='ignore')
                if "+QMTOPEN: 0,0" in response:
                    logging.info(f"MQTT network opened to {host}:{port}")
                    return True
                if "+QMTOPEN: 0,-1" in response or "ERROR" in response:
                    logging.error(f"MQTT open failed: {response}")
                    return False
            time.sleep(0.1)

        logging.error("MQTT open timeout")
        return False

    def mqtt_connect(self, client_id: str, username: str = "", password: str = "") -> bool:
        """连接 MQTT broker"""
        # ThingsBoard 使用 access token 作为用户名
        if username:
            cmd = f'AT+QMTCONN=0,"{client_id}","{username}",""'
        else:
            cmd = f'AT+QMTCONN=0,"{client_id}"'

        self.ser.reset_input_buffer()
        self.ser.write((cmd + "\r\n").encode())

        # 等待 +QMTCONN: 0,0,0 响应
        start_time = time.time()
        while time.time() - start_time < 30:
            if self.ser.in_waiting:
                response = self.ser.read(self.ser.in_waiting).decode('utf-8', errors='ignore')
                if "+QMTCONN: 0,0,0" in response:
                    logging.info("MQTT connected")
                    self.mqtt_connected = True
                    return True
                if "+QMTCONN: 0,0,1" in response:
                    logging.error("MQTT connect refused - bad protocol")
                    return False
                if "+QMTCONN: 0,0,2" in response:
                    logging.error("MQTT connect refused - client ID rejected")
                    return False
                if "+QMTCONN: 0,0,4" in response:
                    logging.error("MQTT connect refused - bad credentials")
                    return False
                if "ERROR" in response:
                    logging.error(f"MQTT connect error: {response}")
                    return False
            time.sleep(0.1)

        logging.error("MQTT connect timeout")
        return False

    def mqtt_publish(self, topic: str, payload: str, qos: int = 1) -> bool:
        """发布 MQTT 消息"""
        if not self.mqtt_connected:
            logging.warning("MQTT not connected")
            return False

        # 使用消息模式发送（适合较长的消息）
        payload_len = len(payload)
        cmd = f'AT+QMTPUBEX=0,0,{qos},0,"{topic}",{payload_len}'

        with self.lock:
            try:
                self.ser.reset_input_buffer()
                self.ser.write((cmd + "\r\n").encode())

                # 等待 > 提示符
                start_time = time.time()
                while time.time() - start_time < 5:
                    if self.ser.in_waiting:
                        response = self.ser.read(self.ser.in_waiting).decode('utf-8', errors='ignore')
                        if ">" in response:
                            break
                        if "ERROR" in response:
                            logging.error(f"MQTT publish error: {response}")
                            return False
                    time.sleep(0.05)
                else:
                    logging.error("MQTT publish: no prompt")
                    return False

                # 发送消息内容
                self.ser.write(payload.encode())

                # 等待发布确认
                start_time = time.time()
                while time.time() - start_time < 10:
                    if self.ser.in_waiting:
                        response = self.ser.read(self.ser.in_waiting).decode('utf-8', errors='ignore')
                        if "+QMTPUBEX: 0,0,0" in response or "+QMTPUB: 0,0,0" in response:
                            logging.debug("MQTT publish success")
                            return True
                        if "ERROR" in response:
                            logging.error(f"MQTT publish failed: {response}")
                            return False
                    time.sleep(0.1)

                logging.warning("MQTT publish: no confirmation (may still succeed)")
                return True  # 可能成功了但没收到确认

            except Exception as e:
                logging.error(f"MQTT publish error: {e}")
                return False

    def mqtt_disconnect(self):
        """断开 MQTT 连接"""
        if self.mqtt_connected:
            self._send_at("AT+QMTDISC=0", "OK", timeout=5)
            self._send_at("AT+QMTCLOSE=0", "OK", timeout=5)
            self.mqtt_connected = False
            logging.info("MQTT disconnected")


class EC800ThingsBoardBridge:
    """EC800 ThingsBoard 数据桥接器"""

    def __init__(self, config_path: str):
        """初始化"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # ThingsBoard 配置
        self.tb_config = self.config.get('thingsboard', {})
        self.tb_host = self.tb_config.get('host', '8.155.1.77')
        self.tb_port = self.tb_config.get('port', 1883)
        self.tb_token = self.tb_config.get('access_token', '')

        # EC800 配置
        ec800_config = self.config.get('ec800', {})
        self.ec800_port = ec800_config.get('port', '/dev/ttyUSB0')
        self.ec800_baudrate = ec800_config.get('baudrate', 115200)

        # 配置日志
        self._setup_logging()

        # EC800 客户端
        self.ec800 = EC800MQTTClient(self.ec800_port, self.ec800_baudrate)

        # 本地 MQTT 客户端
        self.local_client = None
        self.local_connected = Event()

        # 消息队列
        self.msg_queue = Queue()

        # 运行控制
        self.running = False
        self.stop_event = Event()

        # 统计
        self.stats = {
            'wave_forwarded': 0,
            'tide_forwarded': 0,
            'errors': 0,
            'reconnects': 0
        }

        logging.info("EC800 ThingsBoard Bridge initialized")
        logging.info(f"  EC800 port: {self.ec800_port}")
        logging.info(f"  ThingsBoard: {self.tb_host}:{self.tb_port}")

    def _setup_logging(self):
        """配置日志"""
        log_config = self.config.get('logging', {})
        log_dir = Path(log_config.get('log_dir', '/home/obsis/radar/mqtt_system/logs'))
        log_dir.mkdir(parents=True, exist_ok=True)

        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_dir / 'ec800_thingsboard.log',
            maxBytes=log_config.get('max_bytes', 10485760),
            backupCount=log_config.get('backup_count', 5)
        )
        file_handler.setFormatter(logging.Formatter(
            log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ))
        logging.getLogger().addHandler(file_handler)
        logging.getLogger().setLevel(getattr(logging, log_config.get('level', 'INFO')))

    def _setup_local_mqtt(self):
        """配置本地 MQTT"""
        mqtt_config = self.config['mqtt']
        self.local_client = mqtt.Client(client_id="ec800_bridge_local")

        if mqtt_config.get('username'):
            self.local_client.username_pw_set(
                mqtt_config['username'],
                mqtt_config['password']
            )

        self.local_client.on_connect = self._on_local_connect
        self.local_client.on_message = self._on_local_message

        self.local_client.connect(
            mqtt_config['broker_host'],
            mqtt_config['broker_port'],
            mqtt_config['keepalive']
        )
        self.local_client.loop_start()

    def _on_local_connect(self, client, userdata, flags, rc):
        """本地 MQTT 连接回调"""
        if rc == 0:
            logging.info("Connected to local MQTT")
            self.local_connected.set()
            client.subscribe(self.config['mqtt']['topics']['analyzed_data'])
            client.subscribe("tide/observation")
        else:
            logging.error(f"Local MQTT connection failed: {rc}")

    def _on_local_message(self, client, userdata, msg):
        """本地 MQTT 消息回调"""
        try:
            payload = json.loads(msg.payload.decode())
            self.msg_queue.put((msg.topic, payload))
        except Exception as e:
            logging.error(f"Error processing message: {e}")

    def _connect_ec800_mqtt(self) -> bool:
        """连接 EC800 MQTT"""
        # 连接 EC800 串口
        if not self.ec800.connected:
            if not self.ec800.connect():
                return False

        # 检查网络
        if not self.ec800.check_network():
            return False

        # 打开 MQTT 连接
        if not self.ec800.mqtt_open(self.tb_host, self.tb_port):
            return False

        # 连接 MQTT broker
        if not self.ec800.mqtt_connect("wave_monitor", self.tb_token):
            return False

        return True

    def _process_queue(self):
        """处理消息队列"""
        while not self.msg_queue.empty():
            try:
                topic, payload = self.msg_queue.get_nowait()

                if "analyzed" in topic:
                    self._forward_wave_data(payload)
                elif "tide" in topic:
                    self._forward_tide_data(payload)

            except Exception as e:
                logging.error(f"Queue processing error: {e}")
                self.stats['errors'] += 1

    def _forward_wave_data(self, data: dict):
        """转发波浪数据"""
        if not self.ec800.mqtt_connected:
            return

        try:
            results = data.get('results', {})

            telemetry = {
                'Hm0': round(results.get('Hm0', 0), 3),
                'Hs': round(results.get('Hs', 0), 3),
                'Hmax': round(results.get('Hmax', 0), 3),
                'Tp': round(results.get('Tp', 0), 2),
                'Tz': round(results.get('Tz', 0), 2),
                'mean_level': round(results.get('mean_level', 0), 2),
                'wave_direction': results.get('wave_direction'),
                'radar_count': results.get('radar_count', 0)
            }

            # 移除 None 值
            telemetry = {k: v for k, v in telemetry.items() if v is not None}

            payload = json.dumps(telemetry)

            if self.ec800.mqtt_publish("v1/devices/me/telemetry", payload):
                self.stats['wave_forwarded'] += 1
                logging.info(f"Wave data sent via EC800: Hm0={results.get('Hm0', 0):.3f}m")
            else:
                self.stats['errors'] += 1

        except Exception as e:
            logging.error(f"Failed to forward wave data: {e}")
            self.stats['errors'] += 1

    def _forward_tide_data(self, data: dict):
        """转发潮汐数据"""
        if not self.ec800.mqtt_connected:
            return

        try:
            telemetry = {
                'tide_level': round(data.get('tide_level', 0), 3),
                'tide_trend': data.get('trend', 'unknown')
            }

            telemetry = {k: v for k, v in telemetry.items() if v is not None}
            payload = json.dumps(telemetry)

            if self.ec800.mqtt_publish("v1/devices/me/telemetry", payload):
                self.stats['tide_forwarded'] += 1
                logging.info(f"Tide data sent via EC800")
            else:
                self.stats['errors'] += 1

        except Exception as e:
            logging.error(f"Failed to forward tide data: {e}")
            self.stats['errors'] += 1

    def run(self):
        """运行服务"""
        logging.info("=" * 60)
        logging.info("Starting EC800 ThingsBoard Bridge")
        logging.info("=" * 60)

        # 设置本地 MQTT
        self._setup_local_mqtt()

        if not self.local_connected.wait(timeout=10):
            logging.error("Local MQTT connection timeout")
            return

        # 连接 EC800 MQTT
        if not self._connect_ec800_mqtt():
            logging.error("EC800 MQTT connection failed")
            return

        self.running = True
        logging.info("EC800 Bridge running...")

        last_status_time = time.time()
        reconnect_interval = 60

        try:
            while self.running and not self.stop_event.is_set():
                # 处理消息队列
                self._process_queue()

                # 检查连接状态
                if not self.ec800.mqtt_connected:
                    logging.warning("EC800 MQTT disconnected, reconnecting...")
                    if self._connect_ec800_mqtt():
                        self.stats['reconnects'] += 1
                    else:
                        time.sleep(reconnect_interval)

                # 定期打印状态
                if time.time() - last_status_time > 60:
                    logging.info(
                        f"Stats: wave={self.stats['wave_forwarded']}, "
                        f"tide={self.stats['tide_forwarded']}, "
                        f"errors={self.stats['errors']}, "
                        f"reconnects={self.stats['reconnects']}"
                    )
                    last_status_time = time.time()

                time.sleep(0.5)

        except KeyboardInterrupt:
            logging.info("Interrupted")
        finally:
            self.cleanup()

    def stop(self):
        """停止服务"""
        self.running = False
        self.stop_event.set()

    def cleanup(self):
        """清理资源"""
        logging.info("Cleaning up...")

        if self.local_client:
            self.local_client.loop_stop()
            self.local_client.disconnect()

        self.ec800.disconnect()

        logging.info("EC800 Bridge stopped")


def signal_handler(signum, frame):
    if 'bridge' in globals():
        bridge.stop()


if __name__ == '__main__':
    config_path = Path(__file__).parent.parent / 'config' / 'system_config.yaml'

    if not config_path.exists():
        print(f"Config not found: {config_path}")
        sys.exit(1)

    bridge = EC800ThingsBoardBridge(str(config_path))

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        bridge.run()
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
