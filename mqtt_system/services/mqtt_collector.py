#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MQTT数据采集服务
================

功能：
1. 从三个雷达读取Modbus数据
2. 将原始数据发布到MQTT topic: radar/raw
3. 监控设备状态并发布到 system/status
4. 订阅系统配置更新 system/config

Author: Wave Monitoring System
Date: 2025-11-21
"""

import serial
import struct
import time
import json
import logging
import signal
import sys
import yaml
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from pathlib import Path

import paho.mqtt.client as mqtt
from threading import Thread, Event, Lock


class VegaRadarReader:
    """VEGA雷达Modbus读取器（增强QC版本）"""

    def __init__(self, port: str, baudrate: int = 9600, address: int = 246, radar_id: int = 1, qc_config: Dict = None, timeout: float = 0.5):
        self.port = port
        self.baudrate = baudrate
        self.address = address
        self.radar_id = radar_id
        self.timeout = timeout
        self.ser: Optional[serial.Serial] = None
        self.is_connected = False
        self.error_count = 0
        self.success_count = 0

        # ========== 实时QC参数 ==========
        self.qc_config = qc_config or {}
        self.valid_range = self.qc_config.get('valid_range', [0.0, 10.0])
        self.max_rate_of_change = self.qc_config.get('max_rate_of_change', 1.0)
        self.flat_line_count = self.qc_config.get('flat_line_count', 3)
        self.flat_line_tolerance = self.qc_config.get('flat_line_tolerance', 0.001)

        # 采样率（用于变化率计算）
        self.config_sample_rate = 6.0  # Hz，默认6Hz

        # 历史数据缓冲（用于实时QC）
        self.history_buffer = []
        self.history_max_size = 100  # 保留最近100个样本

        # QC统计
        self.qc_stats = {
            'total_samples': 0,
            'out_of_range': 0,
            'rate_exceeded': 0,
            'flat_line': 0,
            'passed': 0
        }

    def connect(self) -> bool:
        """连接串口"""
        try:
            self.ser = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                bytesize=8,
                parity='N',
                stopbits=1,
                timeout=self.timeout
            )
            self.is_connected = True
            logging.info(f"[Radar {self.radar_id}] Connected to {self.port}")
            time.sleep(0.2)
            return True
        except Exception as e:
            logging.error(f"[Radar {self.radar_id}] Connection failed: {e}")
            # 如果Serial()构造成功但后续出错，关闭已打开的串口
            if self.ser and self.ser.is_open:
                self.ser.close()
            self.ser = None
            self.is_connected = False
            return False

    def disconnect(self):
        """断开连接"""
        if self.ser and self.ser.is_open:
            self.ser.close()
            self.is_connected = False
            logging.info(f"[Radar {self.radar_id}] Disconnected")

    def crc16(self, data: bytes) -> int:
        """计算Modbus CRC16"""
        crc = 0xFFFF
        for byte in data:
            crc ^= byte
            for _ in range(8):
                if crc & 1:
                    crc = (crc >> 1) ^ 0xA001
                else:
                    crc >>= 1
        return crc

    def quality_control(self, height: float) -> Tuple[float, int, str]:
        """
        实时质量控制（Layer 1 QC）

        参数:
            height: 原始测距值（米）

        返回:
            (height, quality_score, qc_flag)
            - height: QC后的值（可能被修正或保持原值）
            - quality_score: 质量评分（0-100）
            - qc_flag: 质量标志字符串
        """
        self.qc_stats['total_samples'] += 1
        self.qc_stats['passed'] += 1

        # QC暂时关闭，直接返回原始数据（厂家调试用）
        return height, 100, 'GOOD'

    def read_height(self) -> Optional[Dict]:
        """
        读取高度值并进行实时QC

        返回:
            包含值和质量信息的字典，或None（连接失败）
        """
        if not self.is_connected:
            return None

        try:
            # 读取输入寄存器 2004（功能码04）
            req = struct.pack('>BBHH', self.address, 0x04, 2004, 2)
            req += struct.pack('<H', self.crc16(req))

            self.ser.reset_input_buffer()
            self.ser.write(req)

            time.sleep(0.02)
            resp = self.ser.read(100)

            if len(resp) < 9:  # addr(1)+func(1)+bytecount(1)+data(4)+crc(2)
                self.error_count += 1
                return None

            # 验证CRC
            calc_crc = self.crc16(resp[:-2])
            recv_crc = struct.unpack('<H', resp[-2:])[0]

            if calc_crc != recv_crc:
                self.error_count += 1
                return None

            # 解析高度值
            byte_count = resp[2]
            if byte_count >= 4:
                height = struct.unpack('>f', resp[3:7])[0]
                self.success_count += 1

                # ========== 实时质量控制 ==========
                height_qc, quality_score, qc_flag = self.quality_control(height)

                return {
                    'value': height_qc,
                    'quality': quality_score,
                    'qc_flag': qc_flag,
                    'raw_value': height
                }

        except Exception as e:
            logging.debug(f"[Radar {self.radar_id}] Read error: {e}")
            self.error_count += 1

        return None


class MQTTDataCollector:
    """MQTT数据采集服务"""

    def __init__(self, config_path: str):
        """初始化采集器"""
        # 加载配置
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # 配置日志
        self._setup_logging()

        # 初始化雷达（传递QC配置）
        self.radars = []
        qc_config = self.config['analysis'].get('qc', {})
        for i, port in enumerate(self.config['radar']['ports']):
            radar = VegaRadarReader(
                port=port,
                baudrate=self.config['radar']['baudrate'],
                address=self.config['radar']['modbus_address'],
                radar_id=i + 1,
                qc_config=qc_config,
                timeout=self.config['radar'].get('timeout', 0.5)
            )
            radar.config_sample_rate = self.config['collection']['sample_rate']
            self.radars.append(radar)

        # MQTT客户端
        self.mqtt_client = None
        self.mqtt_connected = Event()

        # 运行控制
        self.running = False
        self.stop_event = Event()

        # 本地缓存（MQTT断连时暂存数据）
        self.cache_dir = Path(self.config['logging']['log_dir']).parent / 'cache'
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_file = self.cache_dir / 'offline_data.jsonl'
        self._cache_lock = Lock()
        self._replay_thread = None

        # 统计信息
        self.stats = {
            'start_time': None,
            'samples_collected': 0,
            'samples_published': 0,
            'samples_cached': 0,
            'cache_replayed': 0,
            'errors': 0,
            'last_sample_time': None
        }

        logging.info("MQTT Data Collector initialized")

    def _setup_logging(self):
        """配置日志"""
        log_config = self.config['logging']

        # 创建日志目录
        if log_config['file_logging']:
            log_dir = Path(log_config['log_dir'])
            log_dir.mkdir(parents=True, exist_ok=True)

            # 文件处理器
            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                log_dir / 'collector.log',
                maxBytes=log_config['max_bytes'],
                backupCount=log_config['backup_count']
            )
            file_handler.setFormatter(logging.Formatter(log_config['format']))
            logging.getLogger().addHandler(file_handler)

        # 设置日志级别
        logging.getLogger().setLevel(getattr(logging, log_config['level']))

    def _setup_mqtt(self):
        """配置MQTT客户端"""
        mqtt_config = self.config['mqtt']

        self.mqtt_client = mqtt.Client(client_id="wave_collector")

        # 设置用户名密码
        if mqtt_config.get('username'):
            self.mqtt_client.username_pw_set(
                mqtt_config['username'],
                mqtt_config['password']
            )

        # 设置回调
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
        self.mqtt_client.on_message = self._on_mqtt_message

        # 连接MQTT broker
        try:
            self.mqtt_client.connect(
                mqtt_config['broker_host'],
                mqtt_config['broker_port'],
                mqtt_config['keepalive']
            )
            self.mqtt_client.loop_start()
            logging.info("MQTT client started")
        except Exception as e:
            logging.error(f"Failed to connect to MQTT broker: {e}")
            raise

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """MQTT连接回调"""
        if rc == 0:
            logging.info("Connected to MQTT broker")
            self.mqtt_connected.set()

            # 订阅配置和命令topic
            topics = self.config['mqtt']['topics']
            client.subscribe(topics['system_config'])
            client.subscribe(topics['system_command'])
            logging.info(f"Subscribed to {topics['system_config']}, {topics['system_command']}")

            # 重发本地缓存数据（在独立线程中执行，避免阻塞MQTT网络线程）
            self._replay_thread = Thread(target=self._replay_cache, daemon=True)
            self._replay_thread.start()
        else:
            logging.error(f"MQTT connection failed with code {rc}")

    def _on_mqtt_disconnect(self, client, userdata, rc):
        """MQTT断开连接回调"""
        logging.warning(f"Disconnected from MQTT broker (code {rc})")
        self.mqtt_connected.clear()

    def _on_mqtt_message(self, client, userdata, msg):
        """MQTT消息回调"""
        try:
            payload = json.loads(msg.payload.decode())
            logging.info(f"Received message on {msg.topic}: {payload}")

            # 处理配置更新
            if msg.topic == self.config['mqtt']['topics']['system_config']:
                self._handle_config_update(payload)

            # 处理系统命令
            elif msg.topic == self.config['mqtt']['topics']['system_command']:
                self._handle_system_command(payload)

        except Exception as e:
            logging.error(f"Error processing MQTT message: {e}")

    def _handle_config_update(self, config_update: Dict):
        """处理配置更新"""
        logging.info(f"Config update received: {config_update}")
        # 这里可以实现动态配置更新逻辑
        # 例如：更新采样率、滤波参数等

    def _handle_system_command(self, command: Dict):
        """处理系统命令"""
        cmd = command.get('command')
        logging.info(f"System command received: {cmd}")

        if cmd == 'stop':
            logging.info("Stop command received, shutting down...")
            self.stop()
        elif cmd == 'restart':
            logging.info("Restart command received")
            # 实现重启逻辑
        elif cmd == 'status':
            self._publish_status()

    def connect_radars(self) -> bool:
        """连接所有雷达"""
        logging.info("Connecting to radars...")
        success = all(radar.connect() for radar in self.radars)

        if success:
            logging.info("All radars connected successfully")
        else:
            logging.warning("Some radars failed to connect")

        return success

    def disconnect_radars(self):
        """断开所有雷达"""
        for radar in self.radars:
            radar.disconnect()

    def collect_sample(self) -> Optional[Dict]:
        """采集一个样本（三个雷达并行，包含QC信息）

        每个雷达在各自线程中读取，读取完成后记录独立时间戳。
        真实时间差通过后续分析阶段的插值对齐来处理。
        """
        sample = {
            'timestamps': [None] * len(self.radars),
            'heights': [None] * len(self.radars),
            'qualities': [0] * len(self.radars),
            'qc_flags': ['NO_DATA'] * len(self.radars),
            'radar_status': [False] * len(self.radars)
        }
        sample_lock = Lock()

        max_retry = self.config['collection'].get('max_retry', 3)
        retry_delay = self.config['collection'].get('retry_delay', 0.1)

        def read_radar(index: int, radar: VegaRadarReader):
            """线程函数：读取单个雷达（带重试）"""
            result = None
            timestamp = None
            for attempt in range(max_retry):
                ts_before = datetime.now(timezone.utc)
                result = radar.read_height()
                if result is not None:
                    timestamp = ts_before
                    break
                if attempt < max_retry - 1:
                    time.sleep(retry_delay)
            if timestamp is None:
                timestamp = datetime.now(timezone.utc)

            with sample_lock:
                sample['timestamps'][index] = timestamp.isoformat()
                if result is not None:
                    sample['heights'][index] = result['value']
                    sample['qualities'][index] = result['quality']
                    sample['qc_flags'][index] = result['qc_flag']
                    sample['radar_status'][index] = True
                else:
                    sample['heights'][index] = None
                    sample['qualities'][index] = 0
                    sample['qc_flags'][index] = 'NO_CONNECTION'
                    sample['radar_status'][index] = False

        # 创建并启动线程（并行读取）
        threads = []
        for i, radar in enumerate(self.radars):
            thread = Thread(target=read_radar, args=(i, radar))
            thread.start()
            threads.append(thread)

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 检查是否至少有一个有效数据
        if any(h is not None for h in sample['heights']):
            return sample
        else:
            return None

    def _publish_data(self, sample: Dict):
        """发布数据到MQTT，失败时写入本地缓存"""
        message = {
            'timestamp': sample['timestamps'][0],
            'sample': sample,
            'metadata': {
                'collector_id': 'wave_collector',
                'sample_rate': self.config['collection']['sample_rate']
            }
        }

        if not self.mqtt_connected.is_set():
            self._cache_message(message)
            return

        try:
            topic = self.config['mqtt']['topics']['raw_data']
            self.mqtt_client.publish(
                topic,
                json.dumps(message),
                qos=1
            )
            self.stats['samples_published'] += 1

        except Exception as e:
            logging.error(f"Failed to publish data: {e}")
            self.stats['errors'] += 1
            self._cache_message(message)

    # 缓存文件最大50MB，超过后丢弃最旧数据
    MAX_CACHE_BYTES = 50 * 1024 * 1024

    def _cache_message(self, message: Dict):
        """将消息写入本地缓存文件（线程安全，有大小限制）"""
        try:
            with self._cache_lock:
                # 检查缓存文件大小
                if self.cache_file.exists() and self.cache_file.stat().st_size >= self.MAX_CACHE_BYTES:
                    logging.warning(f"Cache file exceeds {self.MAX_CACHE_BYTES // (1024*1024)}MB limit, discarding oldest half")
                    with open(self.cache_file, 'r') as f:
                        lines = f.readlines()
                    # 保留后半部分（较新的数据）
                    with open(self.cache_file, 'w') as f:
                        f.writelines(lines[len(lines) // 2:])
                with open(self.cache_file, 'a') as f:
                    f.write(json.dumps(message) + '\n')
            self.stats['samples_cached'] += 1
        except Exception as e:
            logging.error(f"Failed to cache message: {e}")

    def _replay_cache(self):
        """重连后重发本地缓存数据（线程安全，在独立线程中调用）"""
        with self._cache_lock:
            if not self.cache_file.exists():
                return

            try:
                with open(self.cache_file, 'r') as f:
                    lines = f.readlines()

                if not lines:
                    return

                logging.info(f"Replaying {len(lines)} cached messages")
                topic = self.config['mqtt']['topics']['raw_data']
                replayed = 0

                for line in lines:
                    try:
                        message = json.loads(line.strip())
                        self.mqtt_client.publish(topic, json.dumps(message), qos=1)
                        replayed += 1
                    except Exception as e:
                        logging.warning(f"Failed to replay cached message: {e}")

                # 清空缓存文件
                self.cache_file.unlink(missing_ok=True)
                self.stats['cache_replayed'] += replayed
                logging.info(f"Replayed {replayed}/{len(lines)} cached messages")

            except Exception as e:
                logging.error(f"Failed to replay cache: {e}")

    def _publish_status(self):
        """发布系统状态"""
        if not self.mqtt_connected.is_set():
            return

        try:
            # 计算运行时间
            uptime = (datetime.now(timezone.utc) - self.stats['start_time']).total_seconds() \
                if self.stats['start_time'] else 0

            # 计算实际采样率
            actual_rate = self.stats['samples_collected'] / uptime if uptime > 0 else 0

            status = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'component': 'collector',
                'status': 'running' if self.running else 'stopped',
                'radars': [
                    {
                        'id': i + 1,
                        'port': radar.port,
                        'connected': radar.is_connected,
                        'success_count': radar.success_count,
                        'error_count': radar.error_count
                    }
                    for i, radar in enumerate(self.radars)
                ],
                'statistics': {
                    'uptime_seconds': uptime,
                    'samples_collected': self.stats['samples_collected'],
                    'samples_published': self.stats['samples_published'],
                    'errors': self.stats['errors'],
                    'actual_sample_rate': actual_rate,
                    'last_sample_time': self.stats['last_sample_time']
                }
            }

            topic = self.config['mqtt']['topics']['system_status']
            self.mqtt_client.publish(topic, json.dumps(status), qos=1)

        except Exception as e:
            logging.error(f"Failed to publish status: {e}")

    def _publish_status_waiting(self):
        """发布等待雷达连接的状态"""
        if not self.mqtt_connected.is_set():
            return

        try:
            # 计算运行时间
            uptime = (datetime.now(timezone.utc) - self.stats['start_time']).total_seconds() \
                if self.stats['start_time'] else 0

            status = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'component': 'collector',
                'status': 'waiting_for_radars',
                'radars': [
                    {
                        'id': i + 1,
                        'port': radar.port,
                        'connected': False,
                        'success_count': 0,
                        'error_count': radar.error_count
                    }
                    for i, radar in enumerate(self.radars)
                ],
                'statistics': {
                    'uptime_seconds': uptime,
                    'samples_collected': 0,
                    'samples_published': 0,
                    'errors': 0,
                    'actual_sample_rate': 0,
                    'last_sample_time': None
                },
                'message': 'Waiting for radar devices to become available'
            }

            topic = self.config['mqtt']['topics']['system_status']
            self.mqtt_client.publish(topic, json.dumps(status), qos=1)

        except Exception as e:
            logging.error(f"Failed to publish waiting status: {e}")

    def run(self):
        """运行采集服务"""
        logging.info("="*60)
        logging.info("Starting MQTT Data Collector")
        logging.info("="*60)

        # 设置MQTT
        self._setup_mqtt()

        # 等待MQTT连接
        if not self.mqtt_connected.wait(timeout=10):
            logging.error("MQTT connection timeout")
            return

        # 标记服务运行
        self.running = True
        self.stats['start_time'] = datetime.now(timezone.utc)

        # 雷达连接重试机制
        radar_retry_interval = self.config.get('collection', {}).get('radar_retry_interval', 30)
        radars_connected = self.connect_radars()

        if not radars_connected:
            logging.warning("Failed to connect to radars initially, will retry every {} seconds".format(radar_retry_interval))

            # 进入等待模式，定期重试连接
            next_retry_time = time.time() + radar_retry_interval
            status_interval = self.config['monitoring']['status_report_interval']
            next_status_time = time.time() + status_interval

            try:
                while self.running and not self.stop_event.is_set():
                    current_time = time.time()

                    # 尝试重新连接雷达
                    if current_time >= next_retry_time:
                        logging.info("Attempting to reconnect to radars...")
                        radars_connected = self.connect_radars()

                        if radars_connected:
                            logging.info("Radars connected successfully, starting data collection")
                            break
                        else:
                            logging.warning("Radar connection failed, will retry in {} seconds".format(radar_retry_interval))
                            next_retry_time = current_time + radar_retry_interval

                    # 定期发布状态（标记为等待雷达）
                    if current_time >= next_status_time:
                        self._publish_status_waiting()
                        next_status_time = current_time + status_interval

                    # 休眠
                    time.sleep(1)

            except KeyboardInterrupt:
                logging.info("Interrupted by user during radar connection retry")
                self.cleanup()
                return
            except Exception as e:
                logging.error(f"Error during radar connection retry: {e}", exc_info=True)
                self.cleanup()
                return

        # 如果服务已停止，直接返回
        if not self.running or self.stop_event.is_set():
            self.cleanup()
            return

        sample_rate = self.config['collection']['sample_rate']
        interval = 1.0 / sample_rate
        publish_interval = self.config['collection']['publish_interval']
        status_interval = self.config['monitoring']['status_report_interval']

        next_sample_time = time.time()
        next_publish_time = time.time()
        next_status_time = time.time()

        sample_buffer = []
        max_buffer_size = self.config['collection'].get('buffer_size', 1024)

        logging.info(f"Collection started: {sample_rate} Hz")

        try:
            while self.running and not self.stop_event.is_set():
                current_time = time.time()

                # 采集样本
                if current_time >= next_sample_time:
                    sample = self.collect_sample()

                    if sample:
                        if len(sample_buffer) >= max_buffer_size:
                            logging.warning(f"Sample buffer full ({max_buffer_size}), caching oldest to disk")
                            self._cache_message({
                                'timestamp': sample_buffer[0]['timestamps'][0],
                                'sample': sample_buffer[0],
                                'metadata': {'collector_id': 'wave_collector', 'sample_rate': sample_rate}
                            })
                            sample_buffer.pop(0)
                        sample_buffer.append(sample)
                        self.stats['samples_collected'] += 1
                        self.stats['last_sample_time'] = datetime.now(timezone.utc).isoformat()

                        # 定期打印
                        if self.stats['samples_collected'] % 20 == 0:
                            logging.info(
                                f"Collected {self.stats['samples_collected']} samples, "
                                f"published {self.stats['samples_published']}"
                            )

                    next_sample_time += interval

                # 发布数据
                if current_time >= next_publish_time and sample_buffer:
                    for sample in sample_buffer:
                        self._publish_data(sample)
                    sample_buffer.clear()
                    next_publish_time += publish_interval

                # 发布状态
                if current_time >= next_status_time:
                    self._publish_status()
                    next_status_time += status_interval

                # 短暂休眠
                time.sleep(0.01)

        except KeyboardInterrupt:
            logging.info("Interrupted by user")
        except Exception as e:
            logging.error(f"Collection error: {e}", exc_info=True)
        finally:
            self.cleanup()

    def stop(self):
        """停止采集"""
        logging.info("Stopping collector...")
        self.running = False
        self.stop_event.set()

    def cleanup(self):
        """清理资源"""
        logging.info("Cleaning up...")

        # 发布最终状态
        self._publish_status()

        # 断开雷达
        self.disconnect_radars()

        # 等待缓存重发线程完成（最多等10秒）
        if self._replay_thread and self._replay_thread.is_alive():
            logging.info("Waiting for cache replay to finish...")
            self._replay_thread.join(timeout=10)
            if self._replay_thread.is_alive():
                logging.warning("Cache replay thread did not finish in time")

        # 断开MQTT
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()

        logging.info("Collector stopped")


def signal_handler(signum, frame):
    """信号处理器"""
    logging.info(f"Received signal {signum}")
    if 'collector' in globals():
        collector.stop()


if __name__ == '__main__':
    # 配置文件路径
    config_path = Path(__file__).parent.parent / 'config' / 'system_config.yaml'

    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)

    # 创建采集器
    collector = MQTTDataCollector(str(config_path))

    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 运行
    try:
        collector.run()
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
