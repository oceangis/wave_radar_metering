#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MQTT数据存储服务
================

功能：
1. 订阅MQTT topic: radar/raw
2. 将原始数据存储到PostgreSQL
3. 订阅 radar/analyzed 存储分析结果
4. 执行数据清理策略

Author: Wave Monitoring System
Date: 2025-11-21
"""

import json
import logging
import signal
import sys
import yaml
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from queue import Queue
from threading import Thread, Event
import time

import paho.mqtt.client as mqtt
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_batch


class DatabaseManager:
    """数据库连接管理器"""

    def __init__(self, db_config: Dict):
        self.config = db_config
        self.pool = None
        self._init_pool()

    def _init_pool(self):
        """初始化连接池"""
        try:
            pool_config = self.config['connection_pool']
            self.pool = psycopg2.pool.SimpleConnectionPool(
                pool_config['min'],
                pool_config['max'],
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            logging.info("Database connection pool initialized")
        except Exception as e:
            logging.error(f"Failed to initialize database pool: {e}")
            raise

    def get_connection(self):
        """获取数据库连接，连接池不可用时抛出异常而非返回None"""
        if self.pool:
            return self.pool.getconn()
        raise RuntimeError("Database connection pool not initialized")

    def release_connection(self, conn):
        """释放数据库连接"""
        if self.pool and conn:
            self.pool.putconn(conn)

    def close_all(self):
        """关闭所有连接"""
        if self.pool:
            self.pool.closeall()
            logging.info("Database connections closed")


class MQTTStorageService:
    """MQTT数据存储服务"""

    def __init__(self, config_path: str):
        """初始化存储服务"""
        # 加载配置
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # 配置日志
        self._setup_logging()

        # 数据库管理器
        self.db_manager = DatabaseManager(self.config['database'])

        # MQTT客户端
        self.mqtt_client = None
        self.mqtt_connected = Event()

        # 数据缓冲队列
        self.raw_data_queue = Queue(maxsize=1000)
        self.analysis_queue = Queue(maxsize=100)

        # 运行控制
        self.running = False
        self.stop_event = Event()

        # 批量插入配置
        self.batch_size = self.config['storage']['batch_size']
        self.batch_timeout = self.config['storage']['batch_timeout']

        # 统计信息
        self.stats = {
            'raw_data_received': 0,
            'raw_data_stored': 0,
            'analysis_received': 0,
            'analysis_stored': 0,
            'errors': 0,
            'data_dropped': 0,
            'last_cleanup': None
        }

        logging.info("MQTT Storage Service initialized")

    def _setup_logging(self):
        """配置日志"""
        log_config = self.config['logging']

        if log_config['file_logging']:
            log_dir = Path(log_config['log_dir'])
            log_dir.mkdir(parents=True, exist_ok=True)

            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                log_dir / 'storage.log',
                maxBytes=log_config['max_bytes'],
                backupCount=log_config['backup_count']
            )
            file_handler.setFormatter(logging.Formatter(log_config['format']))
            logging.getLogger().addHandler(file_handler)

        logging.getLogger().setLevel(getattr(logging, log_config['level']))

    def _setup_mqtt(self):
        """配置MQTT客户端"""
        mqtt_config = self.config['mqtt']

        self.mqtt_client = mqtt.Client(client_id="wave_storage")

        if mqtt_config.get('username'):
            self.mqtt_client.username_pw_set(
                mqtt_config['username'],
                mqtt_config['password']
            )

        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
        self.mqtt_client.on_message = self._on_mqtt_message

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

            # 订阅topics
            topics = self.config['mqtt']['topics']
            client.subscribe(topics['raw_data'])
            client.subscribe(topics['analyzed_data'])
            logging.info(f"Subscribed to {topics['raw_data']}, {topics['analyzed_data']}")
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
            topics = self.config['mqtt']['topics']

            # 原始数据
            if msg.topic == topics['raw_data']:
                self.stats['raw_data_received'] += 1
                if not self.raw_data_queue.full():
                    self.raw_data_queue.put(payload)
                else:
                    self.stats['data_dropped'] += 1
                    if self.stats['data_dropped'] % 10 == 1:
                        logging.warning(f"Raw data queue full, dropping message (total dropped: {self.stats['data_dropped']})")
                    self.stats['errors'] += 1

            # 分析数据
            elif msg.topic == topics['analyzed_data']:
                self.stats['analysis_received'] += 1
                if not self.analysis_queue.full():
                    self.analysis_queue.put(payload)
                else:
                    self.stats['data_dropped'] += 1
                    if self.stats['data_dropped'] % 10 == 1:
                        logging.warning(f"Analysis queue full, dropping message (total dropped: {self.stats['data_dropped']})")
                    self.stats['errors'] += 1

        except Exception as e:
            logging.error(f"Error processing MQTT message: {e}")
            self.stats['errors'] += 1

    def _store_raw_data_batch(self, batch: List[Dict]):
        """批量存储原始数据 - 支持采集器的批量格式"""
        if not batch:
            return

        conn = None
        cursor = None
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()

            # 准备批量插入数据
            values = []
            for item in batch:
                # 新格式：采集器发送的批量格式
                # {"timestamp": "...", "sample": {"timestamps": [...], "heights": [...], "radar_status": [...]}}
                if 'sample' in item:
                    sample = item['sample']
                    timestamps = sample.get('timestamps', [])
                    heights = sample.get('heights', [])
                    radar_status = sample.get('radar_status', [True, True, True])

                    # 每个时间点有3个雷达的数据
                    # heights数组的结构：[时间点数 * 3] 个值，每3个值对应一个时间点的3个雷达
                    num_timestamps = len(timestamps)

                    for i, ts in enumerate(timestamps):
                        # 解析时间戳
                        timestamp = datetime.fromisoformat(ts.replace('Z', '+00:00'))

                        # 每个时间点对应3个雷达的数据
                        # heights数组索引：i*3, i*3+1, i*3+2
                        for radar_id in range(1, 4):  # radar_id: 1, 2, 3
                            height_idx = i * 3 + (radar_id - 1)
                            if height_idx < len(heights):
                                distance = heights[height_idx]
                                # 雷达状态：True=100, False=0
                                quality = 100 if (radar_id - 1 < len(radar_status) and radar_status[radar_id - 1]) else 0
                                values.append((timestamp, radar_id, distance, quality))

                # 旧格式：单条记录格式（向后兼容）
                # {timestamp, radar_id, distance, quality}
                elif 'radar_id' in item and 'distance' in item:
                    timestamp = datetime.fromisoformat(item['timestamp'].replace('Z', '+00:00'))
                    radar_id = item['radar_id']
                    distance = item['distance']
                    quality = item.get('quality', 100)
                    values.append((timestamp, radar_id, distance, quality))

            # 批量插入
            if values:
                execute_batch(
                    cursor,
                    """
                    INSERT INTO wave_measurements
                    (timestamp, radar_id, distance, quality)
                    VALUES (%s, %s, %s, %s)
                    """,
                    values,
                    page_size=self.batch_size
                )

                conn.commit()
                self.stats['raw_data_stored'] += len(values)

                logging.debug(f"Stored {len(values)} raw data records")

        except Exception as e:
            logging.error(f"Batch insert failed ({len(values)} records): {e}")
            self.stats['errors'] += 1
            if conn:
                conn.rollback()
            # 逐条重试，尽量保存有效数据
            if values:
                saved = 0
                for val in values:
                    try:
                        if not conn:
                            conn = self.db_manager.get_connection()
                            cursor = conn.cursor()
                        cursor.execute(
                            "INSERT INTO wave_measurements (timestamp, radar_id, distance, quality) VALUES (%s, %s, %s, %s)",
                            val
                        )
                        conn.commit()
                        saved += 1
                    except Exception:
                        if conn:
                            conn.rollback()
                if saved > 0:
                    self.stats['raw_data_stored'] += saved
                    logging.info(f"Retry saved {saved}/{len(values)} records individually")
                else:
                    logging.error(f"Retry failed: all {len(values)} records lost")
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                self.db_manager.release_connection(conn)

    def _store_analysis_data(self, analysis: Dict):
        """存储分析结果（包含方向谱数据）"""
        conn = None
        cursor = None
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()

            # 提取分析结果
            results = analysis.get('results', {})
            metadata = analysis.get('metadata', {})
            spectrum = analysis.get('spectrum', {})
            time_domain = analysis.get('time_domain', {})

            # 提取方向谱数据（如果存在）
            directional_data = spectrum.get('directional', {}) if spectrum else {}
            directional_spectrum_json = None
            if directional_data and directional_data.get('S2D'):
                directional_spectrum_json = json.dumps({
                    'S2D': directional_data.get('S2D'),
                    'S1D': directional_data.get('S1D'),
                    'freqs': directional_data.get('freqs'),
                    'dirs': directional_data.get('dirs')
                })

            # 插入分析结果（包含完整谱参数和方向谱）
            cursor.execute(
                """
                INSERT INTO wave_analysis
                (start_time, end_time, duration_seconds, sample_count, sample_rate,
                 hs, tp, tz, theta, fp,
                 hs_radar1, hs_radar2, hs_radar3,
                 phase_diff_12, phase_diff_13,
                 hs_zc, hmax, h1_10, h_mean, tmax, t1_10, ts, tmean, wave_count, mean_level,
                 m_minus1, m0, m1, m2, m4,
                 tm01, te, fm, fz, fe, df, f_min, f_max, nf, epsilon_0,
                 spectrum_data, time_domain_data,
                 wave_direction, mean_direction, directional_spread, direction_at_peak,
                 directional_spectrum, diwasp_method, diwasp_success,
                 collection_start_time, collection_end_time,
                 analysis_version, notes)
                VALUES
                (%s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s,
                 %s, %s, %s,
                 %s, %s,
                 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                 %s, %s,
                 %s, %s, %s, %s,
                 %s, %s, %s,
                 %s, %s,
                 %s, %s)
                """,
                (
                    datetime.fromisoformat(metadata.get('start_time', datetime.now(timezone.utc).isoformat())),
                    datetime.fromisoformat(metadata.get('end_time', datetime.now(timezone.utc).isoformat())),
                    metadata.get('duration_seconds', 0),
                    metadata.get('sample_count', 0),
                    metadata.get('sample_rate', 0),
                    results.get('Hm0'),
                    results.get('Tp'),
                    results.get('Tz'),
                    results.get('wave_direction'),  # theta字段（兼容旧代码）
                    results.get('peak_frequency'),
                    results.get('Hm0_radar1'),
                    results.get('Hm0_radar2'),
                    results.get('Hm0_radar3'),
                    results.get('phase_diff_12'),
                    results.get('phase_diff_13'),
                    # 零交叉法参数
                    results.get('Hs'),
                    results.get('Hmax'),
                    results.get('H1_10'),
                    results.get('Hmean'),
                    results.get('Tmax'),
                    results.get('T1_10'),
                    results.get('Ts'),
                    results.get('Tmean'),
                    results.get('wave_count'),
                    results.get('mean_level'),
                    # 谱矩参数
                    results.get('m_minus1'),
                    results.get('m0'),
                    results.get('m1'),
                    results.get('m2'),
                    results.get('m4'),
                    # 周期和频率参数
                    results.get('Tm01'),
                    results.get('Te'),
                    results.get('fm'),
                    results.get('fz'),
                    results.get('fe'),
                    results.get('df'),
                    results.get('f_min'),
                    results.get('f_max'),
                    results.get('Nf'),
                    results.get('epsilon_0'),
                    # 频谱和时域数据（JSONB）
                    json.dumps(spectrum) if spectrum else None,
                    json.dumps(time_domain) if time_domain else None,
                    # 方向谱参数（新增）
                    results.get('wave_direction'),      # 主波向 Dp
                    results.get('mean_direction'),      # 平均波向
                    results.get('directional_spread'),  # 方向分布宽度
                    results.get('direction_at_peak'),   # 峰值周期波向 DTp
                    # 方向谱数据（JSONB）
                    directional_spectrum_json,
                    results.get('diwasp_method'),       # DIWASP方法
                    results.get('diwasp_success', False),  # DIWASP是否成功
                    # 采集时间
                    datetime.fromisoformat(metadata.get('start_time', datetime.now(timezone.utc).isoformat())),
                    datetime.fromisoformat(metadata.get('end_time', datetime.now(timezone.utc).isoformat())),
                    # 版本和备注
                    'v4.0',  # 更新版本号（包含方向谱）
                    json.dumps(results)
                )
            )

            conn.commit()
            self.stats['analysis_stored'] += 1

            # 日志记录（包含方向信息）
            direction_str = f", Dir={results.get('wave_direction'):.1f}°" if results.get('wave_direction') is not None else ""
            logging.info(f"Stored analysis result: Hs={results.get('Hm0', 0):.3f}m{direction_str}")

        except Exception as e:
            logging.error(f"Failed to store analysis data: {e}")
            self.stats['errors'] += 1
            if conn:
                conn.rollback()
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                self.db_manager.release_connection(conn)

    def _cleanup_old_data(self):
        """清理旧数据"""
        if not self.config['storage']['auto_cleanup']:
            return

        conn = None
        cursor = None
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()

            retention = self.config['storage']['retention']

            # 清理原始数据
            raw_cutoff = datetime.now(timezone.utc) - timedelta(days=retention['raw_data_days'])
            cursor.execute(
                "DELETE FROM wave_measurements WHERE created_at < %s",
                (raw_cutoff,)
            )
            raw_deleted = cursor.rowcount

            # 清理分析数据
            analysis_cutoff = datetime.now(timezone.utc) - timedelta(days=retention['analysis_data_days'])
            cursor.execute(
                "DELETE FROM wave_analysis WHERE created_at < %s",
                (analysis_cutoff,)
            )
            analysis_deleted = cursor.rowcount

            # 清理日志
            logs_cutoff = datetime.now(timezone.utc) - timedelta(days=retention['logs_days'])
            cursor.execute(
                "DELETE FROM system_logs WHERE timestamp < %s",
                (logs_cutoff,)
            )
            logs_deleted = cursor.rowcount

            conn.commit()

            if raw_deleted > 0 or analysis_deleted > 0 or logs_deleted > 0:
                logging.info(
                    f"Cleanup: deleted {raw_deleted} raw, "
                    f"{analysis_deleted} analysis, {logs_deleted} logs"
                )

            self.stats['last_cleanup'] = datetime.now(timezone.utc).isoformat()

        except Exception as e:
            logging.error(f"Cleanup failed: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                self.db_manager.release_connection(conn)

    def _raw_data_worker(self):
        """原始数据存储工作线程"""
        batch = []
        last_insert_time = time.time()

        while self.running and not self.stop_event.is_set():
            try:
                # 非阻塞获取数据
                if not self.raw_data_queue.empty():
                    item = self.raw_data_queue.get(timeout=0.1)
                    batch.append(item)

                current_time = time.time()

                # 批量插入条件：达到batch_size或超时
                if (len(batch) >= self.batch_size) or \
                   (batch and (current_time - last_insert_time) >= self.batch_timeout):
                    self._store_raw_data_batch(batch)
                    batch.clear()
                    last_insert_time = current_time

                time.sleep(0.01)

            except Exception as e:
                logging.error(f"Raw data worker error: {e}")

        # 存储剩余数据
        if batch:
            self._store_raw_data_batch(batch)

    def _analysis_data_worker(self):
        """分析数据存储工作线程"""
        while self.running and not self.stop_event.is_set():
            try:
                if not self.analysis_queue.empty():
                    item = self.analysis_queue.get(timeout=0.1)
                    self._store_analysis_data(item)

                time.sleep(0.1)

            except Exception as e:
                logging.error(f"Analysis data worker error: {e}")

    def _cleanup_worker(self):
        """数据清理工作线程"""
        cleanup_interval = self.config['storage']['cleanup_interval']
        next_cleanup = time.time() + cleanup_interval

        while self.running and not self.stop_event.is_set():
            if time.time() >= next_cleanup:
                logging.info("Running data cleanup...")
                self._cleanup_old_data()
                next_cleanup = time.time() + cleanup_interval

            time.sleep(60)  # 每分钟检查一次

    def _publish_status(self):
        """发布服务状态"""
        if not self.mqtt_connected.is_set():
            return

        try:
            status = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'component': 'storage',
                'status': 'running' if self.running else 'stopped',
                'statistics': self.stats,
                'queue_sizes': {
                    'raw_data': self.raw_data_queue.qsize(),
                    'analysis': self.analysis_queue.qsize()
                }
            }

            topic = self.config['mqtt']['topics']['system_status']
            self.mqtt_client.publish(topic, json.dumps(status), qos=1)

        except Exception as e:
            logging.error(f"Failed to publish status: {e}")

    def run(self):
        """运行存储服务"""
        logging.info("="*60)
        logging.info("Starting MQTT Storage Service")
        logging.info("="*60)

        # 设置MQTT
        self._setup_mqtt()

        # 等待MQTT连接
        if not self.mqtt_connected.wait(timeout=10):
            logging.error("MQTT connection timeout")
            return

        # 启动工作线程
        self.running = True

        raw_worker = Thread(target=self._raw_data_worker, daemon=True)
        analysis_worker = Thread(target=self._analysis_data_worker, daemon=True)
        cleanup_worker = Thread(target=self._cleanup_worker, daemon=True)

        raw_worker.start()
        analysis_worker.start()
        cleanup_worker.start()

        logging.info("Storage service started")

        # 状态报告循环
        status_interval = self.config['monitoring']['status_report_interval']
        next_status_time = time.time() + status_interval

        try:
            while self.running and not self.stop_event.is_set():
                if time.time() >= next_status_time:
                    self._publish_status()
                    logging.info(
                        f"Stats: raw_received={self.stats['raw_data_received']}, "
                        f"raw_stored={self.stats['raw_data_stored']}, "
                        f"analysis_stored={self.stats['analysis_stored']}, "
                        f"errors={self.stats['errors']}"
                    )
                    next_status_time = time.time() + status_interval

                time.sleep(1)

        except KeyboardInterrupt:
            logging.info("Interrupted by user")
        finally:
            self.cleanup()

    def stop(self):
        """停止服务"""
        logging.info("Stopping storage service...")
        self.running = False
        self.stop_event.set()

    def cleanup(self):
        """清理资源"""
        logging.info("Cleaning up...")

        # 发布最终状态
        self._publish_status()

        # 断开MQTT
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()

        # 关闭数据库
        self.db_manager.close_all()

        logging.info("Storage service stopped")


def signal_handler(signum, frame):
    """信号处理器"""
    logging.info(f"Received signal {signum}")
    if 'service' in globals():
        service.stop()


if __name__ == '__main__':
    config_path = Path(__file__).parent.parent / 'config' / 'system_config.yaml'

    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)

    service = MQTTStorageService(str(config_path))

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        service.run()
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
