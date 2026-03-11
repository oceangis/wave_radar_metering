#!/usr/bin/env python3
"""
MQTT Tide Analyzer Service
从雷达1数据计算实时潮位，记录水位观测值
"""

import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import paho.mqtt.client as mqtt
import psycopg2
import yaml
from threading import Event


class TideAnalyzer:
    """潮位分析器 - 记录水位观测值"""

    def __init__(self, config: Dict):
        self.config = config
        self.db_config = config['database']
        self.mqtt_config = config['mqtt']
        self.radar_config = config['radar']

        # 与波浪分析共用滑动窗口间隔
        self.observation_interval = config.get('analysis', {}).get('analysis_interval', 300)
        # 潮位专用窗口时长
        self.tide_window_duration = config.get('analysis', {}).get('tide_window_duration', 300)

        # 自动分析开关
        self.auto_analysis = config.get('analysis', {}).get('auto_analysis', True)

        # 按需分析（串口命令触发）
        self.on_demand_event = Event()

        # MQTT客户端
        self.mqtt_client = None

        # 数据库连接
        self.db_conn = None

        logging.info("TideAnalyzer initialized")
        logging.info(f"  Observation interval: {self.observation_interval}s (shared with wave analysis)")
        logging.info(f"  Tide window duration: {self.tide_window_duration}s")

    def connect_db(self):
        """连接数据库（带指数退避重试）"""
        max_retries = 5
        base_delay = 2
        for attempt in range(max_retries):
            try:
                self.db_conn = psycopg2.connect(
                    host=self.db_config['host'],
                    port=self.db_config['port'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    database=self.db_config['database']
                )
                logging.info("Database connected")
                return
            except Exception as e:
                delay = base_delay * (2 ** attempt)
                if attempt < max_retries - 1:
                    logging.warning(f"DB connect attempt {attempt + 1}/{max_retries} failed: {e}, retrying in {delay}s")
                    time.sleep(delay)
                else:
                    logging.error(f"Failed to connect database after {max_retries} attempts: {e}")
                    raise

    def get_radar1_tide_data(self, hours: float = None) -> Tuple[np.ndarray, np.ndarray]:
        """
        从数据库获取雷达1的潮位数据

        Args:
            hours: 获取最近N小时的数据，如果为None则获取所有数据

        Returns:
            (timestamps, tide_levels): 时间戳数组和潮位数组
        """
        try:
            cursor = self.db_conn.cursor()

            array_height = self.radar_config.get('array_height', 5.0)

            if hours:
                start_time = datetime.now() - timedelta(hours=hours)
                query = """
                    SELECT timestamp, distance
                    FROM wave_measurements
                    WHERE radar_id = 1 AND timestamp > %s
                    ORDER BY timestamp
                """
                cursor.execute(query, (start_time,))
            else:
                query = """
                    SELECT timestamp, distance
                    FROM wave_measurements
                    WHERE radar_id = 1
                    ORDER BY timestamp
                """
                cursor.execute(query)

            results = cursor.fetchall()
            cursor.close()

            if not results:
                return np.array([]), np.array([])

            timestamps = np.array([r[0].timestamp() for r in results])
            distances = np.array([r[1] for r in results])

            # 计算潮位 = 阵列高度 - 雷达测距
            tide_levels = array_height - distances

            logging.info(f"Retrieved {len(timestamps)} radar1 samples")
            logging.info(f"  Time range: {results[0][0]} to {results[-1][0]}")
            logging.info(f"  Tide range: {tide_levels.min():.3f}m to {tide_levels.max():.3f}m")

            return timestamps, tide_levels

        except Exception as e:
            logging.error(f"Failed to get radar1 data: {e}")
            return np.array([]), np.array([])

    def record_tide_observation(self):
        """
        记录当前水位观测值
        使用 tide_window_duration 滑动窗口，对窗口内数据取均值提取水位
        波浪周期(3-25秒)远小于窗口长度，均值天然消除波浪分量
        """
        try:
            # 使用潮位专用窗口时长
            window_hours = self.tide_window_duration / 3600.0
            timestamps, tide_levels = self.get_radar1_tide_data(hours=window_hours)

            if len(tide_levels) == 0:
                return

            # 异常值剔除（3-sigma，基于中位数）
            original_count = len(tide_levels)
            median = np.median(tide_levels)
            std = np.std(tide_levels)

            mask = np.abs(tide_levels - median) < 3 * std
            tide_levels = tide_levels[mask]

            outliers_removed = original_count - len(tide_levels)
            if outliers_removed > 0:
                logging.info(f"Removed {outliers_removed} outliers ({outliers_removed/original_count*100:.1f}%) from water level data")

            if len(tide_levels) == 0:
                logging.warning("All data points were outliers, skipping observation")
                return

            # 窗口内取均值即为当前水位
            observed_tide = float(np.mean(tide_levels))

            logging.debug(f"Water level observation: {observed_tide:.4f}m from {len(tide_levels)} samples")

            radar1_distance = self.radar_config.get('array_height', 5.0) - observed_tide

            # 存储到数据库
            cursor = self.db_conn.cursor()
            query = """
                INSERT INTO tide_observations (observation_time, observed_tide_level,
                                              radar1_distance, array_height, quality_flag)
                VALUES (%s, %s, %s, %s, 0)
                ON CONFLICT (observation_time) DO UPDATE
                SET observed_tide_level = EXCLUDED.observed_tide_level,
                    radar1_distance = EXCLUDED.radar1_distance
            """

            obs_time = datetime.now()

            cursor.execute(query, (obs_time, observed_tide, radar1_distance,
                                  self.radar_config.get('array_height', 5.0)))
            self.db_conn.commit()
            cursor.close()

            # 发布到MQTT
            self.publish_tide_observation(obs_time, observed_tide)

        except Exception as e:
            logging.error(f"Failed to record tide observation: {e}")

    def publish_tide_observation(self, obs_time: datetime, tide_level: float):
        """发布潮位观测值到MQTT"""
        try:
            message = json.dumps({
                'time': obs_time.isoformat(),
                'tide_level': tide_level,
                'array_height': self.radar_config.get('array_height', 5.0)
            })

            self.mqtt_client.publish('tide/observation', message, qos=0)

        except Exception as e:
            logging.error(f"Failed to publish tide observation: {e}")

    def run(self):
        """主运行循环"""
        logging.info("Starting MQTT Tide Analyzer Service")

        # 连接数据库
        self.connect_db()

        # 连接MQTT
        self.mqtt_client = mqtt.Client(client_id="tide_analyzer")
        self.mqtt_client.username_pw_set(
            self.mqtt_config['username'],
            self.mqtt_config['password']
        )

        # 订阅控制命令（按需分析）
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                client.subscribe(self.mqtt_config.get('topics', {}).get('system_command', 'system/command'))
                logging.info("Tide analyzer subscribed to system/command")

        def on_message(client, userdata, msg):
            try:
                payload = json.loads(msg.payload.decode())
                cmd_type = payload.get('type', '').upper()
                if cmd_type == 'TIDE':
                    self.on_demand_event.set()
                    logging.info("[OnDemand] TIDE command received")
                elif cmd_type == 'STOP':
                    self.on_demand_event.clear()
                    logging.info("[Stop] Tide analysis stopped")
            except Exception as e:
                logging.error(f"Command message error: {e}")

        self.mqtt_client.on_connect = on_connect
        self.mqtt_client.on_message = on_message

        self.mqtt_client.connect(
            self.mqtt_config['broker_host'],
            self.mqtt_config['broker_port'],
            self.mqtt_config['keepalive']
        )
        self.mqtt_client.loop_start()

        # 记录启动时间，用于判断是否采集够 tide_window_duration
        start_time = time.time()
        buffer_ready = False

        # 初始化：对齐到观测间隔的整数倍时刻，与波浪分析同步
        current_time = start_time
        time_in_interval = current_time % self.observation_interval
        next_observation_time = current_time + (self.observation_interval - time_in_interval)

        logging.info("MQTT Tide Analyzer Service started")
        logging.info(f"  Auto analysis: {'ON' if self.auto_analysis else 'OFF'}")
        logging.info(f"  Observation interval: {self.observation_interval}s")
        logging.info(f"  Tide window duration: {self.tide_window_duration}s")

        try:
            while True:
                current_time = time.time()

                # 检查是否已采集够 tide_window_duration
                elapsed = current_time - start_time
                if not buffer_ready and elapsed >= self.tide_window_duration:
                    buffer_ready = True
                    logging.info(f"Buffer ready: {elapsed:.0f}s elapsed >= {self.tide_window_duration}s window")

                # 按需分析（串口命令触发，无论 auto_analysis 状态）
                if self.on_demand_event.is_set():
                    self.on_demand_event.clear()
                    if buffer_ready:
                        logging.info("[OnDemand] Recording tide observation...")
                        self.record_tide_observation()
                    else:
                        logging.warning(f"[OnDemand] Insufficient data: {elapsed:.0f}s / {self.tide_window_duration}s")

                # 定时自动分析（仅 auto_analysis=True 时）
                elif self.auto_analysis and buffer_ready and current_time >= next_observation_time:
                    logging.info("Recording tide observation (auto)")
                    self.record_tide_observation()
                    while next_observation_time <= current_time:
                        next_observation_time += self.observation_interval

                time.sleep(1)

        except KeyboardInterrupt:
            logging.info("Shutting down tide analyzer...")
        finally:
            if self.mqtt_client:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
            if self.db_conn:
                self.db_conn.close()
            logging.info("Tide analyzer stopped")


def main():
    # 设置日志
    config_path = Path(__file__).parent.parent / 'config' / 'system_config.yaml'

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    log_config = config.get('logging', {})
    log_level = getattr(logging, log_config.get('level', 'INFO'))

    # 配置日志
    log_dir = Path(log_config.get('log_dir', 'logs'))
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=log_level,
        format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
        handlers=[
            logging.FileHandler(log_dir / 'tide_analyzer.log'),
            logging.StreamHandler()
        ]
    )

    # 启动服务
    analyzer = TideAnalyzer(config)
    analyzer.run()


if __name__ == '__main__':
    main()
