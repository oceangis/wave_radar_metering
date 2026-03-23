#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ThingsBoard MQTT Bridge - 数据转发服务

订阅本地MQTT的波浪分析结果和潮位观测，合并为一条telemetry发送到ThingsBoard
"""

import json
import logging
import signal
import sys
import yaml
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Event

import paho.mqtt.client as mqtt


class ThingsBoardBridge:
    """ThingsBoard 数据转发桥接器"""

    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # ThingsBoard 配置
        self.tb_config = self.config.get('thingsboard', {})
        self.tb_host = self.tb_config.get('host', '8.155.1.77')
        self.tb_port = self.tb_config.get('port', 1883)
        self.tb_token = self.tb_config.get('access_token', '')

        if not self.tb_token:
            raise ValueError("ThingsBoard access_token not configured!")

        self._setup_logging()

        self.local_client = None
        self.local_connected = Event()
        self.tb_client = None
        self.tb_connected = Event()
        self.running = False
        self.stop_event = Event()

        # 缓存最新潮位
        self.latest_tide_level = None

        # 统计
        self.stats = {
            'forwarded': 0,
            'errors': 0,
            'last_forward_time': None
        }

        logging.info("ThingsBoard Bridge initialized")
        logging.info(f"  ThingsBoard: {self.tb_host}:{self.tb_port}")

    def _setup_logging(self):
        log_config = self.config.get('logging', {})
        log_dir = Path(log_config.get('log_dir', '/home/obsis/radar/mqtt_system/logs'))
        log_dir.mkdir(parents=True, exist_ok=True)

        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_dir / 'thingsboard_bridge.log',
            maxBytes=log_config.get('max_bytes', 10485760),
            backupCount=log_config.get('backup_count', 5)
        )
        file_handler.setFormatter(logging.Formatter(
            log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ))
        logging.getLogger().addHandler(file_handler)
        logging.getLogger().setLevel(getattr(logging, log_config.get('level', 'INFO')))

    def _setup_local_mqtt(self):
        mqtt_config = self.config['mqtt']
        self.local_client = mqtt.Client(client_id="thingsboard_bridge_local")

        if mqtt_config.get('username'):
            self.local_client.username_pw_set(
                mqtt_config['username'],
                mqtt_config['password']
            )

        self.local_client.on_connect = self._on_local_connect
        self.local_client.on_disconnect = self._on_local_disconnect
        self.local_client.on_message = self._on_local_message

        try:
            self.local_client.connect(
                mqtt_config['broker_host'],
                mqtt_config['broker_port'],
                mqtt_config['keepalive']
            )
            self.local_client.loop_start()
            logging.info("Local MQTT client started")
        except Exception as e:
            logging.error(f"Failed to connect to local MQTT: {e}")
            raise

    def _setup_thingsboard_mqtt(self):
        self.tb_client = mqtt.Client(client_id="wave_monitor_device")
        self.tb_client.username_pw_set(self.tb_token, "")
        self.tb_client.on_connect = self._on_tb_connect
        self.tb_client.on_disconnect = self._on_tb_disconnect

        try:
            self.tb_client.connect(self.tb_host, self.tb_port, 60)
            self.tb_client.loop_start()
            logging.info(f"ThingsBoard MQTT client connecting to {self.tb_host}:{self.tb_port}")
        except Exception as e:
            logging.error(f"Failed to connect to ThingsBoard: {e}")
            raise

    def _on_local_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to local MQTT broker")
            self.local_connected.set()
            # 订阅波浪分析结果
            client.subscribe(self.config['mqtt']['topics']['analyzed_data'])
            logging.info(f"Subscribed to {self.config['mqtt']['topics']['analyzed_data']}")
            # 订阅潮位观测
            client.subscribe("tide/observation")
            logging.info("Subscribed to tide/observation")
        else:
            logging.error(f"Local MQTT connection failed: {rc}")

    def _on_local_disconnect(self, client, userdata, rc):
        logging.warning(f"Disconnected from local MQTT: {rc}")
        self.local_connected.clear()

    def _on_tb_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to ThingsBoard")
            self.tb_connected.set()
        else:
            error_messages = {
                1: "Incorrect protocol version",
                2: "Invalid client identifier",
                3: "Server unavailable",
                4: "Bad username or password (invalid access token)",
                5: "Not authorized"
            }
            logging.error(f"ThingsBoard connection failed: {error_messages.get(rc, f'Unknown error {rc}')}")

    def _on_tb_disconnect(self, client, userdata, rc):
        logging.warning(f"Disconnected from ThingsBoard: {rc}")
        self.tb_connected.clear()

    def _on_local_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())

            if msg.topic == "tide/observation":
                # 缓存最新潮位
                self.latest_tide_level = payload.get('tide_level')
                logging.debug(f"Tide cached: {self.latest_tide_level}")
            elif msg.topic == self.config['mqtt']['topics']['analyzed_data']:
                self._forward_combined(payload)

        except Exception as e:
            logging.error(f"Error processing message: {e}")
            self.stats['errors'] += 1

    def _forward_combined(self, data: dict):
        """合并波浪+潮位，发送一条telemetry到ThingsBoard"""
        if not self.tb_connected.is_set():
            logging.warning("ThingsBoard not connected, skipping")
            return

        # 跳过错误结果（数据不足等）
        if data.get('metadata', {}).get('error'):
            logging.info(f"Skipping error result: {data['metadata']['error']}")
            return

        try:
            results = data.get('results', {})
            if not results:
                logging.info("Skipping empty results")
                return

            values = {}

            # 时域分析
            for key in ('Hs', 'Hmax', 'H1_10', 'Hmean', 'Ts', 'Tmax', 'T1_10', 'Tmean', 'wave_count'):
                v = results.get(key)
                if v is not None:
                    values[key] = round(float(v), 3) if isinstance(v, float) else v

            # 频域分析
            for key in ('Hm0', 'Tp', 'Tz', 'Tm01', 'Te', 'fp', 'fm', 'fz', 'fe'):
                v = results.get(key)
                if v is not None:
                    values[key] = round(float(v), 3) if key.startswith('H') else round(float(v), 4)
            # fp 从 peak_frequency 取
            if 'fp' not in values and results.get('peak_frequency') is not None:
                values['fp'] = round(float(results['peak_frequency']), 4)

            # 波向
            for key in ('wave_direction', 'mean_direction', 'direction_at_peak', 'directional_spread'):
                v = results.get(key)
                if v is not None:
                    values[key] = round(float(v), 1)

            # 潮位（来自tide_analyzer）
            if self.latest_tide_level is not None:
                values['tide_level'] = round(float(self.latest_tide_level), 3)

            telemetry = {
                'ts': int(datetime.now(timezone.utc).timestamp() * 1000),
                'values': values
            }

            self.tb_client.publish(
                "v1/devices/me/telemetry",
                json.dumps(telemetry),
                qos=1
            )

            self.stats['forwarded'] += 1
            self.stats['last_forward_time'] = datetime.now(timezone.utc).isoformat()

            tide_str = f", tide={values.get('tide_level', 'N/A')}" if 'tide_level' in values else ""
            logging.info(
                f"Forwarded: Hm0={values.get('Hm0', 'N/A')}m, "
                f"Hs={values.get('Hs', 'N/A')}m, "
                f"Tp={values.get('Tp', 'N/A')}s, "
                f"Dir={values.get('wave_direction', 'N/A')}"
                f"{tide_str}"
            )

        except Exception as e:
            logging.error(f"Failed to forward data: {e}")
            self.stats['errors'] += 1

    def run(self):
        logging.info("=" * 60)
        logging.info("Starting ThingsBoard Bridge Service")
        logging.info("=" * 60)

        self._setup_local_mqtt()
        if not self.local_connected.wait(timeout=10):
            logging.error("Local MQTT connection timeout")
            return

        self._setup_thingsboard_mqtt()
        if not self.tb_connected.wait(timeout=30):
            logging.error("ThingsBoard connection timeout")
            return

        self.running = True
        logging.info("ThingsBoard Bridge running...")

        status_interval = 60
        next_status_time = time.time() + status_interval

        try:
            while self.running and not self.stop_event.is_set():
                current_time = time.time()
                if current_time >= next_status_time:
                    logging.info(
                        f"Stats: forwarded={self.stats['forwarded']}, "
                        f"errors={self.stats['errors']}"
                    )
                    next_status_time = current_time + status_interval
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Interrupted by user")
        finally:
            self.cleanup()

    def stop(self):
        logging.info("Stopping ThingsBoard Bridge...")
        self.running = False
        self.stop_event.set()

    def cleanup(self):
        logging.info("Cleaning up...")
        if self.local_client:
            self.local_client.loop_stop()
            self.local_client.disconnect()
        if self.tb_client:
            self.tb_client.loop_stop()
            self.tb_client.disconnect()
        logging.info("ThingsBoard Bridge stopped")


def signal_handler(signum, frame):
    logging.info(f"Received signal {signum}")
    if 'bridge' in globals():
        bridge.stop()


if __name__ == '__main__':
    config_path = Path(__file__).parent.parent / 'config' / 'system_config.yaml'

    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)

    bridge = ThingsBoardBridge(str(config_path))

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        bridge.run()
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
