#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Web服务 - 提供实时数据展示和系统配置
====================================

功能：
1. REST API - 历史数据查询
2. WebSocket - 实时数据推送
3. 系统配置管理
4. 设备状态监控

Author: Wave Monitoring System
Date: 2025-11-21
"""

import json
import logging
import signal
import sys
import time
import yaml
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import asyncio

from functools import wraps

import paho.mqtt.client as mqtt
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from flask_sock import Sock
import psycopg2
from psycopg2.extras import RealDictCursor


class WebService:
    """Web服务"""

    def __init__(self, config_path: str):
        """初始化Web服务"""
        # 保存配置文件路径
        self.config_path = config_path

        # 加载配置
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # 兼容旧配置文件：补充可能缺失的新字段默认值
        a = self.config.setdefault('analysis', {})
        a.setdefault('meter_window', 300)
        a.setdefault('work_window', 1200)

        # 配置日志
        self._setup_logging()

        # Flask应用
        self.app = Flask(__name__,
                        static_folder='../web/static',
                        template_folder='../web/templates')

        # CORS支持
        if self.config['web']['cors_enabled']:
            CORS(self.app, origins=self.config['web']['cors_origins'])

        # WebSocket支持
        self.sock = Sock(self.app)

        # MQTT客户端（订阅实时数据）
        self.mqtt_client = None
        self.latest_data = {
            'raw': None,
            'analyzed': None,
            'status': {}
        }

        # WebSocket客户端列表
        self.ws_clients = []

        # 数据库连接
        self.db_config = self.config['database']

        # API key 认证
        self.api_key = self.config['web'].get('api_key', '')

        # 设置路由
        self._setup_routes()

        logging.info("Web Service initialized")

    def _setup_logging(self):
        """配置日志"""
        log_config = self.config['logging']

        if log_config['file_logging']:
            log_dir = Path(log_config['log_dir'])
            log_dir.mkdir(parents=True, exist_ok=True)

            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                log_dir / 'web.log',
                maxBytes=log_config['max_bytes'],
                backupCount=log_config['backup_count']
            )
            file_handler.setFormatter(logging.Formatter(log_config['format']))
            logging.getLogger().addHandler(file_handler)

        logging.getLogger().setLevel(getattr(logging, log_config['level']))

    def _get_db_connection(self):
        """获取数据库连接"""
        return psycopg2.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            database=self.db_config['database'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )

    def _get_latest_analysis_from_db(self):
        """从数据库获取最新的波浪分析结果"""
        try:
            logging.debug("Attempting to load latest analysis from database...")
            conn = self._get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            cursor.execute(
                """
                SELECT start_time, end_time, collection_start_time, collection_end_time,
                       duration_seconds, sample_count, sample_rate,
                       hs AS Hm0, tp AS Tp, tz AS Tz, theta, fp AS peak_frequency,
                       hs_radar1 AS Hm0_radar1, hs_radar2 AS Hm0_radar2, hs_radar3 AS Hm0_radar3,
                       phase_diff_12, phase_diff_13,
                       hs_zc AS Hs, hmax AS Hmax, h1_10 AS H1_10, h_mean AS Hmean,
                       tmax AS Tmax, t1_10 AS T1_10, ts AS Ts, tmean AS Tmean,
                       wave_count, mean_level,
                       m_minus1, m0, m1, m2, m4,
                       tm01 AS Tm01, te AS Te, fm, fz, fe, df, f_min, f_max, nf, epsilon_0,
                       wave_direction, mean_direction, directional_spread, direction_at_peak,
                       diwasp_method, diwasp_success,
                       spectrum_data, time_domain_data,
                       created_at
                FROM wave_analysis
                ORDER BY created_at DESC
                LIMIT 1
                """
            )

            result = cursor.fetchone()
            cursor.close()
            conn.close()

            if not result:
                logging.info("No wave analysis data found in database")
                return None

            # 转换时间戳为ISO格式
            for time_field in ['start_time', 'end_time', 'collection_start_time', 'collection_end_time', 'created_at']:
                if time_field in result and result[time_field]:
                    result[time_field] = result[time_field].isoformat()

            # 提取metadata字段（不放入results）
            metadata_fields = {'start_time', 'end_time', 'collection_start_time', 'collection_end_time',
                             'duration_seconds', 'sample_count', 'sample_rate', 'spectrum_data',
                             'time_domain_data', 'created_at'}

            # 字段名映射：数据库小写 -> 前端期望的格式
            field_name_mapping = {
                'hm0': 'Hm0', 'hs': 'Hs', 'tp': 'Tp', 'tz': 'Tz', 'fp': 'fp',
                'hmax': 'Hmax', 'h1_10': 'H1_10', 'hmean': 'Hmean',
                'tmax': 'Tmax', 't1_10': 'T1_10', 'ts': 'Ts', 'tmean': 'Tmean',
                'tm01': 'Tm01', 'te': 'Te',
                'hm0_radar1': 'Hm0_radar1', 'hm0_radar2': 'Hm0_radar2', 'hm0_radar3': 'Hm0_radar3',
                'peak_frequency': 'peak_frequency', 'wave_direction': 'wave_direction',
                'mean_direction': 'mean_direction', 'directional_spread': 'directional_spread',
                'direction_at_peak': 'direction_at_peak'
            }

            # 构建与MQTT消息格式兼容的结果对象，转换字段名
            results_data = {}
            for k, v in result.items():
                if k not in metadata_fields:
                    # 使用映射的字段名，如果没有映射则保持原样
                    new_key = field_name_mapping.get(k, k)
                    results_data[new_key] = v

            analyzed_data = {
                'results': results_data,
                'metadata': {
                    'start_time': result.get('start_time'),
                    'end_time': result.get('end_time'),
                    'duration_seconds': result.get('duration_seconds'),
                    'sample_count': result.get('sample_count'),
                    'sample_rate': result.get('sample_rate')
                },
                'spectrum': result.get('spectrum_data'),
                'time_domain': result.get('time_domain_data'),
                'timestamp': result.get('end_time') or result.get('created_at'),  # 使用分析结束时间或创建时间
                '_from_database': True  # 标记数据来源
            }

            logging.info(f"Successfully loaded latest analysis from database (created at: {result.get('created_at')})")
            return analyzed_data

        except Exception as e:
            logging.error(f"Failed to load latest analysis from database: {e}", exc_info=True)
            return None

    def _setup_mqtt(self):
        """配置MQTT客户端"""
        mqtt_config = self.config['mqtt']

        # paho-mqtt 2.x requires callback_api_version parameter
        try:
            from paho.mqtt.client import CallbackAPIVersion
            self.mqtt_client = mqtt.Client(
                callback_api_version=CallbackAPIVersion.VERSION1,
                client_id="wave_web"
            )
        except ImportError:
            # Fallback for older paho-mqtt versions
            self.mqtt_client = mqtt.Client(client_id="wave_web")

        if mqtt_config.get('username'):
            self.mqtt_client.username_pw_set(
                mqtt_config['username'],
                mqtt_config['password']
            )

        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_message = self._on_mqtt_message
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect

        # 创建连接事件（用于等待连接建立）
        import threading
        self.mqtt_connected = threading.Event()

        try:
            logging.info(f"Connecting to MQTT broker at {mqtt_config['broker_host']}:{mqtt_config['broker_port']}")
            self.mqtt_client.connect(
                mqtt_config['broker_host'],
                mqtt_config['broker_port'],
                mqtt_config['keepalive']
            )
            self.mqtt_client.loop_start()
            logging.info("MQTT client loop started, waiting for connection...")

            # 等待MQTT连接建立（最多5秒）
            if self.mqtt_connected.wait(timeout=5):
                logging.info("MQTT connection established successfully")
            else:
                logging.warning("MQTT connection timeout, continuing anyway...")
        except Exception as e:
            logging.error(f"Failed to connect to MQTT broker: {e}")

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """MQTT连接回调"""
        if rc == 0:
            logging.info("MQTT broker connection established")
            topics = self.config['mqtt']['topics']
            client.subscribe(topics['raw_data'])
            client.subscribe(topics['analyzed_data'])
            client.subscribe(topics['system_status'])
            client.subscribe(topics['system_config'])
            logging.info(f"MQTT subscribed to: {topics['raw_data']}, {topics['analyzed_data']}, {topics['system_status']}, {topics['system_config']}")

            # 设置连接事件，通知 _setup_mqtt 连接已建立
            if hasattr(self, 'mqtt_connected'):
                self.mqtt_connected.set()
        else:
            rc_codes = {
                1: "Incorrect protocol version",
                2: "Invalid client identifier",
                3: "Server unavailable",
                4: "Bad username or password",
                5: "Not authorized"
            }
            error_msg = rc_codes.get(rc, f"Unknown error code {rc}")
            logging.error(f"MQTT connection failed: {error_msg}")

    def _on_mqtt_disconnect(self, client, userdata, rc):
        """MQTT断开连接回调"""
        if rc == 0:
            logging.info("MQTT client disconnected normally")
        else:
            logging.warning(f"MQTT client disconnected unexpectedly (code {rc}), will reconnect...")

    def _on_mqtt_message(self, client, userdata, msg):
        """MQTT消息回调"""
        try:
            payload = json.loads(msg.payload.decode())
            topics = self.config['mqtt']['topics']

            # 更新最新数据
            if msg.topic == topics['raw_data']:
                self.latest_data['raw'] = payload
                ws_count = len(self.ws_clients)
                if ws_count > 0:
                    self._broadcast_to_websockets({'type': 'raw', 'data': payload})
                    logging.debug(f"Broadcast raw data to {ws_count} WebSocket clients")

            elif msg.topic == topics['analyzed_data']:
                self.latest_data['analyzed'] = payload
                self._broadcast_to_websockets({'type': 'analyzed', 'data': payload})
                logging.info(f"Received and broadcast analyzed data")

            elif msg.topic == topics['system_status']:
                component = payload.get('component', 'unknown')
                # 记录状态消息的接收时间，用于动态计算倒计时
                payload['_received_at'] = time.time()
                self.latest_data['status'][component] = payload
                self._broadcast_to_websockets({'type': 'status', 'data': payload})

            elif msg.topic == topics['system_config']:
                # 同步其他服务修改的完整配置到内存
                for section in ('analysis', 'radar', 'collection'):
                    if section in payload:
                        self.config.setdefault(section, {}).update(payload[section])
                logging.info(f"Config synced from MQTT: {payload}")

        except Exception as e:
            logging.error(f"Error processing MQTT message: {e}")

    def _broadcast_to_websockets(self, message: Dict):
        """广播消息到所有WebSocket客户端"""
        for ws in self.ws_clients[:]:
            try:
                ws.send(json.dumps(message))
            except Exception as e:
                logging.warning(f"Failed to send to WebSocket client: {e}")
                self.ws_clients.remove(ws)

    def _publish_config(self):
        """将完整配置一次性发布到 MQTT"""
        payload = {
            'analysis': {
                'window_duration':      self.config['analysis']['window_duration'],
                'analysis_interval':    self.config['analysis'].get('analysis_interval', 300),
                'tide_window_duration': self.config['analysis'].get('tide_window_duration', 300),
                'meter_window':         self.config['analysis'].get('meter_window', 300),
                'work_window':          self.config['analysis'].get('work_window', 1200),
                'filter_enable':        self.config['analysis']['filter_enable'],
                'filter_band':          self.config['analysis']['filter_band'],
            },
            'radar': {
                'array_heading': self.config['radar'].get('array_heading', 0.0),
                'array_height':  self.config['radar'].get('array_height', 5.0),
            },
            'collection': {
                'sample_rate': self.config['collection']['sample_rate'],
            },
        }
        topic = self.config['mqtt']['topics']['system_config']
        self.mqtt_client.publish(topic, json.dumps(payload), qos=1)

    def _get_status_with_realtime_countdown(self) -> Dict:
        """获取状态，动态计算实时倒计时"""
        import copy
        status = copy.deepcopy(self.latest_data['status'])

        # 动态计算 analyzer 的 seconds_until_next_analysis
        if 'analyzer' in status:
            analyzer = status['analyzer']
            received_at = analyzer.get('_received_at', 0)
            original_countdown = analyzer.get('seconds_until_next_analysis', 0)
            analysis_interval = analyzer.get('analysis_interval', 300)

            if received_at > 0 and original_countdown > 0:
                # 计算从收到消息到现在经过的时间
                elapsed = time.time() - received_at
                # 计算实时倒计时
                realtime_countdown = max(0, original_countdown - elapsed)
                # 如果倒计时已经过了一个周期，计算新周期内的倒计时
                if realtime_countdown == 0 and elapsed > original_countdown:
                    cycles_passed = int((elapsed - original_countdown) / analysis_interval)
                    remaining_in_cycle = (elapsed - original_countdown) % analysis_interval
                    realtime_countdown = max(0, analysis_interval - remaining_in_cycle)
                analyzer['seconds_until_next_analysis'] = round(realtime_countdown, 1)
                # 重新计算进度
                elapsed_in_cycle = analysis_interval - realtime_countdown
                analyzer['analysis_progress'] = round(min(100, (elapsed_in_cycle / analysis_interval) * 100), 1)

            # 移除内部字段
            analyzer.pop('_received_at', None)

        return status

    def _require_api_key(self, f):
        """API key 认证装饰器，仅在配置了 api_key 且为写操作时生效"""
        @wraps(f)
        def decorated(*args, **kwargs):
            if self.api_key and request.method in ('POST', 'PUT', 'DELETE'):
                key = request.headers.get('X-API-Key', '')
                if key != self.api_key:
                    return jsonify({'success': False, 'error': 'Unauthorized'}), 401
            return f(*args, **kwargs)
        return decorated

    def _setup_routes(self):
        """设置Flask路由"""

        # ==================== Web页面 ====================

        @self.app.route('/')
        def index():
            """首页"""
            return render_template('index.html')

        @self.app.route('/config')
        def config_page():
            """配置页面"""
            return render_template('config.html')

        @self.app.route('/history')
        def history_page():
            """历史数据页面"""
            return render_template('history.html')

        @self.app.route('/test')
        def test_page():
            """WebSocket测试页面"""
            return render_template('test.html')

        @self.app.route('/test_api')
        def test_api_page():
            """API数据测试页面"""
            return render_template('test_api.html')

        @self.app.route('/debug')
        def debug_page():
            """数据调试页面"""
            return render_template('debug.html')

        @self.app.route('/spectrum')
        def spectrum_page():
            """频谱分析页面"""
            return render_template('spectrum.html')

        # ==================== REST API ====================

        @self.app.route('/api/latest', methods=['GET'])
        def get_latest():
            """获取最新数据（内存优先，数据库fallback）"""
            analyzed_data = self.latest_data['analyzed']

            # 如果内存中没有分析数据，从数据库加载最新的
            if not analyzed_data:
                logging.debug("No analyzed data in memory, attempting database fallback...")
                analyzed_data = self._get_latest_analysis_from_db()
                if analyzed_data:
                    logging.info("API /api/latest: Using latest analysis data from database (fallback)")
                else:
                    logging.info("API /api/latest: No analysis data available in memory or database")
            else:
                logging.debug("API /api/latest: Serving analyzed data from memory")

            return jsonify({
                'raw': self.latest_data['raw'],
                'analyzed': analyzed_data,
                'status': self._get_status_with_realtime_countdown()
            })

        @self.app.route('/api/history/raw', methods=['GET'])
        def get_raw_history():
            """获取原始数据历史"""
            try:
                # 获取查询参数（带边界检查）
                hours = max(1, min(720, int(request.args.get('hours', 1))))
                limit = max(1, min(10000, int(request.args.get('limit', 1000))))

                conn = self._get_db_connection()
                cursor = conn.cursor(cursor_factory=RealDictCursor)

                cursor.execute(
                    """
                    SELECT timestamp1, timestamp2, timestamp3,
                           eta1, eta2, eta3, created_at
                    FROM wave_measurements
                    WHERE timestamp1 > NOW() - INTERVAL '%s hours'
                    ORDER BY timestamp1 DESC
                    LIMIT %s
                    """,
                    (hours, limit)
                )

                results = cursor.fetchall()

                # 转换时间戳为ISO格式
                for row in results:
                    for key in ['timestamp1', 'timestamp2', 'timestamp3', 'created_at']:
                        if row[key]:
                            row[key] = row[key].isoformat()

                cursor.close()
                conn.close()

                return jsonify({
                    'success': True,
                    'count': len(results),
                    'data': results
                })

            except Exception as e:
                logging.error(f"Failed to query raw history: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/history/analysis', methods=['GET'])
        def get_analysis_history():
            """获取分析结果历史（包含方向谱数据）"""
            try:
                days = max(1, min(365, int(request.args.get('days', 1))))
                limit = max(1, min(10000, int(request.args.get('limit', 100))))
                include_spectrum = request.args.get('include_spectrum', 'false').lower() == 'true'

                conn = self._get_db_connection()
                cursor = conn.cursor(cursor_factory=RealDictCursor)

                # 基础字段
                base_fields = """id, start_time, end_time, duration_seconds,
                           sample_count, sample_rate,
                           hs, tp, tz, theta, fp,
                           hs_radar1, hs_radar2, hs_radar3,
                           phase_diff_12, phase_diff_13,
                           wave_direction, mean_direction, directional_spread,
                           direction_at_peak, diwasp_method, diwasp_success,
                           collection_start_time, collection_end_time,
                           created_at"""

                # 如果需要包含方向谱数据
                if include_spectrum:
                    base_fields += ", directional_spectrum"

                cursor.execute(
                    f"""
                    SELECT {base_fields}
                    FROM wave_analysis
                    WHERE start_time > NOW() - INTERVAL '%s days'
                    ORDER BY start_time DESC
                    LIMIT %s
                    """,
                    (days, limit)
                )

                results = cursor.fetchall()

                # 转换时间戳
                for row in results:
                    for key in ['start_time', 'end_time', 'created_at', 'collection_start_time', 'collection_end_time']:
                        if key in row and row[key]:
                            row[key] = row[key].isoformat()

                cursor.close()
                conn.close()

                return jsonify({
                    'success': True,
                    'count': len(results),
                    'data': results
                })

            except Exception as e:
                logging.error(f"Failed to query analysis history: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/directional-spectrum/<int:analysis_id>', methods=['GET'])
        def get_directional_spectrum(analysis_id):
            """获取指定分析记录的方向谱数据"""
            try:
                conn = self._get_db_connection()
                cursor = conn.cursor(cursor_factory=RealDictCursor)

                cursor.execute(
                    """
                    SELECT id, start_time, hs, tp,
                           wave_direction, mean_direction, directional_spread,
                           direction_at_peak, diwasp_method, diwasp_success,
                           directional_spectrum
                    FROM wave_analysis
                    WHERE id = %s
                    """,
                    (analysis_id,)
                )

                result = cursor.fetchone()
                cursor.close()
                conn.close()

                if not result:
                    return jsonify({'success': False, 'error': 'Analysis record not found'}), 404

                # 转换时间戳
                if result['start_time']:
                    result['start_time'] = result['start_time'].isoformat()

                return jsonify({
                    'success': True,
                    'data': result
                })

            except Exception as e:
                logging.error(f"Failed to get directional spectrum: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/directional-spectrum/latest', methods=['GET'])
        def get_latest_directional_spectrum():
            """获取最新的方向谱数据"""
            try:
                conn = self._get_db_connection()
                cursor = conn.cursor(cursor_factory=RealDictCursor)

                cursor.execute(
                    """
                    SELECT id, start_time, hs, tp,
                           wave_direction, mean_direction, directional_spread,
                           direction_at_peak, diwasp_method, diwasp_success,
                           directional_spectrum
                    FROM wave_analysis
                    WHERE directional_spectrum IS NOT NULL
                    ORDER BY start_time DESC
                    LIMIT 1
                    """
                )

                result = cursor.fetchone()
                cursor.close()
                conn.close()

                if not result:
                    return jsonify({
                        'success': True,
                        'data': None,
                        'message': 'No directional spectrum data available'
                    })

                # 转换时间戳
                if result['start_time']:
                    result['start_time'] = result['start_time'].isoformat()

                return jsonify({
                    'success': True,
                    'data': result
                })

            except Exception as e:
                logging.error(f"Failed to get latest directional spectrum: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/statistics', methods=['GET'])
        def get_statistics():
            """获取统计信息"""
            try:
                conn = self._get_db_connection()
                cursor = conn.cursor(cursor_factory=RealDictCursor)

                # 获取最近24小时的统计
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) as sample_count,
                        AVG(eta1) as avg_eta1,
                        AVG(eta2) as avg_eta2,
                        AVG(eta3) as avg_eta3,
                        STDDEV(eta1) as std_eta1,
                        STDDEV(eta2) as std_eta2,
                        STDDEV(eta3) as std_eta3
                    FROM wave_measurements
                    WHERE timestamp1 > NOW() - INTERVAL '24 hours'
                    """
                )
                raw_stats = cursor.fetchone()

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) as analysis_count,
                        AVG(hs) as avg_hs,
                        MAX(hs) as max_hs,
                        MIN(hs) as min_hs,
                        AVG(tp) as avg_tp,
                        AVG(theta) as avg_theta
                    FROM wave_analysis
                    WHERE start_time > NOW() - INTERVAL '24 hours'
                    """
                )
                analysis_stats = cursor.fetchone()

                cursor.close()
                conn.close()

                return jsonify({
                    'success': True,
                    'raw_data': raw_stats,
                    'analysis': analysis_stats
                })

            except Exception as e:
                logging.error(f"Failed to get statistics: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/config', methods=['GET', 'POST'])
        @self._require_api_key
        def manage_config():
            """管理系统配置（POST需要API key认证）"""
            if request.method == 'GET':
                return jsonify({
                    'success': True,
                    'config': {
                        'sample_rate': self.config['collection']['sample_rate'],
                        'window_duration': self.config['analysis']['window_duration'],
                        'analysis_interval': self.config['analysis'].get('analysis_interval', 300),
                        'tide_window_duration': self.config['analysis'].get('tide_window_duration', 300),
                        'meter_window': self.config['analysis'].get('meter_window', 300),
                        'work_window': self.config['analysis'].get('work_window', 1200),
                        'filter_enable': self.config['analysis']['filter_enable'],
                        'filter_band': self.config['analysis']['filter_band']
                    },
                    'radar': {
                        'array_heading': self.config['radar'].get('array_heading', 0.0),
                        'array_height': self.config['radar'].get('array_height', 5.0),
                        'elevation_85': self.config['radar'].get('elevation_85',
                                        self.config['radar'].get('array_height', 5.0)),
                        'elevation_85_surveyed': self.config['radar'].get('elevation_85_surveyed', False)
                    }
                })

            elif request.method == 'POST':
                try:
                    new_config = request.json

                    # 参数边界校验
                    bounds = {
                        'sample_rate': (1, 100),
                        'window_duration': (60, 7200),
                        'analysis_interval': (30, 3600),
                        'tide_window_duration': (60, 3600),
                        'meter_window': (60, 3600),
                        'work_window': (60, 7200),
                    }
                    for key, (lo, hi) in bounds.items():
                        if key in new_config:
                            val = new_config[key]
                            if not isinstance(val, (int, float)) or val < lo or val > hi:
                                return jsonify({
                                    'success': False,
                                    'error': f'{key} must be between {lo} and {hi}'
                                }), 400

                    # 更新内存中的配置
                    if 'sample_rate' in new_config:
                        self.config['collection']['sample_rate'] = new_config['sample_rate']
                    if 'window_duration' in new_config:
                        self.config['analysis']['window_duration'] = new_config['window_duration']
                    if 'analysis_interval' in new_config:
                        self.config['analysis']['analysis_interval'] = new_config['analysis_interval']
                    if 'tide_window_duration' in new_config:
                        self.config['analysis']['tide_window_duration'] = new_config['tide_window_duration']
                    if 'meter_window' in new_config:
                        self.config['analysis']['meter_window'] = int(new_config['meter_window'])
                    if 'work_window' in new_config:
                        self.config['analysis']['work_window'] = int(new_config['work_window'])
                    if 'filter_enable' in new_config:
                        self.config['analysis']['filter_enable'] = new_config['filter_enable']
                    if 'filter_band' in new_config:
                        self.config['analysis']['filter_band'] = new_config['filter_band']

                    # 保存到YAML文件
                    with open(self.config_path, 'w', encoding='utf-8') as f:
                        yaml.dump(self.config, f, default_flow_style=False, allow_unicode=True)

                    # 发布完整配置到MQTT
                    self._publish_config()

                    logging.info(f"Configuration updated and saved: {new_config}")
                    return jsonify({'success': True, 'message': 'Config updated and saved'})

                except Exception as e:
                    logging.error(f"Failed to update config: {e}")
                    return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/config/update', methods=['POST'])
        @self._require_api_key
        def update_radar_config():
            """更新雷达阵列配置（需要API key认证）"""
            try:
                data = request.json
                updated = False

                # 更新雷达阵列配置
                if 'analysis' in data:
                    analysis_config = data['analysis']
                    if 'array_heading' in analysis_config:
                        heading = float(analysis_config['array_heading'])
                        if not (0 <= heading < 360):
                            return jsonify({'success': False, 'error': 'array_heading must be in [0, 360)'}), 400
                        self.config['radar']['array_heading'] = heading
                        updated = True
                        logging.info(f"Updated array_heading to {heading}")
                    if 'elevation_85' in analysis_config:
                        elev = float(analysis_config['elevation_85'])
                        if not (0 <= elev <= 200):
                            return jsonify({'success': False, 'error': 'elevation_85 must be in [0, 200]'}), 400
                        self.config['radar']['elevation_85'] = elev
                        self.config['radar']['array_height'] = elev  # 兼容旧代码
                        updated = True
                        logging.info(f"Updated elevation_85 to {elev}")
                    if 'elevation_85_surveyed' in analysis_config:
                        self.config['radar']['elevation_85_surveyed'] = bool(analysis_config['elevation_85_surveyed'])
                        updated = True
                        logging.info(f"Updated elevation_85_surveyed to {self.config['radar']['elevation_85_surveyed']}")
                    if 'array_height' in analysis_config:
                        height = float(analysis_config['array_height'])
                        if not (0.5 <= height <= 200):
                            return jsonify({'success': False, 'error': 'array_height must be in [0.5, 200]'}), 400
                        self.config['radar']['array_height'] = height
                        updated = True
                        logging.info(f"Updated array_height to {height}")

                if updated:
                    # 保存到YAML文件
                    with open(self.config_path, 'w', encoding='utf-8') as f:
                        yaml.dump(self.config, f, default_flow_style=False, allow_unicode=True)

                    # 发布完整配置到MQTT
                    self._publish_config()

                    return jsonify({
                        'success': True,
                        'message': 'Radar config updated successfully'
                    })
                else:
                    return jsonify({
                        'success': False,
                        'message': 'No valid config parameters provided'
                    }), 400

            except Exception as e:
                logging.error(f"Failed to update radar config: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/system/status', methods=['GET'])
        def get_system_status():
            """获取系统状态（动态计算倒计时）"""
            status = self._get_status_with_realtime_countdown()
            return jsonify({
                'success': True,
                'status': status
            })

        @self.app.route('/api/system/command', methods=['POST'])
        @self._require_api_key
        def send_system_command():
            """发送系统命令（需要API key认证）"""
            try:
                command = request.json

                topic = self.config['mqtt']['topics']['system_command']
                self.mqtt_client.publish(
                    topic,
                    json.dumps(command),
                    qos=1
                )

                return jsonify({'success': True, 'message': 'Command sent'})

            except Exception as e:
                logging.error(f"Failed to send command: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500

        # ==================== Tide APIs ====================

        @self.app.route('/api/tide/observations', methods=['GET'])
        def get_tide_observations():
            """获取潮位观测历史数据"""
            try:
                # 获取查询参数（带边界检查）
                hours = max(1, min(720, int(request.args.get('hours', 24))))  # 默认24小时
                limit = max(1, min(10000, int(request.args.get('limit', 1000))))  # 最多返回10000个点

                conn = self._get_db_connection()
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                query = """
                    SELECT observation_time, observed_tide_level,
                           radar1_distance, array_height, quality_flag
                    FROM tide_observations
                    WHERE observation_time >= NOW() - INTERVAL '%s hours'
                    ORDER BY observation_time DESC
                    LIMIT %s
                """
                cursor.execute(query, (hours, limit))
                results = cursor.fetchall()
                cursor.close()
                conn.close()

                # Convert datetime to ISO string
                for row in results:
                    row['observation_time'] = row['observation_time'].isoformat()

                # Reverse to get chronological order
                results.reverse()

                return jsonify({'success': True, 'count': len(results), 'data': results})

            except Exception as e:
                logging.error(f"Failed to get tide observations: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500

        @self.app.route('/api/tide/current', methods=['GET'])
        def get_current_tide():
            """获取当前潮位"""
            try:
                conn = self._get_db_connection()
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                # 优先获取最近2分钟的观测值
                obs_query = """
                    SELECT observation_time, observed_tide_level, array_height
                    FROM tide_observations
                    WHERE observation_time >= NOW() - INTERVAL '2 minutes'
                    ORDER BY observation_time DESC
                    LIMIT 1
                """
                cursor.execute(obs_query)
                observation = cursor.fetchone()

                # 如果没有最近的数据，回退到获取最新的历史记录
                if not observation:
                    fallback_query = """
                        SELECT observation_time, observed_tide_level, array_height
                        FROM tide_observations
                        ORDER BY observation_time DESC
                        LIMIT 1
                    """
                    cursor.execute(fallback_query)
                    observation = cursor.fetchone()

                cursor.close()
                conn.close()

                if not observation:
                    return jsonify({'success': False, 'error': 'No current tide data available'}), 404

                result = {
                    'observed': {
                        'time': observation['observation_time'].isoformat(),
                        'tide_level': float(observation['observed_tide_level']),
                        'array_height': float(observation['array_height'])
                    }
                }

                return jsonify({'success': True, 'data': result})

            except Exception as e:
                logging.error(f"Failed to get current tide: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500

        # ==================== WebSocket ====================

        @self.sock.route('/ws')
        def websocket(ws):
            """WebSocket连接"""
            logging.info("New WebSocket client connected")
            self.ws_clients.append(ws)

            try:
                # 准备初始化数据，使用数据库fallback
                analyzed_data = self.latest_data['analyzed']

                # 如果内存中没有分析数据，从数据库加载最新的
                if not analyzed_data:
                    analyzed_data = self._get_latest_analysis_from_db()
                    if analyzed_data:
                        logging.info("WebSocket: Using latest analysis data from database")

                # 发送初始数据（使用实时倒计时）
                ws.send(json.dumps({
                    'type': 'init',
                    'data': {
                        'raw': self.latest_data['raw'],
                        'analyzed': analyzed_data,
                        'status': self._get_status_with_realtime_countdown()
                    }
                }))

                # 保持连接
                while True:
                    message = ws.receive(timeout=60)  # 60 second timeout
                    if message is None:
                        break

                    # 处理客户端消息
                    try:
                        data = json.loads(message)
                        logging.debug(f"WebSocket message: {data}")

                        # 响应ping消息
                        if data.get('type') == 'ping':
                            ws.send(json.dumps({
                                'type': 'pong',
                                'timestamp': datetime.now(timezone.utc).isoformat()
                            }))
                    except json.JSONDecodeError:
                        pass
                    except Exception as e:
                        logging.warning(f"Error processing WebSocket message: {e}")

            except Exception as e:
                logging.warning(f"WebSocket error: {e}")
            finally:
                if ws in self.ws_clients:
                    self.ws_clients.remove(ws)
                logging.info("WebSocket client disconnected")

    def run(self):
        """运行Web服务"""
        logging.info("="*60)
        logging.info("Starting Web Service")
        logging.info("="*60)

        # 设置MQTT
        self._setup_mqtt()

        # 启动Flask
        web_config = self.config['web']
        logging.info(f"Web server starting on {web_config['host']}:{web_config['port']}")

        self.app.run(
            host=web_config['host'],
            port=web_config['port'],
            debug=web_config['debug']
        )

    def cleanup(self):
        """清理资源"""
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()


def signal_handler(signum, frame):
    """信号处理器"""
    logging.info(f"Received signal {signum}")
    sys.exit(0)


if __name__ == '__main__':
    config_path = Path(__file__).parent.parent / 'config' / 'system_config.yaml'

    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)

    service = WebService(str(config_path))

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        service.run()
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
