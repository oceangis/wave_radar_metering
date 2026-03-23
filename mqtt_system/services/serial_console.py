#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
串口控制台服务
==============
通过 USB-RS232 (/dev/rs232) 与PC上位机通信：
  1. 持续输出三雷达实时数据（1Hz JSON流）
  2. 接受JSON命令触发波浪/潮位分析，返回JSON结果

PC → Pi 命令（JSON，\r\n结尾）：
  {"cmd": "METER"}
  {"cmd": "WORK"}
  {"cmd": "STOP"}
  {"cmd": "STATUS"}
  {"cmd": "CONFIG_GET"}
  {"cmd": "CONFIG_SET", "height": 10.5, "heading": 243.0, ...}

Pi → PC 输出（JSON，\r\n结尾）：
  {"type": "stream", ...}
  {"type": "config", ...}
  {"type": "tide", ...}
  {"type": "wave", ...}
  {"type": "ack", ...}
  {"type": "error", ...}
"""

import json
import logging
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from threading import Event, Lock, Thread

import paho.mqtt.client as mqtt
import serial
import yaml
from logging.handlers import RotatingFileHandler


class SerialConsole:
    def __init__(self, config_path: str):
        self.config_path = config_path
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        # 兼容旧配置：补充默认值
        a = self.config.setdefault('analysis', {})
        a.setdefault('meter_window', 300)
        a.setdefault('work_window', 1200)
        a.setdefault('analysis_interval', 300)

        self.serial_dev = '/dev/rs232'
        self.baud_rate = 115200
        self.ser = None
        self.serial_lock = Lock()

        # MQTT
        self.mqtt_client = None
        self.mqtt_connected = Event()
        self.mqtt_config = self.config['mqtt']

        # 最新雷达原始数据（流输出用）
        self.latest_sample = None
        self.latest_sample_lock = Lock()

        # 等待分析结果
        self.result_lock = Lock()
        self.analysis_result = None
        self.analysis_event = Event()
        self.tide_result = None
        self.tide_event = Event()

        # 流输出控制：set=暂停，clear=正常
        self.stream_pause = Event()

        # 停止信号（打断正在等待结果的_run_analysis）
        self._stop_event = Event()

        # 延迟分析调度（收到METER/WORK后等待窗口时长再分析）
        self._pending_mode = None
        self._pending_deadline = None
        self._pending_lock = Lock()
        self._repeat = False  # 连续滑动窗口模式

        self.running = False
        self._setup_logging()

    def _setup_logging(self):
        log_config = self.config.get('logging', {})
        log_dir = Path(log_config.get('log_dir', 'logs'))
        log_dir.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            handlers=[
                RotatingFileHandler(
                    log_dir / 'serial_console.log',
                    maxBytes=log_config.get('max_bytes', 10 * 1024 * 1024),
                    backupCount=log_config.get('backup_count', 5)
                ),
                logging.StreamHandler()
            ]
        )

        # 串口数据收发专用日志（独立文件，方便后续分析）
        self.data_logger = logging.getLogger('serial_data')
        self.data_logger.setLevel(logging.INFO)
        self.data_logger.propagate = False  # 不传播到根logger，避免重复
        data_handler = RotatingFileHandler(
            log_dir / 'serial_data.log',
            maxBytes=log_config.get('max_bytes', 10 * 1024 * 1024),
            backupCount=log_config.get('backup_count', 5)
        )
        data_handler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d %(message)s',
                                                     datefmt='%Y-%m-%d %H:%M:%S'))
        self.data_logger.addHandler(data_handler)

    # ------------------------------------------------------------------ MQTT
    def _setup_mqtt(self):
        self.mqtt_client = mqtt.Client(client_id="serial_console")
        self.mqtt_client.username_pw_set(
            self.mqtt_config['username'],
            self.mqtt_config['password']
        )
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_message = self._on_mqtt_message
        self.mqtt_client.connect(
            self.mqtt_config['broker_host'],
            self.mqtt_config['broker_port'],
            self.mqtt_config.get('keepalive', 60)
        )
        self.mqtt_client.loop_start()

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Serial console connected to MQTT broker")
            self.mqtt_connected.set()
            client.subscribe(self.mqtt_config['topics']['raw_data'])
            client.subscribe(self.mqtt_config['topics']['analyzed_data'])
            client.subscribe('tide/observation')
            client.subscribe(self.mqtt_config['topics'].get('system_config', 'system/config'))
        else:
            logging.error(f"MQTT connection failed: rc={rc}")

    def _on_mqtt_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())

            if msg.topic == self.mqtt_config['topics']['raw_data']:
                with self.latest_sample_lock:
                    self.latest_sample = payload

            elif msg.topic == self.mqtt_config['topics']['analyzed_data']:
                with self.result_lock:
                    self.analysis_result = payload
                    self.analysis_event.set()

            elif msg.topic == 'tide/observation':
                with self.result_lock:
                    self.tide_result = payload
                    self.tide_event.set()

            elif msg.topic == self.mqtt_config['topics'].get('system_config', 'system/config'):
                for section in ('analysis', 'radar', 'collection'):
                    if section in payload:
                        self.config.setdefault(section, {}).update(payload[section])
                logging.info(f"Config updated via MQTT: {payload}")

        except Exception as e:
            logging.error(f"MQTT message error: {e}")

    # ------------------------------------------------------------------ Serial
    def _open_serial(self) -> bool:
        try:
            self.ser = serial.Serial(
                port=self.serial_dev,
                baudrate=self.baud_rate,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=0.5
            )
            logging.info(f"Serial opened: {self.serial_dev} @ {self.baud_rate}baud")
            return True
        except Exception as e:
            logging.error(f"Serial open failed: {e}")
            return False

    def _send_json(self, data: dict):
        """线程安全写串口（JSON + \r\n）"""
        try:
            with self.serial_lock:
                if self.ser and self.ser.is_open:
                    line = json.dumps(data, ensure_ascii=False) + '\r\n'
                    self.ser.write(line.encode('utf-8', errors='replace'))
                    self.ser.flush()
                    # 记录发送数据（stream类型太频繁，只记type）
                    if data.get('type') == 'stream':
                        pass  # stream数据量大，不记录
                    else:
                        self.data_logger.info(f"TX >> {line.rstrip()}")
        except Exception as e:
            logging.error(f"Serial write error: {e}")

    # ------------------------------------------------------------------ 流输出
    def _stream_loop(self):
        """后台线程：以6Hz向串口输出雷达原始数据 JSON；无数据时每30s推送一次配置心跳"""
        logging.info("Stream loop started")
        last_ts = None
        last_heartbeat = 0.0
        HEARTBEAT_INTERVAL = 30  # 秒

        while self.running:
            if not self.stream_pause.is_set():
                with self.latest_sample_lock:
                    payload = self.latest_sample

                if payload is not None:
                    ts = payload.get('timestamp', '')
                    if ts != last_ts:
                        sample = payload.get('sample', {})
                        heights = sample.get('heights', [None, None, None])
                        statuses = sample.get('radar_status', [False, False, False])
                        try:
                            time_str = ts[11:23]  # HH:MM:SS.mmm
                        except Exception:
                            time_str = datetime.now().strftime('%H:%M:%S.%f')[:12]

                        self._send_json({
                            "type": "stream",
                            "time": time_str,
                            "r1": round(heights[0], 3) if heights[0] is not None and statuses[0] else None,
                            "r2": round(heights[1], 3) if heights[1] is not None and statuses[1] else None,
                            "r3": round(heights[2], 3) if heights[2] is not None and statuses[2] else None,
                            "online": sum(1 for s in statuses if s)
                        })
                        last_ts = ts
                        last_heartbeat = time.time()
                else:
                    # 无雷达数据：定期推送配置心跳，让 PC 端知道连接有效
                    now = time.time()
                    if now - last_heartbeat >= HEARTBEAT_INTERVAL:
                        self._send_config()
                        last_heartbeat = now

            time.sleep(0.05)  # 20Hz 轮询，确保捕获全部 6Hz 样本
        logging.info("Stream loop stopped")

    # ------------------------------------------------------------------ 分析
    def _get_mode_window(self, mode: str) -> int:
        a = self.config.get('analysis', {})
        if mode == 'METER':
            return a.get('meter_window', 300)
        elif mode == 'WORK':
            return a.get('work_window', 1200)
        return 300

    def _trigger_wave(self, mode: str):
        window = self._get_mode_window(mode)
        with self.result_lock:
            self.analysis_event.clear()
            self.analysis_result = None
        self.mqtt_client.publish(
            self.mqtt_config['topics'].get('system_command', 'system/command'),
            json.dumps({
                'type': 'ANALYZE',
                'mode': mode.lower(),
                'window_duration': window,
                'timestamp': datetime.utcnow().isoformat()
            }), qos=1
        )
        logging.info(f"Triggered {mode} analysis (window={window}s)")

    def _trigger_tide(self):
        with self.result_lock:
            self.tide_event.clear()
            self.tide_result = None
        self.mqtt_client.publish(
            self.mqtt_config['topics'].get('system_command', 'system/command'),
            json.dumps({
                'type': 'TIDE',
                'timestamp': datetime.utcnow().isoformat()
            }), qos=1
        )
        logging.info("Triggered TIDE analysis")

    def _wait_event_or_stop(self, event: Event, timeout: float) -> str:
        """等待 event 或 _stop_event，返回 'ok' / 'stopped' / 'timeout'"""
        deadline = time.time() + timeout
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                return 'timeout'
            if self._stop_event.is_set():
                return 'stopped'
            if event.wait(timeout=min(remaining, 0.5)):
                return 'ok'

    def _run_analysis(self, mode: str):
        """触发波浪+潮位分析，潮位先到先发，波浪完成后再发"""
        window = self._get_mode_window(mode)
        self.stream_pause.set()
        self._stop_event.clear()

        # 同时触发波浪和潮位分析
        self._trigger_wave(mode)
        self._trigger_tide()

        # 等潮位结果（通常几秒内完成），先发
        result = self._wait_event_or_stop(self.tide_event, 60)
        if result == 'stopped':
            logging.info("[Stop] Analysis aborted during tide wait")
            self.stream_pause.clear()
            return
        if result == 'ok':
            with self.result_lock:
                tide = self.tide_result
            if tide:
                self._send_json({
                    "type":       "tide",
                    "mode":       mode.lower(),
                    "time":       datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "tide_level": round(tide['tide_level'], 3) if tide.get('tide_level') is not None else None,
                })
        else:
            logging.warning("Tide analysis timeout (60s)")

        # 等波浪结果
        timeout = window + 60
        result = self._wait_event_or_stop(self.analysis_event, timeout)
        if result == 'stopped':
            logging.info("[Stop] Analysis aborted during wave wait")
            self.stream_pause.clear()
            return
        if result == 'ok':
            with self.result_lock:
                wave = self.analysis_result

            if wave:
                r = wave.get('results', {})
                error_msg = wave.get('metadata', {}).get('error')

                if error_msg or not r:
                    # analyzer 返回了错误结果（数据不足等）
                    self._send_json({"type": "error", "cmd": mode,
                                     "message": error_msg or "分析返回空结果"})
                else:
                    def _f(key, dec=3):
                        v = r.get(key)
                        return round(v, dec) if v is not None else None

                    self._send_json({
                        "type":   "wave",
                        "mode":   mode.lower(),
                        "window": window,
                        "time":   datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        # ---- 频域参数 ----
                        "Hm0":            _f('Hm0'),
                        "Tp":             _f('Tp', 2),
                        "Tz":             _f('Tz', 2),
                        "Te":             _f('Te', 2),
                        "Tm01":           _f('Tm01', 2),
                        "peak_frequency": _f('peak_frequency', 4),
                        "fm":             _f('fm', 4),
                        "fz":             _f('fz', 4),
                        "fe":             _f('fe', 4),
                        "df":             _f('df', 4),
                        "f_min":          _f('f_min', 4),
                        "f_max":          _f('f_max', 4),
                        "Nf":             r.get('Nf'),
                        "epsilon_0":      _f('epsilon_0', 4),
                        # ---- 谱矩 ----
                        "m_minus1": _f('m_minus1', 6),
                        "m0":       _f('m0', 6),
                        "m1":       _f('m1', 6),
                        "m2":       _f('m2', 6),
                        "m4":       _f('m4', 6),
                        # ---- 时域参数（零交叉法）----
                        "Hmax":  _f('Hmax'),
                        "Hs":    _f('Hs'),
                        "H1_10": _f('H1_10'),
                        "Hmean": _f('Hmean'),
                        "Tmax":  _f('Tmax', 2),
                        "T1_10": _f('T1_10', 2),
                        "Ts":    _f('Ts', 2),
                        "Tmean": _f('Tmean', 2),
                        "wave_count": r.get('wave_count', 0),
                        # ---- 方向参数 ----
                        "direction":          _f('wave_direction', 1),
                        "mean_direction":     _f('mean_direction', 1),
                        "directional_spread": _f('directional_spread', 1),
                        "direction_at_peak":  _f('direction_at_peak', 1),
                        # ---- 各雷达波高 ----
                        "Hm0_r1": _f('Hm0_radar1'),
                        "Hm0_r2": _f('Hm0_radar2'),
                        "Hm0_r3": _f('Hm0_radar3'),
                        "radar_count": r.get('radar_count', 0),
                    })
            else:
                self._send_json({"type": "error", "cmd": mode, "message": "未收到分析结果"})
        else:
            self._send_json({"type": "error", "cmd": mode,
                             "message": f"超时: {timeout}s内未完成分析（数据不足或系统繁忙）"})

        self.stream_pause.clear()

    # ------------------------------------------------------------------ 配置
    def _save_config(self):
        """写回YAML并通过MQTT广播完整配置"""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(self.config, f, default_flow_style=False, allow_unicode=True)

        a = self.config.get('analysis', {})
        payload = {
            'analysis': {
                'meter_window':         a.get('meter_window', 300),
                'work_window':          a.get('work_window', 1200),
                'analysis_interval':    a.get('analysis_interval', 300),
                'window_duration':      a.get('window_duration', 1200),
                'tide_window_duration': a.get('tide_window_duration', 300),
                'filter_enable':        a.get('filter_enable', True),
                'filter_band':          a.get('filter_band', [0.04, 1.0]),
            },
            'radar': {
                'array_heading': self.config['radar'].get('array_heading', 0.0),
                'elevation_85':  self.config['radar'].get('elevation_85', self.config['radar'].get('array_height', 5.0)),
                'elevation_85_surveyed': self.config['radar'].get('elevation_85_surveyed', False),
            },
            'collection': {
                'sample_rate': self.config.get('collection', {}).get('sample_rate', 6),
            },
        }
        topic = self.mqtt_config['topics'].get('system_config', 'system/config')
        self.mqtt_client.publish(topic, json.dumps(payload), qos=1)
        logging.info("Config saved and published to MQTT")

    def _send_config(self):
        """向串口发送当前配置 JSON"""
        r = self.config.get('radar', {})
        a = self.config.get('analysis', {})
        c = self.config.get('collection', {})
        self._send_json({
            "type":         "config",
            "elevation_85": r.get('elevation_85', r.get('array_height', 5.0)),
            "elevation_85_surveyed": r.get('elevation_85_surveyed', False),
            "heading":      r.get('array_heading', 0.0),
            "sample_rate":  c.get('sample_rate', 6),
            "meter_window": a.get('meter_window', 300),
            "work_window":  a.get('work_window', 1200),
            "interval":     a.get('analysis_interval', 300),
            "tide_window":  a.get('tide_window_duration', 300),
        })

    def _apply_config_set(self, data: dict):
        """处理 CONFIG_SET 命令，支持批量设置"""
        errors = []
        updated = []

        validators = {
            'elevation_85': (0.0,  200.0, float, 'radar',    'elevation_85',      "elevation_85 须在 0 ~ 200.0 m 范围内"),
            'heading':      (0.0,  359.9, float, 'radar',    'array_heading',     "heading 须在 0.0 ~ 359.9 deg 范围内"),
            'meter_window': (60,   3600,  int,   'analysis', 'meter_window',      "meter_window 须在 60 ~ 3600 s 范围内"),
            'work_window':  (60,   7200,  int,   'analysis', 'work_window',       "work_window 须在 60 ~ 7200 s 范围内"),
            'interval':     (30,   3600,  int,   'analysis', 'analysis_interval', "interval 须在 30 ~ 3600 s 范围内"),
        }

        for key, (lo, hi, cast, section, config_key, err_msg) in validators.items():
            if key not in data:
                continue
            try:
                val = cast(data[key])
            except (TypeError, ValueError):
                errors.append(f"{key} 值无效，须为数字")
                continue
            if not (lo <= val <= hi):
                errors.append(err_msg)
                continue
            self.config.setdefault(section, {})[config_key] = val
            updated.append(f"{key}={val}")
            logging.info(f"Config SET: {config_key} = {val}")

        if errors:
            self._send_json({"type": "error", "cmd": "CONFIG_SET", "message": "; ".join(errors)})
            return

        if updated:
            self._save_config()
            self._send_config()
            self._send_json({"type": "ack", "cmd": "CONFIG_SET", "success": True,
                             "message": f"已更新: {', '.join(updated)}"})
        else:
            self._send_json({"type": "error", "cmd": "CONFIG_SET", "message": "未包含任何有效配置项"})

    # ------------------------------------------------------------------ 延迟分析调度
    def _analysis_scheduler_loop(self):
        """后台线程：等待 deadline 到期后触发分析；收到新指令会重置 deadline"""
        while self.running:
            with self._pending_lock:
                mode = self._pending_mode
                deadline = self._pending_deadline

            if mode is None or deadline is None:
                time.sleep(0.2)
                continue

            remaining = deadline - time.time()
            if remaining > 0:
                time.sleep(min(0.2, remaining))
                continue

            # deadline 到期，抢占执行
            with self._pending_lock:
                if self._pending_mode is None:
                    continue
                if self._pending_deadline > time.time():
                    continue  # 刚被重置，重新等待
                mode = self._pending_mode
                self._pending_mode = None
                self._pending_deadline = None

            logging.info(f"[Scheduler] {mode} countdown finished, running analysis")
            try:
                self._run_analysis(mode)
            except Exception as e:
                logging.error(f"[Scheduler] _run_analysis exception: {e}", exc_info=True)
                self.stream_pause.clear()

            # 连续模式：分析完成后重新调度
            if self._repeat and not self._stop_event.is_set():
                self._schedule_analysis(mode)
                logging.info(f"[Scheduler] {mode} repeat scheduled")

    def _schedule_analysis(self, mode: str):
        """设置或重置延迟分析计划，并通知 analyzer 重置进度条"""
        window = self._get_mode_window(mode)
        with self._pending_lock:
            self._pending_mode = mode
            self._pending_deadline = time.time() + window
        # 通知 analyzer 重置进度条倒计时
        self.mqtt_client.publish(
            self.mqtt_config['topics'].get('system_command', 'system/command'),
            json.dumps({
                'type': 'SCHEDULE',
                'mode': mode.lower(),
                'window_duration': window,
                'timestamp': datetime.utcnow().isoformat()
            }), qos=1
        )
        repeat_label = "，连续模式" if self._repeat else ""
        logging.info(f"[Scheduler] {mode} scheduled in {window}s (repeat={self._repeat})")
        self._send_json({"type": "ack", "cmd": mode, "message": f"将在{window}s后开始分析{repeat_label}"})

    def _stop_analysis(self):
        """停止当前待执行的分析，打断正在等待的分析，清除调度，进度条归零"""
        # 打断正在等待结果的 _run_analysis
        self._stop_event.set()
        self._repeat = False

        with self._pending_lock:
            self._pending_mode = None
            self._pending_deadline = None

        # 通知 analyzer 和 tide_analyzer 停止
        self.mqtt_client.publish(
            self.mqtt_config['topics'].get('system_command', 'system/command'),
            json.dumps({
                'type': 'STOP',
                'timestamp': datetime.utcnow().isoformat()
            }), qos=1
        )

        logging.info("[Stop] Analysis stopped")
        self._send_json({"type": "ack", "cmd": "STOP", "success": True, "message": "分析已停止"})

    # ------------------------------------------------------------------ 命令处理
    def _handle_command(self, line: str):
        line = line.strip()
        if not line:
            return

        # 记录接收到的原始数据
        self.data_logger.info(f"RX << {line}")

        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            logging.warning(f"JSON parse error, raw bytes: {line.encode('utf-8', errors='replace')}")
            self._send_json({"type": "error", "cmd": "", "message": f"JSON解析失败: {line}"})
            return

        cmd = data.get('cmd', '').upper()
        logging.info(f"Command received: {cmd}, raw: {data}")

        # 解析 repeat 字段（兼容大小写、数字、字符串）
        repeat_raw = data.get('repeat', data.get('Repeat', data.get('REPEAT', False)))
        if isinstance(repeat_raw, str):
            repeat_val = repeat_raw.lower() in ('true', '1', 'yes')
        else:
            repeat_val = bool(repeat_raw)

        if cmd == 'METER':
            self._repeat = repeat_val
            self._schedule_analysis('METER')

        elif cmd == 'WORK':
            self._repeat = repeat_val
            self._schedule_analysis('WORK')

        elif cmd == 'STOP':
            self._stop_analysis()

        elif cmd == 'STATUS':
            with self.latest_sample_lock:
                payload = self.latest_sample
            if payload:
                sample = payload.get('sample', {})
                ok = sum(1 for s in sample.get('radar_status', []) if s)
            else:
                ok = 0
            self._send_json({
                "type":   "status",
                "time":   datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "online": ok,
            })

        elif cmd == 'CONFIG_GET':
            self._send_config()

        elif cmd == 'CONFIG_SET':
            self._apply_config_set(data)

        else:
            self._send_json({"type": "error", "cmd": cmd,
                             "message": f"未知命令: {cmd}，支持: METER, WORK, STOP, STATUS, CONFIG_GET, CONFIG_SET"})

    # ------------------------------------------------------------------ 主循环
    def run(self):
        logging.info("Starting Serial Console Service")

        self._setup_mqtt()
        if not self.mqtt_connected.wait(timeout=15):
            logging.error("MQTT connection timeout, exiting")
            return

        while not self._open_serial():
            logging.warning(f"Retrying serial port {self.serial_dev} in 10s...")
            time.sleep(10)

        # 上电推送当前配置
        self._send_config()

        self.running = True

        stream_thread = Thread(target=self._stream_loop, daemon=True)
        stream_thread.start()

        scheduler_thread = Thread(target=self._analysis_scheduler_loop, daemon=True)
        scheduler_thread.start()

        buf = b''
        try:
            while self.running:
                try:
                    data = self.ser.read(64)
                    if not data:
                        continue
                    buf += data
                    while True:
                        found = False
                        for sep in (b'\r\n', b'\n', b'\r'):
                            idx = buf.find(sep)
                            if idx >= 0:
                                line = buf[:idx].decode('utf-8', errors='ignore').strip()
                                buf = buf[idx + len(sep):]
                                self._handle_command(line)
                                found = True
                                break
                        if not found:
                            break
                except serial.SerialException as e:
                    logging.error(f"Serial error: {e}, reconnecting in 5s...")
                    time.sleep(5)
                    self._open_serial()
                except Exception as e:
                    logging.error(f"Unexpected error: {e}")

        except KeyboardInterrupt:
            pass
        finally:
            self.running = False
            logging.info("Serial console stopping...")
            if self.ser and self.ser.is_open:
                self.ser.close()
            if self.mqtt_client:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
            logging.info("Serial console stopped")


def main():
    config_path = Path(__file__).parent.parent / 'config' / 'system_config.yaml'
    if not config_path.exists():
        print(f"Config not found: {config_path}")
        sys.exit(1)

    console = SerialConsole(str(config_path))

    def signal_handler(signum, frame):
        console.running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    console.run()


if __name__ == '__main__':
    main()
