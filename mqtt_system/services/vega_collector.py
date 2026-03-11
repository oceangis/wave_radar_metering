#!/usr/bin/env python3
"""
VEGA雷达采集器 - MQTT版本
使用已有的VegaModbusReader类
"""

import sys
sys.path.insert(0, '/home/obsis/radar/sensor_wave_radar/rpi')

import json
import time
import yaml
import logging
from datetime import datetime
import paho.mqtt.client as mqtt
from vega_modbus_read import VegaModbusReader

class VegaCollector:
    def __init__(self, config_path='config/system_config.yaml'):
        # 加载配置
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('VegaCollector')
        
        # MQTT配置
        mqtt_config = self.config['mqtt']
        self.mqtt_client = mqtt.Client(client_id="vega_collector")
        self.mqtt_client.username_pw_set(
            mqtt_config['username'],
            mqtt_config['password']
        )
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.connect(
            mqtt_config['broker_host'],
            mqtt_config['broker_port'],
            mqtt_config['keepalive']
        )
        self.mqtt_client.loop_start()
        
        # 雷达配置
        radar_config = self.config['radar']
        port = radar_config['ports'][0]  # 只用第一个
        
        self.reader = VegaModbusReader(
            port=port,
            baudrate=radar_config['baudrate'],
            address=radar_config['modbus_address']
        )
        
        # 采样配置
        self.sample_rate = self.config['collection']['sample_rate']
        self.publish_interval = self.config['collection']['publish_interval']
        
        self.logger.info("VEGA采集器初始化完成")
        self.logger.info(f"端口: {port}, 波特率: {radar_config['baudrate']}, 地址: {radar_config['modbus_address']}")
        self.logger.info(f"采样率: {self.sample_rate} Hz")
    
    def _on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("MQTT连接成功")
        else:
            self.logger.error(f"MQTT连接失败: {rc}")
    
    def run(self):
        """主循环"""
        if not self.reader.connect():
            self.logger.error("无法连接雷达")
            return
        
        self.logger.info("开始采集数据...")
        
        interval = 1.0 / self.sample_rate
        count = 0
        error_count = 0
        
        try:
            while True:
                count += 1
                
                # 读取SV值
                sv_value = self.reader.read_sv_fast()
                
                if sv_value is not None:
                    # 发送到MQTT
                    message = {
                        'timestamp': datetime.now().isoformat(),
                        'radar_id': 1,
                        'distance': round(sv_value, 4),
                        'quality': 100  # VEGA没有质量值，固定100
                    }
                    
                    topic = self.config['mqtt']['topics']['raw_data']
                    self.mqtt_client.publish(topic, json.dumps(message), qos=1)
                    
                    if count % 40 == 0:  # 每10秒显示一次
                        self.logger.info(f"雷达1: {sv_value:.3f}m (已发送 {count} 条)")
                else:
                    error_count += 1
                    if error_count % 10 == 0:
                        self.logger.warning(f"读取失败 {error_count} 次")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            self.logger.info(f"\n停止采集，共采集 {count} 条，失败 {error_count} 次")
        finally:
            self.reader.disconnect()
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()

if __name__ == '__main__':
    collector = VegaCollector()
    collector.run()
