#!/usr/bin/env python3
"""
波浪监测系统 - 数据模拟器
用于测试前端界面，生成模拟的雷达和波浪数据
"""

import time
import json
import random
import math
from datetime import datetime
import paho.mqtt.client as mqtt
import yaml

# 加载配置
with open('config/system_config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# MQTT 配置
mqtt_config = config['mqtt']
client = mqtt.Client(client_id="wave_simulator")
client.username_pw_set(mqtt_config['username'], mqtt_config['password'])

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ 连接到 MQTT Broker 成功")
    else:
        print(f"❌ 连接失败，返回码: {rc}")

client.on_connect = on_connect
client.connect(mqtt_config['broker_host'], mqtt_config['broker_port'], 60)
client.loop_start()

print("🌊 波浪数据模拟器启动中...")
print("=" * 50)

# 模拟参数
base_level = 2.0  # 基准液位 (米)
wave_height = 0.5  # 波高 (米)
wave_period = 8.0  # 波浪周期 (秒)
sample_rate = 6.0  # 采样率 (Hz)

count = 0
start_time = time.time()

try:
    while True:
        current_time = datetime.now()
        timestamp = current_time.isoformat()
        elapsed = time.time() - start_time
        
        # 为每个雷达生成模拟数据
        for radar_id in [1, 2, 3]:
            # 添加随机相位差
            phase = radar_id * 0.5
            
            # 生成波浪数据 (正弦波 + 噪声)
            wave = wave_height * math.sin(2 * math.pi * elapsed / wave_period + phase)
            noise = random.gauss(0, 0.02)  # 2cm 噪声
            distance = base_level + wave + noise
            
            # 构造消息
            message = {
                "timestamp": timestamp,
                "radar_id": radar_id,
                "distance": round(distance, 4),
                "quality": random.randint(90, 100)
            }
            
            # 发布到 MQTT
            client.publish(
                mqtt_config['topics']['raw_data'],
                json.dumps(message),
                qos=1
            )
        
        count += 1
        
        # 每10秒输出一次状态
        if count % 40 == 0:
            print(f"已发送 {count} 组数据 ({count//4} 秒)")
        
        # 每60秒生成一次波浪分析结果
        if count % 240 == 0:
            analysis = {
                "timestamp": timestamp,
                "hm0": round(wave_height * 1.4, 2),  # 有效波高
                "tp": round(wave_period, 2),  # 峰值周期
                "tz": round(wave_period * 0.8, 2),  # 平均周期
                "wave_direction": round(random.uniform(0, 360), 1),
                "peak_frequency": round(1.0 / wave_period, 3),
                "sample_count": 600,
                "duration": 600.0
            }
            client.publish(
                mqtt_config['topics']['analyzed_data'],
                json.dumps(analysis),
                qos=1
            )
            print(f"📊 发送波浪分析结果: Hs={analysis['hm0']}m, Tp={analysis['tp']}s")
        
        # 按采样率等待
        time.sleep(1.0 / sample_rate)
        
except KeyboardInterrupt:
    print("\n⏹️  停止数据模拟器")
    client.loop_stop()
    client.disconnect()
