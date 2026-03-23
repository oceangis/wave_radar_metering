#!/usr/bin/env python3
"""用最近实测数据对比 IMLM 和 EMEP 方向分析结果"""

import sys
sys.path.insert(0, '/home/pi/radar/mqtt_system/services')

import numpy as np
import psycopg2
import yaml
from datetime import datetime, timedelta, timezone
from scipy.interpolate import interp1d
from scipy.signal import detrend

# 加载配置
with open('/home/pi/radar/mqtt_system/config/system_config.yaml') as f:
    config = yaml.safe_load(f)

# 从数据库取最近 300s 三雷达数据
tz = timezone(timedelta(hours=8))
conn = psycopg2.connect(
    host='localhost', port=5432,
    database='wave_monitoring', user='wave_user', password='wave2025'
)
cur = conn.cursor()

# 取最近的数据时间
cur.execute("SELECT max(timestamp) FROM wave_measurements WHERE radar_id=1")
t_max = cur.fetchone()[0]
t_min = t_max - timedelta(seconds=300)

print(f"数据范围: {t_min} ~ {t_max}")

# 取三个雷达的数据
data = {}
for rid in [1, 2, 3]:
    cur.execute(
        "SELECT timestamp, distance FROM wave_measurements "
        "WHERE radar_id=%s AND timestamp BETWEEN %s AND %s ORDER BY timestamp",
        (rid, t_min, t_max)
    )
    rows = cur.fetchall()
    ts = np.array([(r[0].timestamp()) for r in rows])
    dist = np.array([r[1] for r in rows])
    data[rid] = (ts, dist)
    print(f"  R{rid}: {len(rows)} samples")

conn.close()

# 插值对齐到 R1 时间轴
t_ref = data[1][0]
aligned = {}
for rid in [1, 2, 3]:
    ts, dist = data[rid]
    if rid == 1:
        aligned[rid] = dist
    else:
        f_interp = interp1d(ts, dist, bounds_error=False, fill_value='extrapolate')
        aligned[rid] = f_interp(t_ref)

# 转换为波面高程 η = -(distance - median)
eta = {}
for rid in [1, 2, 3]:
    d = aligned[rid]
    eta[rid] = -(d - np.median(d))

# 重采样到均匀 6Hz
fs = 6.0
t_rel = t_ref - t_ref[0]
duration = t_rel[-1]
t_uniform = np.arange(0, duration, 1.0/fs)

eta_resampled = {}
for rid in [1, 2, 3]:
    f_interp = interp1d(t_rel, eta[rid], bounds_error=False, fill_value=0.0)
    eta_resampled[rid] = detrend(f_interp(t_uniform))

n_samples = len(t_uniform)
print(f"\n重采样后: {n_samples} samples, {duration:.1f}s, fs={fs}Hz")
print(f"  Hs(R1) ≈ {4*np.std(eta_resampled[1]):.3f}m")

# 初始化方向谱分析器
from directional_spectrum import DirectionalSpectrumAnalyzer

analyzer_config = {
    'sample_rate': fs,
    'gravity': 9.81,
    'water_depth': 100.0,
    'freq_range': config['analysis']['filter_band'],
    'direction_resolution': 180,  # 2°分辨率，加速EMEP
    'array_heading': config['radar']['array_heading'],
    'array_height': config['radar']['array_height'],
    'tilt_angles': config['radar'].get('tilt_angles', {}),
    'tilt_azimuths': config['radar'].get('tilt_azimuths', {}),
    'radar_positions': config['radar'].get('diwasp_positions', {}),
}

analyzer = DirectionalSpectrumAnalyzer(analyzer_config)

# R1 平均测距 → 动态基线
r1_mean = float(np.mean(aligned[1]))
analyzer.update_layout(r1_mean)
print(f"  R1 mean distance: {r1_mean:.3f}m")

# 倾斜校正
tilt_factors = analyzer.tilt_factors
eta1 = eta_resampled[1]
eta2 = eta_resampled[2] * tilt_factors['R2']
eta3 = eta_resampled[3] * tilt_factors['R3']

# 对比两种方法
print(f"\n{'='*60}")
print(f"阵列朝向: {config['radar']['array_heading']}°")
print(f"{'='*60}")

for method in ['IMLM', 'EMEP']:
    print(f"\n--- {method} ---")
    results = analyzer.compute_directional_spectrum(eta1, eta2, eta3, method=method)
    if results.get('success', True):
        print(f"  Dp  = {results['Dp']:.1f}°  (主波向)")
        print(f"  DTp = {results['DTp']:.1f}°  (峰值周期方向)")
        print(f"  Mean= {results['mean_direction']:.1f}°")
        print(f"  Spread = {results['directional_spread']:.1f}°")
        print(f"  Hs  = {results['Hs']:.3f}m")
        print(f"  Tp  = {results['Tp']:.2f}s")
    else:
        print(f"  分析失败")

# 也试试 BDM 和 DFTM
for method in ['BDM', 'DFTM']:
    print(f"\n--- {method} ---")
    try:
        results = analyzer.compute_directional_spectrum(eta1, eta2, eta3, method=method)
        if results.get('success', True):
            print(f"  Dp  = {results['Dp']:.1f}°  (主波向)")
            print(f"  DTp = {results['DTp']:.1f}°  (峰值周期方向)")
            print(f"  Spread = {results['directional_spread']:.1f}°")
            print(f"  Hs  = {results['Hs']:.3f}m")
            print(f"  Tp  = {results['Tp']:.2f}s")
        else:
            print(f"  分析失败")
    except Exception as e:
        print(f"  错误: {e}")
