#!/usr/bin/env python3
"""重跑昨天下午的meter分析，对比新旧去尖刺效果"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'services'))

import yaml
import numpy as np
import psycopg2
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(message)s')

# 加载当前配置（含更新后的meter_filter参数）
config_path = os.path.join(os.path.dirname(__file__), 'config', 'system_config.yaml')
with open(config_path) as f:
    config = yaml.safe_load(f)

from mqtt_analyzer import WaveAnalyzer

analyzer = WaveAnalyzer(config)

# 昨天下午分析窗口：start_time → start_time + 300s (DB中start_time是窗口起点)
windows = [
    ('14:10', '2026-03-19 14:10:42+08', '2026-03-19 14:15:42+08'),
    ('14:36', '2026-03-19 14:36:29+08', '2026-03-19 14:41:29+08'),
    ('14:41', '2026-03-19 14:41:31+08', '2026-03-19 14:46:31+08'),
    ('14:59', '2026-03-19 14:59:02+08', '2026-03-19 15:04:02+08'),
    ('16:04', '2026-03-19 16:04:30+08', '2026-03-19 16:09:30+08'),
    ('16:30', '2026-03-19 16:30:56+08', '2026-03-19 16:35:56+08'),
]

# 旧结果（从数据库查询的原始分析结果）
old_results = {
    '14:10': {'Hm0': 0.0668, 'Hs_zc': 0.0460, 'Hmean': 0.0350, 'Tp': 2.00, 'Hmax': 0.0780, 'Dir': 184,
              'R1': 0.0668, 'R2': 0.1207, 'R3': 0.1294},
    '14:36': {'Hm0': 0.2972, 'Hs_zc': 0.2186, 'Hmean': 0.1963, 'Tp': 3.00, 'Hmax': 0.2386, 'Dir': 183,
              'R1': 0.2972, 'R2': 0.2973, 'R3': 0.3031},
    '14:41': {'Hm0': 0.0965, 'Hs_zc': 0.0977, 'Hmean': 0.0430, 'Tp': 3.00, 'Hmax': 0.1362, 'Dir': 279,
              'R1': 0.0965, 'R2': 3.0440, 'R3': 5.1898},
    '14:59': {'Hm0': 0.1416, 'Hs_zc': 0.1016, 'Hmean': 0.0875, 'Tp': 2.00, 'Hmax': 0.1162, 'Dir': 183,
              'R1': 0.1416, 'R2': 0.1666, 'R3': 0.1481},
    '16:04': {'Hm0': 0.3718, 'Hs_zc': 0.2873, 'Hmean': 0.2607, 'Tp': 4.05, 'Hmax': 0.4418, 'Dir': 77,
              'R1': 0.3718, 'R2': 0.3790, 'R3': 0.3796},
    '16:30': {'Hm0': 0.3015, 'Hs_zc': 0.2269, 'Hmean': 0.2011, 'Tp': 3.00, 'Hmax': 0.4897, 'Dir': 80,
              'R1': 0.3015, 'R2': 0.3092, 'R3': 0.3041},
}

# 从数据库读取原始数据
db_cfg = config['database']
conn = psycopg2.connect(
    host=db_cfg['host'], port=db_cfg['port'],
    database=db_cfg['database'],
    user=db_cfg['user'], password=db_cfg['password']
)

print("=" * 100)
print("重分析对比：昨天meter模式 vs 新激进去尖刺")
print("=" * 100)

for label, t_start, t_end in windows:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT timestamp, radar_id, distance
        FROM wave_measurements
        WHERE timestamp BETWEEN %s AND %s
        ORDER BY timestamp ASC
    """, (t_start, t_end))
    rows = cursor.fetchall()
    cursor.close()

    # 组装数据（与_get_window_from_database格式一致）
    time_groups = defaultdict(lambda: {1: np.nan, 2: np.nan, 3: np.nan})
    for ts, radar_id, distance in rows:
        if 1 <= radar_id <= 3:
            time_groups[ts][radar_id] = distance if distance is not None else np.nan

    sorted_times = sorted(time_groups.keys())
    timestamps = [ts.isoformat() for ts in sorted_times]
    eta1 = [time_groups[ts][1] for ts in sorted_times]
    eta2 = [time_groups[ts][2] for ts in sorted_times]
    eta3 = [time_groups[ts][3] for ts in sorted_times]

    data = {
        'timestamps': timestamps,
        'timestamps_r2': timestamps.copy(),
        'timestamps_r3': timestamps.copy(),
        'eta1': eta1,
        'eta2': eta2,
        'eta3': eta3,
    }

    print(f"\n{'─' * 100}")
    print(f"窗口 {label} | {t_start} → {t_end} | 样本数: R1={len(eta1)}")
    print(f"{'─' * 100}")

    # 新meter模式分析
    try:
        result = analyzer.analyze_window(data, mode='meter')
        if result:
            # 结果嵌套在result['results']中
            r = result.get('results', result)
            new = {
                'Hm0': r.get('Hm0', 0),
                'Hs_zc': r.get('Hs', 0),
                'Hmean': r.get('Hmean', 0),
                'Tp': r.get('Tp', 0),
                'Hmax': r.get('Hmax', 0),
                'Dir': r.get('wave_direction', 0),
            }
            old = old_results.get(label, {})

            print(f"\n  {'参数':<10} {'旧值':>10} {'新值':>10} {'变化':>10} {'变化%':>8}")
            print(f"  {'─'*52}")
            for key in ['Hm0', 'Hs_zc', 'Hmean', 'Tp', 'Hmax']:
                ov = old.get(key, 0)
                nv = new.get(key, 0)
                diff = nv - ov
                pct = (diff / ov * 100) if ov != 0 else 0
                flag = " ←" if abs(pct) > 10 else ""
                print(f"  {key:<10} {ov:>10.4f} {nv:>10.4f} {diff:>+10.4f} {pct:>+7.1f}%{flag}")
            # 方向
            print(f"  {'Dir':<10} {old.get('Dir', 0):>10.0f}° {new.get('Dir', 0):>10.0f}°")

            # R2/R3 Hs对比（检测异常是否被修正）
            if old.get('R2', 0) > 1.0 or old.get('R3', 0) > 1.0:
                print(f"\n  *** 旧分析R2/R3异常: R2={old['R2']:.3f}m  R3={old['R3']:.3f}m ***")
                if 'hs_r2' in result or 'Hs_R2' in result:
                    print(f"  *** 新分析: R2={result.get('hs_r2', result.get('Hs_R2', '?'))}m  "
                          f"R3={result.get('hs_r3', result.get('Hs_R3', '?'))}m ***")
        else:
            print(f"  分析返回 None")
    except Exception as e:
        print(f"  分析失败: {e}")
        import traceback
        traceback.print_exc()

conn.close()
print(f"\n{'=' * 100}")
print("完成")
