#!/usr/bin/env python3
"""
直接调用项目 WaveAnalyzer 分析三个时段的数据
从 PostgreSQL 取原始数据，模拟 MQTT 数据流的输入格式，
调用 mqtt_analyzer.py 中的 WaveAnalyzer.analyze_window()
"""

import sys
import os
import logging
import numpy as np
import psycopg2
import yaml
from datetime import datetime, timezone

# 设置路径：使用实际运行的版本（不是旧的 temp/deployment）
services_dir = '/home/pi/radar/mqtt_system/services'
sys.path.insert(0, services_dir)

# 加载配置
config_path = '/home/pi/radar/mqtt_system/config/system_config.yaml'
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 修补 numpy 兼容性问题 (项目代码使用 np.trapz, 新版numpy已改名为 np.trapezoid)
if not hasattr(np, 'trapz'):
    np.trapz = np.trapezoid

# 导入项目的 WaveAnalyzer
from mqtt_analyzer import WaveAnalyzer

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'wave_monitoring',
    'user': 'wave_user',
    'password': 'wave2025',
}

PERIODS = [
    ("9:40-9:45",   "2026-02-10 09:40:00+08", "2026-02-10 09:45:00+08"),
    ("10:04-10:09", "2026-02-10 10:04:00+08", "2026-02-10 10:09:00+08"),
    ("10:28-10:33", "2026-02-10 10:28:00+08", "2026-02-10 10:33:00+08"),
]


def fetch_period_data(conn, t_start, t_end):
    """
    从数据库获取一个时段的三雷达数据，
    返回格式与 MQTTAnalysisService._get_analysis_window() 一致
    """
    sql = """
        SELECT timestamp, radar_id, distance
        FROM wave_measurements
        WHERE timestamp >= %s AND timestamp < %s
        ORDER BY timestamp, radar_id
    """
    cur = conn.cursor()
    cur.execute(sql, (t_start, t_end))
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return None

    # 按时间戳分组
    from collections import OrderedDict
    groups = OrderedDict()
    for ts, radar_id, distance in rows:
        ts_key = ts.isoformat()
        if ts_key not in groups:
            groups[ts_key] = {1: np.nan, 2: np.nan, 3: np.nan}
        groups[ts_key][radar_id] = distance

    timestamps = []
    eta1 = []
    eta2 = []
    eta3 = []

    for ts_key, radars in groups.items():
        timestamps.append(ts_key)
        # 只用 R1，R2/R3 设为 NaN → 触发单雷达模式
        eta1.append(radars[1])
        eta2.append(np.nan)
        eta3.append(np.nan)

    return {
        'timestamps': timestamps,
        'eta1': eta1,
        'eta2': eta2,
        'eta3': eta3,
    }


def print_results(label, analysis):
    """打印分析结果"""
    if analysis is None:
        print(f"  [分析失败，返回 None]")
        return

    r = analysis['results']
    meta = analysis['metadata']

    print(f"\n  ▌元数据")
    print(f"  {'有效样本数':<20s}: {meta['sample_count']}")
    print(f"  {'采样率':<20s}: {meta['sample_rate']} Hz")
    print(f"  {'持续时间':<20s}: {meta['duration_seconds']:.1f} s")
    print(f"  {'活跃雷达数':<20s}: {meta['active_radars']}")

    print(f"\n  ▌频域分析 (Welch谱)")
    print(f"  {'Hm0 (谱有效波高)':<20s}: {r['Hm0']:.4f} m")
    print(f"  {'Tp  (谱峰周期)':<20s}: {r['Tp']:.4f} s")
    print(f"  {'Tz  (零交叉周期)':<20s}: {r['Tz']:.4f} s")
    print(f"  {'Tm01(平均周期)':<20s}: {r['Tm01']:.4f} s")
    print(f"  {'Te  (能量周期)':<20s}: {r['Te']:.4f} s")
    print(f"  {'fp  (谱峰频率)':<20s}: {r['peak_frequency']:.4f} Hz")
    print(f"  {'fm  (平均频率)':<20s}: {r['fm']:.4f} Hz")
    print(f"  {'fz  (零交叉频率)':<20s}: {r['fz']:.4f} Hz")
    print(f"  {'fe  (能量频率)':<20s}: {r['fe']:.4f} Hz")
    print(f"  {'m0':<20s}: {r['m0']:.6f} m²")
    print(f"  {'m1':<20s}: {r['m1']:.6f}")
    print(f"  {'m2':<20s}: {r['m2']:.6f}")
    print(f"  {'m4':<20s}: {r['m4']:.6f}")
    print(f"  {'epsilon_0(谱宽)':<20s}: {r['epsilon_0']:.4f}")
    print(f"  {'df (频率分辨率)':<20s}: {r['df']:.4f} Hz")
    print(f"  {'Nf (频率点数)':<20s}: {r['Nf']}")

    print(f"\n  ▌时域分析 (零交叉法)")
    print(f"  {'Hs (有效波高 H1/3)':<20s}: {r['Hs']:.4f} m")
    print(f"  {'H1/10':<20s}: {r['H1_10']:.4f} m")
    print(f"  {'Hmax':<20s}: {r['Hmax']:.4f} m")
    print(f"  {'Hmean':<20s}: {r['Hmean']:.4f} m")
    print(f"  {'Ts (有效波周期)':<20s}: {r['Ts']:.4f} s")
    print(f"  {'T1/10':<20s}: {r['T1_10']:.4f} s")
    print(f"  {'Tmax':<20s}: {r['Tmax']:.4f} s")
    print(f"  {'Tmean':<20s}: {r['Tmean']:.4f} s")
    print(f"  {'波数':<20s}: {r['wave_count']}")
    print(f"  {'潮位 (cm)':<20s}: {r['mean_level']:.2f} cm")

    if r.get('Hm0_radar2') is not None:
        print(f"\n  ▌各雷达 Hm0")
        print(f"  {'Radar1':<20s}: {r['Hm0_radar1']:.4f} m")
        print(f"  {'Radar2':<20s}: {r['Hm0_radar2']:.4f} m")
        if r.get('Hm0_radar3') is not None:
            print(f"  {'Radar3':<20s}: {r['Hm0_radar3']:.4f} m")


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    analyzer = WaveAnalyzer(config)

    print("=" * 72)
    print("  使用项目 WaveAnalyzer 分析三时段数据 (2026-02-10)")
    print("  分析流程: _preprocess() → _analyze_single_radar() → analyze_window()")
    print("=" * 72)

    all_results = []

    for label, t_start, t_end in PERIODS:
        print(f"\n{'─' * 72}")
        print(f"  时段: {label}")
        print(f"{'─' * 72}")

        data = fetch_period_data(conn, t_start, t_end)
        if data is None:
            print("  [无数据]")
            all_results.append(None)
            continue

        n = len(data['timestamps'])
        print(f"  原始数据点: {n} (每时刻3雷达)")

        # 调用项目的 analyze_window
        analysis = analyzer.analyze_window(data)
        all_results.append(analysis)
        print_results(label, analysis)

    # ====== 汇总对比表 ======
    print(f"\n\n{'=' * 72}")
    print("  三时段对比汇总")
    print(f"{'=' * 72}")

    labels = [p[0] for p in PERIODS]
    header = f"  {'参数':<22s}"
    for lb in labels:
        header += f" | {lb:>12s}"
    print(header)
    print(f"  {'─' * 22}" + ("-+-" + "-" * 12) * 3)

    def row(name, key, fmt=".4f"):
        line = f"  {name:<22s}"
        for res in all_results:
            if res is None:
                line += f" | {'N/A':>12s}"
            else:
                v = res['results'].get(key, float('nan'))
                if isinstance(v, int):
                    line += f" | {v:>12d}"
                elif v is None:
                    line += f" | {'N/A':>12s}"
                else:
                    line += f" | {v:>{12}{fmt}}"
        print(line)

    print("  --- 频域 (Welch) ---")
    row("Hm0 (m)", "Hm0")
    row("Tp (s)", "Tp")
    row("Tz (s)", "Tz")
    row("Tm01 (s)", "Tm01")
    row("Te (s)", "Te")
    row("fp (Hz)", "peak_frequency")
    row("m0 (m²)", "m0", ".6f")
    row("epsilon_0", "epsilon_0")

    print("  --- 时域 (零交叉) ---")
    row("Hs (m)", "Hs")
    row("H1/10 (m)", "H1_10")
    row("Hmax (m)", "Hmax")
    row("Hmean (m)", "Hmean")
    row("Ts (s)", "Ts")
    row("T1/10 (s)", "T1_10")
    row("Tmax (s)", "Tmax")
    row("Tmean (s)", "Tmean")
    row("波数", "wave_count", "d")
    row("潮位 (cm)", "mean_level", ".2f")

    conn.close()
    print(f"\n{'=' * 72}")
    print("  分析完成")
    print(f"{'=' * 72}")


if __name__ == "__main__":
    main()
