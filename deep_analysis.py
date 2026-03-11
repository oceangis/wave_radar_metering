#!/usr/bin/env python3
"""
深度诊断：三时段预处理 + 误差来源分析
"""
import sys, os
import numpy as np
import psycopg2
import yaml
from datetime import datetime

services_dir = '/home/pi/radar/mqtt_system/services'
sys.path.insert(0, services_dir)

if not hasattr(np, 'trapz'):
    np.trapz = np.trapezoid

config_path = '/home/pi/radar/mqtt_system/config/system_config.yaml'
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

DB_CONFIG = {
    'host': 'localhost', 'port': 5432,
    'database': 'wave_monitoring', 'user': 'wave_user', 'password': 'wave2025',
}

PERIODS = [
    ("9:40-9:45",   "2026-02-10 09:40:00+08", "2026-02-10 09:45:00+08", 0.1, 2.0),
    ("10:04-10:09", "2026-02-10 10:04:00+08", "2026-02-10 10:09:00+08", 0.2, 3.0),
    ("10:28-10:33", "2026-02-10 10:28:00+08", "2026-02-10 10:33:00+08", 0.3, 4.0),
]


def fetch_r1(conn, t_start, t_end):
    cur = conn.cursor()
    cur.execute("""
        SELECT timestamp, distance FROM wave_measurements
        WHERE radar_id=1 AND timestamp >= %s AND timestamp < %s
        ORDER BY timestamp
    """, (t_start, t_end))
    rows = cur.fetchall()
    cur.close()
    timestamps = [r[0].isoformat() for r in rows]
    distances = np.array([r[1] for r in rows], dtype=float)
    t_epoch = np.array([r[0].timestamp() for r in rows])
    return timestamps, distances, t_epoch


def analyze_spike_removal(distances, label):
    """分析去尖刺过程的细节"""
    d = distances.copy()

    # IQR
    q25, q75 = np.percentile(d, [25, 75])
    iqr = q75 - q25
    if iqr < 0.001:
        iqr = 0.001
    lower = q25 - 3.0 * iqr
    upper = q75 + 3.0 * iqr
    spike_iqr = (d < lower) | (d > upper)

    # Jump filter (与项目代码一致)
    d_temp = d.copy()
    if np.any(spike_iqr):
        good_temp = ~spike_iqr
        if np.sum(good_temp) > 10:
            d_temp[spike_iqr] = np.interp(
                np.where(spike_iqr)[0],
                np.where(good_temp)[0],
                d_temp[good_temp]
            )
    diff = np.abs(np.diff(d_temp))
    spike_jump_fwd = np.concatenate(([False], diff > 0.2))
    spike_jump_bwd = np.concatenate((diff > 0.2, [False]))
    spike_mask = spike_iqr | spike_jump_fwd | spike_jump_bwd

    # 清洗
    dist_clean = d.copy()
    if np.sum(spike_mask) > 0:
        good = ~spike_mask
        if np.any(good):
            dist_clean[spike_mask] = np.interp(
                np.where(spike_mask)[0],
                np.where(good)[0],
                dist_clean[good]
            )

    n_total = len(d)
    n_iqr = int(np.sum(spike_iqr))
    n_jump_only = int(np.sum(spike_jump_fwd | spike_jump_bwd) - np.sum(spike_iqr & (spike_jump_fwd | spike_jump_bwd)))
    n_spike = int(np.sum(spike_mask))

    # 清洗后统计
    eta_clean = -(dist_clean - np.mean(dist_clean))
    eta_raw = -(d - np.mean(d))

    return {
        'n_total': n_total,
        'n_spike_iqr': n_iqr,
        'n_spike_jump': n_jump_only,
        'n_spike_total': n_spike,
        'spike_pct': n_spike / n_total * 100,
        'iqr': iqr,
        'iqr_lower': lower,
        'iqr_upper': upper,
        'q25': q25, 'q75': q75,
        'raw_std': np.std(d),
        'clean_std': np.std(dist_clean),
        'raw_range': np.ptp(d),
        'clean_range': np.ptp(dist_clean),
        'raw_eta_std': np.std(eta_raw),
        'clean_eta_std': np.std(eta_clean),
        'dist_clean': dist_clean,
        'spike_mask': spike_mask,
    }


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    print("=" * 80)
    print("  三时段深度诊断分析")
    print("=" * 80)

    all_info = []

    for label, t_start, t_end, H_nom, T_nom in PERIODS:
        ts_list, distances, t_epoch = fetch_r1(conn, t_start, t_end)
        info = analyze_spike_removal(distances, label)
        info['label'] = label
        info['H_nom'] = H_nom
        info['T_nom'] = T_nom

        t_rel = t_epoch - t_epoch[0]
        duration = t_rel[-1]
        actual_fs = (len(distances) - 1) / duration

        # 用项目代码跑一遍拿结果
        from mqtt_analyzer import WaveAnalyzer
        analyzer = WaveAnalyzer(config)
        import logging
        logging.disable(logging.CRITICAL)
        data = {
            'timestamps': ts_list,
            'eta1': distances.tolist(),
            'eta2': [np.nan] * len(distances),
            'eta3': [np.nan] * len(distances),
        }
        result = analyzer.analyze_window(data)
        logging.disable(logging.NOTSET)

        r = result['results']
        info['Hm0'] = r['Hm0']
        info['Hs'] = r['Hs']
        info['Tp'] = r['Tp']
        info['Ts'] = r['Ts']
        info['actual_fs'] = actual_fs
        info['duration'] = duration

        # 清洗后的 eta 做独立谱分析对比
        dist_clean = info['dist_clean']
        eta_clean = -(dist_clean - np.mean(dist_clean))
        info['Hm0_from_std'] = 4.0 * np.std(eta_clean)  # 4σ估计

        # 看被删掉的尖峰值分布
        spike_vals = distances[info['spike_mask']]
        clean_vals = distances[~info['spike_mask']]
        info['spike_mean'] = np.mean(spike_vals) if len(spike_vals) > 0 else 0
        info['clean_mean'] = np.mean(clean_vals)
        info['spike_above_pct'] = np.sum(spike_vals > info['iqr_upper']) / max(1, len(spike_vals)) * 100

        all_info.append(info)

        print(f"\n{'─' * 80}")
        print(f"  {label}  (标称: H={H_nom}m, T={T_nom}s)")
        print(f"{'─' * 80}")
        print(f"  原始数据: {info['n_total']}点, 实际采样率={actual_fs:.2f}Hz, 时长={duration:.1f}s")
        print(f"  原始距离: mean={np.mean(distances):.4f}, std={info['raw_std']:.4f}, "
              f"range={info['raw_range']:.4f}m")
        print(f"  原始距离: min={np.min(distances):.4f}, max={np.max(distances):.4f}")
        print()
        print(f"  ▌去尖刺详情")
        print(f"  IQR = {info['iqr']*1000:.1f}mm  (Q25={info['q25']:.4f}, Q75={info['q75']:.4f})")
        print(f"  有效范围: [{info['iqr_lower']:.4f}, {info['iqr_upper']:.4f}]m")
        print(f"  IQR检出:  {info['n_spike_iqr']}点")
        print(f"  跳变检出: {info['n_spike_jump']}点 (>200mm)")
        print(f"  总去除:   {info['n_spike_total']}点 ({info['spike_pct']:.1f}%)")
        print(f"  尖峰均值: {info['spike_mean']:.3f}m (偏离信号中心 "
              f"{abs(info['spike_mean']-info['clean_mean']):.3f}m)")
        print(f"  尖峰方向: {info['spike_above_pct']:.0f}%在IQR上界以上 "
              f"(距离增大=水面下降方向)")
        print()
        print(f"  ▌清洗后 vs 原始")
        print(f"  距离std: {info['raw_std']*1000:.1f}mm → {info['clean_std']*1000:.1f}mm")
        print(f"  距离range: {info['raw_range']*1000:.1f}mm → {info['clean_range']*1000:.1f}mm")
        print(f"  η std:  {info['raw_eta_std']*1000:.1f}mm → {info['clean_eta_std']*1000:.1f}mm")
        print(f"  4σ波高估计: {info['Hm0_from_std']*1000:.1f}mm")
        print()
        print(f"  ▌项目代码输出 vs 标称值")
        print(f"  Hm0={info['Hm0']:.4f}m  (标称{H_nom}m, 误差{(info['Hm0']-H_nom)/H_nom*100:+.1f}%)")
        print(f"  Hs ={info['Hs']:.4f}m  (标称{H_nom}m, 误差{(info['Hs']-H_nom)/H_nom*100:+.1f}%)")
        print(f"  Tp ={info['Tp']:.2f}s   (标称{T_nom}s, 误差{(info['Tp']-T_nom)/T_nom*100:+.1f}%)")
        print(f"  Ts ={info['Ts']:.2f}s   (标称{T_nom}s, 误差{(info['Ts']-T_nom)/T_nom*100:+.1f}%)")

    # ============ 综合对比 ============
    print(f"\n\n{'=' * 80}")
    print("  综合对比分析")
    print(f"{'=' * 80}")

    print(f"\n  ▌尖峰污染程度")
    print(f"  {'时段':<14s} {'尖峰数':>6s} {'占比':>6s} {'IQR':>8s} {'尖峰偏移':>8s}")
    for info in all_info:
        print(f"  {info['label']:<14s} {info['n_spike_total']:>6d} {info['spike_pct']:>5.1f}% "
              f"{info['iqr']*1000:>7.0f}mm {abs(info['spike_mean']-info['clean_mean'])*1000:>7.0f}mm")

    print(f"\n  ▌信噪比分析 (清洗后η的std vs 标称波幅)")
    print(f"  {'时段':<14s} {'标称H':>6s} {'η_std':>8s} {'Hm0=4σ':>8s} {'Hm0项目':>8s} {'Hs项目':>8s} {'噪声底':>8s}")
    for info in all_info:
        H_nom = info['H_nom']
        # 标称波幅 (sinusoidal: std = H/(2√2))
        nominal_std = H_nom / (2 * np.sqrt(2))
        noise_floor = np.sqrt(max(0, info['clean_eta_std']**2 - nominal_std**2))
        print(f"  {info['label']:<14s} {H_nom:>5.1f}m {info['clean_eta_std']*1000:>7.1f}mm "
              f"{info['Hm0_from_std']*1000:>7.1f}mm {info['Hm0']*1000:>7.1f}mm "
              f"{info['Hs']*1000:>7.1f}mm {noise_floor*1000:>7.1f}mm")

    print(f"\n  ▌误差汇总")
    print(f"  {'时段':<14s} {'标称H':>5s} │ {'Hm0':>7s} {'误差':>7s} │ {'Hs':>7s} {'误差':>7s} │ {'Tp':>6s} {'误差':>6s} │ {'Ts':>6s} {'误差':>6s}")
    print(f"  {'─'*14} ┼ {'─'*16} ┼ {'─'*16} ┼ {'─'*14} ┼ {'─'*14}")
    for info in all_info:
        H_nom = info['H_nom']
        T_nom = info['T_nom']
        print(f"  {info['label']:<14s} {H_nom:>4.1f}m │ "
              f"{info['Hm0']:.3f}m {(info['Hm0']-H_nom)/H_nom*100:>+6.1f}% │ "
              f"{info['Hs']:.3f}m {(info['Hs']-H_nom)/H_nom*100:>+6.1f}% │ "
              f"{info['Tp']:.2f}s {(info['Tp']-T_nom)/T_nom*100:>+5.1f}% │ "
              f"{info['Ts']:.2f}s {(info['Ts']-T_nom)/T_nom*100:>+5.1f}%")

    # 分析第三段误差大的原因
    print(f"\n{'=' * 80}")
    print("  第三段 (10:28-10:33) 误差偏大原因分析")
    print(f"{'=' * 80}")

    i3 = all_info[2]
    i1 = all_info[0]
    i2 = all_info[1]

    print(f"""
  1. 尖峰污染更严重:
     - 尖峰占比: {i1['spike_pct']:.1f}% → {i2['spike_pct']:.1f}% → {i3['spike_pct']:.1f}%
     - 尖峰偏离信号中心: {abs(i1['spike_mean']-i1['clean_mean'])*1000:.0f}mm → {abs(i2['spike_mean']-i2['clean_mean'])*1000:.0f}mm → {abs(i3['spike_mean']-i3['clean_mean'])*1000:.0f}mm
     - 去尖刺后的插值点更多，引入更多平滑效应

  2. 去尖刺边界效应:
     - IQR: {i1['iqr']*1000:.0f}mm → {i2['iqr']*1000:.0f}mm → {i3['iqr']*1000:.0f}mm
     - 波高越大 → IQR越宽 → 边界[lower,upper]越宽
     - 第三段IQR={i3['iqr']*1000:.0f}mm, upper={i3['iqr_upper']:.3f}m
     - 但跳变阈值固定200mm，大波时真实波峰的逐点变化更容易触发跳变检测
     - 跳变检出: {i1['n_spike_jump']} → {i2['n_spike_jump']} → {i3['n_spike_jump']}点

  3. 带通滤波的能量损失:
     - 滤波带 [0.04, 1.0]Hz，对4s周期(0.25Hz)信号本身无损
     - 但波高越大 → 谐波分量越多 → 滤波衰减高次谐波 → 波峰被削平
     - 表现: Hs/Hm0比值 = {i1['Hs']/i1['Hm0']:.3f} → {i2['Hs']/i2['Hm0']:.3f} → {i3['Hs']/i3['Hm0']:.3f}

  4. Hm0 vs Hs 的系统差异:
     - Hm0 = 4√m0 包含全频带能量(含噪声)，总是偏高
     - Hs = 零交叉法前1/3波高平均，更接近真实波高
     - 但第三段 Hs=0.276m 也偏低(-8%)，不只是Hm0的问题
""")

    conn.close()


if __name__ == "__main__":
    main()
