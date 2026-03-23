#!/usr/bin/env python3
"""
对比不同基线长度下的 DIWASP 波向精度
当前: 等边三角形 边长 ≈ 0.367m
测试: 等边三角形 边长 = 0.5m
"""
import sys, os, yaml, logging
import numpy as np
import psycopg2
from scipy.signal import detrend, butter, filtfilt
from scipy.interpolate import interp1d

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'services'))
logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

WINDOWS = [
    {'label': '09:11', 'start': '2026-03-20 09:06:48+08', 'end': '2026-03-20 09:11:52+08', 'expect': 66},
    {'label': '09:33', 'start': '2026-03-20 09:28:11+08', 'end': '2026-03-20 09:33:15+08', 'expect': 66},
    {'label': '09:57', 'start': '2026-03-20 09:52:41+08', 'end': '2026-03-20 09:57:44+08', 'expect': 66},
    {'label': '10:21', 'start': '2026-03-20 10:16:53+08', 'end': '2026-03-20 10:21:57+08', 'expect': 66},
    {'label': '10:45', 'start': '2026-03-20 10:40:23+08', 'end': '2026-03-20 10:45:26+08', 'expect': 66},
    {'label': '11:08', 'start': '2026-03-20 11:03:19+08', 'end': '2026-03-20 11:08:22+08', 'expect': 66},
    {'label': '11:28', 'start': '2026-03-20 11:23:39+08', 'end': '2026-03-20 11:28:43+08', 'expect': 66},
    {'label': '14:10', 'start': '2026-03-20 14:04:59+08', 'end': '2026-03-20 14:10:02+08', 'expect': 66},
    {'label': '14:30', 'start': '2026-03-20 14:25:18+08', 'end': '2026-03-20 14:30:22+08', 'expect': 66},
    {'label': '14:54', 'start': '2026-03-20 14:49:36+08', 'end': '2026-03-20 14:54:39+08', 'expect': 246},
]

# 基线配置
BASELINES = {
    '0.37m(当前)': {
        'R1': [0.0, 0.0, 0.0],
        'R2': [-0.1833, 0.3175, 0.0],
        'R3': [0.1833, 0.3175, 0.0],
    },
    '0.50m': {
        'R1': [0.0, 0.0, 0.0],
        'R2': [-0.25, 0.433, 0.0],
        'R3': [0.25, 0.433, 0.0],
    },
    '0.75m': {
        'R1': [0.0, 0.0, 0.0],
        'R2': [-0.375, 0.6495, 0.0],
        'R3': [0.375, 0.6495, 0.0],
    },
    '1.00m': {
        'R1': [0.0, 0.0, 0.0],
        'R2': [-0.5, 0.866, 0.0],
        'R3': [0.5, 0.866, 0.0],
    },
}


def load_config():
    with open(os.path.join(os.path.dirname(__file__), 'config', 'system_config.yaml')) as f:
        return yaml.safe_load(f)


def fetch_data(t_start, t_end):
    conn = psycopg2.connect(host='localhost', port=5432,
                            database='wave_monitoring', user='wave_user', password='wave2025')
    cur = conn.cursor()
    data = {}
    for rid in [1, 2, 3]:
        cur.execute(
            "SELECT timestamp, distance FROM wave_measurements "
            "WHERE radar_id=%s AND timestamp BETWEEN %s AND %s ORDER BY timestamp",
            (rid, t_start, t_end))
        rows = cur.fetchall()
        if not rows:
            conn.close()
            return None
        epochs = np.array([r[0].timestamp() for r in rows])
        distances = np.array([float(r[1]) for r in rows])
        data[rid] = {'epochs': epochs, 'distances': distances}
    conn.close()
    return data


def prepare_wave_data(distances, epochs):
    t_rel = epochs - epochs[0]
    duration = t_rel[-1]
    dist_clean = distances.copy()

    # work mode 去尖刺
    abs_lower, abs_upper = 0.3, 5.1
    spike_abs = (dist_clean < abs_lower) | (dist_clean > abs_upper)
    if np.any(spike_abs):
        med = np.median(dist_clean[~spike_abs]) if np.any(~spike_abs) else np.median(dist_clean)
        dist_clean[spike_abs] = med

    q25, q75 = np.percentile(dist_clean, [25, 75])
    iqr = max(q75 - q25, 0.001)
    lower, upper = q25 - 3.0 * iqr, q75 + 3.0 * iqr
    spike_iqr = (dist_clean < lower) | (dist_clean > upper)

    d_temp = dist_clean.copy()
    if np.any(spike_iqr):
        good = ~spike_iqr
        if np.sum(good) > 10:
            d_temp[spike_iqr] = np.interp(np.where(spike_iqr)[0], np.where(good)[0], d_temp[good])
    diff = np.abs(np.diff(d_temp))
    spike_jump = np.concatenate(([False], diff > 0.5)) | np.concatenate((diff > 0.5, [False]))
    spike_mask = spike_iqr | spike_jump
    if np.any(spike_mask):
        good = ~spike_mask
        if np.any(good):
            dist_clean[spike_mask] = np.interp(np.where(spike_mask)[0], np.where(good)[0], dist_clean[good])

    eta_orig = -(dist_clean - np.median(dist_clean))
    t_uniform = np.arange(0, duration, 1.0 / 6.0)
    if len(t_uniform) > 0 and len(t_rel) > 1:
        eta = interp1d(t_rel, eta_orig, kind='linear', fill_value='extrapolate')(t_uniform)
    else:
        eta = eta_orig
    return eta, dist_clean


def run_analysis(config, raw_data, positions):
    from directional_spectrum import DirectionalSpectrumAnalyzer
    DirectionalSpectrumAnalyzer._last_Dp = None

    t1 = raw_data[1]['epochs']
    d1 = raw_data[1]['distances']
    d2_interp = np.interp(t1, raw_data[2]['epochs'], raw_data[2]['distances'])
    d3_interp = np.interp(t1, raw_data[3]['epochs'], raw_data[3]['distances'])

    # R1参考滤波
    r1_ref_threshold = config['analysis'].get('r1_ref_threshold', 0.15)
    spike_r2 = np.abs(d2_interp - d1) > r1_ref_threshold
    if np.any(spike_r2):
        d2_interp[spike_r2] = d1[spike_r2]
    spike_r3 = np.abs(d3_interp - d1) > r1_ref_threshold
    if np.any(spike_r3):
        d3_interp[spike_r3] = d1[spike_r3]

    eta1, _ = prepare_wave_data(d1, t1)
    eta2, _ = prepare_wave_data(d2_interp, t1)
    eta3, _ = prepare_wave_data(d3_interp, t1)

    min_len = min(len(eta1), len(eta2), len(eta3))
    eta1, eta2, eta3 = eta1[:min_len], eta2[:min_len], eta3[:min_len]

    band = config['analysis'].get('filter_band', [0.04, 1.0])
    f_low = max(band[0], 0.01)
    b_filt, a_filt = butter(4, band, btype='band', fs=6.0)
    padlen = min(3 * int(6.0 / f_low), min_len - 1)
    eta1_f = filtfilt(b_filt, a_filt, detrend(eta1), padlen=padlen)
    eta2_f = filtfilt(b_filt, a_filt, detrend(eta2), padlen=padlen)
    eta3_f = filtfilt(b_filt, a_filt, detrend(eta3), padlen=padlen)

    r1_mean_dist = float(np.mean(d1))

    ds_config = {
        'sample_rate': 6.0,
        'gravity': 9.81,
        'water_depth': config['analysis'].get('water_depth', 100.0),
        'freq_range': config['analysis'].get('filter_band', [0.04, 1.0]),
        'direction_resolution': config['analysis'].get('direction_resolution', 360),
        'array_height': config['radar'].get('array_height', 5.0),
        'radar_positions': positions,
        'tilt_angles': config['radar'].get('tilt_angles', {}),
        'tilt_azimuths': config['radar'].get('tilt_azimuths', {}),
        'array_heading': config['radar'].get('array_heading', 0.0),
    }

    analyzer = DirectionalSpectrumAnalyzer(ds_config)

    dr = analyzer.analyze(
        eta1_f, eta2=eta2_f, eta3=eta3_f,
        method='IMLM',
        r1_mean_distance=r1_mean_dist,
        mode='work'
    )

    # 获取等效基线信息
    eff = analyzer.effective_positions
    bl_12 = np.linalg.norm(eff['R2'][:2] - eff['R1'][:2])
    bl_13 = np.linalg.norm(eff['R3'][:2] - eff['R1'][:2])
    bl_23 = np.linalg.norm(eff['R3'][:2] - eff['R2'][:2])

    return {
        'Dp': dr.get('Dp'),
        'success': dr.get('success', False),
        'eff_bl': (bl_12, bl_13, bl_23),
    }


def angle_error(computed, expected):
    """最短角度差（考虑180°模糊）"""
    if computed is None:
        return None
    d1 = abs((computed - expected + 180) % 360 - 180)
    d2 = abs((computed - (expected + 180) % 360 + 180) % 360 - 180)
    return min(d1, d2)


def main():
    config = load_config()

    # 预加载所有数据
    print("加载原始数据...")
    all_data = []
    for win in WINDOWS:
        raw = fetch_data(win['start'], win['end'])
        all_data.append(raw)

    print()
    print("=" * 100)
    print("  不同基线长度下的 DIWASP 波向精度对比 (IMLM)")
    print(f"  array_heading = {config['radar'].get('array_heading', 0)}°")
    print(f"  tilt = R2:{config['radar']['tilt_angles']['R2']}°@{config['radar']['tilt_azimuths']['R2']}°, "
          f"R3:{config['radar']['tilt_angles']['R3']}°@{config['radar']['tilt_azimuths']['R3']}°")
    print(f"  R1均距 ≈ 4.69m → 倾斜偏移 ≈ {4.69 * np.tan(np.radians(10)):.2f}m")
    print("=" * 100)

    for bl_name, positions in BASELINES.items():
        print(f"\n--- 物理基线: {bl_name} ---")

        # 先跑一次获取等效基线
        if all_data[0] is not None:
            logging.disable(logging.CRITICAL)
            test_r = run_analysis(config, all_data[0], positions)
            logging.disable(logging.NOTSET)
            bl = test_r['eff_bl']
            print(f"    等效基线: R1-R2={bl[0]:.3f}m, R1-R3={bl[1]:.3f}m, R2-R3={bl[2]:.3f}m")

        errors = []
        line = f"    {'时间':<6s}  {'期望':>5s}  {'计算':>6s}  {'误差':>5s}"
        print(line)

        for i, win in enumerate(WINDOWS):
            raw = all_data[i]
            if raw is None:
                print(f"    {win['label']:<6s}  数据不足")
                continue

            logging.disable(logging.CRITICAL)
            result = run_analysis(config, raw, positions)
            logging.disable(logging.NOTSET)

            if not result['success'] or result['Dp'] is None:
                print(f"    {win['label']:<6s}  分析失败")
                continue

            dp = result['Dp']
            err = angle_error(dp, win['expect'])
            errors.append(err)

            print(f"    {win['label']:<6s}  {win['expect']:>4d}°  {dp:>5.1f}°  {err:>4.1f}°")

        if errors:
            errors = np.array(errors)
            print(f"    ────────────────────────────")
            print(f"    平均误差: {np.mean(errors):.1f}°  最大: {np.max(errors):.1f}°  "
                  f"<5°: {np.sum(errors < 5)}/{len(errors)}  <10°: {np.sum(errors < 10)}/{len(errors)}")

    print()


if __name__ == '__main__':
    main()
