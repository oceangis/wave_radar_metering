#!/usr/bin/env python3
"""
重新分析 2026/3/20 的原始数据，查看 DIWASP 计算的波向
"""
import sys, os, yaml, logging
import numpy as np
import psycopg2
from scipy.signal import detrend, butter, filtfilt
from scipy.interpolate import interp1d

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'services'))
logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

# 3/20 的分析窗口（从数据库 wave_analysis 取得的 start_time/end_time）
WINDOWS = [
    {'label': '09:11', 'start': '2026-03-20 09:06:48+08', 'end': '2026-03-20 09:11:52+08'},
    {'label': '09:33', 'start': '2026-03-20 09:28:11+08', 'end': '2026-03-20 09:33:15+08'},
    {'label': '09:57', 'start': '2026-03-20 09:52:41+08', 'end': '2026-03-20 09:57:44+08'},
    {'label': '10:21', 'start': '2026-03-20 10:16:53+08', 'end': '2026-03-20 10:21:57+08'},
    {'label': '10:45', 'start': '2026-03-20 10:40:23+08', 'end': '2026-03-20 10:45:26+08'},
    {'label': '11:08', 'start': '2026-03-20 11:03:19+08', 'end': '2026-03-20 11:08:22+08'},
    {'label': '11:28', 'start': '2026-03-20 11:23:39+08', 'end': '2026-03-20 11:28:43+08'},
    {'label': '14:10', 'start': '2026-03-20 14:04:59+08', 'end': '2026-03-20 14:10:02+08'},
    {'label': '14:30', 'start': '2026-03-20 14:25:18+08', 'end': '2026-03-20 14:30:22+08'},
    {'label': '14:54', 'start': '2026-03-20 14:49:36+08', 'end': '2026-03-20 14:54:39+08'},
]

# 数据库中记录的波向（来自 wave_analysis 表）
DB_DIRECTIONS = [66.0, 69.5, 69.1, 64.0, 61.9, 65.3, 62.1, 67.4, 63.7, 244.4]


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


def prepare_wave_data(distances, epochs, config, mode='work'):
    t_rel = epochs - epochs[0]
    duration = t_rel[-1]
    dist_clean = distances.copy()
    array_height = config['radar'].get('array_height', 5.0)

    # work mode 的去尖刺参数
    abs_margin = 0.5
    iqr_mult = 3.0
    jump_thresh = 0.5

    abs_lower = 0.3
    abs_upper = array_height + abs_margin
    spike_abs = (dist_clean < abs_lower) | (dist_clean > abs_upper)
    if np.any(spike_abs):
        median_dist = np.median(dist_clean[~spike_abs]) if np.any(~spike_abs) else np.median(dist_clean)
        dist_clean[spike_abs] = median_dist

    q25, q75 = np.percentile(dist_clean, [25, 75])
    iqr = max(q75 - q25, 0.001)
    lower = q25 - iqr_mult * iqr
    upper = q75 + iqr_mult * iqr
    spike_iqr = (dist_clean < lower) | (dist_clean > upper)

    d_temp = dist_clean.copy()
    if np.any(spike_iqr):
        good_temp = ~spike_iqr
        if np.sum(good_temp) > 10:
            d_temp[spike_iqr] = np.interp(
                np.where(spike_iqr)[0], np.where(good_temp)[0], d_temp[good_temp])
    diff = np.abs(np.diff(d_temp))
    spike_jump_fwd = np.concatenate(([False], diff > jump_thresh))
    spike_jump_bwd = np.concatenate((diff > jump_thresh, [False]))
    spike_mask = spike_iqr | spike_jump_fwd | spike_jump_bwd
    if np.any(spike_mask):
        good = ~spike_mask
        if np.any(good):
            dist_clean[spike_mask] = np.interp(
                np.where(spike_mask)[0], np.where(good)[0], dist_clean[good])

    median_dist = np.median(dist_clean)
    eta_orig = -(dist_clean - median_dist)

    target_fs = 6.0
    t_uniform = np.arange(0, duration, 1.0 / target_fs)
    if len(t_uniform) > 0 and len(t_rel) > 1:
        eta_resampled = interp1d(t_rel, eta_orig, kind='linear', fill_value='extrapolate')(t_uniform)
    else:
        eta_resampled = eta_orig

    return eta_resampled, dist_clean, median_dist


def analyze_direction(config, raw_data, mode='work'):
    from directional_spectrum import DirectionalSpectrumAnalyzer

    # 重置180°模糊状态
    DirectionalSpectrumAnalyzer._last_Dp = None

    # 插值R2/R3到R1时间轴
    t1 = raw_data[1]['epochs']
    d1 = raw_data[1]['distances']
    d2_interp = np.interp(t1, raw_data[2]['epochs'], raw_data[2]['distances'])
    d3_interp = np.interp(t1, raw_data[3]['epochs'], raw_data[3]['distances'])

    # work模式：宽松的R1参考滤波
    r1_ref_threshold = config['analysis'].get('r1_ref_threshold', 0.15)
    spike_r2 = np.abs(d2_interp - d1) > r1_ref_threshold
    if np.any(spike_r2):
        d2_interp[spike_r2] = d1[spike_r2]
    spike_r3 = np.abs(d3_interp - d1) > r1_ref_threshold
    if np.any(spike_r3):
        d3_interp[spike_r3] = d1[spike_r3]

    # 预处理
    eta1, raw1, med1 = prepare_wave_data(d1, t1, config, mode)
    eta2, raw2, med2 = prepare_wave_data(d2_interp, t1, config, mode)
    eta3, raw3, med3 = prepare_wave_data(d3_interp, t1, config, mode)

    min_len = min(len(eta1), len(eta2), len(eta3))
    eta1 = eta1[:min_len]
    eta2 = eta2[:min_len]
    eta3 = eta3[:min_len]

    # 带通滤波
    sample_rate = 6.0
    band = config['analysis'].get('filter_band', [0.04, 1.0])
    f_low = band[0] if band[0] > 0 else 0.01
    b_filt, a_filt = butter(4, band, btype='band', fs=sample_rate)
    padlen = min(3 * int(sample_rate / f_low), min_len - 1)
    eta1_f = filtfilt(b_filt, a_filt, detrend(eta1), padlen=padlen)
    eta2_f = filtfilt(b_filt, a_filt, detrend(eta2), padlen=padlen)
    eta3_f = filtfilt(b_filt, a_filt, detrend(eta3), padlen=padlen)

    r1_mean_dist = float(np.mean(d1))

    # 构造分析器
    ds_config = {
        'sample_rate': sample_rate,
        'gravity': config['analysis'].get('gravity', 9.81),
        'water_depth': config['analysis'].get('water_depth', 100.0),
        'freq_range': config['analysis'].get('filter_band', [0.04, 1.0]),
        'direction_resolution': config['analysis'].get('direction_resolution', 360),
        'array_height': config['radar'].get('array_height', 5.0),
        'radar_positions': config['radar'].get('diwasp_positions', {}),
        'tilt_angles': config['radar'].get('tilt_angles', {}),
        'tilt_azimuths': config['radar'].get('tilt_azimuths', {}),
        'array_heading': config['radar'].get('array_heading', 0.0),
    }

    analyzer = DirectionalSpectrumAnalyzer(ds_config)

    # 同时用 IMLM 和 EMEP 跑
    results = {}
    for method in ['IMLM', 'EMEP']:
        DirectionalSpectrumAnalyzer._last_Dp = None  # 每次重置
        dr = analyzer.analyze(
            eta1_f, eta2=eta2_f, eta3=eta3_f,
            method=method,
            r1_mean_distance=r1_mean_dist,
            mode='work'
        )
        results[method] = {
            'Dp': dr.get('Dp'),
            'mean_dir': dr.get('mean_direction'),
            'spread': dr.get('directional_spread'),
            'Hs': dr.get('Hs'),
            'Tp': dr.get('Tp'),
            'success': dr.get('success', False),
        }

    # 额外信息：三个通道的信号统计
    std1 = float(np.std(eta1_f))
    std2 = float(np.std(eta2_f))
    std3 = float(np.std(eta3_f))

    # 互相关求相位差（简单验证）
    from scipy.signal import correlate
    corr12 = correlate(eta1_f, eta2_f, mode='full')
    corr13 = correlate(eta1_f, eta3_f, mode='full')
    lag12 = np.argmax(corr12) - (min_len - 1)
    lag13 = np.argmax(corr13) - (min_len - 1)

    return results, {
        'n_samples': min_len,
        'r1_mean': r1_mean_dist,
        'std1': std1, 'std2': std2, 'std3': std3,
        'lag12': lag12, 'lag13': lag13,
        'r2_spike%': float(np.sum(spike_r2)) / len(spike_r2) * 100,
        'r3_spike%': float(np.sum(spike_r3)) / len(spike_r3) * 100,
    }


def main():
    config = load_config()

    print()
    print("=" * 120)
    print("  2026/3/20 波向重算（DIWASP IMLM + EMEP）vs 数据库记录")
    print(f"  array_heading = {config['radar'].get('array_heading', 0)}°, 期望波向 ≈ 66° 或 246°")
    print("=" * 120)
    print()

    hdr = (f"{'时间':<6s} │ {'DB_Dp':>6s} │ {'IMLM_Dp':>8s} {'IMLM_mean':>9s} {'spread':>6s} │ "
           f"{'EMEP_Dp':>8s} {'EMEP_mean':>9s} {'spread':>6s} │ "
           f"{'std_R1':>7s} {'std_R2':>7s} {'std_R3':>7s} │ "
           f"{'lag12':>5s} {'lag13':>5s} │ {'R2sp%':>5s} {'R3sp%':>5s}")
    print(hdr)
    print("─" * 120)

    for i, win in enumerate(WINDOWS):
        raw = fetch_data(win['start'], win['end'])
        if raw is None:
            print(f"{win['label']:<6s} │ 数据不足")
            continue

        results, info = analyze_direction(config, raw, 'work')

        imlm = results['IMLM']
        emep = results['EMEP']
        db_dp = DB_DIRECTIONS[i]

        def fmt_dp(dp):
            return f"{dp:.1f}°" if dp is not None else "N/A"

        def fmt_sp(sp):
            return f"{sp:.1f}°" if sp is not None else "N/A"

        print(f"{win['label']:<6s} │ {db_dp:>5.1f}° │ "
              f"{fmt_dp(imlm['Dp']):>8s} {fmt_dp(imlm['mean_dir']):>9s} {fmt_sp(imlm['spread']):>6s} │ "
              f"{fmt_dp(emep['Dp']):>8s} {fmt_dp(emep['mean_dir']):>9s} {fmt_sp(emep['spread']):>6s} │ "
              f"{info['std1']*1000:>6.1f}mm {info['std2']*1000:>6.1f}mm {info['std3']*1000:>6.1f}mm │ "
              f"{info['lag12']:>5d} {info['lag13']:>5d} │ "
              f"{info['r2_spike%']:>4.1f}% {info['r3_spike%']:>4.1f}%")

    print()
    print("说明：")
    print("  DB_Dp     = 数据库中记录的波向")
    print("  IMLM/EMEP = DIWASP重新计算的主波向(Dp)和平均波向(mean)")
    print("  std_Rx    = 各雷达带通滤波后的信号标准差")
    print("  lag12/13  = R1与R2/R3的互相关峰值延迟（采样点数，正=R2/R3超前）")
    print("  RxSp%     = R1参考滤波替换比例")
    print()


if __name__ == '__main__':
    main()
