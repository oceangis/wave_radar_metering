#!/usr/bin/env python3
"""
对比：今天下午 METER 模式日志中的波向 vs 用当前代码重跑的波向
独立脚本，不依赖 mqtt_analyzer（避免 paho/pandas 等重依赖）
"""
import sys, os, yaml, logging
import numpy as np
import psycopg2
from datetime import datetime
from scipy.signal import detrend, butter, filtfilt
from scipy.interpolate import interp1d

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'services'))
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# 从 analyzer.log 提取的日志结果
LOG_RESULTS = [
    {'time': '14:15', 'start': '2026-03-19 14:10:43+08', 'end': '2026-03-19 14:15:43+08',
     'Hs': 0.058, 'Tp': 2.03, 'Dp': 184.0, 'spread': 62.4, 'r1_mean': 4.696},
    {'time': '14:41', 'start': '2026-03-19 14:36:30+08', 'end': '2026-03-19 14:41:30+08',
     'Hs': 0.290, 'Tp': 3.06, 'Dp': 183.0, 'spread': 65.6, 'r1_mean': 4.691},
    {'time': '14:46', 'start': '2026-03-19 14:41:32+08', 'end': '2026-03-19 14:46:32+08',
     'Hs': 0.089, 'Tp': 3.06, 'Dp': 279.0, 'spread': 79.0, 'r1_mean': 4.690},
    {'time': '14:51', 'start': '2026-03-19 14:46:34+08', 'end': '2026-03-19 14:51:34+08',
     'Hs': 0.007, 'Tp': 25.00, 'Dp': 178.0, 'spread': 77.1, 'r1_mean': 4.693},
    {'time': '15:04', 'start': '2026-03-19 14:59:03+08', 'end': '2026-03-19 15:04:03+08',
     'Hs': 0.132, 'Tp': 2.03, 'Dp': 183.0, 'spread': 56.9, 'r1_mean': 4.690},
    {'time': '16:09', 'start': '2026-03-19 16:04:34+08', 'end': '2026-03-19 16:09:34+08',
     'Hs': 0.365, 'Tp': 3.86, 'Dp': 77.0, 'spread': 45.3, 'r1_mean': 4.692},
    {'time': '16:35', 'start': '2026-03-19 16:30:57+08', 'end': '2026-03-19 16:35:57+08',
     'Hs': 0.286, 'Tp': 3.06, 'Dp': 80.0, 'spread': 41.2, 'r1_mean': 4.688},
]


def load_config():
    with open(os.path.join(os.path.dirname(__file__), 'config', 'system_config.yaml')) as f:
        return yaml.safe_load(f)


def fetch_data(t_start, t_end):
    """从数据库获取三个雷达的原始测距数据"""
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


def prepare_wave_data(distances, epochs, config, mode='meter'):
    """复制 mqtt_analyzer._prepare_wave_data 的核心逻辑"""
    t_rel = epochs - epochs[0]
    duration = t_rel[-1]
    actual_fs = (len(distances) - 1) / duration if duration > 0 else 6.0

    dist_clean = distances.copy()
    array_height = config['radar'].get('array_height', 10.0)
    meter_cfg = config['analysis'].get('meter_filter', {})
    is_meter = (mode == 'meter')

    if is_meter and meter_cfg.get('enabled', False):
        abs_margin = meter_cfg.get('abs_margin', 0.3)
        iqr_mult = meter_cfg.get('iqr_multiplier', 2.0)
        jump_thresh = meter_cfg.get('jump_threshold', 0.15)
    else:
        abs_margin = 0.5
        iqr_mult = 3.0
        jump_thresh = 0.5

    # 绝对范围过滤
    abs_lower = 0.3
    abs_upper = array_height + abs_margin
    spike_abs = (dist_clean < abs_lower) | (dist_clean > abs_upper)
    if np.any(spike_abs):
        median_dist = np.median(dist_clean[~spike_abs]) if np.any(~spike_abs) else np.median(dist_clean)
        dist_clean[spike_abs] = median_dist

    # IQR
    q25, q75 = np.percentile(dist_clean, [25, 75])
    iqr = max(q75 - q25, 0.001)
    lower = q25 - iqr_mult * iqr
    upper = q75 + iqr_mult * iqr
    spike_iqr = (dist_clean < lower) | (dist_clean > upper)

    # 逐点跳变
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

    # η = -(distance - median)
    median_dist = np.median(dist_clean)
    eta_orig = -(dist_clean - median_dist)

    # 重采样到6Hz
    target_fs = 6.0
    t_uniform = np.arange(0, duration, 1.0 / target_fs)
    if len(t_uniform) > 0 and len(t_rel) > 1:
        eta_resampled = interp1d(t_rel, eta_orig, kind='linear', fill_value='extrapolate')(t_uniform)
    else:
        eta_resampled = eta_orig

    return eta_resampled, dist_clean, median_dist


def run_directional_analysis(config, raw_data, mode='meter'):
    """复制 analyze_window 中的三雷达方向谱分析流程"""
    from directional_spectrum import DirectionalSpectrumAnalyzer

    # 重置180°模糊状态
    DirectionalSpectrumAnalyzer._last_Dp = None

    meter_cfg = config['analysis'].get('meter_filter', {})
    is_meter = (mode == 'meter')

    # 插值R2/R3到R1时间轴
    t1 = raw_data[1]['epochs']
    t2 = raw_data[2]['epochs']
    t3 = raw_data[3]['epochs']
    d1 = raw_data[1]['distances']
    d2_interp = np.interp(t1, t2, raw_data[2]['distances'])
    d3_interp = np.interp(t1, t3, raw_data[3]['distances'])

    # R1参考滤波（meter模式使用更紧的阈值）
    if is_meter and meter_cfg.get('enabled', False):
        r1_ref_threshold = meter_cfg.get('r1_ref_threshold', 0.08)
    else:
        r1_ref_threshold = config['analysis'].get('r1_ref_threshold', 0.15)
    spike_r2 = np.abs(d2_interp - d1) > r1_ref_threshold
    if np.any(spike_r2):
        d2_interp[spike_r2] = d1[spike_r2]
    spike_r3 = np.abs(d3_interp - d1) > r1_ref_threshold
    if np.any(spike_r3):
        d3_interp[spike_r3] = d1[spike_r3]

    # Meter模式：R2/R3中值滤波去残余毛刺
    if is_meter and meter_cfg.get('enabled', False):
        from scipy.signal import medfilt
        medfilt_win = meter_cfg.get('r23_medfilt_window', 5)
        if medfilt_win > 1:
            d2_interp = medfilt(d2_interp, kernel_size=medfilt_win)
            d3_interp = medfilt(d3_interp, kernel_size=medfilt_win)

    # 预处理（去尖刺→η→重采样6Hz）
    eta1, raw1, med1 = prepare_wave_data(d1, t1, config, mode)
    eta2, raw2, med2 = prepare_wave_data(d2_interp, t1, config, mode)
    eta3, raw3, med3 = prepare_wave_data(d3_interp, t1, config, mode)

    # 安全截齐
    min_len = min(len(eta1), len(eta2), len(eta3))
    eta1 = eta1[:min_len]
    eta2 = eta2[:min_len]
    eta3 = eta3[:min_len]

    # 带通滤波
    sample_rate = 6.0
    if config['analysis'].get('filter_enable', True):
        if is_meter and meter_cfg.get('enabled', False):
            band = meter_cfg.get('filter_band', [0.05, 1.5])
        else:
            band = config['analysis'].get('filter_band', [0.04, 1.0])
        f_low = band[0] if band[0] > 0 else 0.01
        b_filt, a_filt = butter(4, band, btype='band', fs=sample_rate)
        padlen = min(3 * int(sample_rate / f_low), min_len - 1)
        eta1_f = filtfilt(b_filt, a_filt, detrend(eta1), padlen=padlen)
        eta2_f = filtfilt(b_filt, a_filt, detrend(eta2), padlen=padlen)
        eta3_f = filtfilt(b_filt, a_filt, detrend(eta3), padlen=padlen)
    else:
        eta1_f, eta2_f, eta3_f = eta1, eta2, eta3

    # R1平均测距
    r1_mean_dist = float(np.mean(d1))

    # 构造 DirectionalSpectrumAnalyzer 配置
    ds_config = {
        'sample_rate': sample_rate,
        'gravity': config['analysis'].get('gravity', 9.81),
        'water_depth': config['analysis'].get('water_depth', 100.0),
        'freq_range': config['analysis'].get('filter_band', [0.04, 1.0]),
        'direction_resolution': config['analysis'].get('direction_resolution', 360),
        'array_height': config['radar'].get('array_height', 10.0),
        'radar_positions': config['radar'].get('diwasp_positions', {}),
        'tilt_angles': config['radar'].get('tilt_angles', {}),
        'tilt_azimuths': config['radar'].get('tilt_azimuths', {}),
        'array_heading': config['radar'].get('array_heading', 0.0),
    }

    analyzer = DirectionalSpectrumAnalyzer(ds_config)
    diwasp_method = config['analysis'].get('diwasp_method', 'IMLM')

    dir_results = analyzer.analyze(
        eta1_f, eta2=eta2_f, eta3=eta3_f,
        method=diwasp_method,
        r1_mean_distance=r1_mean_dist
    )

    return {
        'Dp': dir_results.get('Dp'),
        'DTp': dir_results.get('DTp'),
        'mean_direction': dir_results.get('mean_direction'),
        'directional_spread': dir_results.get('directional_spread'),
        'Hs_diwasp': dir_results.get('Hs'),
        'Tp_diwasp': dir_results.get('Tp'),
        'method': dir_results.get('method'),
        'success': dir_results.get('success', False),
        'n_samples': min_len,
        'r1_mean_dist': r1_mean_dist,
    }


def main():
    config = load_config()

    print()
    print("=" * 100)
    print("  今天下午 METER 模式波向对比：日志原值 vs 当前代码重算")
    print("=" * 100)
    print()

    # 表头
    hdr = (f"{'时间':<6s} │ {'日志Dp':>7s} {'重算Dp':>7s} {'ΔDp':>6s} │ "
           f"{'日志spread':>6s} {'重算spread':>7s} │ "
           f"{'日志Hs':>7s} {'重算Hs':>7s} │ "
           f"{'日志Tp':>6s} {'重算Tp':>6s} │ {'采样':>5s} {'R1均距':>6s}")
    print(hdr)
    print("─" * 100)

    for rec in LOG_RESULTS:
        raw = fetch_data(rec['start'], rec['end'])
        if raw is None:
            print(f"{rec['time']:<6s} │ 数据不足，跳过")
            continue

        logging.disable(logging.INFO)  # 静默分析过程日志
        result = run_directional_analysis(config, raw, 'meter')
        logging.disable(logging.NOTSET)

        if not result['success']:
            print(f"{rec['time']:<6s} │ DIWASP 分析失败")
            continue

        new_dp = result['Dp']
        new_spread = result['directional_spread']
        new_hs = result['Hs_diwasp']
        new_tp = result['Tp_diwasp']

        # 最短角度差
        dp_diff = (new_dp - rec['Dp'] + 180) % 360 - 180 if new_dp is not None else None

        dp_s = f"{new_dp:.1f}°" if new_dp is not None else "N/A"
        diff_s = f"{dp_diff:+.1f}°" if dp_diff is not None else "N/A"
        sp_s = f"{new_spread:.1f}°" if new_spread is not None else "N/A"
        hs_s = f"{new_hs*1000:.1f}mm" if new_hs is not None else "N/A"

        print(f"{rec['time']:<6s} │ {rec['Dp']:>6.1f}° {dp_s:>7s} {diff_s:>6s} │ "
              f"{rec['spread']:>5.1f}° {sp_s:>7s} │ "
              f"{rec['Hs']*1000:>6.1f}mm {hs_s:>7s} │ "
              f"{rec['Tp']:>5.2f}s {new_tp:>5.2f}s │ "
              f"{result['n_samples']:>5d} {result['r1_mean_dist']:>5.3f}m")

    print()
    print("注：ΔDp = 重算Dp - 日志Dp（最短角度差，正=顺时针偏移）")
    print()


if __name__ == '__main__':
    main()
