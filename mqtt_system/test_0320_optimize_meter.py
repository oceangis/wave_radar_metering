#!/usr/bin/env python3
"""
针对计量场景优化波向精度
优化方向：
  1. 延长数据窗口（5min → 10min → 20min）
  2. 只在峰值频率附近估算波向（避免噪声频段稀释）
  3. 多窗口滑动平均
"""
import sys, os, yaml, logging, warnings
import numpy as np
import psycopg2
from scipy.signal import detrend, butter, filtfilt, welch
from scipy.interpolate import interp1d

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'services'))
logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')
warnings.filterwarnings('ignore')

# 三个计量波高对应的时间段（取连续数据段）
# ~0.1m: 09:06-09:12 (Tp≈2s)
# ~0.2m: 09:28-09:33 (Tp≈3s)
# ~0.3m: 09:52-09:58 (Tp≈4s)
# 用更长窗口重新测试

TESTS = [
    # 5分钟窗口（当前）
    {'label': '0.1m/5min',  'start': '2026-03-20 09:06:48+08', 'end': '2026-03-20 09:11:52+08', 'expect': 66, 'h13': 0.104},
    {'label': '0.2m/5min',  'start': '2026-03-20 09:28:11+08', 'end': '2026-03-20 09:33:15+08', 'expect': 66, 'h13': 0.211},
    {'label': '0.3m/5min',  'start': '2026-03-20 09:52:41+08', 'end': '2026-03-20 09:57:44+08', 'expect': 66, 'h13': 0.294},
    {'label': '0.3m/5min',  'start': '2026-03-20 10:16:53+08', 'end': '2026-03-20 10:21:57+08', 'expect': 66, 'h13': 0.304},
    {'label': '0.2m/5min',  'start': '2026-03-20 10:40:23+08', 'end': '2026-03-20 10:45:26+08', 'expect': 66, 'h13': 0.214},
    {'label': '0.1m/5min',  'start': '2026-03-20 11:03:19+08', 'end': '2026-03-20 11:08:22+08', 'expect': 66, 'h13': 0.098},
    {'label': '0.1m/5min',  'start': '2026-03-20 11:23:39+08', 'end': '2026-03-20 11:28:43+08', 'expect': 66, 'h13': 0.103},
    {'label': '0.2m/5min',  'start': '2026-03-20 14:04:59+08', 'end': '2026-03-20 14:10:02+08', 'expect': 66, 'h13': 0.207},
    {'label': '0.3m/5min',  'start': '2026-03-20 14:25:18+08', 'end': '2026-03-20 14:30:22+08', 'expect': 66, 'h13': 0.306},
    {'label': '0.3m/5min',  'start': '2026-03-20 14:49:36+08', 'end': '2026-03-20 14:54:39+08', 'expect': 246, 'h13': 0.270},

    # 10分钟窗口
    {'label': '0.1m/10min', 'start': '2026-03-20 09:01:48+08', 'end': '2026-03-20 09:11:52+08', 'expect': 66, 'h13': 0.104},
    {'label': '0.2m/10min', 'start': '2026-03-20 09:23:11+08', 'end': '2026-03-20 09:33:15+08', 'expect': 66, 'h13': 0.211},
    {'label': '0.3m/10min', 'start': '2026-03-20 09:47:41+08', 'end': '2026-03-20 09:57:44+08', 'expect': 66, 'h13': 0.294},
    {'label': '0.3m/10min', 'start': '2026-03-20 10:11:53+08', 'end': '2026-03-20 10:21:57+08', 'expect': 66, 'h13': 0.304},
    {'label': '0.2m/10min', 'start': '2026-03-20 10:35:23+08', 'end': '2026-03-20 10:45:26+08', 'expect': 66, 'h13': 0.214},
    {'label': '0.1m/10min', 'start': '2026-03-20 10:58:19+08', 'end': '2026-03-20 11:08:22+08', 'expect': 66, 'h13': 0.098},
    {'label': '0.2m/10min', 'start': '2026-03-20 14:00:00+08', 'end': '2026-03-20 14:10:02+08', 'expect': 66, 'h13': 0.207},
    {'label': '0.3m/10min', 'start': '2026-03-20 14:20:18+08', 'end': '2026-03-20 14:30:22+08', 'expect': 66, 'h13': 0.306},

    # 20分钟窗口（RADAC标准）
    {'label': '0.1m/20min', 'start': '2026-03-20 08:51:48+08', 'end': '2026-03-20 09:11:52+08', 'expect': 66, 'h13': 0.104},
    {'label': '0.2m/20min', 'start': '2026-03-20 09:13:11+08', 'end': '2026-03-20 09:33:15+08', 'expect': 66, 'h13': 0.211},
    {'label': '0.3m/20min', 'start': '2026-03-20 09:37:41+08', 'end': '2026-03-20 09:57:44+08', 'expect': 66, 'h13': 0.294},
    {'label': '0.3m/20min', 'start': '2026-03-20 10:01:53+08', 'end': '2026-03-20 10:21:57+08', 'expect': 66, 'h13': 0.304},
    {'label': '0.2m/20min', 'start': '2026-03-20 10:25:23+08', 'end': '2026-03-20 10:45:26+08', 'expect': 66, 'h13': 0.214},
    {'label': '0.2m/20min', 'start': '2026-03-20 13:50:00+08', 'end': '2026-03-20 14:10:02+08', 'expect': 66, 'h13': 0.207},
    {'label': '0.3m/20min', 'start': '2026-03-20 14:10:18+08', 'end': '2026-03-20 14:30:22+08', 'expect': 66, 'h13': 0.306},
]


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


def prepare_and_filter(raw_data, config, band):
    t1 = raw_data[1]['epochs']
    d1 = raw_data[1]['distances'].copy()
    d2 = np.interp(t1, raw_data[2]['epochs'], raw_data[2]['distances'])
    d3 = np.interp(t1, raw_data[3]['epochs'], raw_data[3]['distances'])

    r1_thresh = config['analysis'].get('r1_ref_threshold', 0.15)
    sp2 = np.abs(d2 - d1) > r1_thresh
    sp3 = np.abs(d3 - d1) > r1_thresh
    if np.any(sp2): d2[sp2] = d1[sp2]
    if np.any(sp3): d3[sp3] = d1[sp3]

    for d in [d1, d2, d3]:
        ah = config['radar'].get('array_height', 4.6)
        bad = (d < 0.3) | (d > ah + 0.5)
        if np.any(bad):
            med = np.median(d[~bad]) if np.any(~bad) else np.median(d)
            d[bad] = med
        q25, q75 = np.percentile(d, [25, 75])
        iqr = max(q75 - q25, 0.001)
        lo, hi = q25 - 3 * iqr, q75 + 3 * iqr
        out = (d < lo) | (d > hi)
        if np.any(out):
            good = ~out
            if np.sum(good) > 10:
                d[out] = np.interp(np.where(out)[0], np.where(good)[0], d[good])

    t_rel = t1 - t1[0]
    dur = t_rel[-1]
    fs = 6.0
    t_uni = np.arange(0, dur, 1.0 / fs)

    def to_eta(d):
        eta = -(d - np.median(d))
        return interp1d(t_rel, eta, kind='linear', fill_value='extrapolate')(t_uni)

    eta1, eta2, eta3 = to_eta(d1), to_eta(d2), to_eta(d3)
    ml = min(len(eta1), len(eta2), len(eta3))
    eta1, eta2, eta3 = eta1[:ml], eta2[:ml], eta3[:ml]

    b, a = butter(4, band, btype='band', fs=fs)
    f_low = max(band[0], 0.01)
    padlen = min(3 * int(fs / f_low), ml - 1)
    eta1_f = filtfilt(b, a, detrend(eta1), padlen=padlen)
    eta2_f = filtfilt(b, a, detrend(eta2), padlen=padlen)
    eta3_f = filtfilt(b, a, detrend(eta3), padlen=padlen)

    r1_mean = float(np.mean(raw_data[1]['distances']))
    return eta1_f, eta2_f, eta3_f, r1_mean, ml


def run_dftm(config, eta1_f, eta2_f, eta3_f, r1_mean, n_samples, band,
             peak_only=False):
    """
    DFTM 方向估计
    peak_only: 只用峰值频率附近的能量估算方向
    """
    from directional_spectrum import DirectionalSpectrumAnalyzer
    from pydiwasp import dirspec

    DirectionalSpectrumAnalyzer._last_Dp = None

    ds_config = {
        'sample_rate': 6.0, 'gravity': 9.81,
        'water_depth': config['analysis'].get('water_depth', 100.0),
        'freq_range': band,
        'direction_resolution': 360,
        'array_height': config['radar'].get('array_height', 5.0),
        'radar_positions': config['radar'].get('diwasp_positions', {}),
        'tilt_angles': config['radar'].get('tilt_angles', {}),
        'tilt_azimuths': config['radar'].get('tilt_azimuths', {}),
        'array_heading': config['radar'].get('array_heading', 0.0),
    }
    analyzer = DirectionalSpectrumAnalyzer(ds_config)
    analyzer.update_layout(r1_mean)

    eta2_use = eta2_f * analyzer.tilt_factors['R2']
    eta3_use = eta3_f * analyzer.tilt_factors['R3']

    data_matrix = np.column_stack([eta1_f, eta2_use, eta3_use])

    ID = {
        'data': data_matrix,
        'layout': analyzer.layout,
        'datatypes': np.array(['elev', 'elev', 'elev']),
        'depth': ds_config['water_depth'],
        'fs': 6.0
    }

    freqs = np.linspace(band[0], band[1], 128)
    dirs = np.linspace(0, 360, 361)[:-1]

    SM = {
        'freqs': freqs, 'dirs': np.radians(dirs),
        'funit': 'Hz', 'dunit': 'rad',
        'xaxisdir': analyzer.xaxisdir
    }

    nfft = min(256, n_samples // 4)
    nfft = max(64, int(2 ** np.floor(np.log2(nfft))))

    EP = {
        'method': 'DFTM', 'nfft': nfft,
        'dres': 360, 'iter': 100, 'smooth': 'ON'
    }

    options = ['MESSAGE', 0, 'PLOTTYPE', 0]

    logging.disable(logging.CRITICAL)
    try:
        SMout, EPout = dirspec(ID, SM, EP, options)
    finally:
        logging.disable(logging.NOTSET)

    S = np.real(SMout['S'])
    freqs_out = SMout['freqs']
    dirs_out = np.degrees(SMout['dirs'])

    if peak_only:
        # 只用峰值频率附近 ±20% 的频段
        ddir_deg = dirs_out[1] - dirs_out[0] if len(dirs_out) > 1 else 1.0
        S1D = np.sum(S, axis=1) * np.radians(ddir_deg)
        peak_idx = np.argmax(S1D)
        fp = freqs_out[peak_idx]

        # 选取峰值±20%频段
        f_lo = fp * 0.8
        f_hi = fp * 1.2
        peak_mask = (freqs_out >= f_lo) & (freqs_out <= f_hi)

        if np.any(peak_mask):
            S_peak = S[peak_mask, :]
            dir_spectrum = np.sum(S_peak, axis=0)
        else:
            dir_spectrum = np.sum(S, axis=0)
    else:
        dir_spectrum = np.sum(S, axis=0)

    Dp_idx = np.argmax(dir_spectrum)
    Dp_axis = dirs_out[Dp_idx]
    Dp = (180 + analyzer.xaxisdir - Dp_axis) % 360

    return Dp


def angle_error(computed, expected):
    d1 = abs((computed - expected + 180) % 360 - 180)
    d2 = abs((computed - (expected + 180) % 360 + 180) % 360 - 180)
    return min(d1, d2)


def main():
    config = load_config()
    band = config['analysis'].get('filter_band', [0.04, 1.0])

    print()
    print("=" * 100)
    print("  计量波向优化：窗口长度 × 峰值频段聚焦")
    print("  方法: DFTM + elev×3  |  目标: ±3°")
    print("=" * 100)

    # 按窗口长度分组
    groups = {
        '5min': [t for t in TESTS if '/5min' in t['label']],
        '10min': [t for t in TESTS if '/10min' in t['label']],
        '20min': [t for t in TESTS if '/20min' in t['label']],
    }

    for win_label, tests in groups.items():
        print(f"\n--- 窗口: {win_label} ---")
        print(f"{'标签':<14s} {'H1/3':>6s} {'期望':>4s} │ {'全谱':>5s} {'误差':>5s} │ {'峰值±20%':>8s} {'误差':>5s}")
        print("─" * 65)

        errs_full, errs_peak = [], []

        for test in tests:
            raw = fetch_data(test['start'], test['end'])
            if raw is None:
                print(f"{test['label']:<14s} {'数据不足':>20s}")
                continue

            # 检查数据量
            n_points = len(raw[1]['distances'])

            eta1, eta2, eta3, r1_mean, n_samples = prepare_and_filter(raw, config, band)

            dp_full = run_dftm(config, eta1, eta2, eta3, r1_mean, n_samples, band, peak_only=False)
            dp_peak = run_dftm(config, eta1, eta2, eta3, r1_mean, n_samples, band, peak_only=True)

            err_full = angle_error(dp_full, test['expect'])
            err_peak = angle_error(dp_peak, test['expect'])
            errs_full.append(err_full)
            errs_peak.append(err_peak)

            m1 = "✓" if err_full <= 3 else "✗" if err_full > 5 else " "
            m2 = "✓" if err_peak <= 3 else "✗" if err_peak > 5 else " "

            print(f"{test['label']:<14s} {test['h13']:>5.3f}m {test['expect']:>3d}° │ "
                  f"{dp_full:>4.0f}° {err_full:>4.1f}°{m1} │ "
                  f"{dp_peak:>7.0f}° {err_peak:>4.1f}°{m2}")

        if errs_full:
            ef, ep = np.array(errs_full), np.array(errs_peak)
            print(f"{'':>25s} │ avg={np.mean(ef):.1f}° ≤3°:{np.sum(ef<=3)}/{len(ef)} │ "
                  f"avg={np.mean(ep):.1f}° ≤3°:{np.sum(ep<=3)}/{len(ep)}")

    print()


if __name__ == '__main__':
    main()
