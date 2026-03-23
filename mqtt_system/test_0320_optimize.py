#!/usr/bin/env python3
"""
系统优化 DIWASP 波向精度：逐项排查哪个因素影响最大
目标: ±3° (甲方要求)
"""
import sys, os, yaml, logging, warnings
import numpy as np
import psycopg2
from scipy.signal import detrend, butter, filtfilt, welch, csd, medfilt
from scipy.interpolate import interp1d

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'services'))
logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')
warnings.filterwarnings('ignore')

WINDOWS = [
    {'label': '09:11', 'start': '2026-03-20 09:06:48+08', 'end': '2026-03-20 09:11:52+08', 'expect': 66, 'Hs': 0.151},
    {'label': '09:33', 'start': '2026-03-20 09:28:11+08', 'end': '2026-03-20 09:33:15+08', 'expect': 66, 'Hs': 0.261},
    {'label': '09:57', 'start': '2026-03-20 09:52:41+08', 'end': '2026-03-20 09:57:44+08', 'expect': 66, 'Hs': 0.347},
    {'label': '10:21', 'start': '2026-03-20 10:16:53+08', 'end': '2026-03-20 10:21:57+08', 'expect': 66, 'Hs': 0.379},
    {'label': '10:45', 'start': '2026-03-20 10:40:23+08', 'end': '2026-03-20 10:45:26+08', 'expect': 66, 'Hs': 0.288},
    {'label': '11:08', 'start': '2026-03-20 11:03:19+08', 'end': '2026-03-20 11:08:22+08', 'expect': 66, 'Hs': 0.142},
    {'label': '11:28', 'start': '2026-03-20 11:23:39+08', 'end': '2026-03-20 11:28:43+08', 'expect': 66, 'Hs': 0.147},
    {'label': '14:10', 'start': '2026-03-20 14:04:59+08', 'end': '2026-03-20 14:10:02+08', 'expect': 66, 'Hs': 0.275},
    {'label': '14:30', 'start': '2026-03-20 14:25:18+08', 'end': '2026-03-20 14:30:22+08', 'expect': 66, 'Hs': 0.370},
    {'label': '14:54', 'start': '2026-03-20 14:49:36+08', 'end': '2026-03-20 14:54:39+08', 'expect': 246, 'Hs': 0.354},
]


def load_config():
    with open(os.path.join(os.path.dirname(__file__), 'config', 'system_config.yaml')) as f:
        return yaml.safe_load(f)


def fetch_all_data():
    conn = psycopg2.connect(host='localhost', port=5432,
                            database='wave_monitoring', user='wave_user', password='wave2025')
    cur = conn.cursor()
    all_data = []
    for win in WINDOWS:
        data = {}
        for rid in [1, 2, 3]:
            cur.execute(
                "SELECT timestamp, distance FROM wave_measurements "
                "WHERE radar_id=%s AND timestamp BETWEEN %s AND %s ORDER BY timestamp",
                (rid, win['start'], win['end']))
            rows = cur.fetchall()
            if not rows:
                data = None
                break
            epochs = np.array([r[0].timestamp() for r in rows])
            distances = np.array([float(r[1]) for r in rows])
            data[rid] = {'epochs': epochs, 'distances': distances}
        all_data.append(data)
    conn.close()
    return all_data


def despike(dist_clean, array_height=4.6, iqr_mult=3.0, jump_thresh=0.5):
    """去尖刺（保守模式，保留相位信息）"""
    abs_lower, abs_upper = 0.3, array_height + 0.5
    spike_abs = (dist_clean < abs_lower) | (dist_clean > abs_upper)
    if np.any(spike_abs):
        med = np.median(dist_clean[~spike_abs]) if np.any(~spike_abs) else np.median(dist_clean)
        dist_clean[spike_abs] = med

    q25, q75 = np.percentile(dist_clean, [25, 75])
    iqr = max(q75 - q25, 0.001)
    lower, upper = q25 - iqr_mult * iqr, q75 + iqr_mult * iqr
    spike_iqr = (dist_clean < lower) | (dist_clean > upper)

    d_temp = dist_clean.copy()
    if np.any(spike_iqr):
        good = ~spike_iqr
        if np.sum(good) > 10:
            d_temp[spike_iqr] = np.interp(np.where(spike_iqr)[0], np.where(good)[0], d_temp[good])
    diff = np.abs(np.diff(d_temp))
    spike_jump = np.concatenate(([False], diff > jump_thresh)) | np.concatenate((diff > jump_thresh, [False]))
    spike_mask = spike_iqr | spike_jump
    if np.any(spike_mask):
        good = ~spike_mask
        if np.any(good):
            dist_clean[spike_mask] = np.interp(np.where(spike_mask)[0], np.where(good)[0], dist_clean[good])
    return dist_clean


def run_diwasp(config, raw_data, opts):
    """
    用指定选项跑 DIWASP
    opts keys:
        r1_ref_filter: bool  是否做R1参考滤波
        r1_ref_threshold: float
        nfft_override: int or None
        filter_band: [f_low, f_high]
        iterations: int
        medfilt_r23: int or 0  R2/R3中值滤波窗口
    """
    from directional_spectrum import DirectionalSpectrumAnalyzer
    DirectionalSpectrumAnalyzer._last_Dp = None

    t1 = raw_data[1]['epochs']
    d1 = raw_data[1]['distances'].copy()
    d2 = np.interp(t1, raw_data[2]['epochs'], raw_data[2]['distances'])
    d3 = np.interp(t1, raw_data[3]['epochs'], raw_data[3]['distances'])

    # R1参考滤波
    if opts.get('r1_ref_filter', True):
        thresh = opts.get('r1_ref_threshold', 0.15)
        sp2 = np.abs(d2 - d1) > thresh
        sp3 = np.abs(d3 - d1) > thresh
        if np.any(sp2): d2[sp2] = d1[sp2]
        if np.any(sp3): d3[sp3] = d1[sp3]

    # R2/R3中值滤波
    mf = opts.get('medfilt_r23', 0)
    if mf > 1:
        d2 = medfilt(d2, kernel_size=mf)
        d3 = medfilt(d3, kernel_size=mf)

    # 去尖刺
    d1 = despike(d1)
    d2 = despike(d2)
    d3 = despike(d3)

    # η转换+重采样
    t_rel = t1 - t1[0]
    duration = t_rel[-1]
    fs = 6.0
    t_uniform = np.arange(0, duration, 1.0 / fs)

    def to_eta(d, t_r):
        eta = -(d - np.median(d))
        return interp1d(t_r, eta, kind='linear', fill_value='extrapolate')(t_uniform)

    eta1 = to_eta(d1, t_rel)
    eta2 = to_eta(d2, t_rel)
    eta3 = to_eta(d3, t_rel)

    min_len = min(len(eta1), len(eta2), len(eta3))
    eta1, eta2, eta3 = eta1[:min_len], eta2[:min_len], eta3[:min_len]

    # 带通滤波
    band = opts.get('filter_band', [0.04, 1.0])
    f_low = max(band[0], 0.01)
    b_filt, a_filt = butter(4, band, btype='band', fs=fs)
    padlen = min(3 * int(fs / f_low), min_len - 1)
    eta1_f = filtfilt(b_filt, a_filt, detrend(eta1), padlen=padlen)
    eta2_f = filtfilt(b_filt, a_filt, detrend(eta2), padlen=padlen)
    eta3_f = filtfilt(b_filt, a_filt, detrend(eta3), padlen=padlen)

    r1_mean = float(np.mean(raw_data[1]['distances']))

    ds_config = {
        'sample_rate': fs,
        'gravity': 9.81,
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

    # 覆盖nfft
    nfft_override = opts.get('nfft_override', None)
    iterations = opts.get('iterations', 100)

    # 直接调用 compute_directional_spectrum 以控制参数
    import sys as _sys
    _sys.path.insert(0, '/home/pi/radar/mqtt_system/services')
    from pydiwasp import dirspec

    # 倾斜校正
    eta2_use = eta2_f * analyzer.tilt_factors['R2']
    eta3_use = eta3_f * analyzer.tilt_factors['R3']

    analyzer.update_layout(r1_mean)

    data_matrix = np.column_stack([eta1_f, eta2_use, eta3_use])
    n_samples = min_len

    ID = {
        'data': data_matrix,
        'layout': analyzer.layout,
        'datatypes': np.array(['elev', 'elev', 'elev']),
        'depth': ds_config['water_depth'],
        'fs': fs
    }

    freqs = np.linspace(band[0], band[1], 128)
    dirs = np.linspace(0, 360, 361)[:-1]
    dirs_rad = np.radians(dirs)

    SM = {
        'freqs': freqs,
        'dirs': dirs_rad,
        'funit': 'Hz',
        'dunit': 'rad',
        'xaxisdir': analyzer.xaxisdir
    }

    nfft = nfft_override if nfft_override else min(256, n_samples // 4)
    nfft = max(64, int(2 ** np.floor(np.log2(nfft))))

    EP = {
        'method': 'IMLM',
        'nfft': nfft,
        'dres': 360,
        'iter': iterations,
        'smooth': 'ON'
    }

    options = ['MESSAGE', 0, 'PLOTTYPE', 0]

    logging.disable(logging.CRITICAL)
    SMout, EPout = dirspec(ID, SM, EP, options)
    logging.disable(logging.NOTSET)

    S = np.real(SMout['S'])
    freqs_out = SMout['freqs']
    dirs_out = np.degrees(SMout['dirs'])

    # 方向积分
    dir_spectrum = np.sum(S, axis=0)
    Dp_idx = np.argmax(dir_spectrum)
    Dp_axis = dirs_out[Dp_idx]

    # axis-angle → 罗盘来向
    Dp = (180 + analyzer.xaxisdir - Dp_axis) % 360

    # 1D谱
    ddir = dirs_out[1] - dirs_out[0] if len(dirs_out) > 1 else 1.0
    S1D = np.sum(S, axis=1) * np.radians(ddir)
    peak_idx = np.argmax(S1D)
    fp = freqs_out[peak_idx]
    Tp = 1.0 / fp if fp > 0 else 0

    # Hs
    df = freqs_out[1] - freqs_out[0] if len(freqs_out) > 1 else 0.01
    m0 = np.sum(S) * df * np.radians(ddir)
    Hs = 4.0 * np.sqrt(m0)

    # 方向展宽
    dirs_rad_out = np.radians(dirs_out)
    weights = dir_spectrum / np.sum(dir_spectrum)
    ms = np.sum(weights * np.sin(dirs_rad_out))
    mc = np.sum(weights * np.cos(dirs_rad_out))
    spread = np.degrees(np.sqrt(2 * (1 - np.sqrt(ms**2 + mc**2))))

    return {
        'Dp': Dp, 'Tp': Tp, 'Hs': Hs, 'spread': spread, 'nfft': nfft,
        'n_avg': n_samples // nfft  # 大约平均段数
    }


def angle_error(computed, expected):
    d1 = abs((computed - expected + 180) % 360 - 180)
    d2 = abs((computed - (expected + 180) % 360 + 180) % 360 - 180)
    return min(d1, d2)


def cross_spectral_direction(config, raw_data):
    """
    直接用互谱相位估算波向（不经过DIWASP），作为参考
    对于3个高程传感器，利用互谱相位 = k · Δr 求解波向
    """
    from directional_spectrum import DirectionalSpectrumAnalyzer

    t1 = raw_data[1]['epochs']
    d1 = despike(raw_data[1]['distances'].copy())
    d2 = despike(np.interp(t1, raw_data[2]['epochs'], raw_data[2]['distances']))
    d3 = despike(np.interp(t1, raw_data[3]['epochs'], raw_data[3]['distances']))

    t_rel = t1 - t1[0]
    duration = t_rel[-1]
    fs = 6.0
    t_uniform = np.arange(0, duration, 1.0 / fs)

    def to_eta(d, t_r):
        eta = -(d - np.median(d))
        return interp1d(t_r, eta, kind='linear', fill_value='extrapolate')(t_uniform)

    eta1 = detrend(to_eta(d1, t_rel))
    eta2 = detrend(to_eta(d2, t_rel))
    eta3 = detrend(to_eta(d3, t_rel))

    min_len = min(len(eta1), len(eta2), len(eta3))
    eta1, eta2, eta3 = eta1[:min_len], eta2[:min_len], eta3[:min_len]

    # 带通滤波
    band = [0.04, 1.0]
    b, a = butter(4, band, btype='band', fs=fs)
    padlen = min(3 * int(fs / band[0]), min_len - 1)
    eta1 = filtfilt(b, a, eta1, padlen=padlen)
    eta2 = filtfilt(b, a, eta2, padlen=padlen)
    eta3 = filtfilt(b, a, eta3, padlen=padlen)

    # 倾斜校正
    cos10 = np.cos(np.radians(10))
    eta2 *= cos10
    eta3 *= cos10

    # 等效测量点
    ds_config = {
        'sample_rate': fs, 'gravity': 9.81, 'water_depth': 100.0,
        'freq_range': band, 'direction_resolution': 360,
        'array_height': config['radar'].get('array_height', 5.0),
        'radar_positions': config['radar'].get('diwasp_positions', {}),
        'tilt_angles': config['radar'].get('tilt_angles', {}),
        'tilt_azimuths': config['radar'].get('tilt_azimuths', {}),
        'array_heading': config['radar'].get('array_heading', 0.0),
    }
    analyzer = DirectionalSpectrumAnalyzer(ds_config)
    r1_mean = float(np.mean(raw_data[1]['distances']))
    analyzer.update_layout(r1_mean)
    eff = analyzer.effective_positions

    # 互谱计算
    nperseg = 128
    f12, P12 = csd(eta1, eta2, fs=fs, nperseg=nperseg)
    f13, P13 = csd(eta1, eta3, fs=fs, nperseg=nperseg)
    f11, P11 = welch(eta1, fs=fs, nperseg=nperseg)

    # 找峰值频率附近的互谱相位
    valid = (f12 >= 0.04) & (f12 <= 1.0)
    peak_idx = np.argmax(np.abs(P11[valid]))
    fp = f12[valid][peak_idx]

    # 取峰值频率附近几个bin的加权平均相位
    peak_band = (f12 >= fp * 0.7) & (f12 <= fp * 1.3)
    w = np.abs(P11[peak_band])

    phase12 = np.angle(P12[peak_band])
    phase13 = np.angle(P13[peak_band])
    avg_phase12 = np.arctan2(np.sum(w * np.sin(phase12)), np.sum(w * np.cos(phase12)))
    avg_phase13 = np.arctan2(np.sum(w * np.sin(phase13)), np.sum(w * np.cos(phase13)))

    # 波数
    k = (2 * np.pi * fp)**2 / 9.81  # 深水近似

    # Δr
    dx2 = eff['R2'][0] - eff['R1'][0]
    dy2 = eff['R2'][1] - eff['R1'][1]
    dx3 = eff['R3'][0] - eff['R1'][0]
    dy3 = eff['R3'][1] - eff['R1'][1]

    # phase = kx * dx + ky * dy
    # 两个方程两个未知数 (kx, ky)
    A = np.array([[dx2, dy2], [dx3, dy3]])
    b_vec = np.array([avg_phase12, avg_phase13])

    try:
        k_vec = np.linalg.solve(A, b_vec)
        kx, ky = k_vec
        # 数学角度 → 罗盘角度
        math_angle = np.degrees(np.arctan2(ky, kx))
        # 传播方向(去向) → 来向(罗盘)
        compass_from = (90 - math_angle + 180) % 360
        return compass_from, fp, avg_phase12, avg_phase13
    except np.linalg.LinAlgError:
        return None, fp, avg_phase12, avg_phase13


def main():
    config = load_config()

    print("加载数据...")
    all_data = fetch_all_data()

    print()
    print("=" * 110)
    print("  DIWASP IMLM 波向精度优化实验")
    print(f"  目标: ±3°  |  期望波向: 66° (正面来波)")
    print("=" * 110)

    # ===== 方案定义 =====
    schemes = [
        {
            'name': 'A: 当前参数(基线)',
            'opts': {
                'r1_ref_filter': True,
                'r1_ref_threshold': 0.15,
                'filter_band': [0.04, 1.0],
                'nfft_override': None,
                'iterations': 100,
                'medfilt_r23': 0,
            }
        },
        {
            'name': 'B: 去掉R1参考滤波',
            'opts': {
                'r1_ref_filter': False,
                'filter_band': [0.04, 1.0],
                'nfft_override': None,
                'iterations': 100,
                'medfilt_r23': 0,
            }
        },
        {
            'name': 'C: 去R1滤波+小nfft=64',
            'opts': {
                'r1_ref_filter': False,
                'filter_band': [0.04, 1.0],
                'nfft_override': 64,
                'iterations': 100,
                'medfilt_r23': 0,
            }
        },
        {
            'name': 'D: 去R1滤波+窄带0.05-0.5Hz',
            'opts': {
                'r1_ref_filter': False,
                'filter_band': [0.05, 0.5],
                'nfft_override': None,
                'iterations': 100,
                'medfilt_r23': 0,
            }
        },
        {
            'name': 'E: 去R1+窄带+nfft64',
            'opts': {
                'r1_ref_filter': False,
                'filter_band': [0.05, 0.5],
                'nfft_override': 64,
                'iterations': 100,
                'medfilt_r23': 0,
            }
        },
        {
            'name': 'F: 去R1+medfilt5+窄带',
            'opts': {
                'r1_ref_filter': False,
                'filter_band': [0.05, 0.5],
                'nfft_override': None,
                'iterations': 100,
                'medfilt_r23': 5,
            }
        },
        {
            'name': 'G: R1滤波放宽到0.3m+窄带',
            'opts': {
                'r1_ref_filter': True,
                'r1_ref_threshold': 0.30,
                'filter_band': [0.05, 0.5],
                'nfft_override': None,
                'iterations': 100,
                'medfilt_r23': 0,
            }
        },
        {
            'name': 'H: 去R1+nfft64+iter200',
            'opts': {
                'r1_ref_filter': False,
                'filter_band': [0.04, 1.0],
                'nfft_override': 64,
                'iterations': 200,
                'medfilt_r23': 0,
            }
        },
    ]

    # ===== 互谱法参考 =====
    print("\n--- 互谱相位直接求解（参考基线） ---")
    print(f"{'时间':<6s}  {'期望':>5s}  {'互谱波向':>8s}  {'误差':>5s}  {'fp':>6s}  {'φ12':>8s}  {'φ13':>8s}")
    csd_errors = []
    for i, win in enumerate(WINDOWS):
        raw = all_data[i]
        if raw is None:
            continue
        dp, fp, ph12, ph13 = cross_spectral_direction(config, raw)
        if dp is not None:
            err = angle_error(dp, win['expect'])
            csd_errors.append(err)
            print(f"{win['label']:<6s}  {win['expect']:>4d}°  {dp:>7.1f}°  {err:>4.1f}°  {fp:>.3f}Hz  {np.degrees(ph12):>7.2f}°  {np.degrees(ph13):>7.2f}°")
    if csd_errors:
        ce = np.array(csd_errors)
        print(f"互谱法: 平均误差={np.mean(ce):.1f}° 最大={np.max(ce):.1f}° <3°:{np.sum(ce<3)}/{len(ce)} <5°:{np.sum(ce<5)}/{len(ce)}")

    # ===== 各方案对比 =====
    for scheme in schemes:
        print(f"\n--- {scheme['name']} ---")
        errors = []
        for i, win in enumerate(WINDOWS):
            raw = all_data[i]
            if raw is None:
                continue
            try:
                result = run_diwasp(config, raw, scheme['opts'])
                dp = result['Dp']
                err = angle_error(dp, win['expect'])
                errors.append(err)
                mark = " ✓" if err <= 3 else " ✗" if err > 10 else ""
                print(f"  {win['label']}  期望{win['expect']:>3d}°  计算{dp:>5.1f}°  误差{err:>4.1f}°  "
                      f"Tp={result['Tp']:.1f}s  spread={result['spread']:.0f}°  "
                      f"nfft={result['nfft']}  Hs={win['Hs']:.3f}m{mark}")
            except Exception as e:
                print(f"  {win['label']}  失败: {e}")

        if errors:
            ea = np.array(errors)
            within3 = np.sum(ea <= 3)
            within5 = np.sum(ea <= 5)
            print(f"  ── 平均:{np.mean(ea):.1f}° 最大:{np.max(ea):.1f}° "
                  f"≤3°:{within3}/{len(ea)} ≤5°:{within5}/{len(ea)} "
                  f"{'★★★ 达标' if within3 == len(ea) else '★★ 接近' if np.mean(ea) <= 5 else '未达标'}")

    print()
    print("=" * 80)
    print("总结: 甲方要求 ±3°")
    print("=" * 80)


if __name__ == '__main__':
    main()
