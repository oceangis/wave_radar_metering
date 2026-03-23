#!/usr/bin/env python3
"""
用 slope 传感器模型重算波向：
  从三个雷达高程推导出 [η, ∂η/∂x, ∂η/∂y]
  喂给 DIWASP 用 datatypes=['elev', 'slpx', 'slpy']

原理：
  η1 = η(R1),  η2 = η(R2),  η3 = η(R3)
  η2 - η1 ≈ ∂η/∂x · Δx12 + ∂η/∂y · Δy12
  η3 - η1 ≈ ∂η/∂x · Δx13 + ∂η/∂y · Δy13
  求解 → ∂η/∂x, ∂η/∂y
"""
import sys, os, yaml, logging, warnings
import numpy as np
import psycopg2
from scipy.signal import detrend, butter, filtfilt
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


def prepare_three_etas(raw_data, config):
    """预处理：返回三路重采样后的eta（带通滤波前）"""
    t1 = raw_data[1]['epochs']
    d1 = raw_data[1]['distances'].copy()
    d2 = np.interp(t1, raw_data[2]['epochs'], raw_data[2]['distances'])
    d3 = np.interp(t1, raw_data[3]['epochs'], raw_data[3]['distances'])

    # R1参考滤波（当前线上参数）
    r1_thresh = config['analysis'].get('r1_ref_threshold', 0.15)
    sp2 = np.abs(d2 - d1) > r1_thresh
    sp3 = np.abs(d3 - d1) > r1_thresh
    if np.any(sp2): d2[sp2] = d1[sp2]
    if np.any(sp3): d3[sp3] = d1[sp3]

    # 去尖刺
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

    # η + 重采样
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

    r1_mean = float(np.mean(raw_data[1]['distances']))
    return eta1, eta2, eta3, r1_mean, ml


def run_slope_analysis(config, eta1, eta2, eta3, r1_mean, n_samples, method='DFTM'):
    """
    方案: 从三路eta推导 [η, ∂η/∂x, ∂η/∂y]，用 ['elev','slpx','slpy'] 喂给DIWASP
    """
    from directional_spectrum import DirectionalSpectrumAnalyzer
    from pydiwasp import dirspec

    DirectionalSpectrumAnalyzer._last_Dp = None

    ds_config = {
        'sample_rate': 6.0, 'gravity': 9.81,
        'water_depth': config['analysis'].get('water_depth', 100.0),
        'freq_range': config['analysis'].get('filter_band', [0.04, 1.0]),
        'direction_resolution': 360,
        'array_height': config['radar'].get('array_height', 5.0),
        'radar_positions': config['radar'].get('diwasp_positions', {}),
        'tilt_angles': config['radar'].get('tilt_angles', {}),
        'tilt_azimuths': config['radar'].get('tilt_azimuths', {}),
        'array_heading': config['radar'].get('array_heading', 0.0),
    }
    analyzer = DirectionalSpectrumAnalyzer(ds_config)
    analyzer.update_layout(r1_mean)
    eff = analyzer.effective_positions

    # 倾斜校正 R2/R3 幅度
    cos_tilt = analyzer.tilt_factors
    eta2_cor = eta2 * cos_tilt['R2']
    eta3_cor = eta3 * cos_tilt['R3']

    # 等效测量点位置差
    dx12 = eff['R2'][0] - eff['R1'][0]
    dy12 = eff['R2'][1] - eff['R1'][1]
    dx13 = eff['R3'][0] - eff['R1'][0]
    dy13 = eff['R3'][1] - eff['R1'][1]

    # 求解 ∂η/∂x, ∂η/∂y
    # [dx12  dy12] [∂η/∂x]   [η2 - η1]
    # [dx13  dy13] [∂η/∂y] = [η3 - η1]
    A = np.array([[dx12, dy12],
                  [dx13, dy13]])
    det_A = dx12 * dy13 - dx13 * dy12

    deta2 = eta2_cor - eta1  # η2 - η1
    deta3 = eta3_cor - eta1  # η3 - η1

    # 逐样本求解（矩阵是常数，只有右端项变化）
    deta_dx = (dy13 * deta2 - dy12 * deta3) / det_A
    deta_dy = (-dx13 * deta2 + dx12 * deta3) / det_A

    # 带通滤波
    fs = 6.0
    band = config['analysis'].get('filter_band', [0.04, 1.0])
    b, a = butter(4, band, btype='band', fs=fs)
    f_low = max(band[0], 0.01)
    padlen = min(3 * int(fs / f_low), n_samples - 1)

    eta1_f = filtfilt(b, a, detrend(eta1), padlen=padlen)
    deta_dx_f = filtfilt(b, a, detrend(deta_dx), padlen=padlen)
    deta_dy_f = filtfilt(b, a, detrend(deta_dy), padlen=padlen)

    # DIWASP: 三个通道 = [elevation, slope_x, slope_y]
    # layout: 全部在同一点（斜率是从差分导出的，代表阵列中心的局部斜率）
    data_matrix = np.column_stack([eta1_f, deta_dx_f, deta_dy_f])

    # slpx/slpy 的方向角是 DIWASP 内部坐标（axis-angle）
    # xaxisdir 会处理罗盘→内部坐标的转换
    ID = {
        'data': data_matrix,
        'layout': np.array([
            [0.0, 0.0, 0.0],   # x: 全部在原点
            [0.0, 0.0, 0.0],   # y: 全部在原点
            [0.0, 0.0, 0.0]    # z: 水面
        ]),
        'datatypes': np.array(['elev', 'slpx', 'slpy']),
        'depth': ds_config['water_depth'],
        'fs': fs
    }

    freqs = np.linspace(band[0], band[1], 128)
    dirs = np.linspace(0, 360, 361)[:-1]

    SM = {
        'freqs': freqs,
        'dirs': np.radians(dirs),
        'funit': 'Hz',
        'dunit': 'rad',
        'xaxisdir': analyzer.xaxisdir
    }

    nfft = min(256, n_samples // 4)
    nfft = max(64, int(2 ** np.floor(np.log2(nfft))))

    EP = {
        'method': method,
        'nfft': nfft,
        'dres': 360,
        'iter': 100,
        'smooth': 'ON'
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

    # 主波向
    dir_spectrum = np.sum(S, axis=0)
    Dp_idx = np.argmax(dir_spectrum)
    Dp_axis = dirs_out[Dp_idx]
    # axis-angle → 罗盘来向
    Dp = (180 + analyzer.xaxisdir - Dp_axis) % 360

    # 展宽
    dirs_rad = np.radians(dirs_out)
    w = dir_spectrum / np.sum(dir_spectrum) if np.sum(dir_spectrum) > 0 else np.ones_like(dir_spectrum) / len(dir_spectrum)
    ms = np.sum(w * np.sin(dirs_rad))
    mc = np.sum(w * np.cos(dirs_rad))
    spread = np.degrees(np.sqrt(2 * (1 - np.sqrt(ms**2 + mc**2))))

    # Tp
    ddir_deg = dirs_out[1] - dirs_out[0] if len(dirs_out) > 1 else 1.0
    S1D = np.sum(S, axis=1) * np.radians(ddir_deg)
    fp = freqs_out[np.argmax(S1D)]
    Tp = 1.0 / fp if fp > 0 else 0

    return {'Dp': Dp, 'spread': spread, 'Tp': Tp}


def run_elev_analysis(config, eta1, eta2, eta3, r1_mean, n_samples, method='DFTM'):
    """原来的方法: 三个都当 elev"""
    from directional_spectrum import DirectionalSpectrumAnalyzer
    from pydiwasp import dirspec

    DirectionalSpectrumAnalyzer._last_Dp = None

    ds_config = {
        'sample_rate': 6.0, 'gravity': 9.81,
        'water_depth': config['analysis'].get('water_depth', 100.0),
        'freq_range': config['analysis'].get('filter_band', [0.04, 1.0]),
        'direction_resolution': 360,
        'array_height': config['radar'].get('array_height', 5.0),
        'radar_positions': config['radar'].get('diwasp_positions', {}),
        'tilt_angles': config['radar'].get('tilt_angles', {}),
        'tilt_azimuths': config['radar'].get('tilt_azimuths', {}),
        'array_heading': config['radar'].get('array_heading', 0.0),
    }
    analyzer = DirectionalSpectrumAnalyzer(ds_config)
    analyzer.update_layout(r1_mean)

    eta2_use = eta2 * analyzer.tilt_factors['R2']
    eta3_use = eta3 * analyzer.tilt_factors['R3']

    fs = 6.0
    band = config['analysis'].get('filter_band', [0.04, 1.0])
    b, a = butter(4, band, btype='band', fs=fs)
    f_low = max(band[0], 0.01)
    padlen = min(3 * int(fs / f_low), n_samples - 1)

    eta1_f = filtfilt(b, a, detrend(eta1), padlen=padlen)
    eta2_f = filtfilt(b, a, detrend(eta2_use), padlen=padlen)
    eta3_f = filtfilt(b, a, detrend(eta3_use), padlen=padlen)

    data_matrix = np.column_stack([eta1_f, eta2_f, eta3_f])

    ID = {
        'data': data_matrix,
        'layout': analyzer.layout,
        'datatypes': np.array(['elev', 'elev', 'elev']),
        'depth': ds_config['water_depth'],
        'fs': fs
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
        'method': method, 'nfft': nfft,
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

    dir_spectrum = np.sum(S, axis=0)
    Dp_idx = np.argmax(dir_spectrum)
    Dp_axis = dirs_out[Dp_idx]
    Dp = (180 + analyzer.xaxisdir - Dp_axis) % 360

    dirs_rad = np.radians(dirs_out)
    w = dir_spectrum / np.sum(dir_spectrum) if np.sum(dir_spectrum) > 0 else np.ones_like(dir_spectrum) / len(dir_spectrum)
    ms = np.sum(w * np.sin(dirs_rad))
    mc = np.sum(w * np.cos(dirs_rad))
    spread = np.degrees(np.sqrt(2 * (1 - np.sqrt(ms**2 + mc**2))))

    ddir_deg = dirs_out[1] - dirs_out[0] if len(dirs_out) > 1 else 1.0
    S1D = np.sum(S, axis=1) * np.radians(ddir_deg)
    fp = freqs_out[np.argmax(S1D)]
    Tp = 1.0 / fp if fp > 0 else 0

    return {'Dp': Dp, 'spread': spread, 'Tp': Tp}


def angle_error(computed, expected):
    d1 = abs((computed - expected + 180) % 360 - 180)
    d2 = abs((computed - (expected + 180) % 360 + 180) % 360 - 180)
    return min(d1, d2)


def main():
    config = load_config()
    print("加载数据...")
    all_data = fetch_all_data()

    print("预处理...")
    prepped = []
    for raw in all_data:
        if raw is None:
            prepped.append(None)
        else:
            prepped.append(prepare_three_etas(raw, config))

    print()
    print("=" * 100)
    print("  elev×3 vs elev+slope 波向精度对比")
    print(f"  array_heading = {config['radar'].get('array_heading')}°")
    print("=" * 100)

    methods = ['DFTM', 'IMLM']

    for method in methods:
        print(f"\n--- 方法: {method} ---")
        print(f"{'时间':<6s} {'Hs':>5s} {'期望':>4s} │ {'elev×3':>7s} {'误差':>4s} │ {'elev+slope':>10s} {'误差':>4s} │ {'改善':>4s}")
        print("─" * 72)

        errs_old, errs_new = [], []

        for i, win in enumerate(WINDOWS):
            if prepped[i] is None:
                print(f"{win['label']:<6s}  数据不足")
                continue

            eta1, eta2, eta3, r1_mean, n_samples = prepped[i]

            r_old = run_elev_analysis(config, eta1, eta2, eta3, r1_mean, n_samples, method)
            r_new = run_slope_analysis(config, eta1, eta2, eta3, r1_mean, n_samples, method)

            dp_old = r_old['Dp']
            dp_new = r_new['Dp']
            err_old = angle_error(dp_old, win['expect'])
            err_new = angle_error(dp_new, win['expect'])
            errs_old.append(err_old)
            errs_new.append(err_new)

            improve = err_old - err_new
            mark = "✓" if err_new <= 3 else " "
            print(f"{win['label']:<6s} {win['Hs']:>4.2f}m {win['expect']:>3d}° │ "
                  f"{dp_old:>5.1f}° {err_old:>4.1f}° │ "
                  f"{dp_new:>8.1f}° {err_new:>4.1f}°{mark} │ "
                  f"{improve:>+4.1f}°")

        if errs_old and errs_new:
            eo, en = np.array(errs_old), np.array(errs_new)
            print("─" * 72)
            print(f"{'平均':>15s} │ {'':>7s} {np.mean(eo):>4.1f}° │ {'':>10s} {np.mean(en):>4.1f}° │ {np.mean(eo)-np.mean(en):>+4.1f}°")
            print(f"{'最大':>15s} │ {'':>7s} {np.max(eo):>4.1f}° │ {'':>10s} {np.max(en):>4.1f}° │")
            print(f"{'≤3°':>15s} │ {'':>7s} {np.sum(eo<=3):>2d}/10 │ {'':>10s} {np.sum(en<=3):>2d}/10 │")
            print(f"{'≤5°':>15s} │ {'':>7s} {np.sum(eo<=5):>2d}/10 │ {'':>10s} {np.sum(en<=5):>2d}/10 │")

    print()


if __name__ == '__main__':
    main()
