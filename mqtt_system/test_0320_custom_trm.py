#!/usr/bin/env python3
"""
核心突破: 为倾斜雷达创建自定义传递函数

倾斜雷达的实际测量值（cos校正后）:
  η_measured = η(x_eff) + H·tan(α)·∂η/∂s_tilt

其中 ∂η/∂s_tilt = 沿倾斜方向的波面斜率

传递函数:
  R1: trm = 1  (纯高程)
  R2: trm = 1 + H·tan(α)·j·k·(sin(φ2)·cos(θ) + cos(φ2)·sin(θ))
  R3: trm = 1 + H·tan(α)·j·k·(sin(φ3)·cos(θ) + cos(φ3)·sin(θ))

其中 φ = 倾斜方位角(阵列坐标), θ = DIWASP内部方向角(pidirs)
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
    {'label': '09:11', 'start': '2026-03-20 09:06:48+08', 'end': '2026-03-20 09:11:52+08', 'expect': 66, 'h13': 0.104},
    {'label': '09:33', 'start': '2026-03-20 09:28:11+08', 'end': '2026-03-20 09:33:15+08', 'expect': 66, 'h13': 0.211},
    {'label': '09:57', 'start': '2026-03-20 09:52:41+08', 'end': '2026-03-20 09:57:44+08', 'expect': 66, 'h13': 0.294},
    {'label': '10:21', 'start': '2026-03-20 10:16:53+08', 'end': '2026-03-20 10:21:57+08', 'expect': 66, 'h13': 0.304},
    {'label': '10:45', 'start': '2026-03-20 10:40:23+08', 'end': '2026-03-20 10:45:26+08', 'expect': 66, 'h13': 0.214},
    {'label': '11:08', 'start': '2026-03-20 11:03:19+08', 'end': '2026-03-20 11:08:22+08', 'expect': 66, 'h13': 0.098},
    {'label': '11:28', 'start': '2026-03-20 11:23:39+08', 'end': '2026-03-20 11:28:43+08', 'expect': 66, 'h13': 0.103},
    {'label': '14:10', 'start': '2026-03-20 14:04:59+08', 'end': '2026-03-20 14:10:02+08', 'expect': 66, 'h13': 0.207},
    {'label': '14:30', 'start': '2026-03-20 14:25:18+08', 'end': '2026-03-20 14:30:22+08', 'expect': 66, 'h13': 0.306},
    {'label': '14:54', 'start': '2026-03-20 14:49:36+08', 'end': '2026-03-20 14:54:39+08', 'expect': 246, 'h13': 0.270},
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


def run_custom_trm(config, raw_data, method='DFTM'):
    """用自定义传递函数跑DIWASP"""
    from directional_spectrum import DirectionalSpectrumAnalyzer
    from pydiwasp.private.wavenumber import wavenumber
    from pydiwasp.private.diwasp_csd import diwasp_csd
    from pydiwasp.private.DFTM import DFTM
    from pydiwasp.private.IMLM import IMLM
    from pydiwasp.private.smoothspec import smoothspec
    from pydiwasp.interpspec import interpspec

    DirectionalSpectrumAnalyzer._last_Dp = None

    # 准备数据
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
        lo = q25 - 3*iqr
        hi = q75 + 3*iqr
        out = (d < lo) | (d > hi)
        if np.any(out):
            good = ~out
            if np.sum(good) > 10:
                d[out] = np.interp(np.where(out)[0], np.where(good)[0], d[good])

    t_rel = t1 - t1[0]
    dur = t_rel[-1]
    fs = 6.0
    t_uni = np.arange(0, dur, 1.0/fs)

    def to_eta(d):
        return -(d - np.median(d))

    eta1 = interp1d(t_rel, to_eta(d1), fill_value='extrapolate')(t_uni)
    eta2 = interp1d(t_rel, to_eta(d2), fill_value='extrapolate')(t_uni)
    eta3 = interp1d(t_rel, to_eta(d3), fill_value='extrapolate')(t_uni)

    ml = min(len(eta1), len(eta2), len(eta3))
    eta1, eta2, eta3 = eta1[:ml], eta2[:ml], eta3[:ml]

    # 分析器配置
    ds_config = {
        'sample_rate': fs, 'gravity': 9.81, 'water_depth': 100.0,
        'freq_range': [0.04, 1.0], 'direction_resolution': 360,
        'array_height': config['radar'].get('array_height', 5.0),
        'radar_positions': config['radar'].get('diwasp_positions', {}),
        'tilt_angles': config['radar'].get('tilt_angles', {}),
        'tilt_azimuths': config['radar'].get('tilt_azimuths', {}),
        'array_heading': config['radar'].get('array_heading', 0.0),
    }
    analyzer = DirectionalSpectrumAnalyzer(ds_config)
    r1_mean = float(np.mean(raw_data[1]['distances']))
    analyzer.update_layout(r1_mean)

    # cos校正
    eta2_cor = eta2 * analyzer.tilt_factors['R2']
    eta3_cor = eta3 * analyzer.tilt_factors['R3']

    # 带通滤波
    band = [0.04, 1.0]
    b, a = butter(4, band, btype='band', fs=fs)
    padlen = min(3 * int(fs / 0.04), ml - 1)
    e1f = filtfilt(b, a, detrend(eta1), padlen=padlen)
    e2f = filtfilt(b, a, detrend(eta2_cor), padlen=padlen)
    e3f = filtfilt(b, a, detrend(eta3_cor), padlen=padlen)

    data = np.column_stack([e1f, e2f, e3f])
    data = detrend(data, axis=0)
    szd = 3
    n_samples = ml

    # 互谱
    nfft = min(256, n_samples // 4)
    nfft = max(64, int(2 ** np.floor(np.log2(nfft))))

    xps = np.empty((szd, szd, nfft // 2), 'complex128')
    for m in range(szd):
        for n in range(szd):
            xpstmp, Ftmp = diwasp_csd(data[:, m], data[:, n], nfft, fs, flag=2)
            xps[m, n, :] = xpstmp[1:nfft // 2 + 1]
    F = Ftmp[1:nfft // 2 + 1]
    nf = nfft // 2

    wns = wavenumber(2 * np.pi * F, 100.0 * np.ones(len(F)))
    dres = 360
    pidirs = np.linspace(-np.pi, np.pi - 2 * np.pi / dres, num=dres)

    # ===== 自定义传递函数 =====
    tilt_angles = {
        0: 0.0,   # R1
        1: np.radians(config['radar']['tilt_angles']['R2']),   # R2
        2: np.radians(config['radar']['tilt_angles']['R3']),   # R3
    }
    # 倾斜方位角（阵列坐标: 0=+y前方, 90=+x右方）
    tilt_azimuths = {
        0: 0.0,   # R1 不倾斜
        1: np.radians(config['radar']['tilt_azimuths']['R2']),  # R2: 300°
        2: np.radians(config['radar']['tilt_azimuths']['R3']),  # R3: 60°
    }

    layout = analyzer.layout  # 等效测量点位置

    trm = np.empty((szd, nf, len(pidirs)), dtype='complex128')
    kx = np.empty((szd, szd, nf, len(pidirs)))

    for m in range(szd):
        alpha = tilt_angles[m]
        phi = tilt_azimuths[m]

        if alpha > 0:
            # 自定义: trm = 1 + H·tan(α)·j·k·(sin(φ)·cos(θ) + cos(φ)·sin(θ))
            #            = 1 + H·tan(α)·j·k·sin(θ + φ)
            slope_coeff = r1_mean * np.tan(alpha)
            trm[m, :, :] = 1.0 + slope_coeff * 1j * wns[:, np.newaxis] * np.sin(pidirs[np.newaxis, :] + phi)
        else:
            # R1 纯高程
            trm[m, :, :] = np.ones((nf, len(pidirs)))

        for n in range(szd):
            kx[m, n, :, :] = wns[:, np.newaxis] * (
                (layout[0, n] - layout[0, m]) * np.cos(pidirs) +
                (layout[1, n] - layout[1, m]) * np.sin(pidirs)
            )

    # 自谱归一化
    Ss = np.empty((szd, nf), dtype='complex128')
    for m in range(szd):
        tfn = trm[m, :, :]
        Sxps = xps[m, m, :]
        tfn_max = np.max(np.abs(tfn), axis=1)
        Ss[m, :] = Sxps / (tfn_max * np.conj(tfn_max) + 1e-30)

    # 频率选择
    ffs = (F >= 0.04) & (F <= 1.0)

    # 方向谱估算
    if method == 'DFTM':
        S = DFTM(xps[:, :, ffs], trm[:, ffs, :], kx[:, :, ffs, :],
                 Ss[:, ffs], pidirs, 100, 0)
    elif method == 'IMLM':
        S = IMLM(xps[:, :, ffs], trm[:, ffs, :], kx[:, :, ffs, :],
                 Ss[:, ffs], pidirs, 100, 0)

    S = np.real(S)
    S[np.isnan(S) | (S < 0)] = 0

    # 插值到0-360°
    SM1 = {'freqs': F[ffs], 'dirs': pidirs, 'S': S, 'funit': 'Hz', 'dunit': 'rad'}
    freqs_out = np.linspace(0.04, 1.0, 128)
    dirs_out_rad = np.radians(np.linspace(0, 360, 361)[:-1])
    SM_target = {'freqs': freqs_out, 'dirs': dirs_out_rad, 'funit': 'Hz', 'dunit': 'rad',
                 'xaxisdir': analyzer.xaxisdir}
    SMout = interpspec(SM1, SM_target, method='linear')
    SMout = smoothspec(SMout, [[1, 0.5, 0.25], [1, 0.5, 0.25]])

    dirs_out = np.degrees(SMout['dirs'])
    S_final = np.real(SMout['S'])

    # 主波向
    dir_spectrum = np.sum(S_final, axis=0)
    Dp_idx = np.argmax(dir_spectrum)
    Dp_axis = dirs_out[Dp_idx]
    Dp = (180 + analyzer.xaxisdir - Dp_axis) % 360

    return Dp


def run_standard_dftm(config, raw_data):
    """标准DFTM（elev×3，当前方法）"""
    from directional_spectrum import DirectionalSpectrumAnalyzer
    from pydiwasp import dirspec

    DirectionalSpectrumAnalyzer._last_Dp = None

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
        lo, hi = q25 - 3*iqr, q75 + 3*iqr
        out = (d < lo) | (d > hi)
        if np.any(out):
            good = ~out
            if np.sum(good) > 10:
                d[out] = np.interp(np.where(out)[0], np.where(good)[0], d[good])

    t_rel = t1 - t1[0]
    dur = t_rel[-1]
    fs = 6.0
    t_uni = np.arange(0, dur, 1.0/fs)
    def to_eta(d):
        return -(d - np.median(d))
    e1 = interp1d(t_rel, to_eta(d1), fill_value='extrapolate')(t_uni)
    e2 = interp1d(t_rel, to_eta(d2), fill_value='extrapolate')(t_uni)
    e3 = interp1d(t_rel, to_eta(d3), fill_value='extrapolate')(t_uni)
    ml = min(len(e1), len(e2), len(e3))
    e1, e2, e3 = e1[:ml], e2[:ml], e3[:ml]

    ds_config = {
        'sample_rate': fs, 'gravity': 9.81, 'water_depth': 100.0,
        'freq_range': [0.04, 1.0], 'direction_resolution': 360,
        'array_height': config['radar'].get('array_height', 5.0),
        'radar_positions': config['radar'].get('diwasp_positions', {}),
        'tilt_angles': config['radar'].get('tilt_angles', {}),
        'tilt_azimuths': config['radar'].get('tilt_azimuths', {}),
        'array_heading': config['radar'].get('array_heading', 0.0),
    }
    analyzer = DirectionalSpectrumAnalyzer(ds_config)
    r1_mean = float(np.mean(raw_data[1]['distances']))
    analyzer.update_layout(r1_mean)

    band = [0.04, 1.0]
    bf, af = butter(4, band, btype='band', fs=fs)
    padlen = min(3*int(fs/0.04), ml-1)
    e1f = filtfilt(bf, af, detrend(e1), padlen=padlen)
    e2f = filtfilt(bf, af, detrend(e2 * analyzer.tilt_factors['R2']), padlen=padlen)
    e3f = filtfilt(bf, af, detrend(e3 * analyzer.tilt_factors['R3']), padlen=padlen)

    data_m = np.column_stack([e1f, e2f, e3f])
    ID = {'data': data_m, 'layout': analyzer.layout,
          'datatypes': np.array(['elev','elev','elev']),
          'depth': 100.0, 'fs': fs}
    freqs = np.linspace(0.04, 1.0, 128)
    dirs = np.linspace(0, 360, 361)[:-1]
    SM = {'freqs': freqs, 'dirs': np.radians(dirs), 'funit': 'Hz', 'dunit': 'rad',
          'xaxisdir': analyzer.xaxisdir}
    nfft = max(64, int(2**np.floor(np.log2(min(256, ml//4)))))
    EP = {'method': 'DFTM', 'nfft': nfft, 'dres': 360, 'iter': 100, 'smooth': 'ON'}

    logging.disable(logging.CRITICAL)
    try:
        SMout, _ = dirspec(ID, SM, EP, ['MESSAGE', 0, 'PLOTTYPE', 0])
    finally:
        logging.disable(logging.NOTSET)

    S = np.real(SMout['S'])
    dirs_out = np.degrees(SMout['dirs'])
    dir_sp = np.sum(S, axis=0)
    dp_axis = dirs_out[np.argmax(dir_sp)]
    return (180 + analyzer.xaxisdir - dp_axis) % 360


def angle_error(computed, expected):
    d1 = abs((computed - expected + 180) % 360 - 180)
    d2 = abs((computed - (expected + 180) % 360 + 180) % 360 - 180)
    return min(d1, d2)


def main():
    config = load_config()

    print()
    print("=" * 85)
    print("  自定义传递函数 (elev+slope混合) vs 标准DFTM (纯elev)")
    print(f"  R2: tilt={config['radar']['tilt_angles']['R2']}° "
          f"azi={config['radar']['tilt_azimuths']['R2']}°")
    print(f"  R3: tilt={config['radar']['tilt_angles']['R3']}° "
          f"azi={config['radar']['tilt_azimuths']['R3']}°")
    print(f"  slope系数 H·tan(α) = {4.69*np.tan(np.radians(10)):.3f}m")
    print("=" * 85)
    print()
    print(f"{'时间':<6s} {'H1/3':>6s} {'期望':>4s} │ {'标准DFTM':>8s} {'误差':>5s} │ {'自定义TRM':>9s} {'误差':>5s} │ {'改善':>5s}")
    print("─" * 75)

    errs_std, errs_custom = [], []

    for win in WINDOWS:
        raw = fetch_data(win['start'], win['end'])
        if raw is None:
            continue

        logging.disable(logging.CRITICAL)
        try:
            dp_std = run_standard_dftm(config, raw)
        except Exception as e:
            dp_std = None
        try:
            dp_custom = run_custom_trm(config, raw, method='DFTM')
        except Exception as e:
            dp_custom = None
            print(f"  ERROR: {e}")
        finally:
            logging.disable(logging.NOTSET)

        err_std = angle_error(dp_std, win['expect']) if dp_std else None
        err_cust = angle_error(dp_custom, win['expect']) if dp_custom else None

        if err_std is not None: errs_std.append(err_std)
        if err_cust is not None: errs_custom.append(err_cust)

        def fmt(dp, err):
            if dp is None: return "FAIL", "  -  "
            mark = "✓" if err <= 3 else "✗" if err > 10 else " "
            return f"{dp:.0f}°", f"{err:.1f}°{mark}"

        ds, es = fmt(dp_std, err_std) if err_std is not None else ("FAIL", "  -  ")
        dc, ec = fmt(dp_custom, err_cust) if err_cust is not None else ("FAIL", "  -  ")
        improve = (err_std - err_cust) if err_std is not None and err_cust is not None else None
        imp_s = f"{improve:+.1f}°" if improve is not None else "  -  "

        print(f"{win['label']:<6s} {win['h13']:>5.3f}m {win['expect']:>3d}° │ "
              f"{ds:>8s} {es:>5s} │ {dc:>9s} {ec:>5s} │ {imp_s:>5s}")

    print("─" * 75)
    if errs_std:
        es = np.array(errs_std)
        print(f"  标准DFTM:  avg={np.mean(es):.1f}° max={np.max(es):.1f}° ≤3°:{np.sum(es<=3)}/{len(es)} ≤5°:{np.sum(es<=5)}/{len(es)}")
    if errs_custom:
        ec = np.array(errs_custom)
        print(f"  自定义TRM: avg={np.mean(ec):.1f}° max={np.max(ec):.1f}° ≤3°:{np.sum(ec<=3)}/{len(ec)} ≤5°:{np.sum(ec<=5)}/{len(ec)}")
    print()


if __name__ == '__main__':
    main()
