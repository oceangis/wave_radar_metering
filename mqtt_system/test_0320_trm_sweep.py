#!/usr/bin/env python3
"""
深入分析自定义传递函数:
1. 扫描斜率系数，找最优值（验证H·tan(α)=0.827是否最优）
2. 不同k值（频率）下TRM的影响
3. 验证改善是否一致
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


def prepare_data(raw_data, config):
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
    return e1[:ml], e2[:ml], e3[:ml], ml, float(np.mean(raw_data[1]['distances']))


def run_with_slope_coeff(config, e1, e2, e3, ml, r1_mean, slope_coeff):
    """用指定斜率系数跑自定义TRM的DFTM"""
    from directional_spectrum import DirectionalSpectrumAnalyzer
    from pydiwasp.private.wavenumber import wavenumber
    from pydiwasp.private.diwasp_csd import diwasp_csd
    from pydiwasp.private.DFTM import DFTM
    from pydiwasp.private.smoothspec import smoothspec
    from pydiwasp.interpspec import interpspec

    ds_config = {
        'sample_rate': 6.0, 'gravity': 9.81, 'water_depth': 100.0,
        'freq_range': [0.04, 1.0], 'direction_resolution': 360,
        'array_height': config['radar'].get('array_height', 5.0),
        'radar_positions': config['radar'].get('diwasp_positions', {}),
        'tilt_angles': config['radar'].get('tilt_angles', {}),
        'tilt_azimuths': config['radar'].get('tilt_azimuths', {}),
        'array_heading': config['radar'].get('array_heading', 0.0),
    }
    analyzer = DirectionalSpectrumAnalyzer(ds_config)
    analyzer.update_layout(r1_mean)

    fs = 6.0
    band = [0.04, 1.0]
    bf, af = butter(4, band, btype='band', fs=fs)
    padlen = min(3*int(fs/0.04), ml-1)
    e1f = filtfilt(bf, af, detrend(e1), padlen=padlen)
    e2f = filtfilt(bf, af, detrend(e2 * analyzer.tilt_factors['R2']), padlen=padlen)
    e3f = filtfilt(bf, af, detrend(e3 * analyzer.tilt_factors['R3']), padlen=padlen)

    data = detrend(np.column_stack([e1f, e2f, e3f]), axis=0)
    szd = 3

    nfft = max(64, int(2**np.floor(np.log2(min(256, ml//4)))))
    xps = np.empty((szd, szd, nfft//2), 'complex128')
    for m in range(szd):
        for n in range(szd):
            xpstmp, Ftmp = diwasp_csd(data[:, m], data[:, n], nfft, fs, flag=2)
            xps[m, n, :] = xpstmp[1:nfft//2+1]
    F = Ftmp[1:nfft//2+1]
    nf = nfft//2

    wns = wavenumber(2*np.pi*F, 100.0*np.ones(len(F)))
    dres = 360
    pidirs = np.linspace(-np.pi, np.pi - 2*np.pi/dres, num=dres)

    layout = analyzer.layout
    phi2 = np.radians(config['radar']['tilt_azimuths']['R2'])
    phi3 = np.radians(config['radar']['tilt_azimuths']['R3'])

    trm = np.empty((szd, nf, len(pidirs)), dtype='complex128')
    kx = np.empty((szd, szd, nf, len(pidirs)))

    for m in range(szd):
        if m == 0:
            trm[m] = 1.0
        elif m == 1:
            trm[m] = 1.0 + slope_coeff * 1j * wns[:, None] * np.sin(pidirs[None, :] + phi2)
        else:
            trm[m] = 1.0 + slope_coeff * 1j * wns[:, None] * np.sin(pidirs[None, :] + phi3)

        for n in range(szd):
            kx[m, n] = wns[:, None] * (
                (layout[0, n] - layout[0, m]) * np.cos(pidirs) +
                (layout[1, n] - layout[1, m]) * np.sin(pidirs))

    Ss = np.empty((szd, nf), dtype='complex128')
    for m in range(szd):
        tfn_max = np.max(np.abs(trm[m]), axis=1)
        Ss[m] = xps[m, m, :] / (tfn_max * np.conj(tfn_max) + 1e-30)

    ffs = (F >= 0.04) & (F <= 1.0)
    S = DFTM(xps[:, :, ffs], trm[:, ffs, :], kx[:, :, ffs, :],
             Ss[:, ffs], pidirs, 100, 0)
    S = np.real(S)
    S[np.isnan(S) | (S < 0)] = 0

    SM1 = {'freqs': F[ffs], 'dirs': pidirs, 'S': S, 'funit': 'Hz', 'dunit': 'rad'}
    freqs_out = np.linspace(0.04, 1.0, 128)
    dirs_out_rad = np.radians(np.linspace(0, 360, 361)[:-1])
    SM_target = {'freqs': freqs_out, 'dirs': dirs_out_rad, 'funit': 'Hz', 'dunit': 'rad',
                 'xaxisdir': analyzer.xaxisdir}
    SMout = interpspec(SM1, SM_target, method='linear')
    SMout = smoothspec(SMout, [[1, 0.5, 0.25], [1, 0.5, 0.25]])

    dirs_out = np.degrees(SMout['dirs'])
    S_final = np.real(SMout['S'])
    dir_spectrum = np.sum(S_final, axis=0)
    Dp_idx = np.argmax(dir_spectrum)
    Dp_axis = dirs_out[Dp_idx]
    return (180 + analyzer.xaxisdir - Dp_axis) % 360


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
            prepped.append(prepare_data(raw, config))

    # ===== 1. 斜率系数扫描 =====
    print()
    print("=" * 80)
    print("  实验1: 斜率系数扫描 (找最优值)")
    print(f"  理论值 H·tan(10°) = {4.69*np.tan(np.radians(10)):.3f}")
    print("=" * 80)

    coeffs = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.827, 1.0, 1.257, 1.5, 2.0]
    print(f"\n{'系数':>6s}  {'平均误差':>8s}  {'最大':>5s}  {'≤3°':>5s}  {'各窗口误差'}")
    print("─" * 80)

    for coeff in coeffs:
        errs = []
        for i, win in enumerate(WINDOWS):
            if prepped[i] is None:
                continue
            e1, e2, e3, ml, r1m = prepped[i]
            try:
                dp = run_with_slope_coeff(config, e1, e2, e3, ml, r1m, coeff)
                errs.append(angle_error(dp, win['expect']))
            except:
                errs.append(None)

        valid = [e for e in errs if e is not None]
        if valid:
            ea = np.array(valid)
            label = ""
            if abs(coeff - 0) < 0.01:
                label = " ← elev×3 (当前)"
            elif abs(coeff - 0.827) < 0.01:
                label = " ← H·tan(10°)"
            elif abs(coeff - 1.257) < 0.01:
                label = " ← H·tan(15°)"
            err_str = " ".join([f"{e:.0f}" if e is not None else "-" for e in errs])
            print(f"{coeff:>6.3f}  {np.mean(ea):>7.1f}°  {np.max(ea):>4.0f}°  "
                  f"{np.sum(ea<=3):>2d}/{len(ea)}  [{err_str}]{label}")

    # ===== 2. 按波高分类的最优系数 =====
    print()
    print("=" * 80)
    print("  实验2: 最优系数处的波高分类精度")
    print("=" * 80)

    # 用最优系数（从扫描中找）
    best_coeff = None
    best_avg = 999
    for coeff in np.arange(0, 2.5, 0.05):
        errs = []
        for i, win in enumerate(WINDOWS):
            if prepped[i] is None:
                continue
            e1, e2, e3, ml, r1m = prepped[i]
            try:
                dp = run_with_slope_coeff(config, e1, e2, e3, ml, r1m, coeff)
                errs.append(angle_error(dp, win['expect']))
            except:
                pass
        if errs:
            avg = np.mean(errs)
            if avg < best_avg:
                best_avg = avg
                best_coeff = coeff

    print(f"\n  最优斜率系数: {best_coeff:.2f} (理论H·tan(10°)={4.69*np.tan(np.radians(10)):.3f})")
    print(f"  最优平均误差: {best_avg:.1f}°")
    print()

    # 用最优系数详细输出
    print(f"{'时间':<6s} {'H1/3':>6s} {'期望':>4s} │ {'coeff=0':>7s} │ {'最优':>6s} │ {'改善':>4s}")
    print("─" * 50)

    h_groups = {'0.3m': [], '0.2m': [], '0.1m': []}

    for i, win in enumerate(WINDOWS):
        if prepped[i] is None:
            continue
        e1, e2, e3, ml, r1m = prepped[i]
        dp0 = run_with_slope_coeff(config, e1, e2, e3, ml, r1m, 0)
        dpb = run_with_slope_coeff(config, e1, e2, e3, ml, r1m, best_coeff)
        err0 = angle_error(dp0, win['expect'])
        errb = angle_error(dpb, win['expect'])

        m = "✓" if errb <= 3 else " "
        print(f"{win['label']:<6s} {win['h13']:>5.3f}m {win['expect']:>3d}° │ "
              f"{err0:>5.1f}° │ {errb:>4.1f}°{m} │ {err0-errb:>+4.1f}°")

        if win['h13'] >= 0.25:
            h_groups['0.3m'].append(errb)
        elif win['h13'] >= 0.15:
            h_groups['0.2m'].append(errb)
        else:
            h_groups['0.1m'].append(errb)

    print()
    for h, errs in h_groups.items():
        if errs:
            ea = np.array(errs)
            status = "✓ 达标" if np.mean(ea) <= 3 else "✗ 未达标"
            print(f"  H1/3≈{h}: avg={np.mean(ea):.1f}° max={np.max(ea):.0f}° ≤3°:{np.sum(ea<=3)}/{len(ea)} {status}")

    print()


if __name__ == '__main__':
    main()
