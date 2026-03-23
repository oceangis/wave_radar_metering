#!/usr/bin/env python3
"""
混合方案: 5通道 = 3×elev(保留空间相位) + 2×slope(增加方向分辨)
datatypes = ['elev', 'elev', 'elev', 'slpx', 'slpy']
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

    r1_mean = float(np.mean(raw_data[1]['distances']))
    return eta1, eta2, eta3, r1_mean, ml


def run_analysis(config, eta1, eta2, eta3, r1_mean, n_samples, method, mode='elev3'):
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

    # 倾斜校正
    eta2_cor = eta2 * analyzer.tilt_factors['R2']
    eta3_cor = eta3 * analyzer.tilt_factors['R3']

    # 带通滤波
    fs = 6.0
    band = config['analysis'].get('filter_band', [0.04, 1.0])
    b, a = butter(4, band, btype='band', fs=fs)
    f_low = max(band[0], 0.01)
    padlen = min(3 * int(fs / f_low), n_samples - 1)

    eta1_f = filtfilt(b, a, detrend(eta1), padlen=padlen)
    eta2_f = filtfilt(b, a, detrend(eta2_cor), padlen=padlen)
    eta3_f = filtfilt(b, a, detrend(eta3_cor), padlen=padlen)

    if mode == 'hybrid5':
        # 从三路eta导出斜率
        dx12 = eff['R2'][0] - eff['R1'][0]
        dy12 = eff['R2'][1] - eff['R1'][1]
        dx13 = eff['R3'][0] - eff['R1'][0]
        dy13 = eff['R3'][1] - eff['R1'][1]
        det_A = dx12 * dy13 - dx13 * dy12

        deta2 = eta2_f - eta1_f
        deta3 = eta3_f - eta1_f
        deta_dx = (dy13 * deta2 - dy12 * deta3) / det_A
        deta_dy = (-dx13 * deta2 + dx12 * deta3) / det_A

        # 5通道: 3×elev + 2×slope
        data_matrix = np.column_stack([eta1_f, eta2_f, eta3_f, deta_dx, deta_dy])

        # 阵列中心位置（斜率的参考点）
        cx = (eff['R1'][0] + eff['R2'][0] + eff['R3'][0]) / 3
        cy = (eff['R1'][1] + eff['R2'][1] + eff['R3'][1]) / 3

        layout = np.array([
            [eff['R1'][0], eff['R2'][0], eff['R3'][0], cx, cx],
            [eff['R1'][1], eff['R2'][1], eff['R3'][1], cy, cy],
            [0.0, 0.0, 0.0, 0.0, 0.0]
        ])
        datatypes = np.array(['elev', 'elev', 'elev', 'slpx', 'slpy'])

    elif mode == 'slope3':
        # 3通道: elev + slpx + slpy (co-located)
        dx12 = eff['R2'][0] - eff['R1'][0]
        dy12 = eff['R2'][1] - eff['R1'][1]
        dx13 = eff['R3'][0] - eff['R1'][0]
        dy13 = eff['R3'][1] - eff['R1'][1]
        det_A = dx12 * dy13 - dx13 * dy12

        deta2 = eta2_f - eta1_f
        deta3 = eta3_f - eta1_f
        deta_dx = (dy13 * deta2 - dy12 * deta3) / det_A
        deta_dy = (-dx13 * deta2 + dx12 * deta3) / det_A

        data_matrix = np.column_stack([eta1_f, deta_dx, deta_dy])
        layout = np.array([
            [0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0]
        ])
        datatypes = np.array(['elev', 'slpx', 'slpy'])

    else:  # elev3
        data_matrix = np.column_stack([eta1_f, eta2_f, eta3_f])
        layout = analyzer.layout
        datatypes = np.array(['elev', 'elev', 'elev'])

    ID = {
        'data': data_matrix,
        'layout': layout,
        'datatypes': datatypes,
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
    except Exception as e:
        logging.disable(logging.NOTSET)
        return {'Dp': None, 'error': str(e)}
    finally:
        logging.disable(logging.NOTSET)

    S = np.real(SMout['S'])
    dirs_out = np.degrees(SMout['dirs'])

    dir_spectrum = np.sum(S, axis=0)
    Dp_idx = np.argmax(dir_spectrum)
    Dp_axis = dirs_out[Dp_idx]
    Dp = (180 + analyzer.xaxisdir - Dp_axis) % 360

    return {'Dp': Dp}


def angle_error(computed, expected):
    if computed is None:
        return None
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

    print()
    print("=" * 95)
    print("  三种方案对比 (DFTM)")
    print("=" * 95)

    modes = [
        ('elev3', 'elev×3(当前)'),
        ('slope3', 'elev+slope(3ch)'),
        ('hybrid5', 'elev3+slope2(5ch)'),
    ]

    print(f"\n{'时间':<6s} {'Hs':>5s} {'期望':>4s}", end='')
    for _, label in modes:
        print(f" │ {label:>17s}", end='')
    print()
    print("─" * 95)

    all_errors = {m: [] for m, _ in modes}

    for i, win in enumerate(WINDOWS):
        if prepped[i] is None:
            continue
        eta1, eta2, eta3, r1_mean, n_samples = prepped[i]

        line = f"{win['label']:<6s} {win['Hs']:>4.2f}m {win['expect']:>3d}°"

        for mode, label in modes:
            result = run_analysis(config, eta1, eta2, eta3, r1_mean, n_samples, 'DFTM', mode)
            dp = result.get('Dp')
            if dp is not None:
                err = angle_error(dp, win['expect'])
                all_errors[mode].append(err)
                mark = "✓" if err <= 3 else "✗" if err > 10 else " "
                line += f" │ {dp:>5.0f}° err={err:>4.1f}°{mark}"
            else:
                line += f" │ {'FAIL':>17s}"

        print(line)

    print("─" * 95)
    print(f"{'汇总':>15s}", end='')
    for mode, label in modes:
        errs = all_errors[mode]
        if errs:
            ea = np.array(errs)
            print(f" │ avg={np.mean(ea):.1f}° ≤3°:{np.sum(ea<=3)}/{len(ea)}", end='')
        else:
            print(f" │ {'N/A':>17s}", end='')
    print()

    # 也试试 IMLM
    print()
    print("=" * 95)
    print("  三种方案对比 (IMLM)")
    print("=" * 95)

    print(f"\n{'时间':<6s} {'Hs':>5s} {'期望':>4s}", end='')
    for _, label in modes:
        print(f" │ {label:>17s}", end='')
    print()
    print("─" * 95)

    all_errors2 = {m: [] for m, _ in modes}

    for i, win in enumerate(WINDOWS):
        if prepped[i] is None:
            continue
        eta1, eta2, eta3, r1_mean, n_samples = prepped[i]

        line = f"{win['label']:<6s} {win['Hs']:>4.2f}m {win['expect']:>3d}°"

        for mode, label in modes:
            result = run_analysis(config, eta1, eta2, eta3, r1_mean, n_samples, 'IMLM', mode)
            dp = result.get('Dp')
            if dp is not None:
                err = angle_error(dp, win['expect'])
                all_errors2[mode].append(err)
                mark = "✓" if err <= 3 else "✗" if err > 10 else " "
                line += f" │ {dp:>5.0f}° err={err:>4.1f}°{mark}"
            else:
                line += f" │ {'FAIL':>17s}"

        print(line)

    print("─" * 95)
    print(f"{'汇总':>15s}", end='')
    for mode, label in modes:
        errs = all_errors2[mode]
        if errs:
            ea = np.array(errs)
            print(f" │ avg={np.mean(ea):.1f}° ≤3°:{np.sum(ea<=3)}/{len(ea)}", end='')
        else:
            print(f" │ {'N/A':>17s}", end='')
    print()
    print()


if __name__ == '__main__':
    main()
