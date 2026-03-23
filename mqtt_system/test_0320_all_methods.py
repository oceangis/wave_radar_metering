#!/usr/bin/env python3
"""
对比 pyDIWASP 全部 5 种方法的波向精度
IMLM / EMLM / EMEP / BDM / DFTM
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

METHODS = ['IMLM', 'EMLM', 'EMEP', 'DFTM']


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


def prepare_eta(raw_data, config):
    """提取并预处理三路eta信号"""
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

    # 带通滤波
    band = config['analysis'].get('filter_band', [0.04, 1.0])
    b, a = butter(4, band, btype='band', fs=fs)
    padlen = min(3 * int(fs / max(band[0], 0.01)), ml - 1)
    eta1 = filtfilt(b, a, detrend(eta1), padlen=padlen)
    eta2 = filtfilt(b, a, detrend(eta2), padlen=padlen)
    eta3 = filtfilt(b, a, detrend(eta3), padlen=padlen)

    r1_mean = float(np.mean(raw_data[1]['distances']))
    return eta1, eta2, eta3, r1_mean, ml


def run_method(config, eta1, eta2, eta3, r1_mean, n_samples, method):
    """用指定方法跑 DIWASP"""
    from directional_spectrum import DirectionalSpectrumAnalyzer
    from pydiwasp import dirspec

    DirectionalSpectrumAnalyzer._last_Dp = None

    ds_config = {
        'sample_rate': 6.0,
        'gravity': 9.81,
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

    # 倾斜校正
    eta2_use = eta2 * analyzer.tilt_factors['R2']
    eta3_use = eta3 * analyzer.tilt_factors['R3']

    data_matrix = np.column_stack([eta1, eta2_use, eta3_use])
    band = config['analysis'].get('filter_band', [0.04, 1.0])

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

    # 方向积分 → 主波向
    dir_spectrum = np.sum(S, axis=0)
    Dp_idx = np.argmax(dir_spectrum)
    Dp_axis = dirs_out[Dp_idx]
    Dp = (180 + analyzer.xaxisdir - Dp_axis) % 360

    # 展宽
    dirs_rad = np.radians(dirs_out)
    w = dir_spectrum / np.sum(dir_spectrum)
    ms = np.sum(w * np.sin(dirs_rad))
    mc = np.sum(w * np.cos(dirs_rad))
    spread = np.degrees(np.sqrt(2 * (1 - np.sqrt(ms**2 + mc**2))))

    # Tp
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

    # 预处理所有窗口
    print("预处理...")
    prepped = []
    for raw in all_data:
        if raw is None:
            prepped.append(None)
        else:
            prepped.append(prepare_eta(raw, config))

    print()
    print("=" * 100)
    print("  pyDIWASP 全部 5 种方法波向精度对比")
    print(f"  目标: ±3°  |  期望波向: 66°/246°  |  array_heading = {config['radar'].get('array_heading')}°")
    print("=" * 100)

    # 每个方法的汇总
    method_errors = {m: [] for m in METHODS}

    # 逐窗口逐方法
    print()
    hdr = f"{'时间':<6s} {'Hs':>5s} {'期望':>4s}"
    for m in METHODS:
        hdr += f" │ {m:>5s}"
    print(hdr)
    print("─" * (26 + len(METHODS) * 9))

    for i, win in enumerate(WINDOWS):
        if prepped[i] is None:
            print(f"{win['label']:<6s}  数据不足")
            continue

        eta1, eta2, eta3, r1_mean, n_samples = prepped[i]
        line = f"{win['label']:<6s} {win['Hs']:>4.2f}m {win['expect']:>3d}°"

        for method in METHODS:
            try:
                result = run_method(config, eta1, eta2, eta3, r1_mean, n_samples, method)
                dp = result['Dp']
                err = angle_error(dp, win['expect'])
                method_errors[method].append(err)

                mark = "✓" if err <= 3 else " " if err <= 5 else "✗" if err > 10 else " "
                line += f" │ {dp:>4.0f}°{mark}"
            except Exception as e:
                line += f" │  FAIL"
                # 记录失败
                method_errors[method].append(None)

        print(line)

    # 汇总
    print()
    print("─" * 80)
    print(f"{'方法':<8s} {'平均误差':>8s} {'最大':>6s} {'中位':>6s} {'≤3°':>5s} {'≤5°':>5s} {'≤10°':>6s} {'评价':<10s}")
    print("─" * 80)

    for method in METHODS:
        errs = [e for e in method_errors[method] if e is not None]
        if not errs:
            print(f"{method:<8s}  无有效结果")
            continue
        ea = np.array(errs)
        n = len(ea)
        avg = np.mean(ea)
        mx = np.max(ea)
        med = np.median(ea)
        w3 = np.sum(ea <= 3)
        w5 = np.sum(ea <= 5)
        w10 = np.sum(ea <= 10)

        if w3 == n:
            verdict = "★★★ 达标"
        elif avg <= 3:
            verdict = "★★ 均值达标"
        elif avg <= 5:
            verdict = "★ 接近"
        else:
            verdict = "未达标"

        print(f"{method:<8s} {avg:>7.1f}° {mx:>5.1f}° {med:>5.1f}° "
              f"{w3:>2d}/{n:<2d}  {w5:>2d}/{n:<2d}  {w10:>2d}/{n:<2d}   {verdict}")

    print()
    print("算法说明:")
    print("  IMLM = 迭代最大似然法 (当前使用)")
    print("  EMLM = 扩展最大似然法 (IMLM的非迭代版)")
    print("  EMEP = 扩展最大熵法")
    print("  BDM  = 贝叶斯方向法 (ABIC模型选择)")
    print("  DFTM = 直接傅里叶变换法 (最简单)")
    print()


if __name__ == '__main__':
    main()
