#!/usr/bin/env python3
"""
实现 SWAP (Standard Wave Analysis Package) 的 Fourier 系数波向估算方法
这是 RADAC WaveGuide 5 Direction 使用的荷兰标准方法

原理:
  从 z(高程), x(东西斜率), y(南北斜率) 三个信号
  计算互谱的 Fourier 系数 A1, B1
  波向 = atan2(-A1, -B1)
"""
import sys, os, yaml, logging, warnings
import numpy as np
import psycopg2
from scipy.signal import detrend, butter, filtfilt, csd, welch
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


def prepare_signals(raw_data, config):
    """准备 z(η), slope_x, slope_y 三个信号"""
    from directional_spectrum import DirectionalSpectrumAnalyzer

    t1 = raw_data[1]['epochs']
    d1 = raw_data[1]['distances'].copy()
    d2 = np.interp(t1, raw_data[2]['epochs'], raw_data[2]['distances'])
    d3 = np.interp(t1, raw_data[3]['epochs'], raw_data[3]['distances'])

    # R1参考滤波
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

    # 倾斜校正
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
    eff = analyzer.effective_positions

    eta2_cor = eta2 * analyzer.tilt_factors['R2']
    eta3_cor = eta3 * analyzer.tilt_factors['R3']

    # 带通滤波
    band = config['analysis'].get('filter_band', [0.04, 1.0])
    b, a = butter(4, band, btype='band', fs=fs)
    f_low = max(band[0], 0.01)
    padlen = min(3 * int(fs / f_low), ml - 1)
    eta1_f = filtfilt(b, a, detrend(eta1), padlen=padlen)
    eta2_f = filtfilt(b, a, detrend(eta2_cor), padlen=padlen)
    eta3_f = filtfilt(b, a, detrend(eta3_cor), padlen=padlen)

    # 从三路η导出斜率
    # 阵列坐标: x=右方, y=前方(朝向66°真北)
    dx12 = eff['R2'][0] - eff['R1'][0]
    dy12 = eff['R2'][1] - eff['R1'][1]
    dx13 = eff['R3'][0] - eff['R1'][0]
    dy13 = eff['R3'][1] - eff['R1'][1]
    det_A = dx12 * dy13 - dx13 * dy12

    deta2 = eta2_f - eta1_f
    deta3 = eta3_f - eta1_f

    # 阵列坐标系下的斜率
    slope_array_x = (dy13 * deta2 - dy12 * deta3) / det_A
    slope_array_y = (-dx13 * deta2 + dx12 * deta3) / det_A

    # 转换到地理坐标系 (东=x_geo, 北=y_geo)
    # 阵列 y轴 = 朝向66°(从北顺时针), 阵列 x轴 = 朝向156°
    heading_rad = np.radians(config['radar'].get('array_heading', 0.0))
    # 旋转矩阵: 阵列坐标 → 地理坐标
    #   geo_x(东) = array_x * cos(heading+90) - array_y * sin(heading+90)
    #             = -array_x * sin(heading) - array_y * cos(heading)  ... 不对
    # 更简单: 阵列y轴 = compass heading, 阵列x轴 = heading + 90°
    # 地理东 = 阵列x * sin(heading+90°) + 阵列y * sin(heading)
    # 地理北 = 阵列x * cos(heading+90°) + 阵列y * cos(heading)
    xaxis_compass = np.radians(config['radar'].get('array_heading', 0.0) + 90)
    yaxis_compass = np.radians(config['radar'].get('array_heading', 0.0))

    # compass → math angle: math = 90° - compass
    # 但对于旋转矩阵，直接用罗盘角度:
    # geo_east  = array_x * sin(xaxis_compass) + array_y * sin(yaxis_compass)
    # geo_north = array_x * cos(xaxis_compass) + array_y * cos(yaxis_compass)
    slope_east = slope_array_x * np.sin(xaxis_compass) + slope_array_y * np.sin(yaxis_compass)
    slope_north = slope_array_x * np.cos(xaxis_compass) + slope_array_y * np.cos(yaxis_compass)

    return eta1_f, slope_east, slope_north, ml, fs


def swap_direction(z, x, y, fs, nperseg=256):
    """
    SWAP Fourier系数法求波向
    z = 垂直位移 (η)
    x = 东西方向水平位移/斜率
    y = 南北方向水平位移/斜率

    返回: 能量加权平均波向 (罗盘来向, °真北)
    """
    # 自谱和互谱
    f, Czz = welch(z, fs=fs, nperseg=nperseg)
    f, Cxx = welch(x, fs=fs, nperseg=nperseg)
    f, Cyy = welch(y, fs=fs, nperseg=nperseg)

    # scipy.csd 返回 Pxy, 其中 Pxy = conj(FFT_x) * FFT_y
    # SWAP 的 Qzx = Im(FZ* · FX), Czx的co = Re(FZ* · FX)
    f, Pzx = csd(z, x, fs=fs, nperseg=nperseg)
    f, Pzy = csd(z, y, fs=fs, nperseg=nperseg)

    # SWAP定义: Qzx(f) = Im(conj(FZ) * FX)
    # scipy csd: Pzx = conj(FFT_z) * FFT_x / norm
    # 所以 Qzx = Im(Pzx), 但SWAP定义quad = Im(FZ*)FX - Im(FX)FZ*...
    # 实际上 scipy csd 的虚部就是 quad spectrum
    Qzx = np.imag(Pzx)
    Qzy = np.imag(Pzy)

    # 波数估算 (SWAP式: W = sqrt((Cxx+Cyy)/Czz))
    W = np.sqrt(np.abs((Cxx + Cyy) / (Czz + 1e-30)))

    # Fourier 系数
    A1 = Qzx / (W * Czz + 1e-30)
    B1 = Qzy / (W * Czz + 1e-30)

    # 有效频率范围
    valid = (f >= 0.04) & (f <= 1.0) & (Czz > 0)

    # 逐频率波向 (SWAP罗盘来向定义)
    theta_f = np.degrees(np.arctan2(-A1, -B1)) % 360

    # 方法1: 能量加权平均方向 (圆周平均)
    energy = Czz[valid]
    total_energy = np.sum(energy)
    if total_energy == 0:
        return None, None, None

    dirs_rad = np.radians(theta_f[valid])
    weighted_sin = np.sum(energy * np.sin(dirs_rad)) / total_energy
    weighted_cos = np.sum(energy * np.cos(dirs_rad)) / total_energy
    mean_dir = np.degrees(np.arctan2(weighted_sin, weighted_cos)) % 360

    # 方法2: 峰值频率处的波向
    peak_idx = np.argmax(Czz[valid])
    peak_dir = theta_f[valid][peak_idx]
    fp = f[valid][peak_idx]

    # 方法3: 峰值频率附近±20%加权平均
    f_valid = f[valid]
    fp_lo = fp * 0.8
    fp_hi = fp * 1.2
    peak_band = (f_valid >= fp_lo) & (f_valid <= fp_hi)
    if np.any(peak_band):
        e_band = energy[peak_band]
        d_band = dirs_rad[peak_band]
        ws = np.sum(e_band * np.sin(d_band)) / np.sum(e_band)
        wc = np.sum(e_band * np.cos(d_band)) / np.sum(e_band)
        peak_band_dir = np.degrees(np.arctan2(ws, wc)) % 360
    else:
        peak_band_dir = peak_dir

    return mean_dir, peak_dir, peak_band_dir


def angle_error(computed, expected):
    if computed is None:
        return None
    d1 = abs((computed - expected + 180) % 360 - 180)
    d2 = abs((computed - (expected + 180) % 360 + 180) % 360 - 180)
    return min(d1, d2)


def main():
    config = load_config()

    print()
    print("=" * 100)
    print("  SWAP Fourier系数法 vs DFTM 波向精度对比")
    print(f"  array_heading = {config['radar'].get('array_heading')}°  |  目标: ±3°")
    print("=" * 100)
    print()
    print(f"{'时间':<6s} {'H1/3':>6s} {'期望':>4s} │ {'SWAP能量加权':>12s} {'误差':>4s} │ "
          f"{'SWAP峰值':>8s} {'误差':>4s} │ {'SWAP峰值带':>9s} {'误差':>4s} │ {'DFTM':>5s} {'误差':>4s}")
    print("─" * 100)

    errs_mean, errs_peak, errs_band, errs_dftm = [], [], [], []

    for win in WINDOWS:
        raw = fetch_data(win['start'], win['end'])
        if raw is None:
            print(f"{win['label']:<6s}  数据不足")
            continue

        z, sx, sy, n_samples, fs = prepare_signals(raw, config)

        # SWAP法（不同nperseg）
        for nperseg in [128, 256]:
            dir_mean, dir_peak, dir_band = swap_direction(z, sx, sy, fs, nperseg=nperseg)

            if nperseg == 256:
                if dir_mean is not None:
                    errs_mean.append(angle_error(dir_mean, win['expect']))
                if dir_peak is not None:
                    errs_peak.append(angle_error(dir_peak, win['expect']))
                if dir_band is not None:
                    errs_band.append(angle_error(dir_band, win['expect']))

        # DFTM参考
        from directional_spectrum import DirectionalSpectrumAnalyzer
        from pydiwasp import dirspec
        DirectionalSpectrumAnalyzer._last_Dp = None
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
        r1_mean = float(np.mean(raw[1]['distances']))
        analyzer.update_layout(r1_mean)

        # 重新准备eta用于DFTM
        t1 = raw[1]['epochs']
        d1_r = raw[1]['distances'].copy()
        d2_r = np.interp(t1, raw[2]['epochs'], raw[2]['distances'])
        d3_r = np.interp(t1, raw[3]['epochs'], raw[3]['distances'])
        r1_thresh = config['analysis'].get('r1_ref_threshold', 0.15)
        sp2 = np.abs(d2_r - d1_r) > r1_thresh
        sp3 = np.abs(d3_r - d1_r) > r1_thresh
        if np.any(sp2): d2_r[sp2] = d1_r[sp2]
        if np.any(sp3): d3_r[sp3] = d1_r[sp3]
        for d in [d1_r, d2_r, d3_r]:
            ah = config['radar'].get('array_height', 4.6)
            bad = (d < 0.3) | (d > ah + 0.5)
            if np.any(bad):
                med = np.median(d[~bad]) if np.any(~bad) else np.median(d)
                d[bad] = med
        t_rel = t1 - t1[0]
        dur = t_rel[-1]
        t_uni = np.arange(0, dur, 1.0/6.0)
        def to_eta(d):
            return -(d - np.median(d))
        e1 = interp1d(t_rel, to_eta(d1_r), fill_value='extrapolate')(t_uni)
        e2 = interp1d(t_rel, to_eta(d2_r), fill_value='extrapolate')(t_uni)
        e3 = interp1d(t_rel, to_eta(d3_r), fill_value='extrapolate')(t_uni)
        ml = min(len(e1), len(e2), len(e3))
        e1, e2, e3 = e1[:ml], e2[:ml], e3[:ml]
        band = [0.04, 1.0]
        bf, af = butter(4, band, btype='band', fs=6.0)
        padlen = min(3 * int(6.0 / 0.04), ml - 1)
        e1f = filtfilt(bf, af, detrend(e1), padlen=padlen)
        e2f = filtfilt(bf, af, detrend(e2 * analyzer.tilt_factors['R2']), padlen=padlen)
        e3f = filtfilt(bf, af, detrend(e3 * analyzer.tilt_factors['R3']), padlen=padlen)
        data_m = np.column_stack([e1f, e2f, e3f])
        ID = {'data': data_m, 'layout': analyzer.layout,
              'datatypes': np.array(['elev','elev','elev']),
              'depth': 100.0, 'fs': 6.0}
        freqs = np.linspace(0.04, 1.0, 128)
        dirs = np.linspace(0, 360, 361)[:-1]
        SM = {'freqs': freqs, 'dirs': np.radians(dirs), 'funit': 'Hz', 'dunit': 'rad',
              'xaxisdir': analyzer.xaxisdir}
        nfft = max(64, int(2 ** np.floor(np.log2(min(256, ml // 4)))))
        EP = {'method': 'DFTM', 'nfft': nfft, 'dres': 360, 'iter': 100, 'smooth': 'ON'}
        logging.disable(logging.CRITICAL)
        try:
            SMout, _ = dirspec(ID, SM, EP, ['MESSAGE', 0, 'PLOTTYPE', 0])
            S = np.real(SMout['S'])
            dirs_out = np.degrees(SMout['dirs'])
            dir_sp = np.sum(S, axis=0)
            dp_axis = dirs_out[np.argmax(dir_sp)]
            dp_dftm = (180 + analyzer.xaxisdir - dp_axis) % 360
            errs_dftm.append(angle_error(dp_dftm, win['expect']))
        except:
            dp_dftm = None
        finally:
            logging.disable(logging.NOTSET)

        # 输出
        def fmt(d, e):
            if d is None: return "N/A", "N/A"
            mark = "✓" if e <= 3 else "✗" if e > 10 else " "
            return f"{d:.0f}°", f"{e:.1f}°{mark}"

        em = angle_error(dir_mean, win['expect'])
        ep = angle_error(dir_peak, win['expect'])
        eb = angle_error(dir_band, win['expect'])
        ed = angle_error(dp_dftm, win['expect']) if dp_dftm else None

        dm, ems = fmt(dir_mean, em) if em else ("N/A", "N/A")
        dp, eps = fmt(dir_peak, ep) if ep else ("N/A", "N/A")
        db, ebs = fmt(dir_band, eb) if eb else ("N/A", "N/A")
        dd, eds = fmt(dp_dftm, ed) if ed else ("N/A", "N/A")

        print(f"{win['label']:<6s} {win['h13']:>5.3f}m {win['expect']:>3d}° │ "
              f"{dm:>12s} {ems:>5s} │ {dp:>8s} {eps:>5s} │ {db:>9s} {ebs:>5s} │ {dd:>5s} {eds:>5s}")

    print("─" * 100)
    for name, errs in [('SWAP能量加权', errs_mean), ('SWAP峰值', errs_peak),
                       ('SWAP峰值带', errs_band), ('DFTM', errs_dftm)]:
        if errs:
            ea = np.array(errs)
            print(f"  {name:<12s}: avg={np.mean(ea):.1f}°  max={np.max(ea):.1f}°  "
                  f"≤3°:{np.sum(ea<=3)}/{len(ea)}  ≤5°:{np.sum(ea<=5)}/{len(ea)}")

    print()


if __name__ == '__main__':
    main()
