#!/usr/bin/env python3
"""
三时段波浪分析: 时域 + 频域
时段: 9:40-9:45, 9:55-10:00, 10:29-10:34 (2026-02-10)
"""

import numpy as np
import psycopg2
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'wave_monitoring',
    'user': 'wave_user',
    'password': 'wave2025',
}

PERIODS = [
    ("9:40-9:45",  "2026-02-10 09:40:00+08", "2026-02-10 09:45:00+08"),
    ("9:55-10:00", "2026-02-10 09:55:00+08", "2026-02-10 10:00:00+08"),
    ("10:29-10:34","2026-02-10 10:29:00+08", "2026-02-10 10:34:00+08"),
]

# 雷达安装高度(m)
RADAR_HEIGHT = 10.0


def fetch_data(conn, t_start, t_end, radar_id=1):
    """从数据库获取单个雷达的数据"""
    sql = """
        SELECT timestamp, distance
        FROM wave_measurements
        WHERE radar_id = %s
          AND timestamp >= %s
          AND timestamp < %s
        ORDER BY timestamp
    """
    cur = conn.cursor()
    cur.execute(sql, (radar_id, t_start, t_end))
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return None, None
    timestamps = np.array([r[0].timestamp() for r in rows])
    distances = np.array([r[1] for r in rows], dtype=float)

    # 质量控制: 去除异常值 (3σ准则)
    mean_d = np.mean(distances)
    std_d = np.std(distances)
    mask = np.abs(distances - mean_d) < 3 * std_d
    # 同时去除物理不合理值 (距离应在 0~RADAR_HEIGHT 范围附近)
    mask &= (distances > 0) & (distances < RADAR_HEIGHT * 2)
    timestamps = timestamps[mask]
    distances = distances[mask]
    return timestamps, distances


def time_domain_analysis(timestamps, distances):
    """时域分析: 波高、周期等统计量"""
    # 水面高程 = 雷达高度 - 距离
    eta = RADAR_HEIGHT - distances
    # 去均值
    eta_demean = eta - np.mean(eta)

    # --- 零上交叉法 (zero-up-crossing) ---
    wave_heights = []
    wave_periods = []
    crossings = []
    for i in range(len(eta_demean) - 1):
        if eta_demean[i] <= 0 and eta_demean[i + 1] > 0:
            # 线性插值精确交叉时刻
            frac = -eta_demean[i] / (eta_demean[i + 1] - eta_demean[i])
            t_cross = timestamps[i] + frac * (timestamps[i + 1] - timestamps[i])
            crossings.append((i, t_cross))

    for j in range(len(crossings) - 1):
        idx_start = crossings[j][0]
        idx_end = crossings[j + 1][0]
        segment = eta_demean[idx_start:idx_end + 1]
        H = np.max(segment) - np.min(segment)
        T = crossings[j + 1][1] - crossings[j][1]
        wave_heights.append(H)
        wave_periods.append(T)

    wave_heights = np.array(wave_heights)
    wave_periods = np.array(wave_periods)

    if len(wave_heights) == 0:
        return {}

    # 排序（从大到小）
    sorted_H = np.sort(wave_heights)[::-1]
    n = len(sorted_H)
    n13 = max(1, int(np.ceil(n / 3)))
    n10 = max(1, int(np.ceil(n / 10)))

    results = {
        'num_waves': n,
        'Hmax': float(np.max(wave_heights)),
        'Hmean': float(np.mean(wave_heights)),
        'H1/3 (Hs)': float(np.mean(sorted_H[:n13])),
        'H1/10': float(np.mean(sorted_H[:n10])),
        'Hrms': float(np.sqrt(np.mean(wave_heights ** 2))),
        'Tmax': float(wave_periods[np.argmax(wave_heights)]),
        'Tmean': float(np.mean(wave_periods)),
        'T1/3 (Ts)': float(np.mean(wave_periods[np.argsort(wave_heights)[::-1][:n13]])),
        'Tz (零交叉周期)': float(np.mean(wave_periods)),
        'eta_std': float(np.std(eta_demean)),
        'eta_max': float(np.max(eta_demean)),
        'eta_min': float(np.min(eta_demean)),
    }
    return results


def frequency_domain_analysis(timestamps, distances):
    """频域分析: FFT谱 -> 谱参数"""
    eta = RADAR_HEIGHT - distances
    eta_demean = eta - np.mean(eta)

    N = len(eta_demean)
    dt_arr = np.diff(timestamps)
    dt = np.median(dt_arr)
    fs = 1.0 / dt  # 采样频率

    # --- Welch方法估计功率谱密度 ---
    nperseg = min(512, N)
    noverlap = nperseg // 2

    # 手动实现Welch (避免额外依赖)
    step = nperseg - noverlap
    n_segments = (N - nperseg) // step + 1
    window = np.hanning(nperseg)
    win_power = np.mean(window ** 2)

    psd_sum = np.zeros(nperseg // 2 + 1)
    for i in range(n_segments):
        start = i * step
        seg = eta_demean[start:start + nperseg] * window
        fft_seg = np.fft.rfft(seg)
        psd_seg = (np.abs(fft_seg) ** 2) / (fs * nperseg * win_power)
        psd_seg[1:-1] *= 2  # 单边谱
        psd_sum += psd_seg

    psd = psd_sum / n_segments
    freqs = np.fft.rfftfreq(nperseg, d=1.0 / fs)

    # 只取有效波频率范围 (0.04 - 1.0 Hz)
    mask = (freqs >= 0.04) & (freqs <= 1.0)
    f = freqs[mask]
    S = psd[mask]
    df = f[1] - f[0] if len(f) > 1 else 1.0

    # 谱矩
    _trapz = np.trapezoid if hasattr(np, 'trapezoid') else np.trapz
    m0 = _trapz(S, f)
    m1 = _trapz(f * S, f)
    m2 = _trapz(f ** 2 * S, f)
    m4 = _trapz(f ** 4 * S, f)

    # 谱参数
    Hm0 = 4.0 * np.sqrt(m0) if m0 > 0 else 0
    Tp_idx = np.argmax(S)
    fp = f[Tp_idx] if len(f) > 0 else 0
    Tp = 1.0 / fp if fp > 0 else 0
    Tm01 = m0 / m1 if m1 > 0 else 0
    Tm02 = np.sqrt(m0 / m2) if m2 > 0 else 0

    # 谱宽度参数
    epsilon = np.sqrt(1 - m2 ** 2 / (m0 * m4)) if (m0 * m4) > 0 else 0
    nu = np.sqrt(m0 * m2 / m1 ** 2 - 1) if m1 > 0 else 0

    results = {
        'fs (Hz)': float(fs),
        'N_samples': N,
        'Hm0 (谱有效波高)': float(Hm0),
        'Tp (谱峰周期)': float(Tp),
        'fp (谱峰频率)': float(fp),
        'Tm01 (平均周期)': float(Tm01),
        'Tm02 (零交叉周期)': float(Tm02),
        'm0': float(m0),
        'm1': float(m1),
        'm2': float(m2),
        'epsilon (谱宽)': float(epsilon),
        'nu (窄带参数)': float(nu),
    }
    return results


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    print("=" * 72)
    print("  雷达波浪数据分析 —— 2026-02-10 三时段对比")
    print("=" * 72)

    for label, t_start, t_end in PERIODS:
        print(f"\n{'─' * 72}")
        print(f"  时段: {label}")
        print(f"{'─' * 72}")

        timestamps, distances = fetch_data(conn, t_start, t_end, radar_id=1)
        if timestamps is None:
            print("  [无数据]")
            continue

        dt_arr = np.diff(timestamps)
        print(f"  数据点数: {len(timestamps)}, 采样间隔中位数: {np.median(dt_arr)*1000:.1f} ms")
        print(f"  距离范围: {np.min(distances):.4f} ~ {np.max(distances):.4f} m")
        print(f"  平均距离: {np.mean(distances):.4f} m  (平均水面高程: {RADAR_HEIGHT - np.mean(distances):.4f} m)")

        # ======= 时域分析 =======
        td = time_domain_analysis(timestamps, distances)
        print(f"\n  ▌时域分析 (零上交叉法)")
        if td:
            print(f"  {'识别波数':<20s}: {td['num_waves']}")
            print(f"  {'有效波高 H1/3 (Hs)':<20s}: {td['H1/3 (Hs)']:.4f} m")
            print(f"  {'H1/10':<20s}: {td['H1/10']:.4f} m")
            print(f"  {'最大波高 Hmax':<20s}: {td['Hmax']:.4f} m")
            print(f"  {'平均波高 Hmean':<20s}: {td['Hmean']:.4f} m")
            print(f"  {'均方根波高 Hrms':<20s}: {td['Hrms']:.4f} m")
            print(f"  {'有效波周期 T1/3':<20s}: {td['T1/3 (Ts)']:.4f} s")
            print(f"  {'零交叉周期 Tz':<20s}: {td['Tz (零交叉周期)']:.4f} s")
            print(f"  {'平均周期 Tmean':<20s}: {td['Tmean']:.4f} s")
            print(f"  {'最大波对应周期 Tmax':<20s}: {td['Tmax']:.4f} s")
            print(f"  {'水面波动标准差':<20s}: {td['eta_std']:.4f} m")
            print(f"  {'最大波峰':<20s}: {td['eta_max']:.4f} m")
            print(f"  {'最大波谷':<20s}: {td['eta_min']:.4f} m")
        else:
            print("  [未能识别有效波浪]")

        # ======= 频域分析 =======
        fd = frequency_domain_analysis(timestamps, distances)
        print(f"\n  ▌频域分析 (Welch谱估计)")
        print(f"  {'采样频率 fs':<20s}: {fd['fs (Hz)']:.2f} Hz")
        print(f"  {'样本数 N':<20s}: {fd['N_samples']}")
        print(f"  {'谱有效波高 Hm0':<20s}: {fd['Hm0 (谱有效波高)']:.4f} m")
        print(f"  {'谱峰周期 Tp':<20s}: {fd['Tp (谱峰周期)']:.4f} s")
        print(f"  {'谱峰频率 fp':<20s}: {fd['fp (谱峰频率)']:.4f} Hz")
        print(f"  {'平均周期 Tm01':<20s}: {fd['Tm01 (平均周期)']:.4f} s")
        print(f"  {'零交叉周期 Tm02':<20s}: {fd['Tm02 (零交叉周期)']:.4f} s")
        print(f"  {'零阶矩 m0':<20s}: {fd['m0']:.6f} m²")
        print(f"  {'谱宽参数 ε':<20s}: {fd['epsilon (谱宽)']:.4f}")
        print(f"  {'窄带参数 ν':<20s}: {fd['nu (窄带参数)']:.4f}")

    # ======= 三时段对比汇总 =======
    print(f"\n\n{'=' * 72}")
    print("  三时段对比汇总表")
    print(f"{'=' * 72}")
    header = f"  {'参数':<22s} | {'9:40-9:45':>12s} | {'9:55-10:00':>12s} | {'10:29-10:34':>12s}"
    print(header)
    print(f"  {'─' * 22}-+-{'-' * 12}-+-{'-' * 12}-+-{'-' * 12}")

    all_td = []
    all_fd = []
    for label, t_start, t_end in PERIODS:
        ts, dist = fetch_data(conn, t_start, t_end, radar_id=1)
        all_td.append(time_domain_analysis(ts, dist))
        all_fd.append(frequency_domain_analysis(ts, dist))

    def row(name, key, src, fmt=".4f"):
        vals = []
        for d in src:
            v = d.get(key, float('nan'))
            vals.append(f"{v:{fmt}}")
        print(f"  {name:<22s} | {vals[0]:>12s} | {vals[1]:>12s} | {vals[2]:>12s}")

    print("  --- 时域 ---")
    row("Hs (m)", "H1/3 (Hs)", all_td)
    row("H1/10 (m)", "H1/10", all_td)
    row("Hmax (m)", "Hmax", all_td)
    row("Hmean (m)", "Hmean", all_td)
    row("Hrms (m)", "Hrms", all_td)
    row("Ts (s)", "T1/3 (Ts)", all_td)
    row("Tz (s)", "Tz (零交叉周期)", all_td)
    row("Tmean (s)", "Tmean", all_td)
    row("波数", "num_waves", all_td, fmt="d")

    print("  --- 频域 ---")
    row("Hm0 (m)", "Hm0 (谱有效波高)", all_fd)
    row("Tp (s)", "Tp (谱峰周期)", all_fd)
    row("fp (Hz)", "fp (谱峰频率)", all_fd)
    row("Tm01 (s)", "Tm01 (平均周期)", all_fd)
    row("Tm02 (s)", "Tm02 (零交叉周期)", all_fd)
    row("m0 (m²)", "m0", fmt=".6f", src=all_fd)
    row("谱宽 ε", "epsilon (谱宽)", all_fd)
    row("窄带 ν", "nu (窄带参数)", all_fd)

    conn.close()
    print(f"\n{'=' * 72}")
    print("  分析完成")
    print(f"{'=' * 72}")


if __name__ == "__main__":
    main()
