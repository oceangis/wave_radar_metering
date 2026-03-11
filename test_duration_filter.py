#!/usr/bin/env python3
"""
测试：基于持续时间的尖峰判据
多径尖峰特征：持续1-3点，突然跳到远处再跳回
真实波峰特征：在7.5Hz下持续几十~上百点，变化连续
"""
import sys
import numpy as np
import psycopg2
import yaml
from scipy.interpolate import interp1d
from scipy.signal import welch

sys.path.insert(0, '/home/pi/radar/mqtt_system/services')
if not hasattr(np, 'trapz'):
    np.trapz = np.trapezoid

config_path = '/home/pi/radar/mqtt_system/config/system_config.yaml'
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

DB_CONFIG = {
    'host': 'localhost', 'port': 5432,
    'database': 'wave_monitoring', 'user': 'wave_user', 'password': 'wave2025',
}

PERIODS = [
    ("9:40-9:45",   "2026-02-10 09:40:00+08", "2026-02-10 09:45:00+08", 0.1, 2.0),
    ("10:04-10:09", "2026-02-10 10:04:00+08", "2026-02-10 10:09:00+08", 0.2, 3.0),
    ("10:28-10:33", "2026-02-10 10:28:00+08", "2026-02-10 10:33:00+08", 0.3, 4.0),
]


def fetch_r1(conn, t_start, t_end):
    cur = conn.cursor()
    cur.execute("""
        SELECT timestamp, distance FROM wave_measurements
        WHERE radar_id=1 AND timestamp >= %s AND timestamp < %s
        ORDER BY timestamp
    """, (t_start, t_end))
    rows = cur.fetchall()
    cur.close()
    timestamps = [r[0].isoformat() for r in rows]
    distances = np.array([r[1] for r in rows], dtype=float)
    t_epoch = np.array([r[0].timestamp() for r in rows])
    return timestamps, distances, t_epoch


# ============================================================
# 原版去尖刺（与项目 _prepare_wave_data 一致）
# ============================================================
def spike_removal_original(distances):
    d = distances.copy()

    q25, q75 = np.percentile(d, [25, 75])
    iqr = q75 - q25
    if iqr < 0.001:
        iqr = 0.001
    lower = q25 - 3.0 * iqr
    upper = q75 + 3.0 * iqr
    spike_iqr = (d < lower) | (d > upper)

    d_temp = d.copy()
    if np.any(spike_iqr):
        good_temp = ~spike_iqr
        if np.sum(good_temp) > 10:
            d_temp[spike_iqr] = np.interp(
                np.where(spike_iqr)[0],
                np.where(good_temp)[0],
                d_temp[good_temp]
            )
    diff = np.abs(np.diff(d_temp))
    spike_jump_fwd = np.concatenate(([False], diff > 0.2))
    spike_jump_bwd = np.concatenate((diff > 0.2, [False]))
    spike_mask = spike_iqr | spike_jump_fwd | spike_jump_bwd

    dist_clean = d.copy()
    n_spikes = int(np.sum(spike_mask))
    if n_spikes > 0:
        good = ~spike_mask
        if np.any(good):
            dist_clean[spike_mask] = np.interp(
                np.where(spike_mask)[0],
                np.where(good)[0],
                dist_clean[good]
            )
    return dist_clean, spike_mask, n_spikes


# ============================================================
# 改进版：加入持续时间判据
# ============================================================
def spike_removal_duration(distances, min_duration=5, near_margin_iqr=1.5):
    """
    改进的去尖刺算法（持续时间 + 幅度联合判据）：

    1. IQR 标记候选异常点
    2. 区分"近边界"和"远离信号"：
       - 近边界：偏离 IQR 上/下界 < near_margin_iqr * IQR → 可能是真实波峰
       - 远离信号：偏离 >> IQR → 一定是多径/虚假回波
    3. 对近边界的候选点，检查持续时间：
       - 持续 >= min_duration 且变化连续 → 救回（真实波峰）
       - 持续 < min_duration → 剔除
    4. 远离信号的点：无论持续多久都剔除（多径也会持续很多点）
    """
    d = distances.copy()

    # Step 1: IQR 检测
    q25, q75 = np.percentile(d, [25, 75])
    iqr = q75 - q25
    if iqr < 0.001:
        iqr = 0.001
    lower = q25 - 3.0 * iqr
    upper = q75 + 3.0 * iqr
    candidate = (d < lower) | (d > upper)

    # Step 2: 区分近边界 vs 远离信号
    margin = near_margin_iqr * iqr
    near_boundary = candidate & (
        ((d > upper) & (d <= upper + margin)) |
        ((d < lower) & (d >= lower - margin))
    )
    far_away = candidate & ~near_boundary  # 远离信号的一定剔除

    # Step 3: 对近边界点做持续时间+连续性检查
    spike_iqr = far_away.copy()  # 远离的直接标记为spike
    n_rescued = 0

    i = 0
    while i < len(d):
        if near_boundary[i]:
            j = i
            while j < len(d) and near_boundary[j]:
                j += 1
            run_length = j - i

            if run_length >= min_duration:
                # 检查连续性：段内逐点变化是否平滑
                seg = d[i:j]
                max_jump = np.max(np.abs(np.diff(seg))) if len(seg) > 1 else 0
                if max_jump < 0.1:  # 段内逐点变化<100mm，视为连续
                    n_rescued += run_length
                    # 不标记为spike → 保留
                else:
                    spike_iqr[i:j] = True  # 段内不连续，仍然剔除
            else:
                spike_iqr[i:j] = True  # 短脉冲，剔除
            i = j
        else:
            i += 1

    # Step 4: 跳变检测
    d_temp = d.copy()
    if np.any(spike_iqr):
        good_temp = ~spike_iqr
        if np.sum(good_temp) > 10:
            d_temp[spike_iqr] = np.interp(
                np.where(spike_iqr)[0],
                np.where(good_temp)[0],
                d_temp[good_temp]
            )
    diff = np.abs(np.diff(d_temp))
    spike_jump_fwd = np.concatenate(([False], diff > 0.2))
    spike_jump_bwd = np.concatenate((diff > 0.2, [False]))
    spike_mask = spike_iqr | spike_jump_fwd | spike_jump_bwd

    # 插值替换
    dist_clean = d.copy()
    n_spikes = int(np.sum(spike_mask))
    if n_spikes > 0:
        good = ~spike_mask
        if np.any(good):
            dist_clean[spike_mask] = np.interp(
                np.where(spike_mask)[0],
                np.where(good)[0],
                dist_clean[good]
            )

    return dist_clean, spike_mask, n_spikes, n_rescued


# ============================================================
# 完整分析流程（复用项目逻辑）
# ============================================================
def do_analysis(distances, t_epoch, fs_target=6.0):
    """距离→η→重采样→Welch+零交叉"""
    mean_dist = np.mean(distances)
    eta = -(distances - mean_dist)

    t_rel = t_epoch - t_epoch[0]
    duration = t_rel[-1]

    # 重采样到 6Hz
    t_uniform = np.arange(0, duration, 1.0 / fs_target)
    if len(t_uniform) > 1 and len(t_rel) > 1:
        eta_resampled = interp1d(t_rel, eta, kind='linear',
                                  fill_value='extrapolate')(t_uniform)
    else:
        eta_resampled = eta

    # 带通滤波（与项目一致）
    from scipy.signal import butter, filtfilt, detrend
    eta_r = detrend(eta_resampled)
    band = config['analysis']['filter_band']
    b, a = butter(4, band, btype='band', fs=fs_target)
    try:
        eta_filtered = filtfilt(b, a, eta_r,
                                padlen=min(3*(max(len(b),len(a))-1), len(eta_r)-1))
    except Exception:
        eta_filtered = eta_r

    # Welch
    nperseg = min(config['analysis']['nperseg'], len(eta_filtered) // 4)
    f, S = welch(eta_filtered, fs=fs_target, nperseg=nperseg)
    valid = f > 0
    m0 = np.trapz(S, f)
    Hm0 = 4.0 * np.sqrt(m0) if m0 > 0 else 0
    peak_idx = np.argmax(S[1:]) + 1
    fp = f[peak_idx]
    Tp = 1.0 / fp if fp > 0 else 0

    # 零交叉法（下穿零点，与项目一致）
    zero_crossings = []
    for i in range(len(eta_filtered) - 1):
        if eta_filtered[i] >= 0 and eta_filtered[i+1] < 0:
            zero_crossings.append(i)

    wave_heights = []
    wave_periods = []
    for i in range(len(zero_crossings) - 1):
        si = zero_crossings[i]
        ei = zero_crossings[i+1]
        if ei - si < 2:
            continue
        seg = eta_filtered[si:ei]
        H = np.max(seg) - np.min(seg)
        T = (ei - si) / fs_target
        if H > 0 and T > 0:
            wave_heights.append(H)
            wave_periods.append(T)

    if len(wave_heights) > 0:
        wh = np.array(wave_heights)
        wp = np.array(wave_periods)
        idx = np.argsort(wh)[::-1]
        n13 = max(1, len(wh) // 3)
        Hs = float(np.mean(wh[idx[:n13]]))
        Ts = float(np.mean(wp[idx[:n13]]))
        Hmean = float(np.mean(wh))
        Hmax = float(wh[idx[0]])
    else:
        Hs = Ts = Hmean = Hmax = 0

    return {
        'Hm0': Hm0, 'Tp': Tp,
        'Hs': Hs, 'Ts': Ts,
        'Hmean': Hmean, 'Hmax': Hmax,
        'wave_count': len(wave_heights),
        'eta_std': np.std(eta_filtered),
    }


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    print("=" * 85)
    print("  持续时间判据测试：原版 vs 改进版 (min_duration=5)")
    print("=" * 85)

    for label, t_start, t_end, H_nom, T_nom in PERIODS:
        ts_list, distances, t_epoch = fetch_r1(conn, t_start, t_end)

        # 原版
        clean_orig, mask_orig, n_orig = spike_removal_original(distances)
        res_orig = do_analysis(clean_orig, t_epoch)

        # 改进版
        clean_new, mask_new, n_new, n_rescued = spike_removal_duration(distances, min_duration=5)
        res_new = do_analysis(clean_new, t_epoch)

        print(f"\n{'─' * 85}")
        print(f"  {label}  (标称 H={H_nom}m, T={T_nom}s)")
        print(f"{'─' * 85}")

        print(f"\n  ▌去尖刺对比")
        print(f"  {'':30s} {'原版':>12s}   {'改进版':>12s}")
        print(f"  {'剔除点数':<30s} {n_orig:>12d}   {n_new:>12d}")
        print(f"  {'被救回点数(持续>=5)':<30s} {'—':>12s}   {n_rescued:>12d}")
        print(f"  {'剔除比例':<30s} {n_orig/len(distances)*100:>11.1f}%   {n_new/len(distances)*100:>11.1f}%")
        print(f"  {'清洗后距离range':<30s} {np.ptp(clean_orig)*1000:>10.1f}mm   {np.ptp(clean_new)*1000:>10.1f}mm")
        print(f"  {'清洗后η std':<30s} {np.std(-(clean_orig-np.mean(clean_orig)))*1000:>10.1f}mm   "
              f"{np.std(-(clean_new-np.mean(clean_new)))*1000:>10.1f}mm")

        print(f"\n  ▌分析结果对比")
        print(f"  {'':30s} {'原版':>12s}   {'改进版':>12s}   {'标称':>8s}")
        print(f"  {'Hm0':<30s} {res_orig['Hm0']*1000:>10.1f}mm   {res_new['Hm0']*1000:>10.1f}mm   {H_nom*1000:>6.0f}mm")
        print(f"  {'Hs (零交叉)':<30s} {res_orig['Hs']*1000:>10.1f}mm   {res_new['Hs']*1000:>10.1f}mm   {H_nom*1000:>6.0f}mm")
        print(f"  {'Hmean':<30s} {res_orig['Hmean']*1000:>10.1f}mm   {res_new['Hmean']*1000:>10.1f}mm")
        print(f"  {'Hmax':<30s} {res_orig['Hmax']*1000:>10.1f}mm   {res_new['Hmax']*1000:>10.1f}mm")
        print(f"  {'Tp':<30s} {res_orig['Tp']:>10.2f}s    {res_new['Tp']:>10.2f}s    {T_nom:>6.1f}s")
        print(f"  {'Ts':<30s} {res_orig['Ts']:>10.2f}s    {res_new['Ts']:>10.2f}s    {T_nom:>6.1f}s")
        print(f"  {'波数':<30s} {res_orig['wave_count']:>12d}   {res_new['wave_count']:>12d}")

        print(f"\n  ▌误差对比")
        def err(val, nom):
            return (val - nom) / nom * 100 if nom > 0 else 0
        print(f"  {'':30s} {'原版':>12s}   {'改进版':>12s}")
        print(f"  {'Hm0 误差':<30s} {err(res_orig['Hm0'], H_nom):>+11.1f}%   {err(res_new['Hm0'], H_nom):>+11.1f}%")
        print(f"  {'Hs  误差':<30s} {err(res_orig['Hs'], H_nom):>+11.1f}%   {err(res_new['Hs'], H_nom):>+11.1f}%")
        print(f"  {'Tp  误差':<30s} {err(res_orig['Tp'], T_nom):>+11.1f}%   {err(res_new['Tp'], T_nom):>+11.1f}%")
        print(f"  {'Ts  误差':<30s} {err(res_orig['Ts'], T_nom):>+11.1f}%   {err(res_new['Ts'], T_nom):>+11.1f}%")

    # 汇总
    print(f"\n\n{'=' * 85}")
    print("  汇总对比")
    print(f"{'=' * 85}")
    print(f"\n  {'时段':<14s} │ {'标称':>5s} │ {'Hs原版':>8s} {'误差':>7s} │ {'Hs改进':>8s} {'误差':>7s} │ {'改善':>6s}")
    print(f"  {'─'*14} ┼ {'─'*6} ┼ {'─'*17} ┼ {'─'*17} ┼ {'─'*7}")

    for label, t_start, t_end, H_nom, T_nom in PERIODS:
        ts_list, distances, t_epoch = fetch_r1(conn, t_start, t_end)
        clean_orig, _, _ = spike_removal_original(distances)
        clean_new, _, _, _ = spike_removal_duration(distances, min_duration=5)
        ro = do_analysis(clean_orig, t_epoch)
        rn = do_analysis(clean_new, t_epoch)
        eo = (ro['Hs'] - H_nom) / H_nom * 100
        en = (rn['Hs'] - H_nom) / H_nom * 100
        improve = abs(eo) - abs(en)
        print(f"  {label:<14s} │ {H_nom:>4.1f}m │ {ro['Hs']:.3f}m {eo:>+6.1f}% │ "
              f"{rn['Hs']:.3f}m {en:>+6.1f}% │ {improve:>+5.1f}%")

    conn.close()
    print()


if __name__ == "__main__":
    main()
