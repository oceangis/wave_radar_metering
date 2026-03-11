#!/usr/bin/env python3
"""
测试：去尖峰不插值，跨零照常做，极值附近有间隙时局部PCHIP/抛物线拟合估算真实峰值
"""
import sys
import numpy as np
import psycopg2
import yaml
from scipy.interpolate import interp1d, PchipInterpolator
from scipy.signal import welch, butter, filtfilt, detrend

sys.path.insert(0, '/home/pi/radar/mqtt_system/services')
if not hasattr(np, 'trapz'):
    np.trapz = np.trapezoid

with open('/home/pi/radar/mqtt_system/config/system_config.yaml', 'r') as f:
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
        WHERE radar_id=1 AND timestamp >= %s AND timestamp < %s ORDER BY timestamp
    """, (t_start, t_end))
    rows = cur.fetchall()
    cur.close()
    return (
        np.array([r[1] for r in rows], dtype=float),
        np.array([r[0].timestamp() for r in rows]),
    )


def detect_spikes(d):
    """现有去尖峰检测（IQR + 跳变）"""
    q25, q75 = np.percentile(d, [25, 75])
    iqr = max(q75 - q25, 0.001)
    lower = q25 - 3.0 * iqr
    upper = q75 + 3.0 * iqr
    spike_iqr = (d < lower) | (d > upper)

    d_temp = d.copy()
    if np.any(spike_iqr):
        g = ~spike_iqr
        if np.sum(g) > 10:
            d_temp[spike_iqr] = np.interp(np.where(spike_iqr)[0], np.where(g)[0], d_temp[g])
    diff = np.abs(np.diff(d_temp))
    jf = np.concatenate(([False], diff > 0.2))
    jb = np.concatenate((diff > 0.2, [False]))
    mask = spike_iqr | jf | jb
    return mask


# ==== 原版方法：线性插值 + 标准零交叉 ====
def method_original(dist, t_epoch, fs_target=6.0):
    mask = detect_spikes(dist)
    d_clean = dist.copy()
    if np.any(mask):
        g = ~mask
        if np.any(g):
            d_clean[mask] = np.interp(np.where(mask)[0], np.where(g)[0], d_clean[g])

    mean_dist = np.mean(d_clean)
    eta = -(d_clean - mean_dist)
    t_rel = t_epoch - t_epoch[0]
    duration = t_rel[-1]

    t_u = np.arange(0, duration, 1.0 / fs_target)
    eta_r = interp1d(t_rel, eta, kind='linear', fill_value='extrapolate')(t_u)
    eta_r = detrend(eta_r)

    band = config['analysis']['filter_band']
    b, a = butter(4, band, btype='band', fs=fs_target)
    try:
        eta_f = filtfilt(b, a, eta_r, padlen=min(3*(max(len(b),len(a))-1), len(eta_r)-1))
    except:
        eta_f = eta_r

    nperseg = min(config['analysis']['nperseg'], len(eta_f) // 4)
    f, S = welch(eta_f, fs=fs_target, nperseg=nperseg)
    m0 = np.trapz(S, f)
    Hm0 = 4.0 * np.sqrt(m0) if m0 > 0 else 0
    pi = np.argmax(S[1:]) + 1
    Tp = 1.0 / f[pi] if f[pi] > 0 else 0

    zc = []
    for i in range(len(eta_f) - 1):
        if eta_f[i] >= 0 and eta_f[i+1] < 0:
            zc.append(i)
    wh, wp = [], []
    for i in range(len(zc) - 1):
        si, ei = zc[i], zc[i+1]
        if ei - si < 2: continue
        seg = eta_f[si:ei]
        H = np.max(seg) - np.min(seg)
        T = (ei - si) / fs_target
        if H > 0 and T > 0:
            wh.append(H); wp.append(T)

    if wh:
        wh = np.array(wh); wp = np.array(wp)
        idx = np.argsort(wh)[::-1]
        n13 = max(1, len(wh) // 3)
        Hs = float(np.mean(wh[idx[:n13]]))
        Ts = float(np.mean(wp[idx[:n13]]))
        Hmax = float(wh[idx[0]])
    else:
        Hs = Ts = Hmax = 0
    return {'Hm0': Hm0, 'Tp': Tp, 'Hs': Hs, 'Ts': Ts, 'Hmax': Hmax, 'n_waves': len(wh)}


# ==== 改进方法：不插值，跨零在好点上做，极值局部PCHIP估算 ====
def method_local_peak_fit(dist, t_epoch, fs_target=6.0):
    mask = detect_spikes(dist)
    good = ~mask

    # 好点的索引、距离、时间
    idx_good = np.where(good)[0]
    d_good = dist[good]
    t_good = t_epoch[good]

    # 用好点算均值，转eta（好点上）
    mean_dist = np.mean(d_good)
    eta_good = -(d_good - mean_dist)
    t_rel_good = t_good - t_epoch[0]

    # ---- 频域：好点重采样到均匀网格，照常做 ----
    duration = t_rel_good[-1]
    t_u = np.arange(0, duration, 1.0 / fs_target)
    eta_r = interp1d(t_rel_good, eta_good, kind='linear', fill_value='extrapolate')(t_u)
    eta_r = detrend(eta_r)

    band = config['analysis']['filter_band']
    b, a = butter(4, band, btype='band', fs=fs_target)
    try:
        eta_f = filtfilt(b, a, eta_r, padlen=min(3*(max(len(b),len(a))-1), len(eta_r)-1))
    except:
        eta_f = eta_r

    nperseg = min(config['analysis']['nperseg'], len(eta_f) // 4)
    f, S = welch(eta_f, fs=fs_target, nperseg=nperseg)
    m0 = np.trapz(S, f)
    Hm0 = 4.0 * np.sqrt(m0) if m0 > 0 else 0
    pi = np.argmax(S[1:]) + 1
    Tp = 1.0 / f[pi] if f[pi] > 0 else 0

    # ---- 时域：在滤波后的均匀网格上找跨零，极值处回查原始好点做局部拟合 ----
    # 建立均匀网格索引到原始好点索引的映射：
    # 对于每个均匀网格点 t_u[k], 找最近的原始好点
    # 同时标记哪些均匀网格点落在间隙内（附近有spike）

    # 先在滤波后信号上找跨零
    zc = []
    for i in range(len(eta_f) - 1):
        if eta_f[i] >= 0 and eta_f[i+1] < 0:
            zc.append(i)

    wh, wp = [], []
    n_fitted = 0

    for i in range(len(zc) - 1):
        si, ei = zc[i], zc[i+1]
        if ei - si < 2:
            continue

        seg_filt = eta_f[si:ei]
        T = (ei - si) / fs_target

        # 滤波信号的极值
        raw_max = np.max(seg_filt)
        raw_min = np.min(seg_filt)
        pos_max = np.argmax(seg_filt)
        pos_min = np.argmin(seg_filt)

        # 极值对应的绝对时间
        t_max_abs = t_u[si + pos_max] + t_epoch[0]
        t_min_abs = t_u[si + pos_min] + t_epoch[0]

        # 用局部拟合检查能否恢复更好的极值
        est_max = _estimate_extremum_v2(
            t_max_abs, raw_max, idx_good, t_good, eta_good, mask, search_radius=15, find_max=True)
        est_min = _estimate_extremum_v2(
            t_min_abs, raw_min, idx_good, t_good, eta_good, mask, search_radius=15, find_max=False)

        if est_max != raw_max or est_min != raw_min:
            n_fitted += 1

        H = est_max - est_min
        if H > 0 and T > 0:
            wh.append(H)
            wp.append(T)

    if wh:
        wh = np.array(wh); wp = np.array(wp)
        idx = np.argsort(wh)[::-1]
        n13 = max(1, len(wh) // 3)
        Hs = float(np.mean(wh[idx[:n13]]))
        Ts = float(np.mean(wp[idx[:n13]]))
        Hmax = float(wh[idx[0]])
    else:
        Hs = Ts = Hmax = 0
    return {'Hm0': Hm0, 'Tp': Tp, 'Hs': Hs, 'Ts': Ts, 'Hmax': Hmax,
            'n_waves': len(wh), 'n_fitted': n_fitted}


def _estimate_extremum(seg_eta, seg_idx, seg_t, pos, mask, dist_all, t_all, mean_dist, find_max=True):
    """
    检查极值点附近是否有被删除的间隙，有则用局部PCHIP拟合估算真实极值。
    seg_eta: 该波段内好点的eta值
    seg_idx: 该波段内好点在原始数组中的索引
    pos: 极值在seg中的位置
    """
    raw_val = seg_eta[pos]
    orig_idx = seg_idx[pos]  # 极值点在原始数组中的索引

    # 检查极值点左右是否紧邻间隙
    # 向左看：orig_idx-1, orig_idx-2... 是否是spike
    has_gap = False
    gap_left = 0
    gap_right = 0
    for k in range(1, 20):
        if orig_idx - k >= 0 and mask[orig_idx - k]:
            gap_left += 1
        else:
            break
    for k in range(1, 20):
        if orig_idx + k < len(mask) and mask[orig_idx + k]:
            gap_right += 1
        else:
            break

    if gap_left == 0 and gap_right == 0:
        return raw_val  # 极值附近无间隙

    # 取极值附近的好点做局部PCHIP拟合
    # 用seg中极值前后各若干个好点
    n_local = min(6, len(seg_eta))
    lo = max(0, pos - n_local)
    hi = min(len(seg_eta), pos + n_local + 1)

    local_t = seg_t[lo:hi]
    local_eta = seg_eta[lo:hi]

    if len(local_t) < 3:
        return raw_val

    # PCHIP拟合
    pchip = PchipInterpolator(local_t, local_eta)

    # 在间隙区间内密采样找极值
    t_start = seg_t[max(0, pos-1)]
    t_end = seg_t[min(len(seg_t)-1, pos+1)]
    # 扩展到包含间隙时间
    if gap_left > 0 and orig_idx - gap_left >= 0:
        t_start = min(t_start, t_all[orig_idx - gap_left])
    if gap_right > 0 and orig_idx + gap_right < len(t_all):
        t_end = max(t_end, t_all[orig_idx + gap_right])

    t_dense = np.linspace(t_start, t_end, 50)
    eta_dense = pchip(t_dense)

    if find_max:
        return max(raw_val, float(np.max(eta_dense)))
    else:
        return min(raw_val, float(np.min(eta_dense)))


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    print("=" * 80)
    print("  测试: 去尖峰不插值 + 跨零好点上做 + 极值局部PCHIP拟合")
    print("=" * 80)

    for label, t_start, t_end, H_nom, T_nom in PERIODS:
        dist, t_ep = fetch_r1(conn, t_start, t_end)

        r_orig = method_original(dist, t_ep)
        r_new = method_local_peak_fit(dist, t_ep)

        print(f"\n{'─' * 80}")
        print(f"  {label}  (标称 H={H_nom}m, T={T_nom}s)")
        print(f"{'─' * 80}")
        print(f"  局部拟合波数: {r_new.get('n_fitted', 0)}/{r_new['n_waves']}")
        print()

        def err(v, n):
            return (v - n) / n * 100 if n > 0 else 0

        print(f"  {'':24s} {'原版(线性)':>10s}  {'局部拟合':>10s}  {'标称':>8s}")
        print(f"  {'Hm0':<24s} {r_orig['Hm0']*1000:>8.1f}mm  {r_new['Hm0']*1000:>8.1f}mm  {H_nom*1000:>6.0f}mm")
        print(f"  {'Hs':<24s} {r_orig['Hs']*1000:>8.1f}mm  {r_new['Hs']*1000:>8.1f}mm  {H_nom*1000:>6.0f}mm")
        print(f"  {'Hmax':<24s} {r_orig['Hmax']*1000:>8.1f}mm  {r_new['Hmax']*1000:>8.1f}mm")
        print(f"  {'Tp':<24s} {r_orig['Tp']:>8.2f}s   {r_new['Tp']:>8.2f}s   {T_nom:>6.1f}s")
        print(f"  {'Ts':<24s} {r_orig['Ts']:>8.2f}s   {r_new['Ts']:>8.2f}s   {T_nom:>6.1f}s")
        print(f"  {'波数':<24s} {r_orig['n_waves']:>8d}    {r_new['n_waves']:>8d}")
        print()
        print(f"  {'':24s} {'原版误差':>10s}  {'拟合误差':>10s}  {'改善':>8s}")
        eo_hm0 = err(r_orig['Hm0'], H_nom)
        en_hm0 = err(r_new['Hm0'], H_nom)
        eo_hs = err(r_orig['Hs'], H_nom)
        en_hs = err(r_new['Hs'], H_nom)
        eo_tp = err(r_orig['Tp'], T_nom)
        en_tp = err(r_new['Tp'], T_nom)
        print(f"  {'Hm0':<24s} {eo_hm0:>+9.1f}%  {en_hm0:>+9.1f}%  {abs(eo_hm0)-abs(en_hm0):>+7.1f}%")
        print(f"  {'Hs':<24s} {eo_hs:>+9.1f}%  {en_hs:>+9.1f}%  {abs(eo_hs)-abs(en_hs):>+7.1f}%")
        print(f"  {'Tp':<24s} {eo_tp:>+9.1f}%  {en_tp:>+9.1f}%  {abs(eo_tp)-abs(en_tp):>+7.1f}%")

    # 汇总
    print(f"\n{'=' * 80}")
    print("  Hs 误差汇总")
    print(f"{'=' * 80}")
    print(f"  {'时段':<14s} │ {'标称':>5s} │ {'原版Hs':>8s} {'误差':>7s} │ {'拟合Hs':>8s} {'误差':>7s} │ {'改善':>6s}")
    print(f"  {'─'*14} ┼ {'─'*6} ┼ {'─'*17} ┼ {'─'*17} ┼ {'─'*7}")
    for label, t_start, t_end, H_nom, T_nom in PERIODS:
        dist, t_ep = fetch_r1(conn, t_start, t_end)
        ro = method_original(dist, t_ep)
        rn = method_local_peak_fit(dist, t_ep)
        eo = (ro['Hs'] - H_nom) / H_nom * 100
        en = (rn['Hs'] - H_nom) / H_nom * 100
        print(f"  {label:<14s} │ {H_nom:>4.1f}m │ {ro['Hs']:.3f}m {eo:>+6.1f}% │ "
              f"{rn['Hs']:.3f}m {en:>+6.1f}% │ {abs(eo)-abs(en):>+5.1f}%")

    conn.close()
    print()


if __name__ == "__main__":
    main()
