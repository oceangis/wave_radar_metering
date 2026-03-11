#!/usr/bin/env python3
"""
测试：非对称阈值去尖刺
- 距离增大方向（多径方向）：严格 IQR 阈值
- 距离减小方向（波峰方向）：放宽或不做 IQR 剔除
"""
import sys
import numpy as np
import psycopg2
import yaml
from scipy.interpolate import interp1d
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
        [r[0].isoformat() for r in rows],
        np.array([r[1] for r in rows], dtype=float),
        np.array([r[0].timestamp() for r in rows]),
    )


# ==== 原版（项目代码） ====
def spike_original(d):
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

    out = d.copy()
    if np.any(mask):
        g = ~mask
        if np.any(g):
            out[mask] = np.interp(np.where(mask)[0], np.where(g)[0], out[g])
    return out, mask


# ==== 改进版：非对称阈值 ====
def spike_asymmetric(d, upper_k=3.0, lower_k=6.0):
    """
    距离增大（多径方向）：upper = Q75 + upper_k * IQR  (严格, k=3)
    距离减小（波峰方向）：lower = Q25 - lower_k * IQR  (宽松, k=6 甚至更大)

    原理：雷达朝下，距离减小=水面升高=波峰，不会出现多径
    """
    q25, q75 = np.percentile(d, [25, 75])
    iqr = max(q75 - q25, 0.001)
    upper = q75 + upper_k * iqr   # 多径方向，严格
    lower = q25 - lower_k * iqr   # 波峰方向，宽松

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

    out = d.copy()
    if np.any(mask):
        g = ~mask
        if np.any(g):
            out[mask] = np.interp(np.where(mask)[0], np.where(g)[0], out[g])
    return out, mask, {'upper': upper, 'lower': lower, 'iqr': iqr, 'q25': q25, 'q75': q75}


def do_analysis(distances, t_epoch, fs_target=6.0):
    mean_dist = np.mean(distances)
    eta = -(distances - mean_dist)
    t_rel = t_epoch - t_epoch[0]
    duration = t_rel[-1]

    t_u = np.arange(0, duration, 1.0 / fs_target)
    if len(t_u) > 1 and len(t_rel) > 1:
        eta_r = interp1d(t_rel, eta, kind='linear', fill_value='extrapolate')(t_u)
    else:
        eta_r = eta

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


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    print("=" * 85)
    print("  非对称阈值测试: 多径方向严格(k=3) / 波峰方向宽松(k=6)")
    print("=" * 85)

    for label, t_start, t_end, H_nom, T_nom in PERIODS:
        _, dist, t_ep = fetch_r1(conn, t_start, t_end)
        N = len(dist)

        c_orig, m_orig = spike_original(dist)
        c_asym, m_asym, info = spike_asymmetric(dist)

        r_orig = do_analysis(c_orig, t_ep)
        r_asym = do_analysis(c_asym, t_ep)

        # 比较哪些点被原版剔除但改进版保留了
        rescued = m_orig & ~m_asym
        extra   = ~m_orig & m_asym
        n_rescued = int(np.sum(rescued))
        n_extra   = int(np.sum(extra))

        # 被救回的点分布
        if n_rescued > 0:
            rescued_vals = dist[rescued]
            rescued_dir = "距离减小(波峰)" if np.mean(rescued_vals) < np.mean(dist[~m_orig]) else "距离增大(多径)"
        else:
            rescued_dir = "—"

        print(f"\n{'─' * 85}")
        print(f"  {label}  (标称 H={H_nom}m, T={T_nom}s)")
        print(f"{'─' * 85}")
        print(f"  IQR={info['iqr']*1000:.0f}mm  Q25={info['q25']:.4f} Q75={info['q75']:.4f}")
        print(f"  原版边界:  [{info['q25']-3*info['iqr']:.4f}, {info['q75']+3*info['iqr']:.4f}]")
        print(f"  改进边界:  [{info['lower']:.4f}, {info['upper']:.4f}]  (下界放宽到6×IQR)")
        print()
        print(f"  {'':28s} {'原版':>10s}  {'改进版':>10s}  {'差异':>10s}")
        print(f"  {'剔除点数':<28s} {int(np.sum(m_orig)):>10d}  {int(np.sum(m_asym)):>10d}  {int(np.sum(m_asym))-int(np.sum(m_orig)):>+10d}")
        print(f"  {'救回点数':<28s} {'':>10s}  {'':>10s}  {n_rescued:>10d}")
        print(f"  {'救回方向':<28s} {'':>10s}  {'':>10s}  {rescued_dir:>10s}")
        print(f"  {'清洗后range':<28s} {np.ptp(c_orig)*1000:>9.1f}mm  {np.ptp(c_asym)*1000:>9.1f}mm")
        print()

        def err(v, n):
            return (v - n) / n * 100 if n > 0 else 0

        print(f"  {'':28s} {'原版':>10s}  {'改进版':>10s}  {'标称':>8s}")
        print(f"  {'Hm0':<28s} {r_orig['Hm0']*1000:>8.1f}mm  {r_asym['Hm0']*1000:>8.1f}mm  {H_nom*1000:>6.0f}mm")
        print(f"  {'Hs':<28s} {r_orig['Hs']*1000:>8.1f}mm  {r_asym['Hs']*1000:>8.1f}mm  {H_nom*1000:>6.0f}mm")
        print(f"  {'Hmax':<28s} {r_orig['Hmax']*1000:>8.1f}mm  {r_asym['Hmax']*1000:>8.1f}mm")
        print(f"  {'Tp':<28s} {r_orig['Tp']:>8.2f}s   {r_asym['Tp']:>8.2f}s   {T_nom:>6.1f}s")
        print(f"  {'Ts':<28s} {r_orig['Ts']:>8.2f}s   {r_asym['Ts']:>8.2f}s   {T_nom:>6.1f}s")
        print()
        print(f"  {'':28s} {'原版误差':>10s}  {'改进误差':>10s}")
        print(f"  {'Hm0':<28s} {err(r_orig['Hm0'],H_nom):>+9.1f}%  {err(r_asym['Hm0'],H_nom):>+9.1f}%")
        print(f"  {'Hs':<28s} {err(r_orig['Hs'],H_nom):>+9.1f}%  {err(r_asym['Hs'],H_nom):>+9.1f}%")
        print(f"  {'Tp':<28s} {err(r_orig['Tp'],T_nom):>+9.1f}%  {err(r_asym['Tp'],T_nom):>+9.1f}%")

    # 汇总
    print(f"\n{'=' * 85}")
    print("  Hs 误差汇总")
    print(f"{'=' * 85}")
    print(f"  {'时段':<14s} │ {'标称':>5s} │ {'原版Hs':>8s} {'误差':>7s} │ {'改进Hs':>8s} {'误差':>7s} │ {'改善':>6s}")
    print(f"  {'─'*14} ┼ {'─'*6} ┼ {'─'*17} ┼ {'─'*17} ┼ {'─'*7}")
    for label, t_start, t_end, H_nom, T_nom in PERIODS:
        _, dist, t_ep = fetch_r1(conn, t_start, t_end)
        co, _ = spike_original(dist)
        ca, _, _ = spike_asymmetric(dist)
        ro = do_analysis(co, t_ep)
        ra = do_analysis(ca, t_ep)
        eo = (ro['Hs'] - H_nom) / H_nom * 100
        ea = (ra['Hs'] - H_nom) / H_nom * 100
        print(f"  {label:<14s} │ {H_nom:>4.1f}m │ {ro['Hs']:.3f}m {eo:>+6.1f}% │ "
              f"{ra['Hs']:.3f}m {ea:>+6.1f}% │ {abs(eo)-abs(ea):>+5.1f}%")

    conn.close()
    print()


if __name__ == "__main__":
    main()
