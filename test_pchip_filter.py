#!/usr/bin/env python3
"""
测试：现有去尖峰 + PCHIP插值填补空洞（替代线性插值）
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
    """现有去尖峰方法（IQR + 跳变），只检测不填补"""
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


def fill_linear(d, mask):
    """原版：线性插值填补"""
    out = d.copy()
    if np.any(mask):
        g = ~mask
        if np.any(g):
            out[mask] = np.interp(np.where(mask)[0], np.where(g)[0], out[g])
    return out


def fill_pchip(d, mask):
    """改进版：PCHIP插值填补"""
    out = d.copy()
    if np.any(mask):
        g = ~mask
        if np.sum(g) > 3:
            idx_good = np.where(g)[0]
            idx_bad = np.where(mask)[0]
            pchip = PchipInterpolator(idx_good, out[g])
            out[mask] = pchip(idx_bad)
    return out


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

    print("=" * 80)
    print("  测试: 现有去尖峰 + PCHIP插值 vs 线性插值")
    print("=" * 80)

    for label, t_start, t_end, H_nom, T_nom in PERIODS:
        dist, t_ep = fetch_r1(conn, t_start, t_end)
        mask = detect_spikes(dist)

        c_lin = fill_linear(dist, mask)
        c_pch = fill_pchip(dist, mask)

        r_lin = do_analysis(c_lin, t_ep)
        r_pch = do_analysis(c_pch, t_ep)

        # 看填补区域的差异
        filled_idx = np.where(mask)[0]
        diff_fill = np.abs(c_pch[mask] - c_lin[mask])

        print(f"\n{'─' * 80}")
        print(f"  {label}  (标称 H={H_nom}m, T={T_nom}s)")
        print(f"{'─' * 80}")
        print(f"  去尖峰: {int(np.sum(mask))}点 ({np.sum(mask)/len(dist)*100:.1f}%)")
        print(f"  填补点差异(PCHIP vs 线性): mean={np.mean(diff_fill)*1000:.2f}mm, "
              f"max={np.max(diff_fill)*1000:.2f}mm")
        print(f"  清洗后range: 线性={np.ptp(c_lin)*1000:.1f}mm  PCHIP={np.ptp(c_pch)*1000:.1f}mm")
        print()

        def err(v, n):
            return (v - n) / n * 100 if n > 0 else 0

        print(f"  {'':24s} {'线性插值':>10s}  {'PCHIP':>10s}  {'标称':>8s}")
        print(f"  {'Hm0':<24s} {r_lin['Hm0']*1000:>8.1f}mm  {r_pch['Hm0']*1000:>8.1f}mm  {H_nom*1000:>6.0f}mm")
        print(f"  {'Hs':<24s} {r_lin['Hs']*1000:>8.1f}mm  {r_pch['Hs']*1000:>8.1f}mm  {H_nom*1000:>6.0f}mm")
        print(f"  {'Hmax':<24s} {r_lin['Hmax']*1000:>8.1f}mm  {r_pch['Hmax']*1000:>8.1f}mm")
        print(f"  {'Tp':<24s} {r_lin['Tp']:>8.2f}s   {r_pch['Tp']:>8.2f}s   {T_nom:>6.1f}s")
        print(f"  {'Ts':<24s} {r_lin['Ts']:>8.2f}s   {r_pch['Ts']:>8.2f}s   {T_nom:>6.1f}s")
        print(f"  {'波数':<24s} {r_lin['n_waves']:>8d}    {r_pch['n_waves']:>8d}")
        print()
        print(f"  {'':24s} {'线性误差':>10s}  {'PCHIP误差':>10s}")
        print(f"  {'Hm0':<24s} {err(r_lin['Hm0'],H_nom):>+9.1f}%  {err(r_pch['Hm0'],H_nom):>+9.1f}%")
        print(f"  {'Hs':<24s} {err(r_lin['Hs'],H_nom):>+9.1f}%  {err(r_pch['Hs'],H_nom):>+9.1f}%")
        print(f"  {'Tp':<24s} {err(r_lin['Tp'],T_nom):>+9.1f}%  {err(r_pch['Tp'],T_nom):>+9.1f}%")

    # 汇总
    print(f"\n{'=' * 80}")
    print("  Hs 误差汇总")
    print(f"{'=' * 80}")
    print(f"  {'时段':<14s} │ {'标称':>5s} │ {'线性Hs':>8s} {'误差':>7s} │ {'PCHIP_Hs':>8s} {'误差':>7s} │ {'改善':>6s}")
    print(f"  {'─'*14} ┼ {'─'*6} ┼ {'─'*17} ┼ {'─'*17} ┼ {'─'*7}")
    for label, t_start, t_end, H_nom, T_nom in PERIODS:
        dist, t_ep = fetch_r1(conn, t_start, t_end)
        mask = detect_spikes(dist)
        cl = fill_linear(dist, mask)
        cp = fill_pchip(dist, mask)
        rl = do_analysis(cl, t_ep)
        rp = do_analysis(cp, t_ep)
        el = (rl['Hs'] - H_nom) / H_nom * 100
        ep = (rp['Hs'] - H_nom) / H_nom * 100
        print(f"  {label:<14s} │ {H_nom:>4.1f}m │ {rl['Hs']:.3f}m {el:>+6.1f}% │ "
              f"{rp['Hs']:.3f}m {ep:>+6.1f}% │ {abs(el)-abs(ep):>+5.1f}%")

    conn.close()
    print()


if __name__ == "__main__":
    main()
