#!/usr/bin/env python3
"""
批量对比：下午所有 METER 分析的日志结果 vs 新 METER 模式重算结果
"""
import sys, os, yaml, logging, psycopg2
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'services'))
logging.basicConfig(level=logging.WARNING)

def load_config():
    with open(os.path.join(os.path.dirname(__file__), 'config', 'system_config.yaml')) as f:
        return yaml.safe_load(f)

def fetch_data(t_start, t_end):
    conn = psycopg2.connect(host='localhost', port=5432, database='wave_monitoring', user='wave_user', password='wave2025')
    cur = conn.cursor()
    data = {}
    for rid in [1, 2, 3]:
        cur.execute(
            "SELECT timestamp, distance FROM wave_measurements "
            "WHERE radar_id=%s AND timestamp BETWEEN %s AND %s ORDER BY timestamp",
            (rid, t_start, t_end))
        rows = cur.fetchall()
        if not rows:
            return None
        data[rid] = {
            'timestamps': [r[0].isoformat() for r in rows],
            'distances': [float(r[1]) for r in rows]
        }
    conn.close()
    return data

def run_analysis(config, raw_data, mode):
    from mqtt_analyzer import WaveAnalyzer
    analyzer = WaveAnalyzer(config)
    data_window = {
        'timestamps': raw_data[1]['timestamps'],
        'timestamps_r2': raw_data[2]['timestamps'],
        'timestamps_r3': raw_data[3]['timestamps'],
        'eta1': raw_data[1]['distances'],
        'eta2': raw_data[2]['distances'],
        'eta3': raw_data[3]['distances'],
    }
    analysis = analyzer.analyze_window(data_window, mode=mode)
    if analysis is None:
        return None
    return analysis.get('results', {})

def main():
    config = load_config()

    # 从数据库获取今天下午所有分析记录
    conn = psycopg2.connect(host='localhost', port=5432, database='wave_monitoring', user='wave_user', password='wave2025')
    cur = conn.cursor()
    cur.execute('''
        SELECT start_time, end_time, hs, hs_zc, hmax, h1_10, h_mean,
               wave_count, tp, ts, tmean
        FROM wave_analysis
        WHERE start_time >= '2026-03-19 14:00:00+08:00'
          AND start_time <  '2026-03-19 18:30:00+08:00'
        ORDER BY start_time
    ''')
    records = cur.fetchall()
    conn.close()

    print(f"下午共 {len(records)} 次 meter 分析，逐一用新METER模式重算对比")
    print()

    # 表头
    hdr = (f"{'分析时间':<20s} │ {'指标':<10s} │ "
           f"{'日志原值':>9s} │ {'新METER':>9s} │ {'新WORK':>9s} │ {'差异(新M-原)':>12s}")
    print(hdr)
    print("─" * len(hdr))

    for rec in records:
        start_time, end_time = rec[0], rec[1]
        log_hs    = (rec[2] or 0) * 1000   # Hm0谱法
        log_hs_zc = (rec[3] or 0) * 1000   # Hs零交叉
        log_hmax  = (rec[4] or 0) * 1000
        log_h110  = (rec[5] or 0) * 1000
        log_hmean = (rec[6] or 0) * 1000
        log_wc    = rec[7] or 0
        log_tp    = rec[8] or 0
        log_ts    = rec[9] or 0
        log_tmean = rec[10] or 0

        t_label = str(start_time)[:19]

        # 获取原始数据
        raw = fetch_data(start_time, end_time)
        if raw is None:
            print(f"{t_label:<20s} │ 数据不足，跳过")
            print()
            continue

        n_samples = len(raw[1]['distances'])

        # 新 METER 模式重算
        r_meter = run_analysis(config, raw, 'meter')
        # 新 WORK 模式重算
        r_work = run_analysis(config, raw, 'work')

        if r_meter is None or r_work is None:
            print(f"{t_label:<20s} │ 分析失败，跳过")
            print()
            continue

        # 提取新结果
        new_m_hs_zc = r_meter.get('Hs', 0) * 1000
        new_m_hmax  = r_meter.get('Hmax', 0) * 1000
        new_m_hmean = r_meter.get('Hmean', 0) * 1000
        new_m_hm0   = r_meter.get('Hm0', 0) * 1000
        new_m_tp    = r_meter.get('Tp', 0)
        new_m_ts    = r_meter.get('Ts', 0)
        new_m_tmean = r_meter.get('Tmean', 0)
        new_m_wc    = r_meter.get('wave_count', 0)

        new_w_hs_zc = r_work.get('Hs', 0) * 1000
        new_w_hmax  = r_work.get('Hmax', 0) * 1000
        new_w_hmean = r_work.get('Hmean', 0) * 1000
        new_w_hm0   = r_work.get('Hm0', 0) * 1000
        new_w_tp    = r_work.get('Tp', 0)
        new_w_ts    = r_work.get('Ts', 0)
        new_w_tmean = r_work.get('Tmean', 0)
        new_w_wc    = r_work.get('wave_count', 0)

        rows_data = [
            ("Hs(ZC)",  f"{log_hs_zc:.1f}mm", f"{new_m_hs_zc:.1f}mm", f"{new_w_hs_zc:.1f}mm", f"{new_m_hs_zc - log_hs_zc:+.1f}mm"),
            ("Hm0(谱)", f"{log_hs:.1f}mm",    f"{new_m_hm0:.1f}mm",   f"{new_w_hm0:.1f}mm",   f"{new_m_hm0 - log_hs:+.1f}mm"),
            ("Hmax",    f"{log_hmax:.1f}mm",   f"{new_m_hmax:.1f}mm",  f"{new_w_hmax:.1f}mm",  f"{new_m_hmax - log_hmax:+.1f}mm"),
            ("Hmean",   f"{log_hmean:.1f}mm",  f"{new_m_hmean:.1f}mm", f"{new_w_hmean:.1f}mm", f"{new_m_hmean - log_hmean:+.1f}mm"),
            ("Tp",      f"{log_tp:.2f}s",      f"{new_m_tp:.2f}s",     f"{new_w_tp:.2f}s",     f"{new_m_tp - log_tp:+.2f}s"),
            ("Ts",      f"{log_ts:.2f}s",      f"{new_m_ts:.2f}s",     f"{new_w_ts:.2f}s",     f"{new_m_ts - log_ts:+.2f}s"),
            ("Tmean",   f"{log_tmean:.2f}s",   f"{new_m_tmean:.2f}s",  f"{new_w_tmean:.2f}s",  f"{new_m_tmean - log_tmean:+.2f}s"),
            ("波数",    f"{log_wc}",           f"{new_m_wc}",          f"{new_w_wc}",          f"{new_m_wc - log_wc:+d}"),
        ]

        for i, (name, v_log, v_meter, v_work, diff) in enumerate(rows_data):
            t_col = f"{t_label} ({n_samples}pt)" if i == 0 else ""
            print(f"{t_col:<20s} │ {name:<10s} │ {v_log:>9s} │ {v_meter:>9s} │ {v_work:>9s} │ {diff:>12s}")
        print()

if __name__ == '__main__':
    main()
