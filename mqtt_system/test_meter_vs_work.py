#!/usr/bin/env python3
"""
METER vs WORK 模式滤波对比测试
================================
从数据库取 16:30~16:35 的原始数据，分别用 work 和 meter 模式做分析，对比结果。
"""

import sys
import os
import yaml
import logging
import psycopg2
import numpy as np
from datetime import datetime, timedelta

# 设置路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'services'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'system_config.yaml')
    with open(config_path) as f:
        return yaml.safe_load(f)

def fetch_data(t_start, t_end):
    """从数据库获取指定时间范围的三雷达数据"""
    conn = psycopg2.connect(
        host='localhost', port=5432,
        database='wave_monitoring', user='wave_user', password='wave2025'
    )
    cur = conn.cursor()

    data = {}
    for rid in [1, 2, 3]:
        cur.execute(
            "SELECT timestamp, distance FROM wave_measurements "
            "WHERE radar_id=%s AND timestamp BETWEEN %s AND %s ORDER BY timestamp",
            (rid, t_start, t_end)
        )
        rows = cur.fetchall()
        data[rid] = {
            'timestamps': [r[0].isoformat() for r in rows],
            'distances': [float(r[1]) for r in rows]
        }
        print(f"  R{rid}: {len(rows)} samples, "
              f"dist range [{min(data[rid]['distances']):.4f}, {max(data[rid]['distances']):.4f}]m")

    conn.close()
    return data

def run_analysis(config, raw_data, mode):
    """用指定模式运行分析"""
    from mqtt_analyzer import WaveAnalyzer

    analyzer = WaveAnalyzer(config)

    # 构造 analyze_window 需要的数据格式
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

def print_r1_only(label, config, raw_data, mode):
    """只用R1数据做单雷达分析"""
    from mqtt_analyzer import WaveAnalyzer
    analyzer = WaveAnalyzer(config)

    data_window = {
        'timestamps': raw_data[1]['timestamps'],
        'timestamps_r2': raw_data[2]['timestamps'],
        'timestamps_r3': raw_data[3]['timestamps'],
        'eta1': raw_data[1]['distances'],
        'eta2': [float('nan')] * len(raw_data[2]['distances']),
        'eta3': [float('nan')] * len(raw_data[3]['distances']),
    }

    analysis = analyzer.analyze_window(data_window, mode=mode)
    if analysis is None:
        print(f"  {label}: 分析失败")
        return None
    return analysis.get('results', {})

def print_results(label, result):
    """打印关键波浪参数"""
    if result is None:
        print(f"\n{'='*60}")
        print(f"  {label}: 分析失败 (返回 None)")
        print(f"{'='*60}")
        return

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")

    # 零交叉法结果
    print(f"\n  【零交叉法】")
    print(f"    有效波高 Hs    = {result.get('Hs', 0)*1000:.1f} mm")
    print(f"    最大波高 Hmax  = {result.get('Hmax', 0)*1000:.1f} mm")
    print(f"    H1/10         = {result.get('H1_10', 0)*1000:.1f} mm")
    print(f"    平均波高 Hmean = {result.get('Hmean', 0)*1000:.1f} mm")
    print(f"    有效周期 Ts    = {result.get('Ts', 0):.3f} s")
    print(f"    最大周期 Tmax  = {result.get('Tmax', 0):.3f} s")
    print(f"    平均周期 Tmean = {result.get('Tmean', 0):.3f} s")
    print(f"    波浪计数       = {result.get('wave_count', 0)}")

    # 谱分析结果
    print(f"\n  【谱分析法】")
    print(f"    Hm0            = {result.get('Hm0', 0)*1000:.1f} mm")
    print(f"    峰值周期 Tp    = {result.get('Tp', 0):.3f} s")
    print(f"    过零周期 Tz    = {result.get('Tz', 0):.3f} s")
    print(f"    峰值频率 fp    = {result.get('peak_frequency', 0):.4f} Hz")

    # 潮位
    print(f"\n  【潮位】")
    print(f"    平均液位       = {result.get('mean_level', 0):.2f} cm")

    # 雷达数
    print(f"    雷达数         = {result.get('radar_count', '?')}")


def main():
    print("=" * 60)
    print("  METER vs WORK 模式滤波对比测试")
    print("  数据窗口: 2026-03-19 16:04:00 ~ 16:09:00 CST")
    print("=" * 60)

    config = load_config()
    print(f"\n配置 meter_filter: {config['analysis'].get('meter_filter', 'NOT FOUND')}")

    t_start = '2026-03-19 16:04:00+08:00'
    t_end = '2026-03-19 16:09:00+08:00'

    print(f"\n--- 从数据库加载数据 ---")
    raw_data = fetch_data(t_start, t_end)

    # WORK 模式分析
    print(f"\n{'#'*60}")
    print(f"  运行 WORK 模式分析...")
    print(f"{'#'*60}")
    result_work = run_analysis(config, raw_data, mode='work')
    print_results("WORK 模式结果", result_work)

    # METER 模式分析
    print(f"\n{'#'*60}")
    print(f"  运行 METER 模式分析...")
    print(f"{'#'*60}")
    result_meter = run_analysis(config, raw_data, mode='meter')
    print_results("METER 模式结果", result_meter)

    # R1-only 对比
    print(f"\n{'#'*60}")
    print(f"  运行 R1-only WORK 模式...")
    print(f"{'#'*60}")
    r1_work = print_r1_only("R1-WORK", config, raw_data, 'work')
    print_results("R1-only WORK", r1_work)

    print(f"\n{'#'*60}")
    print(f"  运行 R1-only METER 模式...")
    print(f"{'#'*60}")
    r1_meter = print_r1_only("R1-METER", config, raw_data, 'meter')
    print_results("R1-only METER", r1_meter)

    # R1-only 对比表
    if r1_work and r1_meter:
        print(f"\n{'='*60}")
        print(f"  R1-only 对比总结 (真实波高≈197mm)")
        print(f"{'='*60}")
        keys = [
            ('Hs', '有效波高(零交叉)', 1000, 'mm'),
            ('Hmax', '最大波高', 1000, 'mm'),
            ('Hmean', '平均波高', 1000, 'mm'),
            ('Ts', '有效周期', 1, 's'),
            ('Tmean', '平均周期', 1, 's'),
            ('wave_count', '波浪计数', 1, ''),
            ('Hm0', '有效波高(谱法)', 1000, 'mm'),
            ('Tp', '峰值周期', 1, 's'),
        ]
        print(f"  {'参数':<18s} {'WORK':>10s} {'METER':>10s} {'差异':>10s}")
        print(f"  {'-'*50}")
        for key, name, scale, unit in keys:
            w = r1_work.get(key, 0) * scale
            m = r1_meter.get(key, 0) * scale
            diff = m - w
            sign = '+' if diff > 0 else ''
            print(f"  {name:<18s} {w:>9.1f}{unit} {m:>9.1f}{unit} {sign}{diff:>8.1f}{unit}")

    # 对比
    if result_work and result_meter:
        print(f"\n{'='*60}")
        print(f"  对比总结")
        print(f"{'='*60}")
        keys = [
            ('Hs', '有效波高(零交叉)', 1000, 'mm'),
            ('Hmax', '最大波高', 1000, 'mm'),
            ('Hmean', '平均波高', 1000, 'mm'),
            ('Ts', '有效周期', 1, 's'),
            ('Tmean', '平均周期', 1, 's'),
            ('wave_count', '波浪计数', 1, ''),
            ('Hm0', '有效波高(谱法)', 1000, 'mm'),
            ('Tp', '峰值周期', 1, 's'),
        ]
        print(f"  {'参数':<18s} {'WORK':>10s} {'METER':>10s} {'差异':>10s}")
        print(f"  {'-'*50}")
        for key, name, scale, unit in keys:
            w = result_work.get(key, 0) * scale
            m = result_meter.get(key, 0) * scale
            diff = m - w
            sign = '+' if diff > 0 else ''
            print(f"  {name:<18s} {w:>9.1f}{unit} {m:>9.1f}{unit} {sign}{diff:>8.1f}{unit}")


if __name__ == '__main__':
    main()
