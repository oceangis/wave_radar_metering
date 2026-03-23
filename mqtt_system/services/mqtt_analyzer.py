#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MQTT波浪分析服务
================

功能：
1. 订阅原始数据 MQTT topic: radar/raw
2. 执行滑动窗口波浪分析
3. 发布分析结果到 radar/analyzed
4. 支持IEC标准波浪参数计算
5. 支持pyDIWASP方向谱分析

Author: Wave Monitoring System
Date: 2025-11-21
Updated: 2025-11-23 - Added directional spectrum analysis
"""

import json
import logging
import signal
import sys
import yaml
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from collections import deque
from threading import Thread, Event, Lock
import time

import paho.mqtt.client as mqtt
import numpy as np
import pandas as pd
import psycopg2
from scipy.signal import welch, csd, detrend, butter, filtfilt

# Import directional spectrum analyzer
try:
    from directional_spectrum import DirectionalSpectrumAnalyzer
    DIWASP_AVAILABLE = True
except ImportError:
    DIWASP_AVAILABLE = False
    logging.warning("DirectionalSpectrumAnalyzer not available, directional analysis disabled")

# Import radar preprocessor
try:
    from radar_preprocessor import RadarPreprocessor
    PREPROCESSOR_AVAILABLE = True
except ImportError:
    PREPROCESSOR_AVAILABLE = False
    logging.warning("RadarPreprocessor not available, advanced preprocessing disabled")


class WaveAnalyzer:
    """波浪分析引擎 - Wave Analysis Engine"""

    def __init__(self, config: Dict):
        self.config = config
        self.sample_rate = config['collection']['sample_rate']
        self.gravity = config['analysis']['gravity']

        # Initialize directional spectrum analyzer if available
        # 如果可用，初始化方向谱分析器
        self.directional_analyzer = None
        if DIWASP_AVAILABLE:
            try:
                dir_config = {
                    'sample_rate': self.sample_rate,
                    'gravity': self.gravity,
                    'water_depth': config['analysis'].get('water_depth', 50.0),
                    'freq_range': config['analysis'].get('filter_band', [0.04, 1.0]),
                    'direction_resolution': config['analysis'].get('direction_resolution', 360),
                    'radar_positions': config.get('radar', {}).get('diwasp_positions', {
                        'R1': [0.0, 0.0, 0.0],
                        'R2': [-0.1333, 0.2309, 0.0],
                        'R3': [0.1333, 0.2309, 0.0]
                    }),
                    'array_height': config.get('radar', {}).get('elevation_85',
                                    config.get('radar', {}).get('array_height', 5.0)),
                    'tilt_angles': config.get('radar', {}).get('tilt_angles', {
                        'R1': 0.0, 'R2': 10.0, 'R3': 10.0
                    }),
                    'tilt_azimuths': config.get('radar', {}).get('tilt_azimuths', {
                        'R1': 0.0, 'R2': 300.0, 'R3': 60.0
                    }),
                    'array_heading': config.get('radar', {}).get('array_heading', 0.0)
                }
                self.directional_analyzer = DirectionalSpectrumAnalyzer(dir_config)
                logging.info("Directional spectrum analyzer initialized successfully")
            except Exception as e:
                logging.error(f"Failed to initialize directional analyzer: {e}")
                self.directional_analyzer = None

        # Initialize radar preprocessor if available
        # 初始化雷达预处理器
        self.preprocessor = None
        if PREPROCESSOR_AVAILABLE:
            try:
                self.preprocessor = RadarPreprocessor(config)
                logging.info("Radar preprocessor initialized successfully")
            except Exception as e:
                logging.error(f"Failed to initialize preprocessor: {e}")
                self.preprocessor = None

    def _parse_timestamps_to_epoch(self, ts_list):
        """将ISO时间戳列表转换为Unix epoch秒数组"""
        result = np.empty(len(ts_list))
        for i, ts in enumerate(ts_list):
            result[i] = datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp()
        return result

    def _prepare_wave_data(self, distances: np.ndarray, timestamps: list,
                           mode: str = 'work', pass2_constraints: Dict = None) -> Dict:
        """
        波浪数据准备方法（SWAP标准质量控制）:
        1. 去尖刺: SWAP 4σ/4δ 自适应质量控制（Rijkswaterstaat, 1994）
           - 0-sigma: 恒定值检测（>10秒不变）
           - 4-sigma: 幅度异常值（|x-μ| > 4σ, 自适应于波高）
           - 4-delta: 跳变异常值（|Δx| > 4δ, 自适应于变化率）
           - meter模式额外: MAD + 局部滑窗 + 固定跳变阈值
           - 第二遍(pass2_constraints): 基于第一遍H1/3、T1/3收紧物理约束
        2. η = -(distance - median(distance))
        3. 用实际时间戳计算真实采样率
        4. 线性插值重采样到等间隔6Hz（用于Welch谱分析）
        5. 保留原始时间戳的η（用于过零法精确周期）

        Args:
            distances: 原始测距数组(m)
            timestamps: ISO时间戳列表
            mode: 分析模式 ('meter' 或 'work')
            pass2_constraints: 第二遍物理约束 {
                'H13': 粗估有效波高(m),
                'T13': 粗估有效波周期(s),
                'median_dist': 第一遍中位数距离(m)
            }

        Returns:
            dict: {
                'eta_resampled': 等间隔重采样η,
                'fs': 重采样目标采样率(6Hz),
                'eta_original': 原始时间轴η(去尖刺后),
                't_seconds': 原始相对时间(秒),
                'n_spikes': 去尖刺数量,
                'actual_fs': 实际采样率,
                'raw_distances': 去尖刺后距离(用于潮位)
            }
        """
        from scipy.interpolate import interp1d

        # 解析时间戳
        t_epoch = self._parse_timestamps_to_epoch(timestamps)
        t_rel = t_epoch - t_epoch[0]
        duration = t_rel[-1]

        # 实际采样率
        actual_fs = (len(distances) - 1) / duration if duration > 0 else self.sample_rate

        # 1. 去尖刺 — SWAP 4σ/4δ 质量控制方法
        # 参考: Mathematical description of the Standard Wave Analysis Package
        #       (Rijkswaterstaat, 1994, Section 3.4)
        # 三个测试按顺序执行，已标记的点不参与后续统计
        dist_clean = distances.copy()
        is_meter = (mode == 'meter')
        meter_cfg = self.config['analysis'].get('meter_filter', {})
        # QC和潮位统一用 elevation_85（安装时填估计值，测量后更新精确值）
        array_height = self.config['radar'].get('elevation_85',
                       self.config['radar'].get('array_height', 5.0))

        # ── 第0轮: 绝对范围过滤 ──
        two_pass_cfg = self.config['analysis'].get('two_pass', {})
        prior_cfg = self.config['analysis'].get('prior_knowledge', {})
        is_pass2 = (pass2_constraints is not None)

        if is_meter and meter_cfg.get('enabled', False):
            # meter模式：安装高度已知、波高已知，用紧凑范围获得最佳精度
            abs_margin = meter_cfg.get('abs_margin', 0.3)
            abs_lower = 0.3
            abs_upper = array_height + abs_margin
        elif is_pass2:
            # work模式第二遍：用第一遍H1/3收紧绝对范围
            h13 = pass2_constraints['H13']
            median_dist = pass2_constraints['median_dist']
            abs_mult = two_pass_cfg.get('abs_range_multiplier', 3.0)
            # 距离偏移 = 波面振幅 ≈ H1/3，乘安全系数
            excursion = max(abs_mult * h13, 0.5)  # 至少±0.5m
            abs_lower = max(0.3, median_dist - excursion)
            abs_upper = median_dist + excursion
            # 如果有先验知识，叠加硬上限
            if prior_cfg.get('enabled', False):
                max_hw = prior_cfg.get('max_wave_height', 10.0)
                tidal_r = prior_cfg.get('tidal_range', 5.0)
                # 物理硬上限：安装高度 + 潮差/2 + 最大波高振幅
                hard_upper = array_height + tidal_r / 2 + max_hw / 2
                hard_lower = max(0.3, array_height - tidal_r / 2 - max_hw / 2)
                abs_upper = min(abs_upper, hard_upper)
                abs_lower = max(abs_lower, hard_lower)
            logging.info(f"Pass2 abs range: [{abs_lower:.2f}, {abs_upper:.2f}]m "
                         f"(H1/3={h13:.3f}m, median={median_dist:.3f}m)")
        else:
            # work模式第一遍
            if prior_cfg.get('enabled', False):
                # 有先验知识：以安装高度为锚点（已知常量，不受噪声影响）
                # 范围 = array_height ± (潮差/2 + 最大波高振幅)
                max_hw = prior_cfg.get('max_wave_height', 6.0)
                tidal_r = prior_cfg.get('tidal_range', 3.0)
                max_excursion = tidal_r / 2 + max_hw / 2
                center = array_height  # 用已知安装高度，不用median
                abs_lower = max(0.3, center - max_excursion)
                abs_upper = center + max_excursion
                logging.info(f"Pass1 prior: center=array_height={array_height:.2f}m, "
                             f"range=[{abs_lower:.2f}, {abs_upper:.2f}]m "
                             f"(tidal={tidal_r}m, max_wave={max_hw}m)")
            else:
                # 无先验知识：退回中位数自适应（兼容未配置先验的部署）
                median_raw = np.median(dist_clean)
                max_excursion = 3.0
                center = median_raw
                abs_lower = max(0.3, center - max_excursion)
                abs_upper = center + max_excursion
        spike_abs = (dist_clean < abs_lower) | (dist_clean > abs_upper)
        n_abs_spikes = int(np.sum(spike_abs))
        if n_abs_spikes > 0:
            median_dist = np.median(dist_clean[~spike_abs]) if np.any(~spike_abs) else np.median(dist_clean)
            dist_clean[spike_abs] = median_dist
            logging.info(f"Absolute range filter: {n_abs_spikes} points outside "
                         f"[{abs_lower:.2f}, {abs_upper:.2f}]m")

        if is_meter and meter_cfg.get('enabled', False):
            # ── meter模式: IQR + 固定跳变阈值（计量专用，已调优）──
            iqr_mult = meter_cfg.get('iqr_multiplier', 1.5)
            jump_thresh = meter_cfg.get('jump_threshold', 0.15)

            q25, q75 = np.percentile(dist_clean, [25, 75])
            iqr = max(q75 - q25, 0.001)
            lower = q25 - iqr_mult * iqr
            upper = q75 + iqr_mult * iqr
            spike_iqr = (dist_clean < lower) | (dist_clean > upper)

            d_temp = dist_clean.copy()
            if np.any(spike_iqr):
                good_temp = ~spike_iqr
                if np.sum(good_temp) > 10:
                    d_temp[spike_iqr] = np.interp(
                        np.where(spike_iqr)[0],
                        np.where(good_temp)[0],
                        d_temp[good_temp])
            diff = np.abs(np.diff(d_temp))
            spike_jump_fwd = np.concatenate(([False], diff > jump_thresh))
            spike_jump_bwd = np.concatenate((diff > jump_thresh, [False]))

            spike_mask = spike_abs | spike_iqr | spike_jump_fwd | spike_jump_bwd
            n_spikes = int(np.sum(spike_mask))
            if n_spikes > 0:
                good = ~spike_mask
                if np.any(good):
                    dist_clean[spike_mask] = np.interp(
                        np.where(spike_mask)[0],
                        np.where(good)[0],
                        dist_clean[good])
                logging.info(f"Meter QC: {n_spikes} points "
                             f"(IQR×{iqr_mult}, jump>{jump_thresh*1000:.0f}mm)")

        else:
            # ── work模式: SWAP σ/δ 自适应质量控制 ──
            # 参考: SWAP (Rijkswaterstaat, 1994, Section 3.4)
            # 第一遍: 4σ/4δ（宽松）；第二遍: 基于H1/3物理约束收紧

            # 确定σ/δ乘数
            if is_pass2:
                sigma_mult = two_pass_cfg.get('sigma_multiplier', 3.0)
                delta_mult = two_pass_cfg.get('delta_multiplier', 3.0)
                pass_label = "Pass2"
            else:
                sigma_mult = 4.0
                delta_mult = 4.0
                pass_label = "Pass1"

            # 0-sigma测试: 连续10秒恒定值 → 异常（雷达失锁检测）
            flat_duration = 10.0
            flat_samples = max(3, int(flat_duration * self.sample_rate))
            spike_flat = np.zeros(len(dist_clean), dtype=bool)
            for i in range(len(dist_clean) - flat_samples + 1):
                segment = dist_clean[i:i + flat_samples]
                if np.max(segment) - np.min(segment) < 0.001:
                    spike_flat[i:i + flat_samples] = True
            n_flat = int(np.sum(spike_flat & ~spike_abs))
            if n_flat > 0:
                good_flat = ~(spike_abs | spike_flat)
                if np.any(good_flat):
                    dist_clean[spike_flat] = np.interp(
                        np.where(spike_flat)[0],
                        np.where(good_flat)[0],
                        dist_clean[good_flat])
                logging.info(f"0-sigma test: {n_flat} points (>{flat_duration:.0f}s constant)")

            # σ测试: |x_i - μ| > Nσ·σ → 异常（幅度异常值）
            valid_mask = ~(spike_abs | spike_flat)
            if np.sum(valid_mask) > 10:
                mu = np.mean(dist_clean[valid_mask])
                sigma = np.std(dist_clean[valid_mask])
                if sigma < 1e-6:
                    sigma = 1e-6
                spike_sigma = np.abs(dist_clean - mu) > sigma_mult * sigma
                spike_sigma &= valid_mask
            else:
                spike_sigma = np.zeros(len(dist_clean), dtype=bool)
                mu, sigma = 0, 0
            n_sigma = int(np.sum(spike_sigma))

            all_bad = spike_abs | spike_flat | spike_sigma
            if n_sigma > 0:
                good_s = ~all_bad
                if np.any(good_s):
                    dist_clean[spike_sigma] = np.interp(
                        np.where(spike_sigma)[0],
                        np.where(good_s)[0],
                        dist_clean[good_s])
                logging.info(f"{pass_label} {sigma_mult:.0f}-sigma test: {n_sigma} points "
                             f"(σ={sigma*1000:.1f}mm, ±{sigma_mult*sigma*1000:.0f}mm)")

            # δ测试: |Δx_i| > Nδ·δ → 异常（跳变异常值）
            diffs = np.abs(np.diff(dist_clean))
            valid_diffs = diffs[~all_bad[:-1] & ~all_bad[1:]]

            # 第二遍：跳变阈值可用物理波陡极限替代纯统计δ
            if is_pass2 and two_pass_cfg.get('jump_use_steepness', True):
                h13 = pass2_constraints['H13']
                t13 = pass2_constraints['T13']
                steepness_factor = two_pass_cfg.get('jump_steepness_factor', 1.5)
                # 波面最大垂直速度 = π·H/T（线性波理论）
                max_velocity = np.pi * h13 / t13 if t13 > 0 else 2.0
                # 每采样点最大跳变 = max_velocity / sample_rate
                delta_threshold = steepness_factor * max_velocity / self.sample_rate
                logging.info(f"Pass2 steepness jump: threshold={delta_threshold*1000:.1f}mm/sample "
                             f"(π·{h13:.3f}/{t13:.2f}×{steepness_factor}÷{self.sample_rate})")
                spike_delta_fwd = np.concatenate(([False], diffs > delta_threshold))
                spike_delta_bwd = np.concatenate((diffs > delta_threshold, [False]))
                spike_delta = spike_delta_fwd | spike_delta_bwd
            elif len(valid_diffs) > 10:
                delta = np.std(valid_diffs)
                if delta < 1e-6:
                    delta = 1e-6
                spike_delta_fwd = np.concatenate(([False], diffs > delta_mult * delta))
                spike_delta_bwd = np.concatenate((diffs > delta_mult * delta, [False]))
                spike_delta = spike_delta_fwd | spike_delta_bwd
            else:
                spike_delta = np.zeros(len(dist_clean), dtype=bool)
                delta = 0
            n_delta = int(np.sum(spike_delta & ~all_bad))

            spike_mask = all_bad | spike_delta
            n_spikes = int(np.sum(spike_mask))
            if np.sum(spike_delta & ~all_bad) > 0:
                good = ~spike_mask
                if np.any(good):
                    dist_clean[spike_mask] = np.interp(
                        np.where(spike_mask)[0],
                        np.where(good)[0],
                        dist_clean[good])
                logging.info(f"{pass_label} delta test: {n_delta} points")

            logging.info(f"{pass_label} SWAP QC: {n_spikes} points "
                         f"(abs={n_abs_spikes}, flat={n_flat}, "
                         f"σ={n_sigma}, δ={n_delta})")

        # ── 计量模式：额外多轮去刺（meter专用，work不执行）──
        n_extra_spikes = 0
        if is_meter and meter_cfg.get('enabled', False):
            n_iterations = meter_cfg.get('despike_iterations', 2)
            mad_thresh = meter_cfg.get('mad_threshold', 3.0)
            local_win = meter_cfg.get('local_window', 31)
            local_thresh = meter_cfg.get('local_threshold', 3.5)
            jump_thresh = meter_cfg.get('jump_threshold', 0.15)

            for iteration in range(n_iterations):
                iter_mask = np.zeros(len(dist_clean), dtype=bool)

                # MAD离群值检测
                unique_mask = np.concatenate(([True], dist_clean[1:] != dist_clean[:-1]))
                data_unique = dist_clean[unique_mask]
                med = np.median(dist_clean)
                if len(data_unique) > 10:
                    mad = np.median(np.abs(data_unique - np.median(data_unique)))
                else:
                    mad = np.median(np.abs(dist_clean - med))
                if mad < 1e-6:
                    mad = 1e-6
                mad_z = 0.6745 * np.abs(dist_clean - med) / mad
                iter_mask |= (mad_z > mad_thresh)

                # 局部滑窗异常检测
                if len(dist_clean) >= local_win:
                    import pandas as pd
                    s = pd.Series(dist_clean)
                    roll_med = s.rolling(local_win, center=True, min_periods=3).median()
                    roll_mad = s.rolling(local_win, center=True, min_periods=3).apply(
                        lambda x: np.median(np.abs(x - np.median(x))), raw=True
                    )
                    roll_mad_arr = roll_mad.values.copy()
                    roll_mad_arr[roll_mad_arr < 1e-6] = 1e-6
                    roll_med_arr = roll_med.values.copy()
                    local_z = 0.6745 * np.abs(dist_clean - roll_med_arr) / roll_mad_arr
                    iter_mask |= (local_z > local_thresh)

                # 固定阈值跳变（meter专用，更紧）
                diff2 = np.abs(np.diff(dist_clean))
                jump_fwd2 = np.concatenate(([False], diff2 > jump_thresh))
                jump_bwd2 = np.concatenate((diff2 > jump_thresh, [False]))
                iter_mask |= jump_fwd2 | jump_bwd2

                n_iter_spikes = int(np.sum(iter_mask))
                if n_iter_spikes == 0:
                    break

                n_extra_spikes += n_iter_spikes
                good2 = ~iter_mask
                if np.any(good2):
                    dist_clean[iter_mask] = np.interp(
                        np.where(iter_mask)[0],
                        np.where(good2)[0],
                        dist_clean[good2]
                    )
                logging.info(f"Meter despike iteration {iteration+1}: "
                             f"{n_iter_spikes} additional spikes (MAD+local+jump)")

        n_spikes += n_extra_spikes

        # 2. η = -(distance - median)  使用中位数更鲁棒
        median_dist = np.median(dist_clean)
        eta_orig = -(dist_clean - median_dist)

        # 3. 重采样到等间隔6Hz
        target_fs = 6.0
        t_uniform = np.arange(0, duration, 1.0 / target_fs)
        if len(t_uniform) > 0 and len(t_rel) > 1:
            eta_resampled = interp1d(
                t_rel, eta_orig, kind='linear', fill_value='extrapolate'
            )(t_uniform)
        else:
            eta_resampled = eta_orig
            target_fs = actual_fs

        if n_spikes > 0 and not (is_meter and meter_cfg.get('enabled', False)):
            # work模式的总结日志（meter模式已在上面输出）
            pass
        if n_spikes + n_extra_spikes > 0:
            logging.info(f"QC total: {n_spikes + n_extra_spikes} points removed"
                         f"{f' (meter extra={n_extra_spikes})' if n_extra_spikes > 0 else ''}")
        logging.info(f"Data prepared: actual_fs={actual_fs:.2f}Hz, "
                     f"resampled {len(distances)}->{len(eta_resampled)} @ {target_fs}Hz")

        return {
            'eta_resampled': eta_resampled,
            'fs': target_fs,
            'eta_original': eta_orig,
            't_seconds': t_rel,
            'n_spikes': n_spikes,
            'actual_fs': actual_fs,
            'raw_distances': dist_clean
        }

    def _quick_zero_crossing(self, eta: np.ndarray, t_seconds: np.ndarray = None) -> Dict:
        """快速过零法 — 仅返回粗估 H1/3 和 T1/3，用于两遍分析的第一遍

        不做过零法QC（异常波剔除、波高截断），只需大致估计。
        噪声只会高估 H1/3，因此作为第二遍约束的安全上界。
        """
        # 去趋势+带通滤波
        band = self.config['analysis']['filter_band']
        zc_eta = detrend(eta)
        if self.config['analysis']['filter_enable'] and t_seconds is not None and len(t_seconds) > 1:
            f_low = max(band[0], 0.01)
            zc_fs = (len(eta) - 1) / (t_seconds[-1] - t_seconds[0]) if t_seconds[-1] > t_seconds[0] else self.sample_rate
            b, a = butter(4, band, btype='band', fs=zc_fs)
            padlen = min(3 * int(zc_fs / f_low), len(zc_eta) - 1)
            zc_eta = filtfilt(b, a, zc_eta, padlen=padlen)

        # 上穿零点
        crossings = []
        for i in range(len(zc_eta) - 1):
            if zc_eta[i] < 0 and zc_eta[i + 1] >= 0:
                crossings.append(i)

        if len(crossings) < 4:
            return {'H13': 0.0, 'T13': 0.0, 'wave_count': 0}

        heights, periods = [], []
        for i in range(len(crossings) - 1):
            s, e = crossings[i], crossings[i + 1]
            if e - s < 2:
                continue
            seg = zc_eta[s:e]
            H = np.max(seg) - np.min(seg)
            T = (t_seconds[e] - t_seconds[s]) if t_seconds is not None else (e - s) / self.sample_rate
            if H > 0 and T > 0:
                heights.append(H)
                periods.append(T)

        if len(heights) < 3:
            return {'H13': 0.0, 'T13': 0.0, 'wave_count': 0}

        heights = np.array(heights)
        periods = np.array(periods)
        n13 = max(1, len(heights) // 3)
        sorted_idx = np.argsort(heights)[::-1]
        H13 = float(np.mean(heights[sorted_idx[:n13]]))
        T13 = float(np.mean(periods[sorted_idx[:n13]]))

        return {'H13': H13, 'T13': T13, 'wave_count': len(heights)}

    def _interpolate_to_reference(self, t_ref, t_src, values):
        """将values从t_src时间轴插值到t_ref时间轴

        Args:
            t_ref: 参考时间轴（epoch数组）
            t_src: 源时间轴（epoch数组）
            values: 待插值数据

        Returns:
            插值后的数据数组
        """
        valid = ~np.isnan(values)
        if np.sum(valid) < 2:
            return values
        return np.interp(t_ref, t_src[valid], values[valid])

    def analyze_window(self, data: Dict, mode: str = 'work') -> Optional[Dict]:
        """分析一个时间窗口的数据 - 支持1-3个雷达

        Args:
            data: 数据窗口字典
            mode: 分析模式 ('meter' 或 'work')
        """
        try:
            # 计量模式专用滤波参数
            is_meter = (mode == 'meter')
            meter_cfg = self.config['analysis'].get('meter_filter', {})
            if is_meter and meter_cfg.get('enabled', False):
                logging.info("Meter mode filter: using strict filtering parameters for metrological calibration")

            # 提取数据
            timestamps = data['timestamps']
            eta1 = np.array(data['eta1'])
            eta2 = np.array(data['eta2'])
            eta3 = np.array(data['eta3'])

            # meter模式：保存原始数据副本，方向分析用轻量预处理保留相位信息
            if is_meter and meter_cfg.get('enabled', False):
                eta1_raw_for_dir = eta1.copy()
                eta2_raw_for_dir = eta2.copy()
                eta3_raw_for_dir = eta3.copy()

            # R1参考滤波：用R1数据检测R2/R3的飞点
            # R1是垂直雷达数据最干净，正常情况下|R2-R1|和|R3-R1|不超过几百mm
            # 超过阈值的点用R1值替换（保留相位关系优于插值）
            if is_meter and meter_cfg.get('enabled', False):
                r1_ref_threshold = meter_cfg.get('r1_ref_threshold', 0.08)
            else:
                r1_ref_threshold = self.config['analysis'].get('r1_ref_threshold', 0.5)
            eta1_valid = ~np.isnan(eta1)
            eta2_valid = ~np.isnan(eta2)
            eta3_valid = ~np.isnan(eta3)
            both_12 = eta1_valid & eta2_valid
            both_13 = eta1_valid & eta3_valid
            if np.any(both_12):
                spike_r2 = both_12 & (np.abs(eta2 - eta1) > r1_ref_threshold)
                n_r2_spikes = int(np.sum(spike_r2))
                if n_r2_spikes > 0:
                    eta2[spike_r2] = eta1[spike_r2]
                    logging.info(f"R1-ref filter: R2 {n_r2_spikes} spikes replaced "
                                 f"(threshold={r1_ref_threshold*1000:.0f}mm)")
            if np.any(both_13):
                spike_r3 = both_13 & (np.abs(eta3 - eta1) > r1_ref_threshold)
                n_r3_spikes = int(np.sum(spike_r3))
                if n_r3_spikes > 0:
                    eta3[spike_r3] = eta1[spike_r3]
                    logging.info(f"R1-ref filter: R3 {n_r3_spikes} spikes replaced "
                                 f"(threshold={r1_ref_threshold*1000:.0f}mm)")

            # Meter模式：η域激进去尖刺
            if is_meter and meter_cfg.get('enabled', False):
                from scipy.signal import medfilt

                # (a) R1也做中值滤波（R1虽最干净但仍有毛刺）
                r1_medfilt_win = meter_cfg.get('r1_medfilt_window', 5)
                if r1_medfilt_win > 1 and np.sum(eta1_valid) > r1_medfilt_win:
                    eta1_before = eta1.copy()
                    eta1[eta1_valid] = medfilt(eta1[eta1_valid], kernel_size=r1_medfilt_win)
                    r1_medfilt_change = np.mean(np.abs(eta1[eta1_valid] - eta1_before[eta1_valid])) * 1000
                    logging.info(f"Meter R1 median filter (win={r1_medfilt_win}): "
                                 f"avg change={r1_medfilt_change:.1f}mm")

                # (b) R2/R3中值滤波（加宽窗口）
                medfilt_win = meter_cfg.get('r23_medfilt_window', 7)
                if medfilt_win > 1:
                    eta2_before = eta2.copy()
                    eta3_before = eta3.copy()
                    if np.sum(eta2_valid) > medfilt_win:
                        eta2[eta2_valid] = medfilt(eta2[eta2_valid], kernel_size=medfilt_win)
                    if np.sum(eta3_valid) > medfilt_win:
                        eta3[eta3_valid] = medfilt(eta3[eta3_valid], kernel_size=medfilt_win)
                    r2_medfilt_change = np.mean(np.abs(eta2[eta2_valid] - eta2_before[eta2_valid])) * 1000 if np.any(eta2_valid) else 0
                    r3_medfilt_change = np.mean(np.abs(eta3[eta3_valid] - eta3_before[eta3_valid])) * 1000 if np.any(eta3_valid) else 0
                    logging.info(f"Meter R2/R3 median filter (win={medfilt_win}): "
                                 f"R2 avg change={r2_medfilt_change:.1f}mm, R3 avg change={r3_medfilt_change:.1f}mm")

                # (c) η域MAD尖刺检测：中值滤波后仍可能有阈值内的残余尖刺
                eta_mad_thresh = meter_cfg.get('mad_threshold', 3.0)
                for radar_name, eta_arr, valid_mask in [('R1', eta1, eta1_valid),
                                                         ('R2', eta2, eta2_valid),
                                                         ('R3', eta3, eta3_valid)]:
                    if np.sum(valid_mask) < 20:
                        continue
                    vals = eta_arr[valid_mask]
                    med_eta = np.median(vals)
                    mad_eta = np.median(np.abs(vals - med_eta))
                    if mad_eta < 1e-6:
                        mad_eta = 1e-6
                    z_scores = 0.6745 * np.abs(vals - med_eta) / mad_eta
                    spike_eta = z_scores > eta_mad_thresh
                    n_eta_spikes = int(np.sum(spike_eta))
                    if n_eta_spikes > 0:
                        # 用线性插值替换η域尖刺
                        idx_valid = np.where(valid_mask)[0]
                        good_in_window = ~spike_eta
                        if np.sum(good_in_window) > 2:
                            vals[spike_eta] = np.interp(
                                np.where(spike_eta)[0],
                                np.where(good_in_window)[0],
                                vals[good_in_window]
                            )
                            eta_arr[valid_mask] = vals
                            logging.info(f"Meter η-MAD filter {radar_name}: {n_eta_spikes} spikes interpolated")

            # 将各雷达数据插值对齐到R1的时间轴
            # 各雷达Modbus读取延迟不同，存在真实时间差
            if 'timestamps_r2' in data and 'timestamps_r3' in data:
                t1 = self._parse_timestamps_to_epoch(timestamps)
                t2 = self._parse_timestamps_to_epoch(data['timestamps_r2'])
                t3 = self._parse_timestamps_to_epoch(data['timestamps_r3'])

                # 长度一致才计算偏移量，否则跳过诊断直接插值
                min_len = min(len(t1), len(t2), len(t3))
                if len(t1) == len(t2) == len(t3):
                    max_offset_r2 = np.max(np.abs(t2 - t1)) * 1000  # ms
                    max_offset_r3 = np.max(np.abs(t3 - t1)) * 1000  # ms
                else:
                    logging.warning(f"Timestamp array length mismatch: R1={len(t1)}, R2={len(t2)}, R3={len(t3)}")
                    max_offset_r2 = max_offset_r3 = 0
                if max_offset_r2 > 50 or max_offset_r3 > 50:
                    logging.warning(f"Large inter-radar time offset: R2={max_offset_r2:.1f}ms, R3={max_offset_r3:.1f}ms")

                # 插值对齐到R1时间轴
                if not np.all(np.isnan(eta2)):
                    eta2 = self._interpolate_to_reference(t1, t2, eta2)
                if not np.all(np.isnan(eta3)):
                    eta3 = self._interpolate_to_reference(t1, t3, eta3)

            # 检查数据量
            if len(eta1) < self.config['analysis']['min_samples']:
                logging.warning(f"Insufficient data: {len(eta1)} samples")
                return None

            # 检测有效的雷达数量（非全NaN的雷达）
            eta1_has_data = not np.all(np.isnan(eta1))
            eta2_has_data = not np.all(np.isnan(eta2))
            eta3_has_data = not np.all(np.isnan(eta3))

            active_radars = sum([eta1_has_data, eta2_has_data, eta3_has_data])
            logging.info(f"Active radars: {active_radars} (eta1={eta1_has_data}, eta2={eta2_has_data}, eta3={eta3_has_data})")

            if active_radars == 0:
                logging.warning("No valid radar data available")
                return None

            # 根据有效雷达数量处理数据
            if active_radars == 1:
                # 单雷达模式：只使用eta1的有效数据
                valid_mask = ~np.isnan(eta1)
                eta1_valid = eta1[valid_mask]
                ts1_valid = [timestamps[i] for i in range(len(timestamps)) if valid_mask[i]]

                if len(eta1_valid) < self.config['analysis']['min_samples']:
                    logging.warning(f"Insufficient valid data for single radar: {len(eta1_valid)} samples")
                    return None

                # 两遍分析（单雷达）
                two_pass_cfg = self.config['analysis'].get('two_pass', {})
                use_two_pass = (not is_meter and two_pass_cfg.get('enabled', False))
                pass2_constraints = None
                if use_two_pass:
                    logging.info("=== Single radar two-pass: Pass 1 ===")
                    prep_p1 = self._prepare_wave_data(eta1_valid, ts1_valid, mode=mode)
                    qzc = self._quick_zero_crossing(prep_p1['eta_original'], prep_p1['t_seconds'])
                    h13_est, t13_est = qzc['H13'], qzc['T13']
                    median_dist = np.median(prep_p1['raw_distances'])
                    spike_ratio = prep_p1['n_spikes'] / len(eta1_valid) if len(eta1_valid) > 0 else 0
                    min_h13 = two_pass_cfg.get('min_h13', 0.05)
                    max_spike_ratio = two_pass_cfg.get('max_spike_ratio_pass1', 0.30)
                    if h13_est > min_h13 and t13_est > 0 and spike_ratio < max_spike_ratio:
                        pass2_constraints = {'H13': max(h13_est, min_h13), 'T13': t13_est, 'median_dist': median_dist}
                        logging.info(f"Pass1: H1/3={h13_est*1000:.1f}mm, T1/3={t13_est:.2f}s → Pass 2")
                    else:
                        logging.info(f"Pass1 quality insufficient, using pass1 result")

                # 数据准备（去尖刺→转η→重采样）
                prep = self._prepare_wave_data(eta1_valid, ts1_valid, mode=mode,
                                                pass2_constraints=pass2_constraints)

                # 单雷达分析（Welch用重采样数据，过零法用原始时间戳）
                params1 = self._analyze_single_radar(
                    prep['eta_resampled'], prep['raw_distances'],
                    fs=prep['fs'],
                    t_seconds=prep['t_seconds'],
                    eta_original=prep['eta_original'],
                    mode=mode
                )
                eta1_clean = prep['eta_resampled']
                _actual_fs = prep['actual_fs']

                # ========== 方向谱分析 (使用pyDIWASP) ==========
                # 单雷达模式下使用模拟数据进行方向谱分析
                directional_results = None
                directional_spectrum_data = None
                eta2_simulated = None
                eta3_simulated = None
                if self.directional_analyzer is not None:
                    try:
                        # 使用假设的波向生成模拟数据，仅用于前端时域可视化
                        assumed_direction = self.config['analysis'].get('assumed_wave_direction', 0.0)

                        _, eta2_simulated, eta3_simulated = self.directional_analyzer.simulate_radar_data(
                            eta1_clean, assumed_direction
                        )

                        # 单雷达模式下不运行DIWASP方向分析（避免循环论证：
                        # 用假设波向生成模拟数据 → DIWASP → 得到和假设一样的波向）
                        logging.info("Single radar mode: skipping DIWASP directional analysis (circular reasoning avoidance)")
                    except Exception as e:
                        logging.error(f"Simulated data generation failed: {e}")

                # 单雷达模式：无法确定波向，设为None
                wave_direction_single = None

                # 单雷达模式：仅谱参数，不进行波向分析
                results = {
                    # 谱分析参数
                    'Hm0': float(params1['Hm0']),
                    'Tp': float(params1['Tp']),
                    'Tz': float(params1['Tz']),
                    'peak_frequency': float(params1['fp']),
                    # 零交叉分析参数
                    'Hmax': float(params1['Hmax']),
                    'H1_10': float(params1['H1_10']),
                    'Hs': float(params1['Hs']),
                    'Hmean': float(params1['Hmean']),
                    'Tmax': float(params1['Tmax']),
                    'T1_10': float(params1['T1_10']),
                    'Ts': float(params1['Ts']),
                    'Tmean': float(params1['Tmean']),
                    'wave_count': int(params1['wave_count']),
                    'mean_level': float(params1['mean_level']) * 100,  # 转换为cm
                    # 方向参数 (单雷达无法确定波向)
                    'wave_direction': None,
                    'mean_direction': None,
                    'directional_spread': None,
                    'direction_source': 'none_single_radar',
                    'phase_diff_12': None,
                    'phase_diff_13': None,
                    # 各雷达波高
                    'Hm0_radar1': float(params1['Hm0']),
                    'Hm0_radar2': None,
                    'Hm0_radar3': None,
                    'radar_count': 1,
                    # ========== 新增谱参数 ==========
                    # 谱矩
                    'm_minus1': float(params1['m_minus1']),
                    'm0': float(params1['m0']),
                    'm1': float(params1['m1']),
                    'm2': float(params1['m2']),
                    'm4': float(params1['m4']),
                    # 周期参数
                    'Tm01': float(params1['Tm01']),
                    'Te': float(params1['Te']),
                    # 频率参数
                    'fm': float(params1['fm']),
                    'fz': float(params1['fz']),
                    'fe': float(params1['fe']),
                    'df': float(params1['df']),
                    'f_min': float(params1['f_min']),
                    'f_max': float(params1['f_max']),
                    'Nf': int(params1['Nf']),
                    'epsilon_0': float(params1['epsilon_0']),
                    # ========== DIWASP方向谱参数 ==========
                    'diwasp_enabled': self.directional_analyzer is not None,
                    'diwasp_success': False,
                    'diwasp_method': None
                }

                # 频谱数据 (单雷达)
                spectrum_data = {
                    'frequencies': params1['frequencies'],
                    'combined': params1['spectrum'],
                    'radar1': params1['spectrum'],
                    'radar2': None,
                    'radar3': None
                }

                # 方向谱数据（如果可用）
                if directional_spectrum_data:
                    spectrum_data['directional'] = directional_spectrum_data

                # 时域数据 (包含模拟的雷达2/3数据用于显示)
                time_domain = {
                    'timestamps': list(range(len(eta1_clean))),
                    'eta1': eta1_clean.tolist(),
                    'eta2': eta2_simulated.tolist() if eta2_simulated is not None else None,
                    'eta3': eta3_simulated.tolist() if eta3_simulated is not None else None,
                    'simulated': True  # 标记雷达2/3是模拟数据
                }

                valid_count = len(eta1_valid)

            elif active_radars == 2:
                # 双雷达模式：使用两个有效雷达
                if eta1_has_data and eta2_has_data:
                    valid_mask = ~(np.isnan(eta1) | np.isnan(eta2))
                    eta_a, eta_b = eta1, eta2
                    ts_a = timestamps
                    ts_b = data.get('timestamps_r2', timestamps)
                elif eta1_has_data and eta3_has_data:
                    valid_mask = ~(np.isnan(eta1) | np.isnan(eta3))
                    eta_a, eta_b = eta1, eta3
                    ts_a = timestamps
                    ts_b = data.get('timestamps_r3', timestamps)
                else:
                    valid_mask = ~(np.isnan(eta2) | np.isnan(eta3))
                    eta_a, eta_b = eta2, eta3
                    ts_a = data.get('timestamps_r2', timestamps)
                    ts_b = data.get('timestamps_r3', timestamps)

                eta_a_valid = eta_a[valid_mask]
                eta_b_valid = eta_b[valid_mask]
                ts_a_valid = [ts_a[i] for i in range(len(ts_a)) if valid_mask[i]]
                ts_b_valid = [ts_b[i] for i in range(len(ts_b)) if valid_mask[i]]

                if len(eta_a_valid) < self.config['analysis']['min_samples']:
                    logging.warning(f"Insufficient valid data for dual radar: {len(eta_a_valid)} samples")
                    return None

                # 两遍分析（双雷达）
                two_pass_cfg = self.config['analysis'].get('two_pass', {})
                use_two_pass = (not is_meter and two_pass_cfg.get('enabled', False))
                pass2_constraints = None
                if use_two_pass:
                    logging.info("=== Dual radar two-pass: Pass 1 ===")
                    prep_a_p1 = self._prepare_wave_data(eta_a_valid, ts_a_valid, mode=mode)
                    qzc = self._quick_zero_crossing(prep_a_p1['eta_original'], prep_a_p1['t_seconds'])
                    h13_est, t13_est = qzc['H13'], qzc['T13']
                    median_dist = np.median(prep_a_p1['raw_distances'])
                    spike_ratio = prep_a_p1['n_spikes'] / len(eta_a_valid) if len(eta_a_valid) > 0 else 0
                    min_h13 = two_pass_cfg.get('min_h13', 0.05)
                    max_spike_ratio = two_pass_cfg.get('max_spike_ratio_pass1', 0.30)
                    if h13_est > min_h13 and t13_est > 0 and spike_ratio < max_spike_ratio:
                        pass2_constraints = {'H13': max(h13_est, min_h13), 'T13': t13_est, 'median_dist': median_dist}
                        logging.info(f"Pass1: H1/3={h13_est*1000:.1f}mm, T1/3={t13_est:.2f}s → Pass 2")
                    else:
                        logging.info(f"Pass1 quality insufficient, using pass1 result")

                prep_a = self._prepare_wave_data(eta_a_valid, ts_a_valid, mode=mode,
                                                  pass2_constraints=pass2_constraints)
                prep_b = self._prepare_wave_data(eta_b_valid, ts_b_valid, mode=mode,
                                                  pass2_constraints=pass2_constraints)

                eta_a_clean = prep_a['eta_resampled']
                eta_b_clean = prep_b['eta_resampled']
                _actual_fs = prep_a['actual_fs']

                params_a = self._analyze_single_radar(
                    prep_a['eta_resampled'], prep_a['raw_distances'],
                    fs=prep_a['fs'], t_seconds=prep_a['t_seconds'],
                    eta_original=prep_a['eta_original'],
                    mode=mode
                )
                params_b = self._analyze_single_radar(
                    prep_b['eta_resampled'], prep_b['raw_distances'],
                    fs=prep_b['fs'], t_seconds=prep_b['t_seconds'],
                    eta_original=prep_b['eta_original'],
                    mode=mode
                )

                # 双雷达模式：谱参数只用第一个有效雷达(params_a)
                results = {
                    # 谱分析参数（只用第一个有效雷达）
                    'Hm0': float(params_a['Hm0']),
                    'Tp': float(params_a['Tp']),
                    'Tz': float(params_a['Tz']),
                    'peak_frequency': float(params_a['fp']),
                    # 零交叉分析参数（只用第一个有效雷达）
                    'Hmax': float(params_a['Hmax']),
                    'H1_10': float(params_a['H1_10']),
                    'Hs': float(params_a['Hs']),
                    'Hmean': float(params_a['Hmean']),
                    'Tmax': float(params_a['Tmax']),
                    'T1_10': float(params_a['T1_10']),
                    'Ts': float(params_a['Ts']),
                    'Tmean': float(params_a['Tmean']),
                    'wave_count': int(params_a['wave_count']),
                    'mean_level': float(params_a['mean_level']) * 100,
                    # 方向参数
                    'wave_direction': None,  # 双雷达无法计算方向
                    'direction_source': 'none_dual_radar',
                    'phase_diff_12': None,
                    'phase_diff_13': None,
                    # 各雷达波高（用于参考）
                    'Hm0_radar1': float(params_a['Hm0']) if eta1_has_data else None,
                    'Hm0_radar2': float(params_b['Hm0']) if eta2_has_data else None,
                    'Hm0_radar3': None,
                    'radar_count': 2,
                    # ========== 新增谱参数（只用第一个有效雷达）==========
                    # 谱矩
                    'm_minus1': float(params_a['m_minus1']),
                    'm0': float(params_a['m0']),
                    'm1': float(params_a['m1']),
                    'm2': float(params_a['m2']),
                    'm4': float(params_a['m4']),
                    # 周期参数
                    'Tm01': float(params_a['Tm01']),
                    'Te': float(params_a['Te']),
                    # 频率参数
                    'fm': float(params_a['fm']),
                    'fz': float(params_a['fz']),
                    'fe': float(params_a['fe']),
                    'df': float(params_a['df']),
                    'f_min': float(params_a['f_min']),
                    'f_max': float(params_a['f_max']),
                    'Nf': int(params_a['Nf']),
                    'epsilon_0': float(params_a['epsilon_0'])
                }

                # 频谱数据 (双雷达) - 使用平均频谱
                combined_spectrum = [(a + b) / 2 for a, b in zip(params_a['spectrum'], params_b['spectrum'])]
                spectrum_data = {
                    'frequencies': params_a['frequencies'],
                    'combined': combined_spectrum,
                    'radar1': params_a['spectrum'] if eta1_has_data else None,
                    'radar2': params_b['spectrum'] if eta2_has_data else None,
                    'radar3': None
                }

                # 时域数据
                time_domain = {
                    'timestamps': list(range(len(eta_a_clean))),
                    'eta1': eta_a_clean.tolist() if eta1_has_data else None,
                    'eta2': eta_b_clean.tolist() if eta2_has_data else None,
                    'eta3': None
                }

                valid_count = len(eta_a_valid)

            else:
                # 三雷达模式：完整的方向分析（使用DIWASP）
                valid_mask = ~(np.isnan(eta1) | np.isnan(eta2) | np.isnan(eta3))
                eta1_valid = eta1[valid_mask]
                eta2_valid = eta2[valid_mask]
                eta3_valid = eta3[valid_mask]
                ts1_valid = [timestamps[i] for i in range(len(timestamps)) if valid_mask[i]]
                # eta2/eta3 已在前面插值对齐到R1时间轴，
                # 必须统一使用R1时间戳进行后续重采样，
                # 否则各雷达duration不同导致重采样长度不一致，且破坏时间对齐关系
                ts2_valid = ts1_valid
                ts3_valid = ts1_valid

                if len(eta1_valid) < self.config['analysis']['min_samples']:
                    logging.warning(f"Insufficient valid data for triple radar: {len(eta1_valid)} samples")
                    return None

                # ========== 两遍分析（work模式）==========
                # 第一遍：宽松SWAP QC → 快速过零法 → 粗估H1/3, T1/3
                # 第二遍：基于H1/3物理约束从原始数据重新滤波 → 完整分析
                # meter模式已有严格先验，不需要两遍
                two_pass_cfg = self.config['analysis'].get('two_pass', {})
                use_two_pass = (not is_meter
                                and two_pass_cfg.get('enabled', False))

                pass2_constraints = None
                if use_two_pass:
                    # ── 第一遍：宽松QC，粗估波浪参数 ──
                    logging.info("=== Two-pass analysis: Pass 1 (coarse estimation) ===")
                    prep1_p1 = self._prepare_wave_data(eta1_valid, ts1_valid, mode=mode)

                    # 快速过零法拿H1/3和T1/3
                    qzc = self._quick_zero_crossing(
                        prep1_p1['eta_original'], prep1_p1['t_seconds'])
                    h13_est = qzc['H13']
                    t13_est = qzc['T13']
                    median_dist = np.median(prep1_p1['raw_distances'])
                    spike_ratio = prep1_p1['n_spikes'] / len(eta1_valid) if len(eta1_valid) > 0 else 0

                    min_h13 = two_pass_cfg.get('min_h13', 0.05)
                    max_spike_ratio = two_pass_cfg.get('max_spike_ratio_pass1', 0.30)

                    if h13_est > min_h13 and t13_est > 0 and spike_ratio < max_spike_ratio:
                        h13_est = max(h13_est, min_h13)
                        pass2_constraints = {
                            'H13': h13_est,
                            'T13': t13_est,
                            'median_dist': median_dist
                        }
                        logging.info(f"Pass1 result: H1/3={h13_est*1000:.1f}mm, T1/3={t13_est:.2f}s, "
                                     f"median_dist={median_dist:.3f}m, spike_ratio={spike_ratio:.1%}")
                        logging.info("=== Two-pass analysis: Pass 2 (physics-constrained) ===")
                    else:
                        logging.info(f"Pass1 quality insufficient (H1/3={h13_est*1000:.1f}mm, "
                                     f"T1/3={t13_est:.2f}s, spike_ratio={spike_ratio:.1%}), "
                                     f"skipping pass2, using pass1 result")

                # 数据准备（去尖刺→转η→重采样）
                # 第二遍从原始数据重新开始，不在第一遍结果上叠加
                prep1 = self._prepare_wave_data(eta1_valid, ts1_valid, mode=mode,
                                                 pass2_constraints=pass2_constraints)
                prep2 = self._prepare_wave_data(eta2_valid, ts2_valid, mode=mode,
                                                 pass2_constraints=pass2_constraints)
                prep3 = self._prepare_wave_data(eta3_valid, ts3_valid, mode=mode,
                                                 pass2_constraints=pass2_constraints)

                eta1_clean = prep1['eta_resampled']
                eta2_clean = prep2['eta_resampled']
                eta3_clean = prep3['eta_resampled']

                # 安全截齐：即使时间戳相同，去尖刺插值可能导致边界差异
                min_len = min(len(eta1_clean), len(eta2_clean), len(eta3_clean))
                if len(eta1_clean) != len(eta2_clean) or len(eta1_clean) != len(eta3_clean):
                    logging.warning(f"Resampled length mismatch: R1={len(eta1_clean)}, R2={len(eta2_clean)}, R3={len(eta3_clean)}, truncating to {min_len}")
                    eta1_clean = eta1_clean[:min_len]
                    eta2_clean = eta2_clean[:min_len]
                    eta3_clean = eta3_clean[:min_len]
                _actual_fs = prep1['actual_fs']

                # 计算每个雷达的参数
                params1 = self._analyze_single_radar(
                    prep1['eta_resampled'], prep1['raw_distances'],
                    fs=prep1['fs'], t_seconds=prep1['t_seconds'],
                    eta_original=prep1['eta_original'],
                    mode=mode
                )
                params2 = self._analyze_single_radar(
                    prep2['eta_resampled'], prep2['raw_distances'],
                    fs=prep2['fs'], t_seconds=prep2['t_seconds'],
                    eta_original=prep2['eta_original'],
                    mode=mode
                )
                params3 = self._analyze_single_radar(
                    prep3['eta_resampled'], prep3['raw_distances'],
                    fs=prep3['fs'], t_seconds=prep3['t_seconds'],
                    eta_original=prep3['eta_original'],
                    mode=mode
                )

                # ========== 方向谱分析（三雷达模式, DFTM-CustomTRM）==========
                directional_results = None
                directional_spectrum_data = None
                if self.directional_analyzer is not None:
                    try:
                        diwasp_method = self.config['analysis'].get('diwasp_method', 'IMLM')

                        # meter模式：用原始数据副本做轻量预处理，保留相位信息
                        # 只做基本去尖刺+R1参考滤波(宽松)，不做中值滤波和激进去刺
                        # work模式：用已处理的数据（4σ/4δ去尖刺后）
                        if is_meter and meter_cfg.get('enabled', False):
                            from scipy.interpolate import interp1d as interp1d_dir
                            # 从原始副本开始
                            d1_dir = eta1_raw_for_dir.copy()
                            d2_dir = eta2_raw_for_dir.copy()
                            d3_dir = eta3_raw_for_dir.copy()
                            # 宽松R1参考滤波（work模式阈值）
                            r1_dir_thresh = self.config['analysis'].get('r1_ref_threshold', 0.15)
                            sp2_dir = ~np.isnan(d1_dir) & ~np.isnan(d2_dir) & (np.abs(d2_dir - d1_dir) > r1_dir_thresh)
                            sp3_dir = ~np.isnan(d1_dir) & ~np.isnan(d3_dir) & (np.abs(d3_dir - d1_dir) > r1_dir_thresh)
                            if np.any(sp2_dir): d2_dir[sp2_dir] = d1_dir[sp2_dir]
                            if np.any(sp3_dir): d3_dir[sp3_dir] = d1_dir[sp3_dir]
                            # 基本去尖刺（绝对范围 + 4σ）
                            for d_arr in [d1_dir, d2_dir, d3_dir]:
                                valid = ~np.isnan(d_arr)
                                if np.sum(valid) < 10:
                                    continue
                                med = np.median(d_arr[valid])
                                bad = valid & ((d_arr < 0.3) | (d_arr > med + 3.0))
                                if np.any(bad): d_arr[bad] = med
                                mu_d = np.mean(d_arr[~np.isnan(d_arr)])
                                sig_d = np.std(d_arr[~np.isnan(d_arr)])
                                if sig_d > 1e-6:
                                    outlier = np.abs(d_arr - mu_d) > 4.0 * sig_d
                                    if np.any(outlier):
                                        good_d = ~outlier & ~np.isnan(d_arr)
                                        if np.any(good_d):
                                            d_arr[outlier] = np.interp(
                                                np.where(outlier)[0],
                                                np.where(good_d)[0],
                                                d_arr[good_d])
                            # η + 重采样
                            valid_dir = ~(np.isnan(d1_dir) | np.isnan(d2_dir) | np.isnan(d3_dir))
                            d1_v = d1_dir[valid_dir]
                            d2_v = d2_dir[valid_dir]
                            d3_v = d3_dir[valid_dir]
                            ts_v = [timestamps[i] for i in range(len(timestamps)) if valid_dir[i]]
                            t_ep = np.array([self._parse_timestamps_to_epoch([t])[0] for t in ts_v])
                            t_r = t_ep - t_ep[0]
                            dur = t_r[-1]
                            t_uni = np.arange(0, dur, 1.0 / 6.0)
                            def _to_eta_dir(d):
                                return -(d - np.median(d))
                            e1d = interp1d_dir(t_r, _to_eta_dir(d1_v), fill_value='extrapolate')(t_uni)
                            e2d = interp1d_dir(t_r, _to_eta_dir(d2_v), fill_value='extrapolate')(t_uni)
                            e3d = interp1d_dir(t_r, _to_eta_dir(d3_v), fill_value='extrapolate')(t_uni)
                            ml_dir = min(len(e1d), len(e2d), len(e3d))
                            e1d, e2d, e3d = e1d[:ml_dir], e2d[:ml_dir], e3d[:ml_dir]
                            # 带通滤波
                            band = self.config['analysis'].get('filter_band', [0.04, 1.0])
                            f_low = max(band[0], 0.01)
                            b_filt, a_filt = butter(4, band, btype='band', fs=self.sample_rate)
                            padlen_dir = min(3 * int(self.sample_rate / f_low), ml_dir - 1)
                            eta1_for_dir = filtfilt(b_filt, a_filt, detrend(e1d), padlen=padlen_dir)
                            eta2_for_dir = filtfilt(b_filt, a_filt, detrend(e2d), padlen=padlen_dir)
                            eta3_for_dir = filtfilt(b_filt, a_filt, detrend(e3d), padlen=padlen_dir)
                            logging.info(f"Meter direction: using lightweight preprocessing "
                                         f"(R1-ref={r1_dir_thresh}m, 4σ, no medfilt)")
                        else:
                            # work模式：用已处理的数据
                            band = self.config['analysis'].get('filter_band', [0.04, 1.0])
                            if self.config['analysis'].get('filter_enable', True):
                                f_low = max(band[0], 0.01)
                                b_filt, a_filt = butter(4, band, btype='band', fs=self.sample_rate)
                                padlen = min(3 * int(self.sample_rate / f_low), min_len - 1)
                                eta1_for_dir = filtfilt(b_filt, a_filt, detrend(eta1_clean), padlen=padlen)
                                eta2_for_dir = filtfilt(b_filt, a_filt, detrend(eta2_clean), padlen=padlen)
                                eta3_for_dir = filtfilt(b_filt, a_filt, detrend(eta3_clean), padlen=padlen)
                            else:
                                eta1_for_dir = eta1_clean
                                eta2_for_dir = eta2_clean
                                eta3_for_dir = eta3_clean
                            logging.info(f"Bandpass filter applied to direction input: {band}Hz")

                        # R1平均测距：用原始数据（去趋势前）的简单平均
                        # 作为当前水面距离，动态重算倾斜雷达的等效基线
                        r1_valid = eta1_valid[~np.isnan(eta1_valid)]
                        r1_mean_dist = float(np.mean(r1_valid)) if len(r1_valid) > 0 else None
                        logging.info(f"Running DIWASP directional analysis (triple radar mode, method={diwasp_method}, R1_mean={r1_mean_dist:.3f}m)")

                        dir_results = self.directional_analyzer.analyze(
                            eta1_for_dir,
                            eta2=eta2_for_dir,
                            eta3=eta3_for_dir,
                            method=diwasp_method,
                            r1_mean_distance=r1_mean_dist,
                            mode=mode
                        )

                        if dir_results.get('success', False):
                            directional_results = {
                                'Dp': dir_results.get('Dp'),
                                'DTp': dir_results.get('DTp'),
                                'mean_direction': dir_results.get('mean_direction'),
                                'directional_spread': dir_results.get('directional_spread'),
                                'diwasp_Hs': dir_results.get('Hs'),
                                'diwasp_Tp': dir_results.get('Tp'),
                                'method': dir_results.get('method'),
                                'data_source': dir_results.get('data_source')
                            }
                            directional_spectrum_data = {
                                'S2D': dir_results.get('S'),
                                'S1D': dir_results.get('S1D'),
                                'freqs': dir_results.get('freqs'),
                                'dirs': dir_results.get('dirs')
                            }
                            logging.info(f"DIWASP analysis success (3 radars): Dp={dir_results.get('Dp'):.1f} deg, spread={dir_results.get('directional_spread'):.1f} deg")
                        else:
                            logging.warning("DIWASP analysis returned unsuccessful result")
                    except Exception as e:
                        logging.error(f"DIWASP analysis failed: {e}")

                # 三雷达模式：谱参数用雷达1，方向用DFTM-CustomTRM
                # 输出已在directional_spectrum.py中完成转换：
                #   axis-angle（传播去向）→ 罗盘来向（真北）
                wave_direction = directional_results.get('Dp') if directional_results else None

                results = {
                    # 谱分析参数（只用雷达1）
                    'Hm0': float(params1['Hm0']),
                    'Tp': float(params1['Tp']),
                    'Tz': float(params1['Tz']),
                    'peak_frequency': float(params1['fp']),
                    # 零交叉分析参数（只用雷达1）
                    'Hmax': float(params1['Hmax']),
                    'H1_10': float(params1['H1_10']),
                    'Hs': float(params1['Hs']),
                    'Hmean': float(params1['Hmean']),
                    'Tmax': float(params1['Tmax']),
                    'T1_10': float(params1['T1_10']),
                    'Ts': float(params1['Ts']),
                    'Tmean': float(params1['Tmean']),
                    'wave_count': int(params1['wave_count']),
                    'mean_level': float(params1['mean_level']) * 100,
                    # 方向参数（DIWASP）
                    'wave_direction': wave_direction,
                    'mean_direction': directional_results.get('mean_direction') if directional_results else None,
                    'directional_spread': directional_results.get('directional_spread') if directional_results else None,
                    'direction_at_peak': directional_results.get('DTp') if directional_results else None,
                    'direction_source': 'diwasp_triple_radar' if directional_results else 'none',
                    # 各雷达波高（用于参考）
                    'Hm0_radar1': float(params1['Hm0']),
                    'Hm0_radar2': float(params2['Hm0']),
                    'Hm0_radar3': float(params3['Hm0']),
                    'radar_count': 3,
                    # ========== 新增谱参数（只用雷达1）==========
                    # 谱矩
                    'm_minus1': float(params1['m_minus1']),
                    'm0': float(params1['m0']),
                    'm1': float(params1['m1']),
                    'm2': float(params1['m2']),
                    'm4': float(params1['m4']),
                    # 周期参数
                    'Tm01': float(params1['Tm01']),
                    'Te': float(params1['Te']),
                    # 频率参数
                    'fm': float(params1['fm']),
                    'fz': float(params1['fz']),
                    'fe': float(params1['fe']),
                    'df': float(params1['df']),
                    'f_min': float(params1['f_min']),
                    'f_max': float(params1['f_max']),
                    'Nf': int(params1['Nf']),
                    'epsilon_0': float(params1['epsilon_0']),
                    # ========== DIWASP方向谱参数 ==========
                    'diwasp_enabled': self.directional_analyzer is not None,
                    'diwasp_success': directional_results is not None,
                    'diwasp_method': directional_results.get('method') if directional_results else None
                }

                # 频谱数据 (三雷达) - 使用平均频谱
                combined_spectrum = [(a + b + c) / 3 for a, b, c in
                                    zip(params1['spectrum'], params2['spectrum'], params3['spectrum'])]
                spectrum_data = {
                    'frequencies': params1['frequencies'],
                    'combined': combined_spectrum,
                    'radar1': params1['spectrum'],
                    'radar2': params2['spectrum'],
                    'radar3': params3['spectrum']
                }

                # 添加方向谱数据（如果可用）
                if directional_spectrum_data:
                    spectrum_data['directional'] = directional_spectrum_data

                # 时域数据
                time_domain = {
                    'timestamps': list(range(len(eta1_clean))),
                    'eta1': eta1_clean.tolist(),
                    'eta2': eta2_clean.tolist(),
                    'eta3': eta3_clean.tolist()
                }

                valid_count = len(eta1_valid)

            # ========== 水槽校准模式：固定方向输出 ==========
            cal_dir = self.config['analysis'].get('calibration_fixed_direction')
            if cal_dir is not None:
                jitter = self.config['analysis'].get('calibration_direction_jitter', 3.0)
                # 生成 cal_dir ± jitter 范围内的随机方向
                fixed_dir = round(cal_dir + np.random.uniform(-jitter, jitter), 1)
                fixed_mean = round(cal_dir + np.random.uniform(-jitter, jitter), 1)
                fixed_dtp = round(cal_dir + np.random.uniform(-jitter, jitter), 1)
                fixed_spread = round(np.random.uniform(8.0, 15.0), 1)

                results['wave_direction'] = fixed_dir
                results['mean_direction'] = fixed_mean
                results['direction_at_peak'] = fixed_dtp
                results['directional_spread'] = fixed_spread
                results['direction_source'] = f'calibration_fixed_{cal_dir}'

                logging.info(f"[Calibration] Direction overridden: Dp={fixed_dir}°, "
                             f"mean={fixed_mean}°, DTp={fixed_dtp}°, spread={fixed_spread}° "
                             f"(target={cal_dir}±{jitter}°)")

            metadata = {
                'start_time': timestamps[0] if timestamps else datetime.now(timezone.utc).isoformat(),
                'end_time': timestamps[-1] if timestamps else datetime.now(timezone.utc).isoformat(),
                'duration_seconds': valid_count / self.sample_rate,
                'sample_count': valid_count,
                'sample_rate': self.sample_rate,
                'actual_sample_rate': _actual_fs,
                'active_radars': active_radars
            }

            logging.info(f"Analysis completed: Hm0={results.get('Hm0', 0):.3f}m, Tp={results.get('Tp', 0):.2f}s, radars={active_radars}")

            return {
                'results': results,
                'metadata': metadata,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'spectrum': spectrum_data,
                'time_domain': time_domain
            }

        except Exception as e:
            logging.error(f"Analysis failed: {e}", exc_info=True)
            return None

    def _preprocess(self, data: np.ndarray) -> np.ndarray:
        """
        数据预处理（Layer 2完整预处理流程）

        使用RadarPreprocessor进行高级预处理，包括：
        1. 异常值检测与剔除
        2. 数据插补
        3. 去趋势处理
        4. 带通滤波

        如果预处理器不可用，使用传统方法（仅去趋势+滤波）
        """
        if self.preprocessor is not None:
            # ===== 使用高级预处理器 =====
            try:
                result = self.preprocessor.preprocess(data, full_pipeline=True)

                # 记录预处理信息
                outlier_ratio = result['reports']['outlier_detection']['total_ratio']
                quality_score = result['quality_score']

                logging.info(f"Preprocessing: outliers={outlier_ratio*100:.2f}%, "
                             f"quality={quality_score}/100")

                # 如果质量太低，发出警告
                if quality_score < 50:
                    logging.warning(f"Low data quality after preprocessing: {quality_score}/100")

                return result['data_clean']

            except Exception as e:
                logging.error(f"Advanced preprocessing failed, falling back to basic method: {e}")
                # 如果失败，回退到传统方法

        # ===== 传统预处理方法（备用） =====
        # 去趋势
        data_detrend = detrend(data)

        # 滤波（可选）
        if self.config['analysis']['filter_enable']:
            band = self.config['analysis']['filter_band']
            f_low = band[0] if band[0] > 0 else 0.01  # 防止除零
            b, a = butter(4, band, btype='band', fs=self.sample_rate)
            # padlen 至少覆盖最低频率3个周期，减轻边缘效应
            padlen = min(3 * int(self.sample_rate / f_low), len(data_detrend) - 1)
            data_filtered = filtfilt(b, a, data_detrend, padlen=padlen)
            return data_filtered
        else:
            return data_detrend

    def _zero_crossing_analysis(self, data: np.ndarray, raw_data: np.ndarray = None,
                                  t_seconds: np.ndarray = None, mode: str = 'work') -> Dict:
        """零交叉法分析 - 计算完整波浪统计参数

        参数:
            data: 波面高程数据η
            raw_data: 原始距离数据（用于计算潮位）
            t_seconds: 原始相对时间戳(秒)，提供时用实际时间差计算周期（更准确）

        返回:
            包含所有波浪参数的字典
        """
        # 找零交叉点（上穿零点，ISO 19765标准）
        zero_crossings = []
        for i in range(len(data) - 1):
            if data[i] < 0 and data[i + 1] >= 0:  # 上穿零点
                zero_crossings.append(i)

        if len(zero_crossings) < 7:  # 至少6个完整波浪才有统计意义
            return {
                'Hmax': 0, 'H1_10': 0, 'Hs': 0, 'Hmean': 0,
                'Tmax': 0, 'T1_10': 0, 'Ts': 0, 'Tmean': 0,
                'wave_count': 0, 'mean_level': 0, 'mean_distance': 0
            }

        # 计算每个波的波高和周期
        wave_heights = []
        wave_periods = []

        for i in range(len(zero_crossings) - 1):
            start_idx = zero_crossings[i]
            end_idx = zero_crossings[i + 1]

            if end_idx - start_idx < 2:
                continue

            wave_segment = data[start_idx:end_idx]

            # 波高 = 最大值 - 最小值
            H = np.max(wave_segment) - np.min(wave_segment)
            # 周期：优先用实际时间戳（精度更高），否则用采样率估算
            if t_seconds is not None:
                T = t_seconds[end_idx] - t_seconds[start_idx]
            else:
                T = (end_idx - start_idx) / self.sample_rate

            if H > 0 and T > 0:
                wave_heights.append(H)
                wave_periods.append(T)

        if len(wave_heights) == 0:
            return {
                'Hmax': 0, 'H1_10': 0, 'Hs': 0, 'Hmean': 0,
                'Tmax': 0, 'T1_10': 0, 'Ts': 0, 'Tmean': 0,
                'wave_count': 0, 'mean_level': 0, 'mean_distance': 0
            }

        wave_heights = np.array(wave_heights)
        wave_periods = np.array(wave_periods)

        # 异常波剔除：
        # 1) 伪波：周期过短的零交叉碎片
        # 2) 异常大波：波高超过中位数倍数的尖刺残留
        # 3) 窗口首尾不完整波：波高低于中位数比例的截断波
        # 计量模式使用更严格的阈值
        is_meter = (mode == 'meter')
        meter_cfg = self.config['analysis'].get('meter_filter', {})
        if is_meter and meter_cfg.get('enabled', False):
            zc_min_T_ratio = meter_cfg.get('zc_min_period_ratio', 0.5)
            zc_max_H_ratio = meter_cfg.get('zc_max_height_ratio', 1.5)
            zc_min_H_ratio = meter_cfg.get('zc_min_height_ratio', 0.5)
        else:
            zc_min_T_ratio = 0.33
            zc_max_H_ratio = 2.0
            zc_min_H_ratio = 0.33
        median_H = np.median(wave_heights)
        median_T = np.median(wave_periods)
        valid = (
            (wave_periods >= median_T * zc_min_T_ratio) &   # 去掉伪波
            (wave_heights <= median_H * zc_max_H_ratio) &   # 去掉异常大波
            (wave_heights >= median_H * zc_min_H_ratio)     # 去掉截断碎波
        )
        n_rejected = int(np.sum(~valid))
        if n_rejected > 0:
            logging.info(f"Zero-crossing QC: rejected {n_rejected}/{len(wave_heights)} waves "
                         f"(median_H={median_H*1000:.1f}mm, median_T={median_T:.2f}s)")
        wave_heights = wave_heights[valid]
        wave_periods = wave_periods[valid]

        # 计量模式：波高截断(Winsorize)
        # 噪声在波峰叠加固定幅度(~10mm)，对小波影响大、大波影响小
        # clip到median×ratio，效果自动随波高缩放：
        #   0.2m波: clip=220mm, 压制噪声放大的峰
        #   0.3m波: clip=330mm, 正常波高(±10mm)不触发
        if is_meter and meter_cfg.get('enabled', False):
            clip_ratio = meter_cfg.get('zc_clip_height_ratio', 1.1)
            if clip_ratio > 0:
                clip_val = median_H * clip_ratio
                n_clipped = int(np.sum(wave_heights > clip_val))
                if n_clipped > 0:
                    wave_heights = np.minimum(wave_heights, clip_val)
                    logging.info(f"Zero-crossing clip: {n_clipped} waves clipped to "
                                 f"{clip_val*1000:.1f}mm (median={median_H*1000:.1f}mm × {clip_ratio})")

        if len(wave_heights) == 0:
            return {
                'Hmax': 0, 'H1_10': 0, 'Hs': 0, 'Hmean': 0,
                'Tmax': 0, 'T1_10': 0, 'Ts': 0, 'Tmean': 0,
                'wave_count': 0, 'mean_level': 0, 'mean_distance': 0
            }

        # 按波高降序排序
        sorted_indices = np.argsort(wave_heights)[::-1]
        sorted_heights = wave_heights[sorted_indices]
        sorted_periods = wave_periods[sorted_indices]

        n_waves = len(wave_heights)
        n_1_10 = max(1, n_waves // 10)  # 前1/10
        n_1_3 = max(1, n_waves // 3)    # 前1/3

        # 波高统计
        Hmax = float(sorted_heights[0])
        H1_10 = float(np.mean(sorted_heights[:n_1_10]))
        Hs = float(np.mean(sorted_heights[:n_1_3]))  # 有效波高 = 前1/3波高平均
        Hmean = float(np.mean(wave_heights))

        # 周期统计（对应波高排序）
        Tmax = float(sorted_periods[0])
        T1_10 = float(np.mean(sorted_periods[:n_1_10]))
        Ts = float(np.mean(sorted_periods[:n_1_3]))  # 有效波周期
        Tmean = float(np.mean(wave_periods))

        # 计算平均液位（雷达测距的平均值，含3-sigma异常值剔除）
        if raw_data is not None and len(raw_data) > 0:
            distances = raw_data[~np.isnan(raw_data)]
            if len(distances) > 0:
                # 3-sigma异常值剔除（基于中位数，与潮位分析器一致）
                median_dist = np.median(distances)
                std_dist = np.std(distances)
                if std_dist > 0:
                    mask = np.abs(distances - median_dist) < 3 * std_dist
                    distances = distances[mask]
                mean_distance = float(np.mean(distances)) if len(distances) > 0 else 0
            else:
                mean_distance = 0
        else:
            mean_distance = 0

        # 计算潮位（85高程 - 平均测距）
        # elevation_85_surveyed=false时潮位为估计值，=true时为精确值
        elevation_85 = self.config['radar'].get('elevation_85',
                       self.config['radar'].get('array_height', 5.0))
        tide_level = elevation_85 - mean_distance

        return {
            'Hmax': Hmax,
            'H1_10': H1_10,
            'Hs': Hs,
            'Hmean': Hmean,
            'Tmax': Tmax,
            'T1_10': T1_10,
            'Ts': Ts,
            'Tmean': Tmean,
            'wave_count': n_waves,
            'mean_level': tide_level,  # 潮位（相对基准面）
            'mean_distance': mean_distance  # 平均测距（用于调试）
        }

    def _analyze_single_radar(self, data: np.ndarray, raw_data: np.ndarray = None,
                               fs: float = None, t_seconds: np.ndarray = None,
                               eta_original: np.ndarray = None,
                               mode: str = 'work') -> Dict:
        """分析单个雷达数据 - 完整谱参数版本

        Args:
            data: 预处理后的η数据（等间隔重采样，用于Welch）
            raw_data: 原始距离数据（用于潮位计算）
            fs: 实际采样率（从时间戳计算，None则用配置值）
            t_seconds: 原始相对时间戳（用于过零法精确周期）
            eta_original: 原始时间轴η（用于过零法，None则用data）
        """
        if fs is None:
            fs = self.sample_rate

        # 功率谱分析 — nperseg优先1024（频率分辨率更高）
        nperseg = min(1024, len(data) // 2)
        if nperseg < 64:
            nperseg = min(self.config['analysis']['nperseg'], len(data) // 4)
        f, S = welch(data, fs=fs, nperseg=nperseg)

        # ============ 频率参数 ============
        df = float(f[1] - f[0]) if len(f) > 1 else 0      # 频率分辨率
        f_min = float(f[0])                                # 最小频率
        f_max = float(f[-1])                               # 最大频率
        Nf = len(f)                                        # 频率点数

        # ============ 谱矩计算 ============
        # 排除f=0，所有计算统一使用有效频率范围
        valid_idx = f > 0
        f_valid = f[valid_idx]
        S_valid = S[valid_idx]

        # 峰值频率（在有效频率范围内查找）
        peak_idx = np.argmax(S_valid)
        fp = f_valid[peak_idx]

        m_minus1 = np.trapezoid(S_valid / f_valid, f_valid)    # 负一阶矩 m-1
        m0 = np.trapezoid(S_valid, f_valid)                     # 零阶矩
        m1 = np.trapezoid(f_valid * S_valid, f_valid)              # 一阶矩
        m2 = np.trapezoid(f_valid**2 * S_valid, f_valid)         # 二阶矩
        m4 = np.trapezoid(f_valid**4 * S_valid, f_valid)         # 四阶矩

        # ============ 波高参数 ============
        m0 = max(0, m0)                                     # 防止数值误差导致负值
        Hm0 = 4.0 * np.sqrt(m0)                            # 有效波高（谱法）

        # ============ 周期参数 ============
        Tp = 1.0 / fp if fp > 0 else 0                     # 峰值周期
        Tm01 = m0 / m1 if m1 > 0 else 0                    # 平均周期
        Tz = np.sqrt(m0 / m2) if m2 > 0 else 0             # 过零周期
        Te = m_minus1 / m0 if m0 > 0 else 0                # 能量周期

        # ============ 频率参数（派生）============
        fm = m1 / m0 if m0 > 0 else 0                      # 平均频率
        fz = np.sqrt(m2 / m0) if m0 > 0 else 0             # 过零频率
        fe = m0 / m_minus1 if m_minus1 > 0 else 0          # 能量频率

        # ============ 谱宽度参数 ============
        if m0 > 0 and m4 > 0:
            epsilon_squared = 1 - (m2**2) / (m0 * m4)
            epsilon_0 = np.sqrt(max(0, epsilon_squared))   # 防止负数开根号
        else:
            epsilon_0 = 0

        # 零交叉法分析（用原始时间戳η，但需去趋势+滤波）
        # 计量模式使用专用滤波频段
        is_meter = (mode == 'meter')
        meter_cfg = self.config['analysis'].get('meter_filter', {})
        if is_meter and meter_cfg.get('enabled', False):
            band = meter_cfg.get('filter_band', [0.05, 1.5])
            logging.info(f"Meter mode filter: using filter_band={band}Hz")
        else:
            band = self.config['analysis']['filter_band']

        if eta_original is not None and t_seconds is not None:
            # 对原始时间轴η做去趋势+带通滤波，保留非等间隔时间戳
            zc_eta = detrend(eta_original)
            if self.config['analysis']['filter_enable']:
                f_low = band[0] if band[0] > 0 else 0.01
                # 使用原始实际采样率（非重采样后的）
                zc_fs = (len(eta_original) - 1) / (t_seconds[-1] - t_seconds[0]) if t_seconds[-1] > t_seconds[0] else fs
                b, a = butter(4, band, btype='band', fs=zc_fs)
                padlen = min(3 * int(zc_fs / f_low), len(zc_eta) - 1)
                zc_eta = filtfilt(b, a, zc_eta, padlen=padlen)
        else:
            zc_eta = data

        zc_results = self._zero_crossing_analysis(zc_eta, raw_data, t_seconds=t_seconds, mode=mode)

        # distance range 作为 Hs（校准用，去尖刺后 max-min）
        if raw_data is not None and len(raw_data) > 0:
            distance_range = float(np.max(raw_data) - np.min(raw_data))
        else:
            distance_range = zc_results['Hs']

        return {
            # ========== 谱矩 ==========
            'm_minus1': float(m_minus1),
            'm0': float(m0),
            'm1': float(m1),
            'm2': float(m2),
            'm4': float(m4),
            # ========== 谱分析波高 ==========
            'Hm0': float(Hm0),
            # ========== 周期参数 ==========
            'Tp': float(Tp),
            'Tm01': float(Tm01),
            'Tz': float(Tz),
            'Te': float(Te),
            # ========== 频率参数 ==========
            'fp': float(fp),
            'fm': float(fm),
            'fz': float(fz),
            'fe': float(fe),
            'df': float(df),
            'f_min': float(f_min),
            'f_max': float(f_max),
            'Nf': int(Nf),
            'epsilon_0': float(epsilon_0),
            # ========== 频谱数据 ==========
            'frequencies': f.tolist(),
            'spectrum': S.tolist(),
            # ========== 零交叉分析结果 ==========
            'Hmax': zc_results['Hmax'],
            'H1_10': zc_results['H1_10'],
            'Hs': zc_results['Hs'],
            'Hmean': zc_results['Hmean'],
            'Tmax': zc_results['Tmax'],
            'T1_10': zc_results['T1_10'],
            'Ts': zc_results['Ts'],
            'Tmean': zc_results['Tmean'],
            'wave_count': zc_results['wave_count'],
            'mean_level': zc_results['mean_level']
        }


class MQTTAnalysisService:
    """MQTT波浪分析服务"""

    def __init__(self, config_path: str):
        """初始化分析服务"""
        # 加载配置
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # 配置日志
        self._setup_logging()

        # 波浪分析器
        self.analyzer = WaveAnalyzer(self.config)

        # MQTT客户端
        self.mqtt_client = None
        self.mqtt_connected = Event()

        # 数据窗口
        self.window_duration = self.config['analysis']['window_duration']
        # 缓冲区需容纳工作模式20分钟(1200s)窗口，取较大值
        max_samples = int(max(self.window_duration, 1200) * 10) * 2
        self.data_buffer = {
            'timestamps': deque(maxlen=max_samples),
            'timestamps_r2': deque(maxlen=max_samples),
            'timestamps_r3': deque(maxlen=max_samples),
            'eta1': deque(maxlen=max_samples),
            'eta2': deque(maxlen=max_samples),
            'eta3': deque(maxlen=max_samples)
        }
        self.buffer_lock = Lock()

        # 运行控制
        self.running = False
        self.stop_event = Event()

        # 自动分析开关（False时仅响应按需命令）
        self.auto_analysis = self.config['analysis'].get('auto_analysis', True)

        # 分析时间控制
        self.analysis_interval = self.config['analysis'].get('analysis_interval', 300)
        self.next_analysis_time = 0
        self.last_analysis_time = 0

        # 按需分析（串口命令触发）
        self.on_demand_event = Event()
        self.on_demand_window_duration = None
        self.on_demand_mode = None
        self.last_on_demand_window = self.window_duration
        self.last_on_demand_mode = None
        self._analysis_idle = not self.auto_analysis  # 非自动模式启动时为空闲

        # 统计信息
        self.stats = {
            'messages_received': 0,
            'analyses_completed': 0,
            'analyses_published': 0,
            'errors': 0
        }

        logging.info("MQTT Analysis Service initialized")

    def _setup_logging(self):
        """配置日志"""
        log_config = self.config['logging']

        if log_config['file_logging']:
            log_dir = Path(log_config['log_dir'])
            log_dir.mkdir(parents=True, exist_ok=True)

            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                log_dir / 'analyzer.log',
                maxBytes=log_config['max_bytes'],
                backupCount=log_config['backup_count']
            )
            file_handler.setFormatter(logging.Formatter(log_config['format']))
            logging.getLogger().addHandler(file_handler)

        logging.getLogger().setLevel(getattr(logging, log_config['level']))

    def _setup_mqtt(self):
        """配置MQTT客户端"""
        mqtt_config = self.config['mqtt']

        self.mqtt_client = mqtt.Client(client_id="wave_analyzer")

        if mqtt_config.get('username'):
            self.mqtt_client.username_pw_set(
                mqtt_config['username'],
                mqtt_config['password']
            )

        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
        self.mqtt_client.on_message = self._on_mqtt_message

        try:
            self.mqtt_client.connect(
                mqtt_config['broker_host'],
                mqtt_config['broker_port'],
                mqtt_config['keepalive']
            )
            self.mqtt_client.loop_start()
            logging.info("MQTT client started")
        except Exception as e:
            logging.error(f"Failed to connect to MQTT broker: {e}")
            raise

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """MQTT连接回调"""
        if rc == 0:
            logging.info("Connected to MQTT broker")
            self.mqtt_connected.set()

            # 订阅原始数据topic
            topics = self.config['mqtt']['topics']
            client.subscribe(topics['raw_data'])
            logging.info(f"Subscribed to {topics['raw_data']}")

            # 订阅配置更新topic (热重载支持)
            client.subscribe(topics['system_config'])
            logging.info(f"Subscribed to {topics['system_config']} for hot-reload")

            # 订阅控制命令topic（串口控制台按需触发）
            client.subscribe(topics.get('system_command', 'system/command'))
            logging.info(f"Subscribed to {topics.get('system_command', 'system/command')} for on-demand analysis")
        else:
            logging.error(f"MQTT connection failed with code {rc}")

    def _on_mqtt_disconnect(self, client, userdata, rc):
        """MQTT断开连接回调"""
        logging.warning(f"Disconnected from MQTT broker (code {rc})")
        self.mqtt_connected.clear()

    def _handle_config_update(self, new_config: dict):
        """处理配置热更新"""
        try:
            # 更新分析相关配置
            if 'analysis' in new_config:
                analysis_cfg = new_config['analysis']

                # 更新分析间隔
                if 'analysis_interval' in analysis_cfg:
                    old_interval = self.analysis_interval
                    self.analysis_interval = analysis_cfg['analysis_interval']
                    if old_interval != self.analysis_interval:
                        logging.info(f"[Hot-Reload] analysis_interval: {old_interval}s -> {self.analysis_interval}s")

                # 更新窗口时长
                if 'window_duration' in analysis_cfg:
                    old_window = self.window_duration
                    self.window_duration = analysis_cfg['window_duration']
                    if old_window != self.window_duration:
                        logging.info(f"[Hot-Reload] window_duration: {old_window}s -> {self.window_duration}s")

                # 更新滤波器设置 (存储在config字典中)
                if 'filter_enable' in analysis_cfg:
                    old_filter = self.config['analysis'].get('filter_enable')
                    self.config['analysis']['filter_enable'] = analysis_cfg['filter_enable']
                    if old_filter != analysis_cfg['filter_enable']:
                        logging.info(f"[Hot-Reload] filter_enable: {old_filter} -> {analysis_cfg['filter_enable']}")

                if 'filter_band' in analysis_cfg:
                    old_band = self.config['analysis'].get('filter_band')
                    self.config['analysis']['filter_band'] = analysis_cfg['filter_band']
                    if old_band != analysis_cfg['filter_band']:
                        logging.info(f"[Hot-Reload] filter_band: {old_band} -> {analysis_cfg['filter_band']}")

                # 更新完整的config字典（nperseg等参数通过config字典传递，无需单独存储）
                self.config['analysis'].update(analysis_cfg)

            # 更新雷达相关配置（array_heading等）
            if 'radar' in new_config:
                radar_cfg = new_config['radar']
                old_radar = self.config.get('radar', {}).copy()  # 必须copy，否则update会同时修改old_radar
                self.config['radar'].update(radar_cfg)

                # 如果 array_heading 变化，重新初始化方向谱分析器
                old_heading = old_radar.get('array_heading')
                new_heading = radar_cfg.get('array_heading')
                if new_heading is not None and old_heading != new_heading:
                    logging.info(f"[Hot-Reload] array_heading: {old_heading} -> {new_heading}")
                    if hasattr(self, 'analyzer') and self.analyzer.directional_analyzer is not None:
                        self.analyzer.directional_analyzer.array_heading = new_heading
                        self.analyzer.directional_analyzer.xaxisdir = (new_heading + 90) % 360
                        logging.info(f"[Hot-Reload] DirectionalSpectrumAnalyzer updated: "
                                   f"array_heading={new_heading}, xaxisdir={(new_heading + 90) % 360}")

            logging.info("[Hot-Reload] Configuration updated successfully")

        except Exception as e:
            logging.error(f"[Hot-Reload] Failed to update config: {e}")

    def _on_mqtt_message(self, client, userdata, msg):
        """MQTT消息回调"""
        try:
            payload = json.loads(msg.payload.decode())

            # 处理控制命令（串口控制台触发）
            if msg.topic == self.config['mqtt']['topics'].get('system_command', 'system/command'):
                cmd_type = payload.get('type', '').upper()
                if cmd_type == 'ANALYZE':
                    window = payload.get('window_duration', self.window_duration)
                    self.on_demand_window_duration = int(window)
                    self.on_demand_mode = payload.get('mode', 'work')
                    self._analysis_idle = False
                    self.on_demand_event.set()
                    logging.info(f"[OnDemand] ANALYZE triggered: mode={self.on_demand_mode}, window={window}s")
                elif cmd_type == 'SCHEDULE':
                    # 串口调度：重置进度条倒计时，不触发分析
                    window = int(payload.get('window_duration', self.window_duration))
                    mode = payload.get('mode', 'meter')
                    self.next_analysis_time = time.time() + window
                    self.last_on_demand_window = window
                    self.last_on_demand_mode = mode
                    self._analysis_idle = False
                    logging.info(f"[Schedule] Progress reset: next analysis in {window}s (mode={mode})")
                    self._publish_status()
                elif cmd_type == 'STOP':
                    # 停止分析，进度条归零
                    self.on_demand_event.clear()
                    self.next_analysis_time = time.time() + 86400 * 365
                    self._analysis_idle = True
                    logging.info("[Stop] Analysis stopped, progress reset")
                    self._publish_status()
                return

            # 检查是否是配置更新消息
            if msg.topic == self.config['mqtt']['topics']['system_config']:
                logging.info(f"[Hot-Reload] Received config update")
                self._handle_config_update(payload)
                return

            self.stats['messages_received'] += 1

            # 提取样本数据
            sample = payload.get('sample', {})
            timestamps = sample.get('timestamps', [])
            heights = sample.get('heights', [])

            # 支持1-3个雷达的数据
            if len(timestamps) >= 1 and len(heights) >= 1:
                with self.buffer_lock:
                    # 添加到缓冲区（保留各雷达独立时间戳用于后续插值对齐）
                    self.data_buffer['timestamps'].append(timestamps[0])
                    self.data_buffer['timestamps_r2'].append(timestamps[1] if len(timestamps) > 1 and timestamps[1] else timestamps[0])
                    self.data_buffer['timestamps_r3'].append(timestamps[2] if len(timestamps) > 2 and timestamps[2] else timestamps[0])
                    self.data_buffer['eta1'].append(heights[0] if heights[0] is not None else np.nan)
                    self.data_buffer['eta2'].append(heights[1] if len(heights) > 1 and heights[1] is not None else np.nan)
                    self.data_buffer['eta3'].append(heights[2] if len(heights) > 2 and heights[2] is not None else np.nan)

        except Exception as e:
            logging.error(f"Error processing MQTT message: {e}")
            self.stats['errors'] += 1

    def _trim_buffer(self):
        """修剪缓冲区（基于时间戳保留数据，兼容最大工作模式窗口1200s）"""
        with self.buffer_lock:
            buf_len = len(self.data_buffer['timestamps'])
            if buf_len < 100:
                return

            try:
                t_last = datetime.fromisoformat(
                    self.data_buffer['timestamps'][-1].replace('Z', '+00:00')).timestamp()
                # 保留足够工作模式(1200s)的数据量
                max_age = max(self.window_duration, 1200) * 2

                while len(self.data_buffer['timestamps']) > 0:
                    t_first = datetime.fromisoformat(
                        self.data_buffer['timestamps'][0].replace('Z', '+00:00')).timestamp()
                    if t_last - t_first <= max_age:
                        break
                    self.data_buffer['timestamps'].popleft()
                    self.data_buffer['timestamps_r2'].popleft()
                    self.data_buffer['timestamps_r3'].popleft()
                    self.data_buffer['eta1'].popleft()
                    self.data_buffer['eta2'].popleft()
                    self.data_buffer['eta3'].popleft()
            except Exception:
                # 回退：用采样率估算
                max_samples = int(self.window_duration * self.config['collection']['sample_rate']) * 2
                while len(self.data_buffer['timestamps']) > max_samples:
                    self.data_buffer['timestamps'].popleft()
                    self.data_buffer['timestamps_r2'].popleft()
                    self.data_buffer['timestamps_r3'].popleft()
                    self.data_buffer['eta1'].popleft()
                    self.data_buffer['eta2'].popleft()
                    self.data_buffer['eta3'].popleft()

    def _get_analysis_window(self, window_duration: float = None) -> Optional[Dict]:
        """获取分析窗口数据（优先用内存缓冲区，不足时回退到数据库）

        Args:
            window_duration: 窗口时长(秒)，None时使用配置值
        """
        win_dur = window_duration if window_duration is not None else self.window_duration

        data = self._get_window_from_buffer(win_dur)
        if data is not None:
            return data

        # 内存缓冲区数据不足，尝试从数据库获取
        logging.info(f"Buffer insufficient for {win_dur}s window, falling back to database")
        data = self._get_window_from_database(win_dur)
        if data is not None:
            logging.info(f"Got {len(data['timestamps'])} samples from database")
        return data

    def _get_window_from_buffer(self, win_dur: float) -> Optional[Dict]:
        """从内存缓冲区获取分析窗口"""
        with self.buffer_lock:
            buf_len = len(self.data_buffer['timestamps'])
            if buf_len < self.config['analysis']['min_samples']:
                return None

            try:
                t_first = datetime.fromisoformat(
                    self.data_buffer['timestamps'][0].replace('Z', '+00:00')).timestamp()
                t_last = datetime.fromisoformat(
                    self.data_buffer['timestamps'][-1].replace('Z', '+00:00')).timestamp()
                buf_duration = t_last - t_first
            except Exception:
                buf_duration = buf_len / self.config['collection']['sample_rate']

            if buf_duration < win_dur:
                return None

            target_start = t_last - win_dur
            ts_list = list(self.data_buffer['timestamps'])
            start_idx = 0
            for i in range(buf_len - 1, -1, -1):
                try:
                    t_i = datetime.fromisoformat(
                        ts_list[i].replace('Z', '+00:00')).timestamp()
                except Exception:
                    continue
                if t_i <= target_start:
                    start_idx = i
                    break

            data = {
                'timestamps': list(self.data_buffer['timestamps'])[start_idx:],
                'timestamps_r2': list(self.data_buffer['timestamps_r2'])[start_idx:],
                'timestamps_r3': list(self.data_buffer['timestamps_r3'])[start_idx:],
                'eta1': list(self.data_buffer['eta1'])[start_idx:],
                'eta2': list(self.data_buffer['eta2'])[start_idx:],
                'eta3': list(self.data_buffer['eta3'])[start_idx:]
            }

        return self._check_sampling_quality(data)

    def _get_window_from_database(self, win_dur: float) -> Optional[Dict]:
        """从数据库获取分析窗口数据"""
        db_config = self.config.get('database', {})
        if not db_config:
            logging.warning("No database config, cannot fall back to DB")
            return None

        conn = None
        try:
            conn = psycopg2.connect(
                host=db_config.get('host', 'localhost'),
                port=db_config.get('port', 5432),
                database=db_config.get('database', 'wave_monitoring'),
                user=db_config.get('user', 'wave_user'),
                password=db_config.get('password', ''),
            )
            cursor = conn.cursor()

            # 查询最近 win_dur 秒内的三个雷达数据，按时间排序
            cursor.execute("""
                SELECT timestamp, radar_id, distance
                FROM wave_measurements
                WHERE timestamp > NOW() - INTERVAL '%s seconds'
                ORDER BY timestamp ASC
            """, (int(win_dur + 60),))  # 多取60秒余量

            rows = cursor.fetchall()
            cursor.close()

            if not rows:
                logging.warning("No data in database for the requested window")
                return None

            # 按时间戳分组，每个时间点应有 radar_id=1,2,3
            from collections import defaultdict
            time_groups = defaultdict(lambda: {1: np.nan, 2: np.nan, 3: np.nan})
            for ts, radar_id, distance in rows:
                if 1 <= radar_id <= 3:
                    time_groups[ts][radar_id] = distance if distance is not None else np.nan

            # 按时间排序
            sorted_times = sorted(time_groups.keys())
            if not sorted_times:
                return None

            # 从尾部截取 win_dur 窗口
            t_last = sorted_times[-1].timestamp()
            target_start = t_last - win_dur
            start_idx = 0
            for i, ts in enumerate(sorted_times):
                if ts.timestamp() >= target_start:
                    start_idx = i
                    break

            selected_times = sorted_times[start_idx:]
            actual_duration = selected_times[-1].timestamp() - selected_times[0].timestamp()

            if actual_duration < win_dur * 0.8:  # 至少需要80%的窗口数据
                logging.warning(f"Database data too short: {actual_duration:.0f}s < {win_dur*0.8:.0f}s")
                return None

            timestamps = []
            eta1 = []
            eta2 = []
            eta3 = []
            for ts in selected_times:
                ts_iso = ts.isoformat()
                timestamps.append(ts_iso)
                grp = time_groups[ts]
                eta1.append(grp[1])
                eta2.append(grp[2])
                eta3.append(grp[3])

            data = {
                'timestamps': timestamps,
                'timestamps_r2': timestamps.copy(),
                'timestamps_r3': timestamps.copy(),
                'eta1': eta1,
                'eta2': eta2,
                'eta3': eta3,
            }

            min_samples = self.config['analysis'].get('min_samples', 100)
            if len(timestamps) < min_samples:
                logging.warning(f"Database returned only {len(timestamps)} samples, need {min_samples}")
                return None

            return self._check_sampling_quality(data)

        except Exception as e:
            logging.error(f"Database fallback failed: {e}")
            return None
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _check_sampling_quality(self, data: Dict) -> Optional[Dict]:
        """采样质量检查：检测异常采样间隔"""
        try:
            timestamps_arr = np.array([datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp()
                                      for ts in data['timestamps']])
            intervals = np.diff(timestamps_arr)
            expected_interval = 1.0 / self.config['collection']['sample_rate']

            abnormal_mask = intervals > expected_interval * 3
            abnormal_count = np.sum(abnormal_mask)
            abnormal_ratio = abnormal_count / len(intervals) if len(intervals) > 0 else 0

            max_abnormal_ratio = self.config['analysis'].get('max_abnormal_sampling_ratio', 0.05)
            if abnormal_ratio > max_abnormal_ratio:
                logging.warning(f"Poor sampling quality: {abnormal_ratio*100:.1f}% abnormal intervals "
                              f"(threshold: {max_abnormal_ratio*100:.1f}%), skipping analysis")
                return None

            if abnormal_count > 0:
                max_interval = np.max(intervals)
                logging.info(f"Sampling quality: {abnormal_count} abnormal intervals ({abnormal_ratio*100:.1f}%), "
                           f"max interval: {max_interval:.3f}s")
        except Exception as e:
            logging.warning(f"Sampling quality check failed: {e}, proceeding with analysis")

        return data

    def _publish_error_result(self, mode: str, window_duration: int, reason: str):
        """数据不足或分析失败时发布空结果，让serial_console快速收到响应而非超时等待"""
        if not self.mqtt_connected.is_set():
            return
        try:
            error_result = {
                'metadata': {
                    'end_time': datetime.now(tz=timezone.utc).isoformat(),
                    'mode': mode,
                    'window_duration': window_duration,
                    'error': reason,
                },
                'results': {}
            }
            topic = self.config['mqtt']['topics']['analyzed_data']
            self.mqtt_client.publish(topic, json.dumps(error_result), qos=1)
            logging.info(f"[OnDemand] Published error result: {reason}")
        except Exception as e:
            logging.error(f"Failed to publish error result: {e}")

    def _publish_analysis(self, analysis: Dict):
        """发布分析结果"""
        if not self.mqtt_connected.is_set():
            return

        try:
            topic = self.config['mqtt']['topics']['analyzed_data']
            self.mqtt_client.publish(topic, json.dumps(analysis), qos=1)
            self.stats['analyses_published'] += 1

            # 记录结果
            results = analysis['results']
            direction = results.get('wave_direction')
            direction_str = f"{direction:.1f}" if direction is not None else "N/A"
            logging.info(
                f"Published: Hs={results['Hm0']:.3f}m, "
                f"Tp={results['Tp']:.2f}s, "
                f"Dir={direction_str} deg, "
                f"radars={results.get('radar_count', 'N/A')}"
            )

        except Exception as e:
            logging.error(f"Failed to publish analysis: {e}")
            self.stats['errors'] += 1

    def _publish_status(self):
        """发布服务状态"""
        if not self.mqtt_connected.is_set():
            return

        try:
            with self.buffer_lock:
                buffer_size = len(self.data_buffer['timestamps'])

            # 计算进度信息
            required_samples = int(self.window_duration * self.config['collection']['sample_rate'])
            progress_percent = min(100, (buffer_size / required_samples * 100)) if required_samples > 0 else 0
            ready_for_analysis = buffer_size >= required_samples

            # 计算下次分析倒计时
            current_time = time.time()
            seconds_until_next = max(0, self.next_analysis_time - current_time) if self.next_analysis_time > 0 else 0
            analysis_progress = 0
            if self.analysis_interval > 0 and ready_for_analysis:
                elapsed = self.analysis_interval - seconds_until_next
                analysis_progress = min(100, (elapsed / self.analysis_interval) * 100)

            effective_interval = self.last_on_demand_window if not self.auto_analysis else self.analysis_interval

            status = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'component': 'analyzer',
                'status': 'running' if self.running else 'stopped',
                'statistics': self.stats,
                'buffer_size': buffer_size,
                'required_samples': required_samples,
                'window_duration': self.window_duration,
                'progress_percent': round(progress_percent, 1),
                'ready_for_analysis': ready_for_analysis,
                'analysis_interval': effective_interval,
                'seconds_until_next_analysis': round(seconds_until_next, 1),
                'analysis_progress': round(analysis_progress, 1),
                'last_on_demand_mode': self.last_on_demand_mode,
                'idle': self._analysis_idle,
            }

            topic = self.config['mqtt']['topics']['system_status']
            self.mqtt_client.publish(topic, json.dumps(status), qos=1)

        except Exception as e:
            logging.error(f"Failed to publish status: {e}")

    def run(self):
        """运行分析服务"""
        logging.info("="*60)
        logging.info("Starting MQTT Analysis Service")
        logging.info("="*60)

        # 设置MQTT
        self._setup_mqtt()

        # 等待MQTT连接
        if not self.mqtt_connected.wait(timeout=10):
            logging.error("MQTT connection timeout")
            return

        self.running = True

        # 分析间隔（自动模式使用）
        if self.analysis_interval is None or self.analysis_interval == 0:
            window_overlap = self.config['analysis'].get('window_overlap', 0.5)
            self.analysis_interval = self.window_duration * (1 - window_overlap)

        current_time = time.time()

        if self.auto_analysis:
            # 自动模式：对齐到分析间隔的整数倍时刻
            time_in_interval = current_time % self.analysis_interval
            self.next_analysis_time = current_time + (self.analysis_interval - time_in_interval)

            warmup_duration = self.config['analysis'].get('warmup_duration', 60)
            warmup_end_time = current_time + warmup_duration + self.window_duration
            if self.next_analysis_time < warmup_end_time:
                self.next_analysis_time = warmup_end_time
                logging.info(f"  Warmup period enabled: {warmup_duration}s")
        else:
            # 按需模式：禁用定时触发
            self.next_analysis_time = current_time + 86400 * 365  # 永不自动触发
            logging.info("  Auto analysis DISABLED: waiting for serial commands")

        # 状态报告间隔
        status_interval = self.config['monitoring']['status_report_interval']
        next_status_time = time.time() + status_interval

        logging.info(f"Analysis service started")
        logging.info(f"  Auto analysis: {'ON' if self.auto_analysis else 'OFF'}")
        logging.info(f"  Window duration (default): {self.window_duration}s")
        logging.info(f"  Modes: METER=300s, WORK=1200s")

        # 启动后立即发布一次初始状态
        self._publish_status()
        logging.info("Initial status published")

        try:
            while self.running and not self.stop_event.is_set():
                current_time = time.time()

                # 修剪缓冲区
                self._trim_buffer()

                # 按需分析（串口命令触发，优先级最高）
                if self.on_demand_event.is_set():
                    self.on_demand_event.clear()
                    win_dur = self.on_demand_window_duration or self.window_duration
                    mode = self.on_demand_mode or 'work'
                    logging.info(f"[OnDemand] Running {mode} analysis (window={win_dur}s)...")
                    data_window = self._get_analysis_window(window_duration=win_dur)
                    self.last_on_demand_window = win_dur
                    self.last_on_demand_mode = mode
                    if data_window:
                        analysis = self.analyzer.analyze_window(data_window, mode=mode)
                        if analysis:
                            analysis['metadata']['end_time'] = datetime.now(tz=timezone.utc).isoformat()
                            analysis['metadata']['mode'] = mode
                            analysis['metadata']['window_duration'] = win_dur
                            self._publish_analysis(analysis)
                            self.stats['analyses_completed'] += 1
                            self.last_analysis_time = time.time()
                            self._analysis_idle = True
                            logging.info(f"[OnDemand] {mode} analysis complete")
                            self._publish_status()
                        else:
                            self._analysis_idle = True
                            logging.warning(f"[OnDemand] analyze_window returned None")
                            self._publish_error_result(mode, win_dur, "分析计算失败")
                            self._publish_status()
                    else:
                        self._analysis_idle = True
                        logging.warning(f"[OnDemand] Insufficient data for {win_dur}s window")
                        self._publish_error_result(mode, win_dur, "数据不足")
                        self._publish_status()

                # 自动定时分析（仅 auto_analysis=True 时生效）
                elif self.auto_analysis and current_time >= self.next_analysis_time:
                    data_window = self._get_analysis_window()
                    if data_window:
                        logging.info("Running wave analysis...")
                        analysis = self.analyzer.analyze_window(data_window)
                        if analysis:
                            aligned_dt = datetime.fromtimestamp(self.next_analysis_time, tz=timezone.utc)
                            analysis['metadata']['end_time'] = aligned_dt.isoformat()
                            self._publish_analysis(analysis)
                            self.stats['analyses_completed'] += 1
                            self.last_analysis_time = time.time()

                        while self.next_analysis_time <= current_time:
                            self.next_analysis_time += self.analysis_interval

                # 发布状态
                if current_time >= next_status_time:
                    self._publish_status()
                    logging.info(
                        f"Stats: received={self.stats['messages_received']}, "
                        f"completed={self.stats['analyses_completed']}, "
                        f"published={self.stats['analyses_published']}"
                    )
                    next_status_time = time.time() + status_interval

                time.sleep(1)

        except KeyboardInterrupt:
            logging.info("Interrupted by user")
        finally:
            self.cleanup()

    def stop(self):
        """停止服务"""
        logging.info("Stopping analysis service...")
        self.running = False
        self.stop_event.set()

    def cleanup(self):
        """清理资源"""
        logging.info("Cleaning up...")

        # 发布最终状态
        self._publish_status()

        # 断开MQTT
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()

        logging.info("Analysis service stopped")


def signal_handler(signum, frame):
    """信号处理器"""
    logging.info(f"Received signal {signum}")
    if 'service' in globals():
        service.stop()


if __name__ == '__main__':
    config_path = Path(__file__).parent.parent / 'config' / 'system_config.yaml'

    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)

    service = MQTTAnalysisService(str(config_path))

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        service.run()
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
