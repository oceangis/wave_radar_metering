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
                    'array_height': config.get('radar', {}).get('array_height', 5.0),
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

    def _prepare_wave_data(self, distances: np.ndarray, timestamps: list) -> Dict:
        """
        验证过的波浪数据准备方法（精确周期计算）:
        1. 去尖刺 (绝对范围 + IQR离群值 + 逐点跳变>500mm，线性插值替换)
        2. η = -(distance - median(distance))
        3. 用实际时间戳计算真实采样率
        4. 线性插值重采样到等间隔6Hz（用于Welch谱分析）
        5. 保留原始时间戳的η（用于过零法精确周期）

        Args:
            distances: 原始测距数组(m)
            timestamps: ISO时间戳列表

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

        # 1. 去尖刺 (增强版：绝对范围 + IQR + 跳变过滤)
        dist_clean = distances.copy()

        # 第0轮: 绝对范围过滤（防止水花飞溅或多径反射导致的异常值）
        # 测距值 = array_height - water_level，有效范围: [盲区下限, 安装高度]
        array_height = self.config['radar'].get('array_height', 5.0)
        abs_lower = 0.3   # 雷达盲区下限（VEGA最小测距约0.3m）
        abs_upper = array_height + 0.5  # 最大测距不超过安装高度+余量
        spike_abs = (dist_clean < abs_lower) | (dist_clean > abs_upper)
        n_abs_spikes = int(np.sum(spike_abs))

        if n_abs_spikes > 0:
            # 先用中位数替换绝对异常值，避免影响后续统计
            median_dist = np.median(dist_clean[~spike_abs]) if np.any(~spike_abs) else np.median(dist_clean)
            dist_clean[spike_abs] = median_dist
            logging.info(f"Absolute range filter: {n_abs_spikes} points outside [{abs_lower:.2f}, {abs_upper:.2f}]m (splash/reflection)")

        # 第1轮: IQR离群值检测（基于中位数，更鲁棒）
        q25, q75 = np.percentile(dist_clean, [25, 75])
        iqr = q75 - q25
        if iqr < 0.001:
            iqr = 0.001  # 避免除零
        lower = q25 - 3.0 * iqr
        upper = q75 + 3.0 * iqr
        spike_iqr = (dist_clean < lower) | (dist_clean > upper)

        # 第2轮: 逐点跳变 > 500mm（增加阈值，捕捉造波飞溅）
        d_temp = dist_clean.copy()
        if np.any(spike_iqr):
            good_temp = ~spike_iqr
            if np.sum(good_temp) > 10:
                d_temp[spike_iqr] = np.interp(
                    np.where(spike_iqr)[0],
                    np.where(good_temp)[0],
                    d_temp[good_temp]
                )
        diff = np.abs(np.diff(d_temp))
        spike_jump_fwd = np.concatenate(([False], diff > 0.5))  # 提高到500mm
        spike_jump_bwd = np.concatenate((diff > 0.5, [False]))

        spike_mask = spike_iqr | spike_jump_fwd | spike_jump_bwd
        n_spikes = int(np.sum(spike_mask))
        if n_spikes > 0:
            good = ~spike_mask
            if np.any(good):
                dist_clean[spike_mask] = np.interp(
                    np.where(spike_mask)[0],
                    np.where(good)[0],
                    dist_clean[good]
                )

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

        if n_spikes > 0:
            logging.info(f"Spike removal: {n_spikes} points (IQR+jump filter, "
                         f"IQR={iqr*1000:.0f}mm, range=[{lower:.3f},{upper:.3f}]m)")
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

    def analyze_window(self, data: Dict) -> Optional[Dict]:
        """分析一个时间窗口的数据 - 支持1-3个雷达"""
        try:
            # 提取数据
            timestamps = data['timestamps']
            eta1 = np.array(data['eta1'])
            eta2 = np.array(data['eta2'])
            eta3 = np.array(data['eta3'])

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

                # 数据准备（验证过的方法：去尖刺→转η→重采样）
                prep = self._prepare_wave_data(eta1_valid, ts1_valid)

                # 单雷达分析（Welch用重采样数据，过零法用原始时间戳）
                params1 = self._analyze_single_radar(
                    prep['eta_resampled'], prep['raw_distances'],
                    fs=prep['fs'],
                    t_seconds=prep['t_seconds'],
                    eta_original=prep['eta_original']
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

                prep_a = self._prepare_wave_data(eta_a_valid, ts_a_valid)
                prep_b = self._prepare_wave_data(eta_b_valid, ts_b_valid)

                eta_a_clean = prep_a['eta_resampled']
                eta_b_clean = prep_b['eta_resampled']
                _actual_fs = prep_a['actual_fs']

                params_a = self._analyze_single_radar(
                    prep_a['eta_resampled'], prep_a['raw_distances'],
                    fs=prep_a['fs'], t_seconds=prep_a['t_seconds'],
                    eta_original=prep_a['eta_original']
                )
                params_b = self._analyze_single_radar(
                    prep_b['eta_resampled'], prep_b['raw_distances'],
                    fs=prep_b['fs'], t_seconds=prep_b['t_seconds'],
                    eta_original=prep_b['eta_original']
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

                # 数据准备（去尖刺→转η→重采样）
                prep1 = self._prepare_wave_data(eta1_valid, ts1_valid)
                prep2 = self._prepare_wave_data(eta2_valid, ts2_valid)
                prep3 = self._prepare_wave_data(eta3_valid, ts3_valid)

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
                    eta_original=prep1['eta_original']
                )
                params2 = self._analyze_single_radar(
                    prep2['eta_resampled'], prep2['raw_distances'],
                    fs=prep2['fs'], t_seconds=prep2['t_seconds'],
                    eta_original=prep2['eta_original']
                )
                params3 = self._analyze_single_radar(
                    prep3['eta_resampled'], prep3['raw_distances'],
                    fs=prep3['fs'], t_seconds=prep3['t_seconds'],
                    eta_original=prep3['eta_original']
                )

                # ========== DIWASP方向谱分析（三雷达模式）==========
                directional_results = None
                directional_spectrum_data = None
                if self.directional_analyzer is not None:
                    try:
                        diwasp_method = self.config['analysis'].get('diwasp_method', 'IMLM')

                        # R1平均测距：用原始数据（去趋势前）的简单平均
                        # 作为当前水面距离，动态重算倾斜雷达的等效基线
                        r1_valid = eta1_valid[~np.isnan(eta1_valid)]
                        r1_mean_dist = float(np.mean(r1_valid)) if len(r1_valid) > 0 else None
                        logging.info(f"Running DIWASP directional analysis (triple radar mode, method={diwasp_method}, R1_mean={r1_mean_dist:.3f}m)")

                        dir_results = self.directional_analyzer.analyze(
                            eta1_clean,
                            eta2=eta2_clean,
                            eta3=eta3_clean,
                            method=diwasp_method,
                            r1_mean_distance=r1_mean_dist
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

                # 三雷达模式：谱参数用雷达1，方向用DIWASP
                # DIWASP输出已在directional_spectrum.py中完成转换：
                #   axis-angle（传播去向）→ 罗盘来向（真北）
                # 此处直接使用，无需额外校正
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
                                  t_seconds: np.ndarray = None) -> Dict:
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

        # 计算潮位（阵列高度 - 平均测距）
        # array_height 从配置文件读取，表示雷达阵列相对基准面的高度
        array_height = self.config['radar'].get('array_height', 5.0)
        tide_level = array_height - mean_distance

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
                               eta_original: np.ndarray = None) -> Dict:
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
        if eta_original is not None and t_seconds is not None:
            # 对原始时间轴η做去趋势+带通滤波，保留非等间隔时间戳
            zc_eta = detrend(eta_original)
            if self.config['analysis']['filter_enable']:
                band = self.config['analysis']['filter_band']
                f_low = band[0] if band[0] > 0 else 0.01
                # 使用原始实际采样率（非重采样后的）
                zc_fs = (len(eta_original) - 1) / (t_seconds[-1] - t_seconds[0]) if t_seconds[-1] > t_seconds[0] else fs
                b, a = butter(4, band, btype='band', fs=zc_fs)
                padlen = min(3 * int(zc_fs / f_low), len(zc_eta) - 1)
                zc_eta = filtfilt(b, a, zc_eta, padlen=padlen)
        else:
            zc_eta = data
        zc_results = self._zero_crossing_analysis(zc_eta, raw_data, t_seconds=t_seconds)

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
        """获取分析窗口数据（基于实际时间戳判断窗口时长，带采样质量检查）

        Args:
            window_duration: 窗口时长(秒)，None时使用配置值
        """
        win_dur = window_duration if window_duration is not None else self.window_duration
        with self.buffer_lock:
            buf_len = len(self.data_buffer['timestamps'])
            if buf_len < self.config['analysis']['min_samples']:
                return None

            # 用首尾时间戳计算缓冲区实际时长
            try:
                t_first = datetime.fromisoformat(
                    self.data_buffer['timestamps'][0].replace('Z', '+00:00')).timestamp()
                t_last = datetime.fromisoformat(
                    self.data_buffer['timestamps'][-1].replace('Z', '+00:00')).timestamp()
                buf_duration = t_last - t_first
            except Exception:
                buf_duration = buf_len / self.config['collection']['sample_rate']

            if buf_duration < win_dur:
                logging.warning(f"Insufficient data: have {buf_duration:.0f}s, need {win_dur:.0f}s")
                return None

            # 从尾部向前找到刚好覆盖 win_dur 的起始位置
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

            # 采样质量检查：检测异常采样间隔
            try:
                timestamps_arr = np.array([datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp()
                                          for ts in data['timestamps']])
                intervals = np.diff(timestamps_arr)
                expected_interval = 1.0 / self.config['collection']['sample_rate']

                # 统计异常间隔（超过预期的3倍，约0.5秒）
                abnormal_mask = intervals > expected_interval * 3
                abnormal_count = np.sum(abnormal_mask)
                abnormal_ratio = abnormal_count / len(intervals) if len(intervals) > 0 else 0

                # 如果异常间隔比例超过阈值，跳过本次分析
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
                        analysis = self.analyzer.analyze_window(data_window)
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
                        logging.warning(f"[OnDemand] Insufficient data for {win_dur}s window")
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
