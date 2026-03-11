#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
雷达数据预处理模块
==================

功能：
1. 异常值检测与剔除（Outlier Detection and Removal）
2. 去趋势处理（Detrending）
3. 滤波（Filtering）
4. 频带选择与抗混叠（Frequency Band Selection and Anti-Aliasing）

作者：Wave Monitoring System - Signal Processing Team
日期：2026-01-12

理论依据：
- IEC 61400-3 海洋观测标准
- IOOS质量控制手册
- Tucker & Pitt (2001) 海洋工程波浪理论
"""

import numpy as np
import pandas as pd
from scipy.signal import butter, filtfilt, detrend
from scipy.interpolate import interp1d
from typing import Dict, Tuple, Optional, List
import logging


# ============================================================================
# 质量控制标志定义（Quality Control Flags）
# ============================================================================

class QCFlags:
    """质量控制标志常量"""
    # 质量评分（0-100）
    GOOD = 100              # 优质数据
    PROBABLY_GOOD = 80      # 轻微问题
    SUSPECT = 50            # 可疑数据
    BAD = 20                # 不建议使用
    MISSING = 0             # 无数据

    # 具体原因代码
    OUT_OF_RANGE = 10       # 超出物理范围
    RATE_EXCEEDED = 20      # 变化率过大
    FLAT_LINE = 30          # 扁平线（设备故障）
    OUTLIER = 40            # 孤立异常值
    MULTIPATH = 50          # 多径反射
    SPRAY = 60              # 浪花/飞沫
    NO_ECHO = 70            # 回波丢失
    ATTENUATION = 80        # 雨雾衰减
    OBSTRUCTION = 90        # 遮挡


class RadarPreprocessor:
    """
    雷达数据预处理器

    实现四个关键预处理步骤：
    1. 异常值检测与剔除
    2. 去趋势处理
    3. 滤波
    4. 频带选择与抗混叠
    """

    def __init__(self, config: Dict):
        """
        初始化预处理器

        参数:
            config: 系统配置字典
        """
        self.config = config
        self.sample_rate = config['collection']['sample_rate']
        self.qc_config = config['analysis'].get('qc', {})

        # 提取QC参数
        self.valid_range = self.qc_config.get('valid_range', [0.0, 10.0])
        self.max_rate = self.qc_config.get('max_rate_of_change', 1.0)
        self.flat_line_count = self.qc_config.get('flat_line_count', 3)
        self.flat_line_tolerance = self.qc_config.get('flat_line_tolerance', 0.001)

        # 异常值检测参数
        outlier_cfg = self.qc_config.get('outlier_detection', {})
        self.outlier_method = outlier_cfg.get('method', 'mad')
        self.outlier_threshold = outlier_cfg.get('threshold', 4.5)
        self.outlier_window = outlier_cfg.get('window_size', 7)

        # 插值参数
        interp_cfg = self.qc_config.get('interpolation', {})
        self.max_gap_ratio = interp_cfg.get('max_gap_ratio', 0.05)
        self.interp_method = interp_cfg.get('method', 'cubic')

        # 滤波器参数
        self.filter_enable = config['analysis'].get('filter_enable', True)
        self.filter_band = config['analysis'].get('filter_band', [0.04, 1.0])

        logging.info(f"RadarPreprocessor initialized: sample_rate={self.sample_rate}Hz, "
                     f"filter_band={self.filter_band}Hz, outlier_method={self.outlier_method}")

    # ========================================================================
    # 第一步：异常值检测与剔除 (Outlier Detection and Removal)
    # ========================================================================

    def detect_outliers_global(self, data: np.ndarray) -> Tuple[np.ndarray, Dict]:
        """
        全局异常值检测（基于整体统计特性）

        参数:
            data: 输入数据数组

        返回:
            outlier_mask: 布尔数组，True表示异常值
            report: 检测报告字典
        """
        outlier_mask = np.zeros(len(data), dtype=bool)
        report = {
            'method': self.outlier_method,
            'threshold': self.outlier_threshold,
            'count': 0,
            'ratio': 0.0
        }

        if self.outlier_method == 'sigma':
            # 3-sigma原则（假设正态分布）
            mean = np.nanmean(data)
            std = np.nanstd(data)
            z_scores = np.abs((data - mean) / std)
            outlier_mask = z_scores > self.outlier_threshold
            report['mean'] = float(mean)
            report['std'] = float(std)

        elif self.outlier_method == 'mad':
            # MAD (Median Absolute Deviation) 方法（更鲁棒）
            # 去除连续重复值后计算MAD，避免大量重复值导致MAD被压缩
            # （雷达实际更新率~3Hz但采样率6Hz，约50-65%为重复值）
            unique_mask = np.concatenate(([True], data[1:] != data[:-1]))
            data_unique = data[unique_mask & ~np.isnan(data)]
            median = np.nanmedian(data)
            dup_ratio = 1.0 - np.sum(unique_mask) / len(data)
            if len(data_unique) > 10:
                mad = np.nanmedian(np.abs(data_unique - median))
            else:
                mad = np.nanmedian(np.abs(data - median))
            # 当重复率极高时(>50%)，MAD可能仍被压缩，改用IQR/1.349作为鲁棒尺度
            if dup_ratio > 0.5 and len(data_unique) > 20:
                q75, q25 = np.nanpercentile(data_unique, [75, 25])
                iqr_scale = (q75 - q25) / 1.349  # IQR标准化到与std可比
                if iqr_scale > mad:
                    mad = iqr_scale
            # 修正因子0.6745使MAD与标准差可比
            modified_z_scores = 0.6745 * np.abs(data - median) / (mad + 1e-10)
            outlier_mask = modified_z_scores > self.outlier_threshold
            report['median'] = float(median)
            report['mad'] = float(mad)
            report['duplicate_ratio'] = float(dup_ratio)
            report['unique_samples'] = int(np.sum(unique_mask))

        # 处理NaN
        outlier_mask = outlier_mask | np.isnan(data)

        report['count'] = int(np.sum(outlier_mask))
        report['ratio'] = float(np.sum(outlier_mask) / len(data))

        logging.debug(f"Global outlier detection: {report['count']}/{len(data)} "
                      f"({report['ratio']*100:.2f}%) outliers detected")

        return outlier_mask, report

    def detect_outliers_local(self, data: np.ndarray) -> Tuple[np.ndarray, Dict]:
        """
        局部异常值检测（滑动窗口检测尖峰）

        用于检测短时突发干扰：鸟类、漂浮物、飞沫等

        参数:
            data: 输入数据数组

        返回:
            spike_mask: 布尔数组，True表示尖峰
            report: 检测报告字典
        """
        spike_mask = np.zeros(len(data), dtype=bool)
        window = self.outlier_window

        if len(data) < window:
            return spike_mask, {'count': 0, 'ratio': 0.0}

        # 使用pandas滚动窗口计算局部统计量
        series = pd.Series(data)
        rolling_median = series.rolling(window, center=True, min_periods=1).median()
        rolling_mad = series.rolling(window, center=True, min_periods=1).apply(
            lambda x: np.median(np.abs(x - np.median(x)))
        )

        # 局部修正Z分数
        local_z_scores = 0.6745 * np.abs(data - rolling_median) / (rolling_mad + 1e-10)
        spike_mask = local_z_scores > self.outlier_threshold

        report = {
            'window_size': window,
            'count': int(np.sum(spike_mask)),
            'ratio': float(np.sum(spike_mask) / len(data))
        }

        logging.debug(f"Local spike detection: {report['count']}/{len(data)} "
                      f"({report['ratio']*100:.2f}%) spikes detected")

        return spike_mask, report

    def detect_multipath(self, data: np.ndarray) -> Tuple[np.ndarray, Dict]:
        """
        多径反射检测

        特征：测距值为实际距离的整数倍（2x, 3x）

        参数:
            data: 输入数据数组

        返回:
            multipath_mask: 布尔数组，True表示多径
            report: 检测报告字典
        """
        multipath_mask = np.zeros(len(data), dtype=bool)
        median = np.nanmedian(data)

        # 检测2倍、3倍关系（容差±10cm）
        tolerance = 0.10
        is_double = np.abs(data - 2 * median) < tolerance
        is_triple = np.abs(data - 3 * median) < tolerance

        multipath_mask = is_double | is_triple

        report = {
            'count': int(np.sum(multipath_mask)),
            'ratio': float(np.sum(multipath_mask) / len(data)),
            'median_distance': float(median)
        }

        if report['count'] > 0:
            logging.warning(f"Multipath detected: {report['count']} samples "
                            f"({report['ratio']*100:.2f}%)")

        return multipath_mask, report

    def detect_spray(self, data: np.ndarray) -> Tuple[np.ndarray, Dict]:
        """
        浪花/飞沫检测

        特征：距离突然变小 + 高频波动

        参数:
            data: 输入数据数组

        返回:
            spray_mask: 布尔数组，True表示飞沫
            report: 检测报告字典
        """
        spray_mask = np.zeros(len(data), dtype=bool)
        median = np.nanmedian(data)

        # 1. 检测异常近距离（比中位数小50cm以上）
        # 注意：0.10m阈值对Hs<0.5m的波浪过于激进，会把正常波峰误判为飞沫
        # 真实飞沫通常偏离中位数>50cm且伴随极高频波动
        is_near = data < (median - 0.50)

        # 2. 检测高频波动（1秒窗口标准差）
        window = self.sample_rate  # 1秒
        series = pd.Series(data)
        rolling_std = series.rolling(window, center=True, min_periods=1).std()
        is_noisy = rolling_std > 0.15  # 标准差>15cm（原5cm对正常波浪过于敏感）

        # 飞沫 = 偏近 AND 高频波动
        spray_mask = is_near & is_noisy.values

        report = {
            'count': int(np.sum(spray_mask)),
            'ratio': float(np.sum(spray_mask) / len(data))
        }

        if report['ratio'] > 0.01:  # 超过1%
            logging.info(f"Sea spray detected: {report['count']} samples "
                         f"({report['ratio']*100:.2f}%)")

        return spray_mask, report

    def detect_rate_of_change(self, data: np.ndarray) -> Tuple[np.ndarray, Dict]:
        """
        变化率异常检测 — 捕捉孤立毛刺

        原理：真实波浪的相邻采样变化量受波陡限制，
        超过 max_rate / sample_rate 的跳变必为毛刺。

        参数:
            data: 输入数据数组

        返回:
            rate_mask: 布尔数组，True表示变化率异常
            report: 检测报告字典
        """
        rate_mask = np.zeros(len(data), dtype=bool)
        max_change = self.max_rate / self.sample_rate  # m/sample

        diff = np.abs(np.diff(data))
        # 标记跳变点和跳变后一个点（毛刺通常是单点，前后都有大跳变）
        jumps = diff > max_change
        rate_mask[1:] |= jumps      # 跳变后的点
        rate_mask[:-1] |= jumps     # 跳变前的点

        # 只保留孤立毛刺（前后都跳变的点），避免误标真实波浪的陡峰
        isolated = np.zeros(len(data), dtype=bool)
        for i in range(1, len(data) - 1):
            if rate_mask[i]:
                # 检查是否为孤立点：与前后都有大跳变
                d_prev = abs(data[i] - data[i-1])
                d_next = abs(data[i] - data[i+1])
                if d_prev > max_change and d_next > max_change:
                    isolated[i] = True

        report = {
            'max_change_per_sample': float(max_change),
            'count': int(np.sum(isolated)),
            'ratio': float(np.sum(isolated) / len(data))
        }
        return isolated, report

    def detect_all_outliers(self, data: np.ndarray) -> Tuple[np.ndarray, Dict]:
        """
        综合异常值检测（整合所有检测方法）

        参数:
            data: 输入数据数组

        返回:
            combined_mask: 综合异常值掩码
            report: 完整检测报告
        """
        report = {}

        # 0. 变化率检测（最高效的毛刺捕捉）
        rate_mask, rate_report = self.detect_rate_of_change(data)
        report['rate_of_change'] = rate_report

        # 1. 全局异常检测
        global_mask, global_report = self.detect_outliers_global(data)
        report['global_outliers'] = global_report

        # 2. 局部尖峰检测
        spike_mask, spike_report = self.detect_outliers_local(data)
        report['local_spikes'] = spike_report

        # 3. 多径检测
        multipath_mask, multipath_report = self.detect_multipath(data)
        report['multipath'] = multipath_report

        # 4. 飞沫检测
        spray_mask, spray_report = self.detect_spray(data)
        report['spray'] = spray_report

        # 综合掩码（任何一种异常都标记）
        combined_mask = rate_mask | global_mask | spike_mask | multipath_mask | spray_mask

        report['total_outliers'] = int(np.sum(combined_mask))
        report['total_ratio'] = float(np.sum(combined_mask) / len(data))

        logging.info(f"Total outliers detected: {report['total_outliers']}/{len(data)} "
                     f"({report['total_ratio']*100:.2f}%)")

        return combined_mask, report

    # ========================================================================
    # 第二步：数据插补 (Data Interpolation)
    # ========================================================================

    def interpolate_gaps(self, data: np.ndarray, mask: np.ndarray) -> Tuple[np.ndarray, Dict]:
        """
        数据插补（填补缺失值和异常值）

        参数:
            data: 原始数据数组
            mask: 布尔掩码，True表示需要插补的位置

        返回:
            interpolated_data: 插补后的数据
            report: 插补报告
        """
        interpolated_data = data.copy()
        gap_ratio = np.sum(mask) / len(data)

        report = {
            'gap_count': int(np.sum(mask)),
            'gap_ratio': float(gap_ratio),
            'method': 'none',
            'success': False
        }

        # 检查缺失率（放宽到15%，孤立毛刺可安全插值）
        hard_limit = max(self.max_gap_ratio * 3, 0.15)
        if gap_ratio > hard_limit:
            logging.warning(f"Gap ratio {gap_ratio*100:.2f}% exceeds hard limit "
                            f"{hard_limit*100:.2f}%, data marked as low quality")
            report['reason'] = 'excessive_gaps'
            return interpolated_data, report
        elif gap_ratio > self.max_gap_ratio:
            logging.info(f"Gap ratio {gap_ratio*100:.2f}% exceeds soft limit "
                         f"{self.max_gap_ratio*100:.2f}%, attempting interpolation anyway")

        if gap_ratio == 0:
            report['success'] = True
            return interpolated_data, report

        # 检查最大连续间隙长度（超过则插值不可靠）
        # 最大允许间隙 = 3秒（约1个短周期波浪，cubic插值可靠范围）
        max_gap_samples = int(self.sample_rate * 3)
        longest_gap = 0
        current_gap = 0
        for m in mask:
            if m:
                current_gap += 1
                longest_gap = max(longest_gap, current_gap)
            else:
                current_gap = 0
        report['longest_gap_samples'] = longest_gap
        report['longest_gap_seconds'] = float(longest_gap / self.sample_rate)
        if longest_gap > max_gap_samples:
            logging.warning(f"Longest gap {longest_gap} samples "
                            f"({longest_gap/self.sample_rate:.1f}s) exceeds max "
                            f"{max_gap_samples} samples ({max_gap_samples/self.sample_rate:.1f}s)")
            report['reason'] = 'gap_too_long'
            report['success'] = False
            return interpolated_data, report

        # 找出有效数据的索引
        valid_indices = np.where(~mask)[0]
        valid_values = data[~mask]

        if len(valid_indices) < 2:
            logging.error("Insufficient valid data for interpolation")
            report['reason'] = 'insufficient_data'
            return interpolated_data, report

        # 需要插补的索引
        gap_indices = np.where(mask)[0]

        try:
            # 选择插补方法
            if self.interp_method == 'linear':
                f = interp1d(valid_indices, valid_values, kind='linear',
                             bounds_error=False, fill_value=np.nan)
                interpolated_data[mask] = f(gap_indices)
                report['method'] = 'linear'

            elif self.interp_method == 'cubic':
                if len(valid_indices) >= 4:  # 三次样条需要至少4个点
                    f = interp1d(valid_indices, valid_values, kind='cubic',
                                 bounds_error=False, fill_value=np.nan)
                    interpolated_data[mask] = f(gap_indices)
                    report['method'] = 'cubic'
                else:
                    # 退化到线性插值
                    f = interp1d(valid_indices, valid_values, kind='linear',
                                 bounds_error=False, fill_value=np.nan)
                    interpolated_data[mask] = f(gap_indices)
                    report['method'] = 'linear_fallback'

            report['success'] = True
            logging.info(f"Interpolation successful: {report['gap_count']} samples "
                         f"filled using {report['method']} method")

        except Exception as e:
            logging.error(f"Interpolation failed: {e}")
            report['reason'] = str(e)
            report['success'] = False

        return interpolated_data, report

    # ========================================================================
    # 第三步：去趋势处理 (Detrending)
    # ========================================================================

    def detrend_data(self, data: np.ndarray, method: str = 'linear') -> Tuple[np.ndarray, Dict]:
        """
        去趋势处理（去除低频漂移和潮汐影响）

        参数:
            data: 输入数据数组
            method: 去趋势方法 ('linear', 'constant', 'highpass')

        返回:
            detrended_data: 去趋势后的数据
            report: 去趋势报告
        """
        report = {'method': method, 'success': False}

        try:
            if method == 'linear':
                # 线性去趋势（去除直流偏移和线性漂移）
                detrended_data = detrend(data, type='linear')
                report['trend_removed'] = 'linear_drift'

            elif method == 'constant':
                # 去除平均值
                detrended_data = detrend(data, type='constant')
                report['mean_value'] = float(np.mean(data))

            elif method == 'highpass':
                # 高通滤波去趋势（保留>0.03Hz的成分）
                fc = 0.03  # 截止频率0.03Hz（周期33秒）
                b, a = butter(4, fc, btype='high', fs=self.sample_rate)
                detrended_data = filtfilt(b, a, data)
                report['trend_removed'] = 'highpass_filtered'
                report['cutoff_freq'] = fc

            else:
                logging.warning(f"Unknown detrend method '{method}', using linear")
                detrended_data = detrend(data, type='linear')
                report['method'] = 'linear'

            # 计算去除的趋势幅度
            trend = data - detrended_data
            report['trend_amplitude'] = float(np.max(trend) - np.min(trend))
            report['success'] = True

            logging.debug(f"Detrending successful: method={method}, "
                          f"trend_amplitude={report['trend_amplitude']:.4f}m")

        except Exception as e:
            logging.error(f"Detrending failed: {e}")
            report['error'] = str(e)
            detrended_data = data

        return detrended_data, report

    # ========================================================================
    # 第四步：滤波 (Filtering)
    # ========================================================================

    def apply_filter(self, data: np.ndarray, filter_type: str = 'bandpass') -> Tuple[np.ndarray, Dict]:
        """
        应用数字滤波器

        参数:
            data: 输入数据数组
            filter_type: 滤波器类型 ('bandpass', 'lowpass', 'highpass')

        返回:
            filtered_data: 滤波后的数据
            report: 滤波报告
        """
        if not self.filter_enable:
            logging.info("Filtering disabled in config")
            return data, {'enabled': False}

        report = {
            'enabled': True,
            'filter_type': filter_type,
            'filter_band': self.filter_band,
            'filter_order': 4,
            'success': False
        }

        try:
            order = 4  # Butterworth滤波器阶数
            band = self.filter_band

            # padlen 至少覆盖最低频率3个周期，减轻 filtfilt 边缘效应
            f_low = band[0] if isinstance(band, list) else band
            padlen = min(3 * int(self.sample_rate / f_low), len(data) - 1)

            if filter_type == 'bandpass':
                # 带通滤波器（保留波浪频段）
                b, a = butter(order, band, btype='bandpass', fs=self.sample_rate)
                filtered_data = filtfilt(b, a, data, padlen=padlen)
                report['passband'] = f"{band[0]}-{band[1]} Hz"

            elif filter_type == 'lowpass':
                # 低通滤波器（去除高频噪声）
                fc = band[1] if isinstance(band, list) else band
                b, a = butter(order, fc, btype='lowpass', fs=self.sample_rate)
                filtered_data = filtfilt(b, a, data, padlen=padlen)
                report['cutoff'] = f"{fc} Hz"

            elif filter_type == 'highpass':
                # 高通滤波器（去除低频趋势）
                fc = band[0] if isinstance(band, list) else band
                b, a = butter(order, fc, btype='highpass', fs=self.sample_rate)
                filtered_data = filtfilt(b, a, data, padlen=padlen)
                report['cutoff'] = f"{fc} Hz"

            else:
                logging.warning(f"Unknown filter type '{filter_type}', skipping")
                filtered_data = data
                report['success'] = False
                return filtered_data, report

            # 检查能量守恒（滤波后能量不应损失太多）
            energy_before = np.sum(data**2)
            energy_after = np.sum(filtered_data**2)
            energy_ratio = energy_after / (energy_before + 1e-10)
            report['energy_ratio'] = float(energy_ratio)

            if energy_ratio < 0.8 or energy_ratio > 1.2:
                logging.warning(f"Energy ratio {energy_ratio:.3f} outside expected range [0.8, 1.2]")

            report['success'] = True
            logging.info(f"Filtering successful: {filter_type}, band={band}, "
                         f"energy_ratio={energy_ratio:.3f}")

        except Exception as e:
            logging.error(f"Filtering failed: {e}")
            report['error'] = str(e)
            filtered_data = data

        return filtered_data, report

    # ========================================================================
    # 完整预处理流程 (Complete Preprocessing Pipeline)
    # ========================================================================

    def preprocess(self, data: np.ndarray, full_pipeline: bool = True) -> Dict:
        """
        完整预处理流程

        步骤：
        1. 异常值检测与剔除
        2. 数据插补
        3. 去趋势处理
        4. 滤波

        参数:
            data: 原始数据数组（雷达测距值，单位：米）
            full_pipeline: 是否执行完整流程（False仅做异常检测）

        返回:
            结果字典，包含：
            - data_clean: 预处理后的数据
            - data_original: 原始数据
            - outlier_mask: 异常值掩码
            - reports: 各步骤详细报告
            - quality_score: 最终质量评分（0-100）
        """
        logging.info(f"Starting preprocessing: {len(data)} samples, full_pipeline={full_pipeline}")

        results = {
            'data_original': data.copy(),
            'data_clean': data.copy(),
            'outlier_mask': np.zeros(len(data), dtype=bool),
            'reports': {},
            'quality_score': 100
        }

        # ====================================================================
        # 步骤1：异常值检测与剔除
        # ====================================================================
        outlier_mask, outlier_report = self.detect_all_outliers(data)
        results['outlier_mask'] = outlier_mask
        results['reports']['outlier_detection'] = outlier_report

        # 根据异常率降低质量分数
        outlier_ratio = outlier_report['total_ratio']
        if outlier_ratio > 0.10:  # >10%异常
            results['quality_score'] -= 30
        elif outlier_ratio > 0.05:  # >5%异常
            results['quality_score'] -= 15
        elif outlier_ratio > 0.02:  # >2%异常
            results['quality_score'] -= 5

        if not full_pipeline:
            # 仅执行异常检测，不进行后续处理
            return results

        # ====================================================================
        # 步骤2：数据插补
        # ====================================================================
        data_interpolated, interp_report = self.interpolate_gaps(data, outlier_mask)
        results['reports']['interpolation'] = interp_report

        if not interp_report['success']:
            logging.warning("Interpolation failed, using original data")
            results['quality_score'] -= 20
            data_interpolated = data.copy()

        # ====================================================================
        # 步骤3：去趋势处理
        # ====================================================================
        data_detrended, detrend_report = self.detrend_data(data_interpolated, method='linear')
        results['reports']['detrending'] = detrend_report

        if not detrend_report['success']:
            logging.warning("Detrending failed, using interpolated data")
            results['quality_score'] -= 10
            data_detrended = data_interpolated

        # ====================================================================
        # 步骤4：滤波
        # ====================================================================
        data_filtered, filter_report = self.apply_filter(data_detrended, filter_type='bandpass')
        results['reports']['filtering'] = filter_report

        if not filter_report.get('success', False):
            logging.warning("Filtering failed, using detrended data")
            results['quality_score'] -= 10
            data_filtered = data_detrended

        # ====================================================================
        # 最终输出
        # ====================================================================
        results['data_clean'] = data_filtered

        # 计算最终统计量
        results['statistics'] = {
            'original': {
                'mean': float(np.mean(data)),
                'std': float(np.std(data)),
                'min': float(np.min(data)),
                'max': float(np.max(data))
            },
            'clean': {
                'mean': float(np.mean(data_filtered)),
                'std': float(np.std(data_filtered)),
                'min': float(np.min(data_filtered)),
                'max': float(np.max(data_filtered))
            }
        }

        logging.info(f"Preprocessing completed: quality_score={results['quality_score']}, "
                     f"outliers={outlier_report['total_ratio']*100:.2f}%")

        return results

    # ========================================================================
    # 三雷达交叉验证 (Cross-Validation)
    # ========================================================================

    def cross_validate_radars(self, eta1: np.ndarray, eta2: np.ndarray,
                               eta3: np.ndarray) -> Dict:
        """
        三雷达交叉验证（检测单个雷达的系统性错误）

        原理：三个雷达测量同一海面，波高应该接近

        参数:
            eta1, eta2, eta3: 三个雷达的波面数据

        返回:
            验证报告
        """
        report = {
            'cross_validation': True,
            'radars_active': 3,
            'consistent': True,
            'warnings': []
        }

        # 计算各雷达的标准差（代表波高）
        std1 = np.nanstd(eta1)
        std2 = np.nanstd(eta2)
        std3 = np.nanstd(eta3)

        report['std_values'] = {
            'radar1': float(std1),
            'radar2': float(std2),
            'radar3': float(std3)
        }

        # 检查一致性（差异<20%）
        max_deviation = self.qc_config.get('cross_validation', {}).get('max_deviation', 0.20)

        mean_std = (std1 + std2 + std3) / 3
        if mean_std == 0:
            # 三雷达标准差全为0（静止海面或设备故障），视为一致
            return report
        dev1 = abs(std1 - mean_std) / mean_std
        dev2 = abs(std2 - mean_std) / mean_std
        dev3 = abs(std3 - mean_std) / mean_std

        if dev1 > max_deviation:
            report['warnings'].append(f"Radar 1 deviation {dev1*100:.1f}% exceeds {max_deviation*100:.0f}%")
            report['consistent'] = False

        if dev2 > max_deviation:
            report['warnings'].append(f"Radar 2 deviation {dev2*100:.1f}% exceeds {max_deviation*100:.0f}%")
            report['consistent'] = False

        if dev3 > max_deviation:
            report['warnings'].append(f"Radar 3 deviation {dev3*100:.1f}% exceeds {max_deviation*100:.0f}%")
            report['consistent'] = False

        if report['warnings']:
            logging.warning(f"Cross-validation warnings: {report['warnings']}")

        return report


# ============================================================================
# 实用工具函数 (Utility Functions)
# ============================================================================

def generate_preprocessing_report(results: Dict) -> str:
    """
    生成可读的预处理报告

    参数:
        results: preprocess()函数的返回结果

    返回:
        格式化的报告字符串
    """
    report_lines = [
        "="*70,
        "雷达数据预处理报告 (Radar Data Preprocessing Report)",
        "="*70,
        "",
        f"数据长度: {len(results['data_original'])} samples",
        f"质量评分: {results['quality_score']}/100",
        "",
        "--- 异常值检测结果 ---",
    ]

    outlier_rep = results['reports']['outlier_detection']
    report_lines.extend([
        f"  全局异常值: {outlier_rep['global_outliers']['count']} "
        f"({outlier_rep['global_outliers']['ratio']*100:.2f}%)",
        f"  局部尖峰: {outlier_rep['local_spikes']['count']} "
        f"({outlier_rep['local_spikes']['ratio']*100:.2f}%)",
        f"  多径反射: {outlier_rep['multipath']['count']} "
        f"({outlier_rep['multipath']['ratio']*100:.2f}%)",
        f"  浪花/飞沫: {outlier_rep['spray']['count']} "
        f"({outlier_rep['spray']['ratio']*100:.2f}%)",
        f"  总计异常: {outlier_rep['total_outliers']} "
        f"({outlier_rep['total_ratio']*100:.2f}%)",
        "",
    ])

    if 'interpolation' in results['reports']:
        interp_rep = results['reports']['interpolation']
        report_lines.extend([
            "--- 数据插补 ---",
            f"  方法: {interp_rep['method']}",
            f"  插补样本: {interp_rep['gap_count']} ({interp_rep['gap_ratio']*100:.2f}%)",
            f"  成功: {'✓' if interp_rep['success'] else '✗'}",
            "",
        ])

    if 'detrending' in results['reports']:
        detrend_rep = results['reports']['detrending']
        report_lines.extend([
            "--- 去趋势处理 ---",
            f"  方法: {detrend_rep['method']}",
            f"  趋势幅度: {detrend_rep.get('trend_amplitude', 0):.4f} m",
            "",
        ])

    if 'filtering' in results['reports']:
        filter_rep = results['reports']['filtering']
        if filter_rep.get('enabled'):
            report_lines.extend([
                "--- 滤波 ---",
                f"  类型: {filter_rep['filter_type']}",
                f"  频段: {filter_rep.get('passband', filter_rep.get('cutoff', 'N/A'))}",
                f"  能量比: {filter_rep.get('energy_ratio', 1.0):.3f}",
                "",
            ])

    stats = results['statistics']
    report_lines.extend([
        "--- 统计对比 ---",
        f"  原始数据: mean={stats['original']['mean']:.4f}m, "
        f"std={stats['original']['std']:.4f}m",
        f"  清洗数据: mean={stats['clean']['mean']:.4f}m, "
        f"std={stats['clean']['std']:.4f}m",
        "",
        "="*70,
    ])

    return "\n".join(report_lines)


# ============================================================================
# 示例用法 (Example Usage)
# ============================================================================

if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # 模拟配置
    config = {
        'collection': {'sample_rate': 6},
        'analysis': {
            'filter_enable': True,
            'filter_band': [0.03, 1.0],
            'qc': {
                'valid_range': [0.0, 10.0],
                'max_rate_of_change': 1.0,
                'flat_line_count': 3,
                'flat_line_tolerance': 0.001,
                'outlier_detection': {
                    'method': 'mad',
                    'threshold': 4.5,
                    'window_size': 60
                },
                'interpolation': {
                    'max_gap_ratio': 0.05,
                    'method': 'linear'
                }
            }
        }
    }

    # 生成测试数据（正弦波 + 噪声 + 异常值）
    np.random.seed(42)
    t = np.linspace(0, 100, 600)  # 100秒，6Hz
    clean_wave = 0.5 * np.sin(2 * np.pi * 0.1 * t) + 2.0  # 10秒周期波浪
    noise = 0.05 * np.random.randn(len(t))
    data = clean_wave + noise

    # 添加人工异常
    data[100] = 5.0  # 尖峰
    data[200] = 0.1  # 异常低值
    data[300:305] = np.nan  # 缺失数据

    # 创建预处理器
    preprocessor = RadarPreprocessor(config)

    # 执行预处理
    results = preprocessor.preprocess(data, full_pipeline=True)

    # 打印报告
    print(generate_preprocessing_report(results))

    # 保存结果（可选）
    # np.save('preprocessed_data.npy', results['data_clean'])
