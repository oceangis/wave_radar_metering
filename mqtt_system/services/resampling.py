#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
时间对齐与重采样模块
====================

功能：
1. 三雷达时间戳对齐 (Time Alignment)
2. 不均匀采样数据重采样到均匀时间网格 (Resampling)
3. 时间偏差检测与报警 (Time Deviation Detection)
4. 多种插值方法 (Linear, Cubic, Sinc)

作者：Wave Monitoring System - Signal Processing Team
日期：2026-01-12

理论依据：
- Shannon-Nyquist Sampling Theorem
- Oppenheim & Schafer (2009) - Discrete-Time Signal Processing
- Press et al. (2007) - Numerical Recipes
"""

import numpy as np
import pandas as pd
import logging
from scipy.interpolate import interp1d, CubicSpline
from scipy.signal import resample
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone
from dataclasses import dataclass


@dataclass
class TimeStampSet:
    """时间戳集合"""
    timestamps: List[datetime]  # 时间戳列表
    values: np.ndarray  # 对应的值
    radar_id: int  # 雷达ID


class TimeAlignmentResampler:
    """
    时间对齐与重采样器

    实现标准预处理流程的第5步：重采样与时间对齐
    """

    def __init__(self, config: Dict):
        """
        初始化重采样器

        参数:
            config: 系统配置字典
        """
        self.config = config

        # 采样率配置
        self.target_sample_rate = config['collection']['sample_rate']
        self.target_interval = 1.0 / self.target_sample_rate  # 秒

        # 重采样配置
        preproc_config = config.get('preprocessing', {})
        resample_config = preproc_config.get('resampling', {})

        self.enable = resample_config.get('enable', True)
        self.method = resample_config.get('method', 'cubic')
        self.time_alignment = resample_config.get('time_alignment', 'first_radar')
        self.max_time_deviation = resample_config.get('max_time_deviation', 0.1)  # 秒

        # 统计信息
        self.stats = {
            'total_resamples': 0,
            'max_deviation_detected': 0.0,
            'mean_deviation': 0.0,
            'warnings': 0
        }

        logging.info(f"TimeAlignmentResampler initialized: enable={self.enable}, "
                     f"method={self.method}, target_rate={self.target_sample_rate}Hz")

    def parse_timestamps(self, timestamps: List[str]) -> List[datetime]:
        """
        解析ISO格式时间戳

        参数:
            timestamps: ISO格式时间戳字符串列表

        返回:
            datetime对象列表
        """
        parsed = []
        for ts in timestamps:
            if isinstance(ts, str):
                # 解析ISO格式
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            elif isinstance(ts, datetime):
                dt = ts
            else:
                raise ValueError(f"Unsupported timestamp type: {type(ts)}")
            parsed.append(dt)
        return parsed

    def compute_time_deviations(self,
                                 timestamps_list: List[List[datetime]]) -> Dict:
        """
        计算三雷达时间戳偏差

        参数:
            timestamps_list: 三雷达时间戳列表 [[R1_times], [R2_times], [R3_times]]

        返回:
            deviation_report: 偏差报告字典
        """
        if len(timestamps_list) != 3:
            raise ValueError("Expected 3 radar timestamp lists")

        n_samples = min(len(ts) for ts in timestamps_list)

        # 计算逐样本的时间差（相对于R1）
        deviations = {
            'R1-R2': [],
            'R1-R3': [],
            'R2-R3': []
        }

        for i in range(n_samples):
            t1 = timestamps_list[0][i]
            t2 = timestamps_list[1][i]
            t3 = timestamps_list[2][i]

            # 时间差（秒）
            delta_12 = (t2 - t1).total_seconds()
            delta_13 = (t3 - t1).total_seconds()
            delta_23 = (t3 - t2).total_seconds()

            deviations['R1-R2'].append(delta_12)
            deviations['R1-R3'].append(delta_13)
            deviations['R2-R3'].append(delta_23)

        # 统计分析
        report = {
            'n_samples': n_samples,
            'deviations': {}
        }

        for key, values in deviations.items():
            arr = np.array(values)
            report['deviations'][key] = {
                'mean': float(np.mean(arr)),
                'std': float(np.std(arr)),
                'min': float(np.min(arr)),
                'max': float(np.max(arr)),
                'abs_max': float(np.max(np.abs(arr)))
            }

        # 计算总体最大偏差
        max_deviation = max(
            report['deviations'][key]['abs_max']
            for key in report['deviations']
        )

        report['max_deviation'] = max_deviation

        # 更新统计
        self.stats['max_deviation_detected'] = max(
            self.stats['max_deviation_detected'],
            max_deviation
        )

        # 检查是否超过阈值
        if max_deviation > self.max_time_deviation:
            self.stats['warnings'] += 1
            logging.warning(
                f"Time deviation {max_deviation*1000:.2f}ms exceeds threshold "
                f"{self.max_time_deviation*1000:.2f}ms"
            )
            report['warning'] = True
        else:
            report['warning'] = False

        return report

    def generate_uniform_time_grid(self,
                                    start_time: datetime,
                                    end_time: datetime,
                                    sample_rate: Optional[float] = None) -> np.ndarray:
        """
        生成均匀时间网格

        参数:
            start_time: 起始时间
            end_time: 结束时间
            sample_rate: 采样率（Hz），默认使用target_sample_rate

        返回:
            time_seconds: 相对于起始时间的秒数数组
        """
        if sample_rate is None:
            sample_rate = self.target_sample_rate

        interval = 1.0 / sample_rate
        duration = (end_time - start_time).total_seconds()

        # 生成均匀时间网格
        n_samples = int(np.floor(duration * sample_rate)) + 1
        time_seconds = np.arange(n_samples) * interval

        return time_seconds

    def resample_single_radar(self,
                               timestamps: List[datetime],
                               values: np.ndarray,
                               target_times: np.ndarray,
                               reference_time: datetime,
                               method: Optional[str] = None) -> Tuple[np.ndarray, Dict]:
        """
        重采样单个雷达的数据到目标时间网格

        参数:
            timestamps: 原始时间戳列表
            values: 原始值数组
            target_times: 目标时间网格（相对秒数）
            reference_time: 参考时间（用于转换相对时间）
            method: 插值方法 ('linear', 'cubic', 'sinc')

        返回:
            (resampled_values, report)
            - resampled_values: 重采样后的值
            - report: 重采样报告
        """
        if method is None:
            method = self.method

        # 将原始时间戳转换为相对秒数
        original_times = np.array([
            (ts - reference_time).total_seconds()
            for ts in timestamps
        ])

        report = {
            'method': method,
            'original_samples': len(values),
            'target_samples': len(target_times),
            'success': False
        }

        try:
            if method == 'linear':
                # 线性插值（最快）
                f = interp1d(original_times, values,
                             kind='linear',
                             bounds_error=False,
                             fill_value='extrapolate')
                resampled_values = f(target_times)

            elif method == 'cubic':
                # 三次样条插值（推荐）
                if len(values) < 4:
                    # 数据点不足，退化到线性
                    logging.warning("Insufficient points for cubic, using linear")
                    f = interp1d(original_times, values,
                                 kind='linear',
                                 bounds_error=False,
                                 fill_value='extrapolate')
                    resampled_values = f(target_times)
                    report['method'] = 'linear_fallback'
                else:
                    cs = CubicSpline(original_times, values,
                                     bc_type='natural',
                                     extrapolate=True)
                    resampled_values = cs(target_times)

            elif method == 'sinc':
                # Sinc插值（频域重采样，最精确但最慢）
                # 使用scipy.signal.resample
                target_n = len(target_times)
                resampled_values = resample(values, target_n)

                # 注意：scipy.resample假设原始数据是均匀采样的
                # 对于不均匀采样，需要先用cubic插值到均匀网格
                logging.warning("Sinc method assumes uniform input, consider using cubic")

            else:
                raise ValueError(f"Unknown resampling method: {method}")

            report['success'] = True
            report['min_value'] = float(np.min(resampled_values))
            report['max_value'] = float(np.max(resampled_values))
            report['mean_value'] = float(np.mean(resampled_values))

        except Exception as e:
            logging.error(f"Resampling failed: {e}")
            report['error'] = str(e)
            # 失败时返回NaN数组
            resampled_values = np.full(len(target_times), np.nan)

        return resampled_values, report

    def align_and_resample(self,
                           samples: List[Dict],
                           window_duration: Optional[float] = None) -> Dict:
        """
        对齐并重采样三雷达数据

        参数:
            samples: 样本列表，每个样本包含 {'timestamps': [...], 'heights': [...]}
            window_duration: 窗口持续时间（秒），默认使用所有数据

        返回:
            result: 包含对齐和重采样后数据的字典
        """
        if not self.enable:
            logging.info("Resampling disabled, returning original data")
            return {'enabled': False, 'samples': samples}

        # 提取三雷达的时间戳和值
        n_radars = 3
        timestamps_list = [[] for _ in range(n_radars)]
        values_list = [[] for _ in range(n_radars)]

        for sample in samples:
            timestamps = sample.get('timestamps', [None] * n_radars)
            heights = sample.get('heights', [None] * n_radars)

            for i in range(n_radars):
                if timestamps[i] is not None and heights[i] is not None:
                    ts = self.parse_timestamps([timestamps[i]])[0]
                    timestamps_list[i].append(ts)
                    values_list[i].append(heights[i])

        # 检查数据完整性
        sample_counts = [len(ts) for ts in timestamps_list]
        if min(sample_counts) < 10:
            logging.error(f"Insufficient samples for resampling: {sample_counts}")
            return {
                'enabled': True,
                'success': False,
                'error': 'insufficient_samples',
                'sample_counts': sample_counts
            }

        # 计算时间偏差
        deviation_report = self.compute_time_deviations(timestamps_list)

        # 确定参考时间和时间范围
        if self.time_alignment == 'first_radar':
            reference_time = timestamps_list[0][0]
        elif self.time_alignment == 'earliest':
            reference_time = min(ts[0] for ts in timestamps_list)
        elif self.time_alignment == 'mean':
            # 使用平均时间作为参考
            mean_timestamp = np.mean([ts[0].timestamp() for ts in timestamps_list])
            reference_time = datetime.fromtimestamp(mean_timestamp, tz=timezone.utc)
        else:
            reference_time = timestamps_list[0][0]

        # 确定时间窗口
        if window_duration is None:
            # 使用所有数据的时间范围
            all_start_times = [ts[0] for ts in timestamps_list]
            all_end_times = [ts[-1] for ts in timestamps_list]
            start_time = max(all_start_times)  # 最晚的起始时间
            end_time = min(all_end_times)  # 最早的结束时间
        else:
            start_time = reference_time
            end_time = reference_time + pd.Timedelta(seconds=window_duration)

        # 生成均匀时间网格
        target_times = self.generate_uniform_time_grid(start_time, end_time)

        # 重采样三个雷达
        resampled_data = []
        resample_reports = []

        for i in range(n_radars):
            values_array = np.array(values_list[i])
            resampled_values, report = self.resample_single_radar(
                timestamps_list[i],
                values_array,
                target_times,
                start_time,
                method=self.method
            )
            resampled_data.append(resampled_values)
            resample_reports.append(report)

        # 转换为numpy数组 (N×3)
        resampled_array = np.column_stack(resampled_data)

        # 生成绝对时间戳
        resampled_timestamps = [
            start_time + pd.Timedelta(seconds=t)
            for t in target_times
        ]

        # 更新统计
        self.stats['total_resamples'] += 1

        result = {
            'enabled': True,
            'success': True,
            'method': self.method,
            'reference_time': start_time.isoformat(),
            'n_samples': len(target_times),
            'sample_rate': self.target_sample_rate,
            'time_interval': self.target_interval,
            'resampled_data': resampled_array,  # (N×3) array
            'timestamps': resampled_timestamps,  # 绝对时间戳列表
            'relative_times': target_times,  # 相对时间（秒）
            'deviation_report': deviation_report,
            'resample_reports': resample_reports,
            'original_sample_counts': sample_counts
        }

        logging.info(f"Resampling completed: {len(target_times)} samples at {self.target_sample_rate}Hz, "
                     f"max deviation {deviation_report['max_deviation']*1000:.2f}ms")

        return result

    def validate_resampling_quality(self,
                                     original_values: np.ndarray,
                                     resampled_values: np.ndarray) -> Dict:
        """
        验证重采样质量

        检查：
        1. 能量守恒（信号能量变化）
        2. 幅值范围保持
        3. NaN值比例

        参数:
            original_values: 原始值
            resampled_values: 重采样后的值

        返回:
            quality_report: 质量报告
        """
        report = {
            'valid': True,
            'warnings': []
        }

        # 1. 能量守恒检查
        energy_original = np.sum(original_values ** 2)
        energy_resampled = np.sum(resampled_values ** 2)
        energy_ratio = energy_resampled / (energy_original + 1e-10)

        report['energy_ratio'] = float(energy_ratio)

        if energy_ratio < 0.9 or energy_ratio > 1.1:
            report['warnings'].append(
                f"Energy ratio {energy_ratio:.3f} outside expected range [0.9, 1.1]"
            )

        # 2. 幅值范围检查
        range_original = np.max(original_values) - np.min(original_values)
        range_resampled = np.max(resampled_values) - np.min(resampled_values)
        range_ratio = range_resampled / (range_original + 1e-10)

        report['range_ratio'] = float(range_ratio)

        if range_ratio < 0.8 or range_ratio > 1.2:
            report['warnings'].append(
                f"Range ratio {range_ratio:.3f} outside expected range [0.8, 1.2]"
            )

        # 3. NaN值检查
        nan_count = np.sum(np.isnan(resampled_values))
        nan_ratio = nan_count / len(resampled_values)

        report['nan_count'] = int(nan_count)
        report['nan_ratio'] = float(nan_ratio)

        if nan_ratio > 0.01:  # 超过1%
            report['warnings'].append(
                f"NaN ratio {nan_ratio*100:.2f}% exceeds 1%"
            )

        if report['warnings']:
            report['valid'] = False
            logging.warning(f"Resampling quality issues: {report['warnings']}")

        return report

    def get_stats(self) -> Dict:
        """
        获取重采样统计信息

        返回:
            stats: 统计字典
        """
        stats = self.stats.copy()
        stats['enabled'] = self.enable
        stats['method'] = self.method
        stats['target_sample_rate'] = self.target_sample_rate
        return stats


# ============================================================================
# 实用工具函数
# ============================================================================

def generate_resampling_report(result: Dict) -> str:
    """
    生成重采样报告

    参数:
        result: align_and_resample() 的返回结果

    返回:
        格式化的报告字符串
    """
    if not result.get('enabled'):
        return "重采样功能未启用"

    if not result.get('success'):
        return f"重采样失败: {result.get('error', 'unknown')}"

    lines = [
        "=" * 70,
        "时间对齐与重采样报告 (Time Alignment & Resampling Report)",
        "=" * 70,
        "",
        f"重采样方法: {result['method']}",
        f"目标采样率: {result['sample_rate']} Hz",
        f"时间间隔: {result['time_interval']*1000:.3f} ms",
        f"重采样样本数: {result['n_samples']}",
        f"参考时间: {result['reference_time']}",
        "",
        "--- 原始样本数 ---"
    ]

    for i, count in enumerate(result['original_sample_counts']):
        lines.append(f"  雷达 {i+1}: {count} samples")

    lines.append("\n--- 时间偏差分析 ---")

    deviation_rep = result['deviation_report']
    for pair, stats in deviation_rep['deviations'].items():
        lines.extend([
            f"{pair}:",
            f"  平均: {stats['mean']*1000:.3f} ms",
            f"  标准差: {stats['std']*1000:.3f} ms",
            f"  范围: [{stats['min']*1000:.2f}, {stats['max']*1000:.2f}] ms",
            f"  最大绝对值: {stats['abs_max']*1000:.2f} ms",
            ""
        ])

    max_dev = deviation_rep['max_deviation']
    lines.append(f"总体最大偏差: {max_dev*1000:.2f} ms")

    if deviation_rep.get('warning'):
        lines.append("⚠️ 警告: 时间偏差超过阈值")

    lines.append("\n--- 重采样统计 ---")

    for i, report in enumerate(result['resample_reports']):
        if report['success']:
            lines.extend([
                f"雷达 {i+1}:",
                f"  方法: {report['method']}",
                f"  原始样本: {report['original_samples']}",
                f"  目标样本: {report['target_samples']}",
                f"  值范围: [{report['min_value']:.4f}, {report['max_value']:.4f}]",
                ""
            ])
        else:
            lines.append(f"雷达 {i+1}: 重采样失败 - {report.get('error', 'unknown')}\n")

    lines.append("=" * 70)

    return "\n".join(lines)


def plot_time_alignment_diagnostic(timestamps_list: List[List[datetime]],
                                    save_path: Optional[str] = None):
    """
    绘制时间对齐诊断图

    参数:
        timestamps_list: 三雷达时间戳列表
        save_path: 保存路径（可选）
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        logging.warning("matplotlib not available, skipping plot")
        return

    fig, axes = plt.subplots(2, 1, figsize=(12, 8))

    # 子图1: 时间戳序列
    ax1 = axes[0]
    for i, timestamps in enumerate(timestamps_list):
        times_sec = [(ts - timestamps[0]).total_seconds() for ts in timestamps]
        ax1.plot(times_sec, label=f'Radar {i+1}')

    ax1.set_xlabel('Sample Index')
    ax1.set_ylabel('Time (seconds)')
    ax1.set_title('Timestamp Sequences (relative to first sample)')
    ax1.legend()
    ax1.grid(True)

    # 子图2: 时间差
    ax2 = axes[1]
    n_samples = min(len(ts) for ts in timestamps_list)

    deviations = {
        'R1-R2': [],
        'R1-R3': [],
        'R2-R3': []
    }

    for i in range(n_samples):
        t1 = timestamps_list[0][i]
        t2 = timestamps_list[1][i]
        t3 = timestamps_list[2][i]

        deviations['R1-R2'].append((t2 - t1).total_seconds() * 1000)  # ms
        deviations['R1-R3'].append((t3 - t1).total_seconds() * 1000)
        deviations['R2-R3'].append((t3 - t2).total_seconds() * 1000)

    for key, values in deviations.items():
        ax2.plot(values, label=key, alpha=0.7)

    ax2.set_xlabel('Sample Index')
    ax2.set_ylabel('Time Difference (ms)')
    ax2.set_title('Inter-Radar Time Deviations')
    ax2.legend()
    ax2.grid(True)
    ax2.axhline(y=0, color='k', linestyle='--', linewidth=0.5)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150)
        logging.info(f"Time alignment diagnostic plot saved to {save_path}")
    else:
        plt.show()


# ============================================================================
# 示例用法
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
        'preprocessing': {
            'resampling': {
                'enable': True,
                'method': 'cubic',
                'time_alignment': 'first_radar',
                'max_time_deviation': 0.1
            }
        }
    }

    # 创建重采样器
    resampler = TimeAlignmentResampler(config)

    # 模拟三雷达数据（不同时间戳）
    print("生成模拟数据...")
    n_samples = 100
    base_time = datetime.now(timezone.utc)

    # 模拟并行采集的时间戳（有偏移）
    samples = []
    for i in range(n_samples):
        t_base = base_time + pd.Timedelta(seconds=i * 0.16667)  # 6Hz

        # 三个雷达有不同的时间戳偏移（模拟并行采集延迟）
        t1 = t_base
        t2 = t_base + pd.Timedelta(milliseconds=np.random.uniform(0, 30))
        t3 = t_base + pd.Timedelta(milliseconds=np.random.uniform(0, 50))

        # 模拟波浪信号
        wave = 0.5 * np.sin(2 * np.pi * 0.1 * i * 0.16667) + 2.0

        sample = {
            'timestamps': [t1.isoformat(), t2.isoformat(), t3.isoformat()],
            'heights': [
                wave + np.random.randn() * 0.02,
                wave + np.random.randn() * 0.02,
                wave + np.random.randn() * 0.02
            ]
        }
        samples.append(sample)

    # 执行重采样
    print("\n执行时间对齐与重采样...")
    result = resampler.align_and_resample(samples)

    # 打印报告
    print("\n" + generate_resampling_report(result))

    # 验证质量
    if result['success']:
        print("\n--- 质量验证 ---")
        for i in range(3):
            original = np.array([s['heights'][i] for s in samples])
            resampled = result['resampled_data'][:, i]
            quality = resampler.validate_resampling_quality(original, resampled)
            print(f"雷达 {i+1}: 能量比={quality['energy_ratio']:.3f}, "
                  f"范围比={quality['range_ratio']:.3f}, "
                  f"NaN={quality['nan_count']}")
