#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
雷达数据预处理模块 V2.0 - 完整5步流程
========================================

实现GPT标准预处理流程的完整版本：
1. 异常值检测与质量控制 (Outlier Detection and QC)
2. 几何与基准修正 (Geometry Correction) ← 新增
3. 去趋势处理 (Detrending)
4. 滤波 (Filtering)
5. 重采样与时间对齐 (Resampling) ← 新增

作者：Wave Monitoring System - Signal Processing Team
日期：2026-01-12
版本：2.0

变更历史：
- v2.0 (2026-01-12): 集成几何修正和重采样模块
- v1.0 (2025-11-21): 初始版本，4步流程
"""

import numpy as np
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

# 导入原有模块（步骤1, 3, 4）
from radar_preprocessor import RadarPreprocessor, QCFlags

# 导入新增模块（步骤2, 5）
try:
    from geometry_correction import GeometryCorrector
except ImportError:
    GeometryCorrector = None
    logging.warning("geometry_correction module not available, geometry correction disabled")

try:
    from resampling import TimeAlignmentResampler
except ImportError:
    TimeAlignmentResampler = None
    logging.warning("resampling module not available, resampling disabled")


class RadarPreprocessorV2:
    """
    雷达数据预处理器 V2.0

    完整实现5步标准预处理流程
    """

    def __init__(self, config: Dict):
        """
        初始化预处理器

        参数:
            config: 系统配置字典
        """
        self.config = config
        self.sample_rate = config['collection']['sample_rate']

        # 初始化各步骤模块
        self.qc_processor = RadarPreprocessor(config)  # 步骤1, 3, 4

        # 预处理配置
        preproc_config = config.get('preprocessing', {})
        self.enable_geometry = (
            preproc_config.get('geometry_correction', {}).get('enable', True)
            and GeometryCorrector is not None
        )
        self.enable_resampling = (
            preproc_config.get('resampling', {}).get('enable', True)
            and TimeAlignmentResampler is not None
        )

        self.geometry_corrector = GeometryCorrector(config) if self.enable_geometry else None
        self.resampler = TimeAlignmentResampler(config) if self.enable_resampling else None

        # 统计信息
        self.processing_stats = {
            'total_processed': 0,
            'geometry_corrections': 0,
            'resampling_operations': 0,
            'quality_issues': 0
        }

        logging.info("RadarPreprocessorV2 initialized with 5-step pipeline")

        # 启动时验证几何配置
        if self.enable_geometry and self.geometry_corrector:
            validation = self.geometry_corrector.validate_geometry()
            if not validation['valid']:
                logging.error(f"Geometry validation failed: {validation['errors']}")

    # ========================================================================
    # 单雷达预处理（用于历史数据分析）
    # ========================================================================

    def preprocess_single_radar(self,
                                  data: np.ndarray,
                                  radar_id: int = 1,
                                  full_pipeline: bool = True) -> Dict:
        """
        单个雷达的完整预处理流程（不含重采样）

        步骤：
        1. 异常值检测与剔除
        2. 几何修正（斜测距 → 垂直距离）
        3. 数据插补
        4. 去趋势处理
        5. 滤波

        参数:
            data: 原始测距数据（米）
            radar_id: 雷达编号（1-3）
            full_pipeline: 是否执行完整流程

        返回:
            results: 包含各步骤结果的字典
        """
        logging.info(f"Starting single radar preprocessing: Radar {radar_id}, "
                     f"{len(data)} samples")

        results = {
            'radar_id': radar_id,
            'data_original': data.copy(),
            'reports': {},
            'quality_score': 100
        }

        # ====================================================================
        # 步骤1: 异常值检测
        # ====================================================================
        outlier_mask, outlier_report = self.qc_processor.detect_all_outliers(data)
        results['outlier_mask'] = outlier_mask
        results['reports']['outlier_detection'] = outlier_report

        outlier_ratio = outlier_report['total_ratio']
        if outlier_ratio > 0.10:
            results['quality_score'] -= 30
        elif outlier_ratio > 0.05:
            results['quality_score'] -= 15
        elif outlier_ratio > 0.02:
            results['quality_score'] -= 5

        if not full_pipeline:
            return results

        # ====================================================================
        # 步骤2: 几何修正（新增！）
        # ====================================================================
        if self.enable_geometry:
            data_corrected = np.zeros_like(data)
            geometry_reports = []

            for i, value in enumerate(data):
                if outlier_mask[i]:
                    # 异常值跳过几何修正
                    data_corrected[i] = value
                else:
                    corrected, report = self.geometry_corrector.correct_slant_distance(
                        value, radar_id
                    )
                    data_corrected[i] = corrected
                    if i == 0:  # 保存第一个报告作为参考
                        geometry_reports.append(report)

            results['data_geometry_corrected'] = data_corrected
            results['reports']['geometry_correction'] = {
                'enabled': True,
                'radar_id': radar_id,
                'sample_report': geometry_reports[0] if geometry_reports else None
            }
            self.processing_stats['geometry_corrections'] += 1
        else:
            data_corrected = data.copy()
            results['reports']['geometry_correction'] = {'enabled': False}

        # ====================================================================
        # 步骤3: 数据插补
        # ====================================================================
        data_interpolated, interp_report = self.qc_processor.interpolate_gaps(
            data_corrected, outlier_mask
        )
        results['reports']['interpolation'] = interp_report

        if not interp_report['success']:
            logging.warning("Interpolation failed")
            results['quality_score'] -= 20
            data_interpolated = data_corrected

        # ====================================================================
        # 步骤4: 去趋势处理
        # ====================================================================
        data_detrended, detrend_report = self.qc_processor.detrend_data(
            data_interpolated, method='linear'
        )
        results['reports']['detrending'] = detrend_report

        if not detrend_report['success']:
            results['quality_score'] -= 10
            data_detrended = data_interpolated

        # ====================================================================
        # 步骤5: 滤波
        # ====================================================================
        data_filtered, filter_report = self.qc_processor.apply_filter(
            data_detrended, filter_type='bandpass'
        )
        results['reports']['filtering'] = filter_report

        if not filter_report.get('success', False):
            results['quality_score'] -= 10
            data_filtered = data_detrended

        # ====================================================================
        # 最终输出
        # ====================================================================
        results['data_clean'] = data_filtered

        # 计算统计量
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

        self.processing_stats['total_processed'] += 1

        logging.info(f"Single radar preprocessing completed: quality_score={results['quality_score']}")

        return results

    # ========================================================================
    # 三雷达联合预处理（含时间对齐）
    # ========================================================================

    def preprocess_three_radars(self,
                                 samples: List[Dict],
                                 include_resampling: bool = True) -> Dict:
        """
        三雷达联合预处理（完整5步流程）

        步骤：
        1-4. 对每个雷达分别进行QC、几何修正、去趋势、滤波
        5. 时间对齐与重采样（联合处理）

        参数:
            samples: 样本列表，每个样本包含 {'timestamps': [...], 'heights': [...]}
            include_resampling: 是否包含重采样步骤

        返回:
            results: 包含三雷达预处理结果的字典
        """
        logging.info(f"Starting three-radar preprocessing: {len(samples)} samples, "
                     f"resampling={include_resampling}")

        results = {
            'n_samples': len(samples),
            'radars': {},
            'resampling': None,
            'overall_quality': 100
        }

        # ====================================================================
        # 步骤1-4: 对每个雷达分别处理
        # ====================================================================

        # 提取三雷达数据
        radar_data = {1: [], 2: [], 3: []}

        for sample in samples:
            heights = sample.get('heights', [None, None, None])
            for i in range(3):
                if heights[i] is not None:
                    radar_data[i + 1].append(heights[i])

        # 分别预处理
        for radar_id in [1, 2, 3]:
            data = np.array(radar_data[radar_id])

            if len(data) < 10:
                logging.warning(f"Radar {radar_id}: insufficient data ({len(data)} samples)")
                results['radars'][radar_id] = {
                    'error': 'insufficient_data',
                    'quality_score': 0
                }
                continue

            # 执行单雷达预处理（步骤1-4）
            radar_results = self.preprocess_single_radar(
                data,
                radar_id=radar_id,
                full_pipeline=True
            )

            results['radars'][radar_id] = radar_results

            # 更新总体质量分数
            results['overall_quality'] = min(
                results['overall_quality'],
                radar_results['quality_score']
            )

        # ====================================================================
        # 步骤5: 时间对齐与重采样（新增！）
        # ====================================================================

        if include_resampling and self.enable_resampling:
            try:
                resampling_result = self.resampler.align_and_resample(samples)
                results['resampling'] = resampling_result

                if resampling_result['success']:
                    # 将重采样后的数据覆盖到各雷达结果中
                    for radar_id in [1, 2, 3]:
                        if radar_id in results['radars']:
                            results['radars'][radar_id]['data_resampled'] = \
                                resampling_result['resampled_data'][:, radar_id - 1]

                    self.processing_stats['resampling_operations'] += 1
                    logging.info("Resampling completed successfully")
                else:
                    logging.warning("Resampling failed")
                    results['overall_quality'] -= 10

            except Exception as e:
                logging.error(f"Resampling error: {e}")
                results['resampling'] = {
                    'enabled': True,
                    'success': False,
                    'error': str(e)
                }
                results['overall_quality'] -= 10
        else:
            results['resampling'] = {'enabled': False}

        # ====================================================================
        # 三雷达交叉验证
        # ====================================================================

        # 提取清洗后的数据进行交叉验证
        eta1 = results['radars'].get(1, {}).get('data_clean', np.array([]))
        eta2 = results['radars'].get(2, {}).get('data_clean', np.array([]))
        eta3 = results['radars'].get(3, {}).get('data_clean', np.array([]))

        if len(eta1) > 0 and len(eta2) > 0 and len(eta3) > 0:
            # 确保长度一致
            min_len = min(len(eta1), len(eta2), len(eta3))
            cross_val = self.qc_processor.cross_validate_radars(
                eta1[:min_len],
                eta2[:min_len],
                eta3[:min_len]
            )
            results['cross_validation'] = cross_val

            if not cross_val['consistent']:
                results['overall_quality'] -= 15
                self.processing_stats['quality_issues'] += 1

        logging.info(f"Three-radar preprocessing completed: "
                     f"overall_quality={results['overall_quality']}")

        return results

    # ========================================================================
    # 便捷方法：从样本列表到高程数据
    # ========================================================================

    def samples_to_elevations(self,
                               samples: List[Dict],
                               correct_geometry: bool = True) -> Tuple[np.ndarray, Dict]:
        """
        将样本列表转换为高程数据（含几何修正）

        参数:
            samples: 样本列表
            correct_geometry: 是否进行几何修正

        返回:
            (elevations, report)
            - elevations: 高程数组 (N×3)
            - report: 转换报告
        """
        # 提取测距数据
        distances = []
        for sample in samples:
            heights = sample.get('heights', [None, None, None])
            if all(h is not None for h in heights):
                distances.append(heights)

        if not distances:
            logging.error("No valid samples for conversion")
            return np.array([]), {'error': 'no_valid_samples'}

        distances_array = np.array(distances)

        # 几何修正 + 转换为高程
        elevations = self.geometry_corrector.distance_to_elevation(
            distances_array,
            radar_ids=[1, 2, 3],
            correct_geometry=correct_geometry
        )

        report = {
            'n_samples': len(elevations),
            'geometry_corrected': correct_geometry,
            'array_height': self.geometry_corrector.array_height,
            'statistics': {
                'mean': float(np.mean(elevations)),
                'std': float(np.std(elevations)),
                'min': float(np.min(elevations)),
                'max': float(np.max(elevations))
            }
        }

        return elevations, report

    # ========================================================================
    # 统计与报告
    # ========================================================================

    def get_processing_stats(self) -> Dict:
        """获取预处理统计信息"""
        stats = self.processing_stats.copy()

        # 添加各模块统计
        stats['qc_stats'] = {
            'method': self.qc_processor.outlier_method,
            'threshold': self.qc_processor.outlier_threshold
        }

        if self.enable_geometry:
            stats['geometry_stats'] = self.geometry_corrector.get_correction_stats()

        if self.enable_resampling:
            stats['resampling_stats'] = self.resampler.get_stats()

        return stats

    def reset_stats(self):
        """重置统计计数器"""
        self.processing_stats = {
            'total_processed': 0,
            'geometry_corrections': 0,
            'resampling_operations': 0,
            'quality_issues': 0
        }


# ============================================================================
# 实用工具函数
# ============================================================================

def generate_full_preprocessing_report(results: Dict) -> str:
    """
    生成完整的预处理报告（5步流程）

    参数:
        results: preprocess_three_radars() 的返回结果

    返回:
        格式化的报告字符串
    """
    lines = [
        "=" * 80,
        "雷达数据预处理报告 V2.0 - 完整5步流程",
        "Radar Data Preprocessing Report - Full 5-Step Pipeline",
        "=" * 80,
        "",
        f"样本数量: {results['n_samples']}",
        f"总体质量评分: {results['overall_quality']}/100",
        ""
    ]

    # 各雷达报告
    for radar_id in [1, 2, 3]:
        if radar_id not in results['radars']:
            continue

        radar_res = results['radars'][radar_id]

        if 'error' in radar_res:
            lines.append(f"雷达 {radar_id}: 错误 - {radar_res['error']}")
            continue

        lines.extend([
            f"--- 雷达 {radar_id} ---",
            f"质量评分: {radar_res['quality_score']}/100",
            ""
        ])

        # 步骤1: 异常检测
        outlier_rep = radar_res['reports']['outlier_detection']
        lines.append("  [步骤1] 异常值检测:")
        lines.append(f"    总异常数: {outlier_rep['total_outliers']} "
                     f"({outlier_rep['total_ratio']*100:.2f}%)")

        # 步骤2: 几何修正
        geom_rep = radar_res['reports']['geometry_correction']
        lines.append("  [步骤2] 几何修正:")
        if geom_rep['enabled']:
            if 'sample_report' in geom_rep and geom_rep['sample_report']:
                sample = geom_rep['sample_report']
                lines.append(f"    倾角: {sample['tilt_angle']:.2f}°")
                lines.append(f"    修正系数: {sample['cos_tilt']:.6f}")
                lines.append(f"    相对误差: {sample['relative_error']*100:.3f}%")
        else:
            lines.append("    未启用")

        # 步骤3: 插值
        interp_rep = radar_res['reports'].get('interpolation', {})
        if interp_rep:
            lines.append("  [步骤3] 数据插补:")
            lines.append(f"    方法: {interp_rep['method']}")
            lines.append(f"    插补样本: {interp_rep['gap_count']}")

        # 步骤4: 去趋势
        detrend_rep = radar_res['reports'].get('detrending', {})
        if detrend_rep:
            lines.append("  [步骤4] 去趋势:")
            lines.append(f"    方法: {detrend_rep['method']}")
            if 'trend_amplitude' in detrend_rep:
                lines.append(f"    趋势幅度: {detrend_rep['trend_amplitude']:.4f}m")

        # 步骤5: 滤波
        filter_rep = radar_res['reports'].get('filtering', {})
        if filter_rep and filter_rep.get('enabled'):
            lines.append("  [步骤5] 滤波:")
            lines.append(f"    类型: {filter_rep['filter_type']}")
            lines.append(f"    频段: {filter_rep.get('passband', 'N/A')}")

        lines.append("")

    # 步骤5: 重采样（三雷达联合）
    if 'resampling' in results and results['resampling']:
        resample_res = results['resampling']
        lines.append("--- [步骤5] 时间对齐与重采样 ---")

        if resample_res.get('enabled'):
            if resample_res.get('success'):
                lines.append(f"  方法: {resample_res['method']}")
                lines.append(f"  重采样率: {resample_res['sample_rate']} Hz")
                lines.append(f"  重采样样本数: {resample_res['n_samples']}")

                dev_rep = resample_res['deviation_report']
                lines.append(f"  最大时间偏差: {dev_rep['max_deviation']*1000:.2f} ms")

                if dev_rep.get('warning'):
                    lines.append("  ⚠️ 警告: 时间偏差超过阈值")
            else:
                lines.append(f"  失败: {resample_res.get('error', 'unknown')}")
        else:
            lines.append("  未启用")

        lines.append("")

    # 交叉验证
    if 'cross_validation' in results:
        cross_val = results['cross_validation']
        lines.append("--- 三雷达交叉验证 ---")
        lines.append(f"  一致性: {'通过' if cross_val['consistent'] else '未通过'}")

        if cross_val.get('warnings'):
            for warning in cross_val['warnings']:
                lines.append(f"  ⚠️ {warning}")

        lines.append("")

    lines.append("=" * 80)

    return "\n".join(lines)


# ============================================================================
# 示例用法
# ============================================================================

if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # 模拟完整配置
    config = {
        'collection': {'sample_rate': 6},
        'radar': {
            'array_height': 5.0,
            'tilt_angles': {'R1': 0.0, 'R2': 10.0, 'R3': 10.0},
            'diwasp_positions': {
                'R1': [0.0, 0.0, 0.0],
                'R2': [-0.1333, 0.2309, 0.0],
                'R3': [0.1333, 0.2309, 0.0]
            }
        },
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
                    'threshold': 3.5,
                    'window_size': 60
                },
                'interpolation': {
                    'max_gap_ratio': 0.05,
                    'method': 'cubic'
                }
            }
        },
        'preprocessing': {
            'geometry_correction': {
                'enable': True,
                'use_tilt_angles': True,
                'platform_motion_compensation': False
            },
            'resampling': {
                'enable': True,
                'method': 'cubic',
                'time_alignment': 'first_radar',
                'max_time_deviation': 0.1
            }
        }
    }

    # 创建预处理器
    preprocessor = RadarPreprocessorV2(config)

    # 测试单雷达预处理
    print("\n=== 测试单雷达预处理 ===")
    np.random.seed(42)
    test_data = 3.0 + 0.5 * np.sin(2 * np.pi * 0.1 * np.arange(100) / 6) + \
                0.05 * np.random.randn(100)

    single_result = preprocessor.preprocess_single_radar(
        test_data,
        radar_id=2,  # 测试10°倾角的雷达
        full_pipeline=True
    )

    print(f"质量评分: {single_result['quality_score']}")
    print(f"几何修正报告: {single_result['reports']['geometry_correction']}")

    # 获取统计信息
    stats = preprocessor.get_processing_stats()
    print(f"\n统计信息:")
    print(f"  已处理: {stats['total_processed']}")
    print(f"  几何修正: {stats['geometry_corrections']}")

    print("\n预处理器V2.0初始化完成！")
