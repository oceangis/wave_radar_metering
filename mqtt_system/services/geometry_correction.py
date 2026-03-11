#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
雷达几何修正模块
==================

功能：
1. 倾角修正 (Tilt Angle Correction)
2. 三维几何转换 (3D Geometry Transformation)
3. 平台姿态补偿 (Platform Motion Compensation) - 预留接口
4. 空间位置修正 (Spatial Position Correction)

作者：Wave Monitoring System - Signal Processing Team
日期：2026-01-12

理论依据：
- Tucker & Pitt (2001) - Waves in Ocean Engineering
- ISO 19901-1 - Metocean design and operating considerations
- Holthuijsen (2007) - Waves in Oceanic and Coastal Waters
"""

import numpy as np
import logging
from typing import Dict, Tuple, List, Optional
from dataclasses import dataclass


@dataclass
class RadarGeometry:
    """雷达几何参数"""
    radar_id: int
    tilt_angle: float  # 倾角（度）
    tilt_azimuth: float = 0.0  # 倾斜方位角（度，正北为0，顺时针）
    array_height: float = 5.0  # 安装高度（米）
    position_x: float = 0.0  # 相对位置X（米）
    position_y: float = 0.0  # 相对位置Y（米）
    position_z: float = 0.0  # 相对位置Z（米）


class GeometryCorrector:
    """
    雷达几何修正器

    实现标准预处理流程的第2步：几何与基准修正
    """

    def __init__(self, config: Dict):
        """
        初始化几何修正器

        参数:
            config: 系统配置字典
        """
        self.config = config
        self.radar_config = config.get('radar', {})

        # 提取几何参数
        self.array_height = self.radar_config.get('array_height', 5.0)
        self.tilt_angles = self.radar_config.get('tilt_angles', {})
        self.tilt_azimuths = self.radar_config.get('tilt_azimuths', {})

        # 相对位置（用于DIWASP）
        self.relative_positions = self.radar_config.get('relative_positions', {})
        self.diwasp_positions = self.radar_config.get('diwasp_positions', {})

        # 预处理配置
        preproc_config = config.get('preprocessing', {})
        geo_config = preproc_config.get('geometry_correction', {})

        self.enable = geo_config.get('enable', True)
        self.use_tilt_angles = geo_config.get('use_tilt_angles', True)
        self.platform_motion = geo_config.get('platform_motion_compensation', False)

        # 构建雷达几何对象
        self.radar_geometries = self._build_radar_geometries()

        # 修正统计
        self.correction_stats = {
            'total_corrections': 0,
            'max_correction': 0.0,
            'mean_correction': 0.0
        }

        logging.info(f"GeometryCorrector initialized: enable={self.enable}, "
                     f"use_tilt_angles={self.use_tilt_angles}")

        if self.enable:
            self._log_geometry_info()

    def _build_radar_geometries(self) -> Dict[int, RadarGeometry]:
        """构建雷达几何配置"""
        geometries = {}

        for i in range(1, 4):  # R1, R2, R3
            radar_key = f'R{i}'

            # 倾角（度）
            tilt_angle = self.tilt_angles.get(radar_key, 0.0)

            # 倾斜方位角（度，默认0）
            tilt_azimuth = self.tilt_azimuths.get(radar_key, 0.0)

            # 相对位置（来自relative_positions或diwasp_positions）
            if radar_key in self.diwasp_positions:
                pos = self.diwasp_positions[radar_key]
                pos_x, pos_y, pos_z = pos[0], pos[1], pos[2] if len(pos) > 2 else 0.0
            elif i in self.relative_positions:
                pos = self.relative_positions[i]
                pos_x, pos_y = pos[0], pos[1]
                pos_z = 0.0
            else:
                pos_x, pos_y, pos_z = 0.0, 0.0, 0.0

            geometries[i] = RadarGeometry(
                radar_id=i,
                tilt_angle=tilt_angle,
                tilt_azimuth=tilt_azimuth,
                array_height=self.array_height,
                position_x=pos_x,
                position_y=pos_y,
                position_z=pos_z
            )

        return geometries

    def _log_geometry_info(self):
        """记录几何配置信息"""
        logging.info("=" * 60)
        logging.info("雷达几何配置 (Radar Geometry Configuration)")
        logging.info("=" * 60)

        for i, geom in self.radar_geometries.items():
            logging.info(f"Radar {i}:")
            logging.info(f"  倾角 (Tilt Angle): {geom.tilt_angle:.2f}°")
            logging.info(f"  倾斜方位 (Tilt Azimuth): {geom.tilt_azimuth:.2f}°")
            logging.info(f"  安装高度 (Array Height): {geom.array_height:.3f}m")
            logging.info(f"  相对位置 (Position): "
                         f"({geom.position_x:.4f}, {geom.position_y:.4f}, {geom.position_z:.4f})m")

            # 计算修正系数
            cos_tilt = np.cos(np.radians(geom.tilt_angle))
            correction_ratio = 1.0 - cos_tilt
            logging.info(f"  修正系数 (Correction Factor): cos({geom.tilt_angle}°) = {cos_tilt:.6f}")
            logging.info(f"  相对误差 (Relative Error): {correction_ratio * 100:.3f}%")
            logging.info("")

    def correct_slant_distance(self,
                                slant_distance: float,
                                radar_id: int) -> Tuple[float, Dict]:
        """
        斜测距到垂直距离的修正

        原理：
            雷达沿倾角 θ 测量，得到斜测距 d_slant
            垂直距离 d_vertical = d_slant × cos(θ)

        参数:
            slant_distance: 斜测距值（米）
            radar_id: 雷达编号（1-3）

        返回:
            (vertical_distance, report)
            - vertical_distance: 垂直距离（米）
            - report: 修正报告字典
        """
        if not self.enable or not self.use_tilt_angles:
            # 几何修正未启用，直接返回原值
            return slant_distance, {'corrected': False}

        if radar_id not in self.radar_geometries:
            logging.warning(f"Unknown radar_id {radar_id}, skipping correction")
            return slant_distance, {'corrected': False, 'error': 'unknown_radar'}

        geom = self.radar_geometries[radar_id]

        # 计算垂直距离
        tilt_rad = np.radians(geom.tilt_angle)
        cos_tilt = np.cos(tilt_rad)
        vertical_distance = slant_distance * cos_tilt

        # 修正量
        correction = slant_distance - vertical_distance

        # 更新统计
        self.correction_stats['total_corrections'] += 1
        self.correction_stats['max_correction'] = max(
            self.correction_stats['max_correction'],
            abs(correction)
        )

        # 运行平均
        n = self.correction_stats['total_corrections']
        prev_mean = self.correction_stats['mean_correction']
        self.correction_stats['mean_correction'] = (
            (prev_mean * (n - 1) + abs(correction)) / n
        )

        report = {
            'corrected': True,
            'radar_id': radar_id,
            'slant_distance': float(slant_distance),
            'vertical_distance': float(vertical_distance),
            'correction': float(correction),
            'tilt_angle': float(geom.tilt_angle),
            'cos_tilt': float(cos_tilt),
            'relative_error': float(correction / slant_distance) if slant_distance > 0 else 0.0
        }

        return vertical_distance, report

    def correct_batch(self,
                      slant_distances: np.ndarray,
                      radar_ids: List[int]) -> Tuple[np.ndarray, List[Dict]]:
        """
        批量修正斜测距

        参数:
            slant_distances: 斜测距数组 (N×3)
            radar_ids: 雷达ID列表 [1, 2, 3]

        返回:
            (vertical_distances, reports)
            - vertical_distances: 垂直距离数组 (N×3)
            - reports: 修正报告列表
        """
        if slant_distances.ndim == 1:
            slant_distances = slant_distances.reshape(-1, 1)

        n_samples, n_radars = slant_distances.shape
        vertical_distances = np.zeros_like(slant_distances)
        reports = []

        for i, radar_id in enumerate(radar_ids):
            if not self.enable or not self.use_tilt_angles:
                vertical_distances[:, i] = slant_distances[:, i]
                continue

            geom = self.radar_geometries.get(radar_id)
            if geom is None:
                vertical_distances[:, i] = slant_distances[:, i]
                logging.warning(f"Radar {radar_id} geometry not found, skipping correction")
                continue

            # 向量化修正
            tilt_rad = np.radians(geom.tilt_angle)
            cos_tilt = np.cos(tilt_rad)
            vertical_distances[:, i] = slant_distances[:, i] * cos_tilt

            # 计算统计
            corrections = slant_distances[:, i] - vertical_distances[:, i]

            report = {
                'radar_id': radar_id,
                'n_samples': n_samples,
                'tilt_angle': float(geom.tilt_angle),
                'cos_tilt': float(cos_tilt),
                'mean_correction': float(np.mean(corrections)),
                'max_correction': float(np.max(np.abs(corrections))),
                'std_correction': float(np.std(corrections))
            }
            reports.append(report)

        return vertical_distances, reports

    def distance_to_elevation(self,
                               distances: np.ndarray,
                               radar_ids: List[int],
                               correct_geometry: bool = True) -> np.ndarray:
        """
        将测距转换为水面高程

        公式：
            η = array_height - distance_vertical

        参数:
            distances: 测距数组 (N×3)，单位：米
            radar_ids: 雷达ID列表 [1, 2, 3]
            correct_geometry: 是否进行几何修正（默认True）

        返回:
            elevations: 水面高程数组 (N×3)，单位：米
        """
        if correct_geometry:
            # 先进行几何修正
            vertical_distances, _ = self.correct_batch(distances, radar_ids)
        else:
            vertical_distances = distances

        # 转换为高程
        elevations = self.array_height - vertical_distances

        return elevations

    def compute_corrected_diwasp_positions(self) -> Dict[str, List[float]]:
        """
        计算修正后的DIWASP位置

        考虑倾角对空间位置的影响（如果倾斜方位已知）

        返回:
            corrected_positions: 修正后的位置字典 {'R1': [x, y, z], ...}
        """
        corrected_positions = {}

        for i in range(1, 4):
            radar_key = f'R{i}'
            geom = self.radar_geometries[i]

            # 原始位置
            x0, y0, z0 = geom.position_x, geom.position_y, geom.position_z

            if geom.tilt_angle == 0.0:
                # 无倾角，位置不变
                corrected_positions[radar_key] = [x0, y0, z0]
                continue

            # 倾角影响（简化模型：仅考虑垂直收缩）
            # 更精确的模型需要知道倾斜的方位角和测量时的瞬时波向
            # 这里采用保守估计：假设倾斜不影响水平位置
            tilt_rad = np.radians(geom.tilt_angle)

            # 如果有倾斜方位信息，可以计算水平投影
            azimuth_rad = np.radians(geom.tilt_azimuth)

            # 倾斜导致的水平偏移（沿倾斜方位）
            # 假设测距为 d，则水平偏移为 d × sin(θ)
            # 但这里 d 是变量，我们只能修正相对位置
            # 保守做法：保持原位置不变，仅修正测距

            corrected_positions[radar_key] = [x0, y0, z0]

            logging.debug(f"DIWASP position {radar_key}: "
                          f"({x0:.4f}, {y0:.4f}, {z0:.4f}) [no horizontal correction]")

        return corrected_positions

    def get_correction_stats(self) -> Dict:
        """
        获取修正统计信息

        返回:
            stats: 统计字典
        """
        stats = self.correction_stats.copy()
        stats['enabled'] = self.enable
        stats['use_tilt_angles'] = self.use_tilt_angles

        # 添加每个雷达的修正系数
        stats['radar_corrections'] = {}
        for i, geom in self.radar_geometries.items():
            cos_tilt = np.cos(np.radians(geom.tilt_angle))
            stats['radar_corrections'][f'R{i}'] = {
                'tilt_angle': geom.tilt_angle,
                'cos_tilt': cos_tilt,
                'correction_factor': 1.0 - cos_tilt,
                'relative_error_percent': (1.0 - cos_tilt) * 100
            }

        return stats

    def validate_geometry(self) -> Dict:
        """
        验证几何配置的一致性

        检查：
        1. 倾角是否在合理范围（0-30°）
        2. DIWASP位置是否与relative_positions一致
        3. 三雷达是否形成有效阵列（非共线）

        返回:
            validation_report: 验证报告
        """
        report = {
            'valid': True,
            'warnings': [],
            'errors': []
        }

        # 1. 检查倾角范围
        for i, geom in self.radar_geometries.items():
            if geom.tilt_angle < 0 or geom.tilt_angle > 30:
                report['warnings'].append(
                    f"Radar {i} tilt angle {geom.tilt_angle}° outside typical range [0, 30]°"
                )

        # 2. 检查位置一致性
        diwasp_pos = self.diwasp_positions
        rel_pos = self.relative_positions

        for i in range(1, 4):
            radar_key = f'R{i}'
            if radar_key in diwasp_pos and i in rel_pos:
                diwasp_xy = diwasp_pos[radar_key][:2]
                rel_xy = rel_pos[i]

                diff = np.linalg.norm(np.array(diwasp_xy) - np.array(rel_xy))
                if diff > 0.01:  # 1cm容差
                    report['warnings'].append(
                        f"Radar {i} position mismatch: "
                        f"DIWASP={diwasp_xy}, relative_positions={rel_xy}, diff={diff:.4f}m"
                    )

        # 3. 检查阵列几何（非共线性）
        positions = np.array([
            [geom.position_x, geom.position_y]
            for geom in self.radar_geometries.values()
        ])

        # 计算三角形面积（行列式的一半）
        if positions.shape[0] == 3:
            vec1 = positions[1] - positions[0]
            vec2 = positions[2] - positions[0]
            area = 0.5 * abs(np.cross(vec1, vec2))

            if area < 0.001:  # 面积<0.001 m²，近似共线
                report['errors'].append(
                    f"Radars are nearly collinear (area={area:.6f} m²), "
                    "directional spectrum analysis may fail"
                )
                report['valid'] = False
            else:
                report['array_area'] = float(area)
                logging.info(f"Radar array area: {area:.4f} m²")

        # 4. 检查基线长度
        baselines = {}
        baselines['R1-R2'] = np.linalg.norm(positions[1] - positions[0])
        baselines['R1-R3'] = np.linalg.norm(positions[2] - positions[0])
        baselines['R2-R3'] = np.linalg.norm(positions[1] - positions[2])

        report['baselines'] = {k: float(v) for k, v in baselines.items()}

        # 检查基线是否太短（<0.1m）或太长（>100m）
        for name, length in baselines.items():
            if length < 0.1:
                report['warnings'].append(
                    f"Baseline {name} = {length:.3f}m is very short, "
                    "may limit directional resolution"
                )
            elif length > 100:
                report['warnings'].append(
                    f"Baseline {name} = {length:.1f}m is very long, "
                    "may cause spatial aliasing"
                )

        # 记录结果
        if report['errors']:
            logging.error(f"Geometry validation failed: {report['errors']}")
        if report['warnings']:
            logging.warning(f"Geometry validation warnings: {report['warnings']}")
        if report['valid'] and not report['warnings']:
            logging.info("Geometry validation passed")

        return report


# ============================================================================
# 实用工具函数
# ============================================================================

def generate_correction_report(corrector: GeometryCorrector) -> str:
    """
    生成几何修正报告

    参数:
        corrector: GeometryCorrector 实例

    返回:
        格式化的报告字符串
    """
    stats = corrector.get_correction_stats()

    lines = [
        "=" * 70,
        "几何修正报告 (Geometry Correction Report)",
        "=" * 70,
        "",
        f"修正状态: {'启用' if stats['enabled'] else '禁用'}",
        f"倾角修正: {'启用' if stats['use_tilt_angles'] else '禁用'}",
        f"修正次数: {stats['total_corrections']}",
        "",
        "--- 各雷达修正系数 ---"
    ]

    for radar_key, info in stats['radar_corrections'].items():
        lines.extend([
            f"{radar_key}:",
            f"  倾角: {info['tilt_angle']:.2f}°",
            f"  余弦系数: {info['cos_tilt']:.6f}",
            f"  修正因子: {info['correction_factor']:.6f}",
            f"  相对误差: {info['relative_error_percent']:.3f}%",
            ""
        ])

    if stats['total_corrections'] > 0:
        lines.extend([
            "--- 修正统计 ---",
            f"平均修正量: {stats['mean_correction']:.5f} m",
            f"最大修正量: {stats['max_correction']:.5f} m",
            ""
        ])

    lines.append("=" * 70)

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

    # 模拟配置
    config = {
        'radar': {
            'array_height': 5.0,
            'tilt_angles': {
                'R1': 0.0,
                'R2': 10.0,
                'R3': 10.0
            },
            'tilt_azimuths': {
                'R1': 0.0,
                'R2': 0.0,
                'R3': 0.0
            },
            'diwasp_positions': {
                'R1': [0.0, 0.0, 0.0],
                'R2': [-0.1333, 0.2309, 0.0],
                'R3': [0.1333, 0.2309, 0.0]
            },
            'relative_positions': {
                1: [0.0, 0.0],
                2: [-0.1333, 0.2309],
                3: [0.1333, 0.2309]
            }
        },
        'preprocessing': {
            'geometry_correction': {
                'enable': True,
                'use_tilt_angles': True,
                'platform_motion_compensation': False
            }
        }
    }

    # 创建修正器
    corrector = GeometryCorrector(config)

    # 验证几何配置
    validation = corrector.validate_geometry()
    print(f"\n验证结果: {'通过' if validation['valid'] else '失败'}")

    # 测试单个修正
    print("\n--- 单个修正测试 ---")
    slant_distance = 3.0  # 米
    for radar_id in [1, 2, 3]:
        vertical, report = corrector.correct_slant_distance(slant_distance, radar_id)
        if report['corrected']:
            print(f"Radar {radar_id}: {slant_distance:.4f}m → {vertical:.4f}m "
                  f"(修正量: {report['correction']:.4f}m, {report['relative_error']*100:.3f}%)")
        else:
            print(f"Radar {radar_id}: {slant_distance:.4f}m (无修正)")

    # 测试批量修正
    print("\n--- 批量修正测试 ---")
    n_samples = 100
    slant_distances = np.random.uniform(2.5, 3.5, (n_samples, 3))

    vertical_distances, reports = corrector.correct_batch(slant_distances, [1, 2, 3])

    for report in reports:
        print(f"Radar {report['radar_id']}: "
              f"平均修正 {report['mean_correction']*100:.2f}cm, "
              f"最大修正 {report['max_correction']*100:.2f}cm")

    # 打印完整报告
    print("\n" + generate_correction_report(corrector))
