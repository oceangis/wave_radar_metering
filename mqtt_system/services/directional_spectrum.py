#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Directional Spectrum Analysis Module - 方向谱分析模块
=====================================================

This module provides directional wave spectrum analysis using pyDIWASP.
本模块使用pyDIWASP进行方向波浪谱分析。

Features / 功能:
1. Simulated data generation for R2/R3 based on R1 data
   基于雷达1数据模拟生成雷达2和雷达3的数据
2. Phase delay calculation based on wave direction and sensor positions
   基于波向和传感器位置计算相位延迟
3. Tilt angle correction for inclined sensors
   倾斜传感器的倾角校正
4. Directional spectrum computation using EMEP/IMLM methods
   使用EMEP/IMLM方法计算方向谱

Radar Configuration / 雷达配置:
- R1 = (0.00, 0.000) - vertical (pointing down) / 垂直朝下
- R2 = (-0.25, 0.433) - tilted 15 degrees forward / 向前倾斜15度
- R3 = (0.25, 0.433) - tilted 15 degrees forward / 向前倾斜15度

Author: Wave Monitoring System
Date: 2025-11-23
"""

import numpy as np
import logging
from typing import Dict, Optional, Tuple
from scipy.signal import welch, detrend


class DirectionalSpectrumAnalyzer:
    """
    Directional Wave Spectrum Analyzer using pyDIWASP
    使用pyDIWASP的方向波浪谱分析器
    """

    # Default radar positions (in meters) / 默认雷达位置（米）
    # R1: center, vertical / R1: 中心，垂直
    # R2: left-rear, tilted 10 deg / R2: 左后方，倾斜10度
    # R3: right-rear, tilted 10 deg / R3: 右后方，倾斜10度
    # 等边三角形阵列，基线约0.2666米
    DEFAULT_POSITIONS = {
        'R1': np.array([0.0, 0.0, 0.0]),       # x, y, z (z=0 for vertical)
        'R2': np.array([-0.1333, 0.2309, 0.0]),  # 与配置文件一致
        'R3': np.array([0.1333, 0.2309, 0.0])
    }

    # Tilt angles in degrees / 倾斜角度（度）
    # 默认值，实际值从配置文件读取
    TILT_ANGLES = {
        'R1': 0.0,    # vertical / 垂直
        'R2': 10.0,   # tilted forward / 向前倾斜
        'R3': 10.0    # tilted forward / 向前倾斜
    }

    # 180°模糊消除：记录上一次有效波向
    _last_Dp = None

    def __init__(self, config: Dict):
        """
        Initialize the directional spectrum analyzer.
        初始化方向谱分析器。

        Parameters / 参数:
            config: Configuration dictionary containing:
                    配置字典，包含：
                - sample_rate: Sampling frequency in Hz / 采样频率（Hz）
                - gravity: Gravitational acceleration / 重力加速度
                - water_depth: Water depth in meters / 水深（米）
                - freq_range: [f_min, f_max] frequency range / 频率范围
                - direction_resolution: Number of directional bins / 方向分辨率
                - array_height: Installation height above water (m) / 安装高度（米）
                - tilt_angles: Per-radar tilt angles {'R1': 0, 'R2': 10, 'R3': 10}
                - tilt_azimuths: Per-radar tilt direction {'R1': 0, 'R2': 0, 'R3': 0}
                - array_heading: Compass heading of array Y-axis (forward direction, degrees from true north)
                                 阵列Y轴（前方）的罗盘朝向（相对真北，顺时针度数）
        """
        self.config = config
        self.sample_rate = config.get('sample_rate', 6.0)
        self.gravity = config.get('gravity', 9.81)
        self.water_depth = config.get('water_depth', 100.0)
        self.freq_range = config.get('freq_range', [0.04, 1.0])
        self.direction_resolution = config.get('direction_resolution', 360)
        self.array_height = config.get('array_height', 5.0)

        # 阵列朝向 → DIWASP坐标系的x轴罗盘方向
        # array_heading = Y轴（前方）罗盘朝向，X轴 = Y轴 + 90°（右手系）
        self.array_heading = config.get('array_heading', 0.0)
        self.xaxisdir = (self.array_heading + 90) % 360

        # 各雷达倾斜角度和方位角（默认值对应当前等边三角形支架）
        # R1: 垂直朝下，无倾斜
        # R2: 倾斜10°，方位角300°（从三角形中心指向R2方向）
        # R3: 倾斜10°，方位角60°（从三角形中心指向R3方向）
        # 方位角定义：0°=+y（前方），90°=+x（右方），顺时针
        tilt_angles_cfg = config.get('tilt_angles', {})
        self.tilt_angles_per_radar = {
            'R1': tilt_angles_cfg.get('R1', 0.0),
            'R2': tilt_angles_cfg.get('R2', 10.0),
            'R3': tilt_angles_cfg.get('R3', 10.0)
        }
        tilt_azimuths_cfg = config.get('tilt_azimuths', {})
        self.tilt_azimuths_per_radar = {
            'R1': tilt_azimuths_cfg.get('R1', 0.0),
            'R2': tilt_azimuths_cfg.get('R2', 300.0),
            'R3': tilt_azimuths_cfg.get('R3', 60.0)
        }

        # 各雷达倾斜投影因子 cos(θ)
        self.tilt_factors = {
            k: np.cos(np.radians(v)) for k, v in self.tilt_angles_per_radar.items()
        }

        # Setup radar positions (计算等效测量点) / 设置雷达位置
        self._setup_radar_positions()

        # 计算等效基线长度
        baseline_12 = np.linalg.norm(self.effective_positions['R2'] - self.effective_positions['R1'])
        baseline_13 = np.linalg.norm(self.effective_positions['R3'] - self.effective_positions['R1'])
        baseline_23 = np.linalg.norm(self.effective_positions['R3'] - self.effective_positions['R2'])

        logging.info(f"DirectionalSpectrumAnalyzer initialized")
        logging.info(f"  Sample rate: {self.sample_rate} Hz, Array height: {self.array_height} m")
        logging.info(f"  Array heading (Y-axis): {self.array_heading:.1f}°, xaxisdir: {self.xaxisdir:.1f}°")
        logging.info(f"  Tilt angles: R1={self.tilt_angles_per_radar['R1']:.1f}°, "
                     f"R2={self.tilt_angles_per_radar['R2']:.1f}°, R3={self.tilt_angles_per_radar['R3']:.1f}°")
        logging.info(f"  Mount positions:     R1={self.positions['R1'][:2].tolist()}, "
                     f"R2={self.positions['R2'][:2].tolist()}, R3={self.positions['R3'][:2].tolist()}")
        logging.info(f"  Effective positions: R1={self.effective_positions['R1'][:2].tolist()}, "
                     f"R2={self.effective_positions['R2'][:2].tolist()}, R3={self.effective_positions['R3'][:2].tolist()}")
        logging.info(f"  Effective baselines: R1-R2={baseline_12:.3f}m, R1-R3={baseline_13:.3f}m, R2-R3={baseline_23:.3f}m")

    def _setup_radar_positions(self):
        """
        Setup radar mount positions (fixed).
        设置雷达安装位置（固定不变）。
        """
        radar_config = self.config.get('radar_positions', {})

        if radar_config:
            self.positions = {
                'R1': np.array(radar_config.get('R1', self.DEFAULT_POSITIONS['R1'])),
                'R2': np.array(radar_config.get('R2', self.DEFAULT_POSITIONS['R2'])),
                'R3': np.array(radar_config.get('R3', self.DEFAULT_POSITIONS['R3']))
            }
        else:
            self.positions = self.DEFAULT_POSITIONS.copy()

        # 用默认 array_height 初始化等效位置和 layout
        self.update_layout(self.array_height)

    def update_layout(self, water_distance: float):
        """
        根据实际水面距离动态重算等效测量点位置和DIWASP layout。

        倾斜传感器的雷达波束打到水面的位置偏离安装点正下方：
            offset = water_distance × tan(tilt)
        DIWASP 需要的是等效测量点位置，不是安装点位置。

        Args:
            water_distance: R1实际平均测距（雷达到水面的垂直距离，米）
        """
        self.effective_positions = {}
        for key in ['R1', 'R2', 'R3']:
            pos = self.positions[key].copy()
            tilt = self.tilt_angles_per_radar[key]

            if tilt > 0:
                tilt_rad = np.radians(tilt)
                azimuth_rad = np.radians(self.tilt_azimuths_per_radar[key])

                # 用实际水面距离计算水平偏移
                offset = water_distance * np.tan(tilt_rad)

                # 偏移方向：azimuth=0 → +y（前方），azimuth=90 → +x（右方）
                pos[0] += offset * np.sin(azimuth_rad)
                pos[1] += offset * np.cos(azimuth_rad)

            self.effective_positions[key] = pos

        # DIWASP layout 使用等效测量点位置 [x; y; z]
        self.layout = np.array([
            [self.effective_positions['R1'][0], self.effective_positions['R2'][0], self.effective_positions['R3'][0]],
            [self.effective_positions['R1'][1], self.effective_positions['R2'][1], self.effective_positions['R3'][1]],
            [0.0, 0.0, 0.0]
        ])

    def estimate_peak_frequency(self, data: np.ndarray) -> float:
        """
        Estimate the peak frequency from time series data.
        从时间序列数据估计峰值频率。

        Parameters / 参数:
            data: Surface elevation time series / 水面高程时间序列

        Returns / 返回:
            Peak frequency in Hz / 峰值频率（Hz）
        """
        nperseg = min(256, len(data) // 4)
        f, S = welch(data, fs=self.sample_rate, nperseg=nperseg)

        # Find peak in valid frequency range
        valid_mask = (f >= self.freq_range[0]) & (f <= self.freq_range[1])
        if not np.any(valid_mask):
            return 0.1  # default

        f_valid = f[valid_mask]
        S_valid = S[valid_mask]
        peak_idx = np.argmax(S_valid)

        return f_valid[peak_idx]

    def calculate_wavenumber(self, frequency: float) -> float:
        """
        Calculate wavenumber from frequency using dispersion relation.
        使用色散关系从频率计算波数。

        For deep water: k = omega^2 / g
        For intermediate water: iterative solution

        Parameters / 参数:
            frequency: Wave frequency in Hz / 波浪频率（Hz）

        Returns / 返回:
            Wavenumber k in rad/m / 波数k（rad/m）
        """
        omega = 2 * np.pi * frequency

        # Deep water approximation as initial guess
        k0 = omega**2 / self.gravity

        # Iterative solution for intermediate water
        k = k0
        for _ in range(20):
            tanh_kh = np.tanh(k * self.water_depth)
            k_new = omega**2 / (self.gravity * tanh_kh)
            if abs(k_new - k) < 1e-10:
                break
            k = k_new

        return k

    def simulate_radar_data(self, eta1: np.ndarray,
                           assumed_direction: float = 0.0) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Simulate R2 and R3 data based on R1 data and assumed wave direction.
        基于雷达1数据和假设的波向模拟生成雷达2和雷达3的数据。

        The simulation accounts for:
        模拟考虑了：
        1. Phase delay due to spatial separation / 空间分离导致的相位延迟
        2. Tilt angle effect (cos(15)) on measured amplitude / 倾斜角度对测量幅度的影响

        Parameters / 参数:
            eta1: R1 surface elevation data / 雷达1水面高程数据
            assumed_direction: Assumed wave direction in degrees (from North, clockwise)
                              假设的波向（度，从北顺时针）

        Returns / 返回:
            Tuple of (eta1, eta2, eta3) / (eta1, eta2, eta3)的元组
        """
        # Detrend the input data
        eta1_clean = detrend(eta1)

        # Estimate peak frequency and wavenumber
        fp = self.estimate_peak_frequency(eta1_clean)
        k = self.calculate_wavenumber(fp)

        logging.debug(f"Simulating radar data: fp={fp:.4f} Hz, k={k:.4f} rad/m, dir={assumed_direction} deg")

        # Convert direction to radians (mathematical convention: 0=East, CCW positive)
        # 将方向转换为弧度（数学惯例：0=东，逆时针为正）
        theta_rad = np.radians(90 - assumed_direction)  # Convert from compass to math

        # Wave direction unit vector
        kx = k * np.cos(theta_rad)
        ky = k * np.sin(theta_rad)

        # Calculate spatial phase differences for R2 and R3 relative to R1
        # 使用等效测量点位置计算相位差
        dx2 = self.effective_positions['R2'][0] - self.effective_positions['R1'][0]
        dy2 = self.effective_positions['R2'][1] - self.effective_positions['R1'][1]
        dx3 = self.effective_positions['R3'][0] - self.effective_positions['R1'][0]
        dy3 = self.effective_positions['R3'][1] - self.effective_positions['R1'][1]

        # Phase delays (in radians)
        phase2 = kx * dx2 + ky * dy2
        phase3 = kx * dx3 + ky * dy3

        logging.debug(f"Phase delays: R2={np.degrees(phase2):.2f} deg, R3={np.degrees(phase3):.2f} deg")

        # Apply phase shift using Hilbert transform approach
        # 使用希尔伯特变换方法应用相位偏移
        eta2, eta3 = self._apply_phase_shift(eta1_clean, phase2, phase3)

        # Apply tilt correction (amplitude reduction due to tilted sensors)
        # 应用倾斜校正（由于传感器倾斜导致的幅度减小）
        eta2 = eta2 * self.tilt_factors['R2']
        eta3 = eta3 * self.tilt_factors['R3']

        # Add some realistic measurement noise (1% of std)
        # 添加一些实际的测量噪声（标准差的1%）
        noise_level = 0.01 * np.std(eta1_clean)
        eta2 = eta2 + np.random.normal(0, noise_level, len(eta2))
        eta3 = eta3 + np.random.normal(0, noise_level, len(eta3))

        return eta1_clean, eta2, eta3

    def _apply_phase_shift(self, data: np.ndarray,
                          phase2: float, phase3: float) -> Tuple[np.ndarray, np.ndarray]:
        """
        Apply frequency-domain phase shift to create phase-delayed signals.
        应用频域相位偏移以创建相位延迟信号。

        Parameters / 参数:
            data: Input time series / 输入时间序列
            phase2: Phase shift for R2 in radians / R2的相位偏移（弧度）
            phase3: Phase shift for R3 in radians / R3的相位偏移（弧度）

        Returns / 返回:
            Tuple of phase-shifted signals / 相位偏移信号的元组
        """
        # FFT of input data
        n = len(data)
        fft_data = np.fft.fft(data)

        # Create frequency array
        freqs = np.fft.fftfreq(n, 1/self.sample_rate)

        # For a narrowband signal, we can approximate with constant phase shift
        # For broadband, we should scale phase with frequency
        # 对于窄带信号，我们可以用恒定相位偏移近似
        # 对于宽带，我们应该用频率缩放相位

        # 相位随波数缩放：phase(f) = k(f) * delta_x
        # 深水色散关系：k = ω²/g = (2πf)²/g，所以 k ∝ f²
        # 因此 phase_scale = k(f)/k(fp) = (f/fp)²
        fp = self.estimate_peak_frequency(data)

        phase_scale = (np.abs(freqs) / fp) ** 2
        phase_scale[freqs == 0] = 0

        # Create phase-shifted FFTs
        fft2 = fft_data * np.exp(-1j * phase2 * phase_scale)
        fft3 = fft_data * np.exp(-1j * phase3 * phase_scale)

        # Inverse FFT
        eta2 = np.real(np.fft.ifft(fft2))
        eta3 = np.real(np.fft.ifft(fft3))

        return eta2, eta3

    def compute_directional_spectrum(self, eta1: np.ndarray, eta2: np.ndarray,
                                    eta3: np.ndarray, method: str = 'EMEP') -> Dict:
        """
        Compute directional wave spectrum using pyDIWASP.
        使用pyDIWASP计算方向波浪谱。

        Parameters / 参数:
            eta1: Surface elevation from R1 / R1的水面高程
            eta2: Surface elevation from R2 / R2的水面高程
            eta3: Surface elevation from R3 / R3的水面高程
            method: Estimation method ('EMEP', 'IMLM', 'BDM', 'DFTM')
                   估计方法

        Returns / 返回:
            Dictionary containing:
            包含以下内容的字典：
            - S: 2D directional spectrum S(f, theta) / 二维方向谱
            - freqs: Frequency array / 频率数组
            - dirs: Direction array / 方向数组
            - Hs: Significant wave height / 有效波高
            - Tp: Peak period / 峰值周期
            - Dp: Dominant direction / 主波向
            - DTp: Direction at peak period / 峰值周期方向
        """
        try:
            # Import pyDIWASP
            import sys
            sys.path.insert(0, '/home/obsis/radar/mqtt_system/services')
            from pydiwasp import dirspec

            # Prepare data matrix [N x 3]
            # 安全截齐：确保三个通道等长
            min_len = min(len(eta1), len(eta2), len(eta3))
            if len(eta1) != min_len or len(eta2) != min_len or len(eta3) != min_len:
                logging.warning(f"DIWASP input length mismatch: eta1={len(eta1)}, eta2={len(eta2)}, eta3={len(eta3)}, truncating to {min_len}")
                eta1 = eta1[:min_len]
                eta2 = eta2[:min_len]
                eta3 = eta3[:min_len]
            n_samples = min_len
            data = np.column_stack([eta1, eta2, eta3])

            # Instrument Data structure (ID)
            # 仪器数据结构
            ID = {
                'data': data,
                'layout': self.layout,
                'datatypes': np.array(['elev', 'elev', 'elev']),  # surface elevation sensors
                'depth': self.water_depth,
                'fs': self.sample_rate
            }

            # Spectral Matrix structure (SM) - defines output grid
            # 谱矩阵结构 - 定义输出网格
            freqs = np.linspace(self.freq_range[0], self.freq_range[1], 128)
            dirs = np.linspace(0, 360, self.direction_resolution + 1)[:-1]
            dirs_rad = np.radians(dirs)

            SM = {
                'freqs': freqs,
                'dirs': dirs_rad,
                'funit': 'Hz',
                'dunit': 'rad',
                'xaxisdir': self.xaxisdir  # x轴罗盘方向 = array_heading + 90
            }

            # Estimation Parameters (EP)
            # 估计参数
            nfft = min(512, n_samples // 4)
            nfft = int(2 ** np.floor(np.log2(nfft)))  # Round to power of 2

            EP = {
                'method': method,
                'nfft': nfft,
                'dres': self.direction_resolution,
                'iter': 100,
                'smooth': 'ON'
            }

            # Options
            options = ['MESSAGE', 0, 'PLOTTYPE', 0]  # Suppress output and plotting

            logging.info(f"Running DIWASP with method={method}, nfft={nfft}, dres={self.direction_resolution}")

            # Run directional spectrum estimation
            SMout, EPout = dirspec(ID, SM, EP, options)

            if len(SMout) == 0:
                logging.error("DIWASP returned empty result")
                return self._fallback_analysis(eta1)

            # Extract results
            S = np.real(SMout['S'])  # 2D spectrum
            freqs_out = SMout['freqs']
            dirs_out = np.degrees(SMout['dirs'])  # Convert to degrees

            # Calculate wave parameters from spectrum
            # 从谱计算波浪参数
            df = freqs_out[1] - freqs_out[0] if len(freqs_out) > 1 else 0.01
            ddir = dirs_out[1] - dirs_out[0] if len(dirs_out) > 1 else 2.0

            # Significant wave height
            m0 = np.sum(S) * df * np.radians(ddir)
            Hs = 4.0 * np.sqrt(m0)

            # 1D frequency spectrum (integrate over directions)
            S1D = np.sum(S, axis=1) * np.radians(ddir)

            # Peak period
            peak_idx = np.argmax(S1D)
            fp = freqs_out[peak_idx]
            Tp = 1.0 / fp if fp > 0 else 0

            # Direction at peak period
            DTp_idx = np.argmax(S[peak_idx, :])
            DTp = dirs_out[DTp_idx]

            # Dominant direction (integrated over all frequencies)
            dir_spectrum = np.sum(S, axis=0)
            Dp_idx = np.argmax(dir_spectrum)
            Dp = dirs_out[Dp_idx]

            # Mean direction (circular mean)
            # 平均方向（圆周平均）
            dirs_rad_out = np.radians(dirs_out)
            weights = dir_spectrum / np.sum(dir_spectrum) if np.sum(dir_spectrum) > 0 else np.ones_like(dir_spectrum) / len(dir_spectrum)
            mean_sin = np.sum(weights * np.sin(dirs_rad_out))
            mean_cos = np.sum(weights * np.cos(dirs_rad_out))
            mean_dir = np.degrees(np.arctan2(mean_sin, mean_cos)) % 360

            # Directional spread
            # 方向分布宽度
            sigma_theta = np.sqrt(2 * (1 - np.sqrt(mean_sin**2 + mean_cos**2)))
            dir_spread = np.degrees(sigma_theta)

            # 将所有方向从 axis-angle（DIWASP内部，传播去向）转换为罗盘来向（真北）
            # compangle(θ, xaxisdir) = (180 + xaxisdir - θ) % 360
            # 同时完成：axis-angle→罗盘 + 去向→来向
            Dp = (180 + self.xaxisdir - Dp) % 360
            DTp = (180 + self.xaxisdir - DTp) % 360
            mean_dir = (180 + self.xaxisdir - mean_dir) % 360

            # 180°模糊消除：3个同类型传感器无法区分来向和去向，
            # 如果新方向与上一次结果相差接近180°（±30°），翻转到一致的半圆
            if DirectionalSpectrumAnalyzer._last_Dp is not None:
                diff = (Dp - DirectionalSpectrumAnalyzer._last_Dp + 180) % 360 - 180
                if abs(abs(diff) - 180) < 30:
                    Dp_flipped = (Dp + 180) % 360
                    DTp = (DTp + 180) % 360
                    mean_dir = (mean_dir + 180) % 360
                    logging.info(f"180° ambiguity resolved: {Dp:.1f}° -> {Dp_flipped:.1f}° (prev={DirectionalSpectrumAnalyzer._last_Dp:.1f}°)")
                    Dp = Dp_flipped
            DirectionalSpectrumAnalyzer._last_Dp = Dp

            results = {
                'S': S.tolist(),  # 2D spectrum [freq x dir]
                'S1D': S1D.tolist(),  # 1D frequency spectrum
                'freqs': freqs_out.tolist(),
                'dirs': dirs_out.tolist(),
                'Hs': float(Hs),
                'Tp': float(Tp),
                'fp': float(fp),
                'Dp': float(Dp),          # 主波向（罗盘来向，真北）
                'DTp': float(DTp),         # 峰值周期方向（罗盘来向，真北）
                'mean_direction': float(mean_dir),  # 平均波向（罗盘来向，真北）
                'directional_spread': float(dir_spread),
                'method': method,
                'success': True
            }

            logging.info(f"Directional analysis complete: Hs={Hs:.3f}m, Tp={Tp:.2f}s, "
                         f"Dp={Dp:.1f}° (compass FROM, true north)")

            return results

        except Exception as e:
            logging.error(f"DIWASP analysis failed: {e}", exc_info=True)
            return self._fallback_analysis(eta1)

    def _fallback_analysis(self, eta1: np.ndarray) -> Dict:
        """
        Fallback to simple spectral analysis if DIWASP fails.
        如果DIWASP失败，回退到简单的谱分析。

        Parameters / 参数:
            eta1: Surface elevation data / 水面高程数据

        Returns / 返回:
            Basic spectral results / 基本谱结果
        """
        logging.warning("Using fallback spectral analysis (no directional information)")

        # Simple Welch spectrum
        nperseg = min(256, len(eta1) // 4)
        f, S = welch(detrend(eta1), fs=self.sample_rate, nperseg=nperseg)

        # Calculate basic parameters
        valid_mask = (f >= self.freq_range[0]) & (f <= self.freq_range[1])
        f_valid = f[valid_mask]
        S_valid = S[valid_mask]

        df = f_valid[1] - f_valid[0] if len(f_valid) > 1 else 0.01
        m0 = np.trapezoid(S_valid, f_valid)
        Hs = 4.0 * np.sqrt(m0)

        peak_idx = np.argmax(S_valid)
        fp = f_valid[peak_idx]
        Tp = 1.0 / fp if fp > 0 else 0

        return {
            'S': None,
            'S1D': S_valid.tolist(),
            'freqs': f_valid.tolist(),
            'dirs': None,
            'Hs': float(Hs),
            'Tp': float(Tp),
            'fp': float(fp),
            'Dp': None,  # Cannot determine without directional analysis
            'DTp': None,
            'mean_direction': None,
            'directional_spread': None,
            'method': 'fallback',
            'success': False
        }

    def analyze(self, eta1: np.ndarray, eta2: np.ndarray = None,
               eta3: np.ndarray = None, assumed_direction: float = 0.0,
               method: str = 'EMEP', r1_mean_distance: float = None) -> Dict:
        """
        Main analysis entry point.
        主分析入口。

        If only R1 data is available, R2 and R3 will be simulated.
        如果只有R1数据可用，将模拟生成R2和R3数据。

        Parameters / 参数:
            eta1: R1 surface elevation data (required) / R1水面高程数据（必需）
            eta2: R2 surface elevation data (optional) / R2水面高程数据（可选）
            eta3: R3 surface elevation data (optional) / R3水面高程数据（可选）
            assumed_direction: Direction for simulation if R2/R3 missing (degrees)
                              如果R2/R3缺失时用于模拟的假设方向（度）
            method: DIWASP method ('EMEP', 'IMLM') / DIWASP方法
            r1_mean_distance: R1窗口内平均测距（米），用于动态重算等效基线。
                              如果为None，使用初始化时的array_height。

        Returns / 返回:
            Directional spectrum analysis results / 方向谱分析结果
        """
        # 动态更新等效测量点位置（基于R1实际平均测距）
        if r1_mean_distance is not None and r1_mean_distance > 0:
            self.update_layout(r1_mean_distance)
            logging.info(f"Layout updated: water_distance={r1_mean_distance:.2f}m")

        # Check data availability
        has_eta2 = eta2 is not None and len(eta2) > 0 and not np.all(np.isnan(eta2))
        has_eta3 = eta3 is not None and len(eta3) > 0 and not np.all(np.isnan(eta3))

        # If R2 and R3 are not available, simulate them
        if not has_eta2 or not has_eta3:
            logging.info(f"Simulating R2/R3 data (assumed direction: {assumed_direction} deg)")
            eta1_proc, eta2_sim, eta3_sim = self.simulate_radar_data(eta1, assumed_direction)

            # Use actual data if available, otherwise use simulated
            eta2_use = eta2 if has_eta2 else eta2_sim
            eta3_use = eta3 if has_eta3 else eta3_sim
        else:
            # 数据已在 mqtt_analyzer._preprocess() 中完成去趋势和滤波
            # 此处不再重复 detrend，避免对已滤波数据二次处理
            eta1_proc = eta1
            # 倾斜矫正：倾斜雷达的测距波动 Δd = Δη / cos(θ)
            # 需要乘以 cos(θ) 还原真实波面高程
            eta2_use = eta2 * self.tilt_factors['R2']
            eta3_use = eta3 * self.tilt_factors['R3']

        # Run directional spectrum analysis
        results = self.compute_directional_spectrum(eta1_proc, eta2_use, eta3_use, method)

        # Add metadata about data source
        results['data_source'] = {
            'eta1': 'actual',
            'eta2': 'actual' if has_eta2 else 'simulated',
            'eta3': 'actual' if has_eta3 else 'simulated',
            'assumed_direction': assumed_direction if not (has_eta2 and has_eta3) else None
        }

        return results


def test_directional_analyzer():
    """
    Test function for the DirectionalSpectrumAnalyzer.
    方向谱分析器的测试函数。
    """
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt

    logging.basicConfig(level=logging.INFO)

    # Configuration
    config = {
        'sample_rate': 6.0,
        'gravity': 9.81,
        'water_depth': 100.0,  # 深水近似，对雷达测量影响很小
        'freq_range': [0.04, 1.0],
        'direction_resolution': 360  # 1°分辨率
    }

    # Create analyzer
    analyzer = DirectionalSpectrumAnalyzer(config)

    # Generate synthetic test data
    # 生成合成测试数据
    duration = 300  # seconds
    t = np.arange(0, duration, 1/config['sample_rate'])

    # Simulate a wave field with known direction (45 degrees from North)
    true_direction = 45.0  # degrees
    true_fp = 0.1  # Hz (10 second waves)
    true_Hs = 2.0  # meters

    # Generate R1 data (sum of spectral components)
    np.random.seed(42)
    eta1 = np.zeros_like(t)

    # Add several frequency components
    for f in np.linspace(0.05, 0.15, 10):
        amp = true_Hs / 4 * np.exp(-((f - true_fp) / 0.02)**2)
        phase = np.random.uniform(0, 2*np.pi)
        eta1 += amp * np.cos(2 * np.pi * f * t + phase)

    # Add noise
    eta1 += np.random.normal(0, 0.05, len(t))

    logging.info(f"Generated test data: duration={duration}s, samples={len(t)}")
    logging.info(f"True parameters: Hs={true_Hs}m, fp={true_fp}Hz, dir={true_direction}deg")

    # Run analysis (use IMLM as it's more stable than EMEP)
    results = analyzer.analyze(eta1, assumed_direction=true_direction, method='IMLM')

    # Print results
    logging.info("\n=== Analysis Results ===")
    logging.info(f"Hs: {results['Hs']:.3f} m (true: {true_Hs} m)")
    logging.info(f"Tp: {results['Tp']:.2f} s (true: {1/true_fp:.2f} s)")
    logging.info(f"Dp: {results.get('Dp', 'N/A')} deg (true: {true_direction} deg)")
    logging.info(f"Method: {results['method']}")
    logging.info(f"Success: {results['success']}")

    # Plot results if successful
    if results['success'] and results['S'] is not None:
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))

        # 1D spectrum
        axes[0].plot(results['freqs'], results['S1D'])
        axes[0].set_xlabel('Frequency (Hz)')
        axes[0].set_ylabel('Spectral Density (m^2/Hz)')
        axes[0].set_title('1D Frequency Spectrum')
        axes[0].grid(True)

        # 2D spectrum (polar plot)
        S = np.array(results['S'])
        freqs = np.array(results['freqs'])
        dirs = np.array(results['dirs'])

        ax2 = fig.add_subplot(122, projection='polar')
        dirs_rad = np.radians(dirs)
        R, Theta = np.meshgrid(freqs, dirs_rad)
        ax2.contourf(Theta, R, S.T, levels=20)
        ax2.set_title(f'Directional Spectrum\nDp={results["Dp"]:.1f} deg')

        plt.tight_layout()
        plt.savefig('/tmp/directional_spectrum_test.png', dpi=150)
        logging.info("Plot saved to /tmp/directional_spectrum_test.png")

    return results


if __name__ == '__main__':
    test_directional_analyzer()
