#!/usr/bin/env python3
"""
理论估算: 倾斜角 10° vs 15° 对波向精度的影响
"""
import numpy as np

# 硬件参数
water_dist = 4.69  # R1平均测距 (m)
mount_R2 = np.array([-0.1828, 0.3160])
mount_R3 = np.array([0.1828, 0.3160])
mount_R1 = np.array([0.0, 0.0])

tilt_azimuth_R2 = 300  # 度
tilt_azimuth_R3 = 60   # 度

print("=" * 70)
print("  倾斜角 10° vs 15° 理论对比")
print("=" * 70)

for tilt in [10, 15, 20]:
    offset = water_dist * np.tan(np.radians(tilt))
    cos_tilt = np.cos(np.radians(tilt))

    # 等效测量点
    eff_R2 = mount_R2.copy()
    eff_R2[0] += offset * np.sin(np.radians(tilt_azimuth_R2))
    eff_R2[1] += offset * np.cos(np.radians(tilt_azimuth_R2))

    eff_R3 = mount_R3.copy()
    eff_R3[0] += offset * np.sin(np.radians(tilt_azimuth_R3))
    eff_R3[1] += offset * np.cos(np.radians(tilt_azimuth_R3))

    bl_12 = np.linalg.norm(eff_R2 - mount_R1)
    bl_13 = np.linalg.norm(eff_R3 - mount_R1)
    bl_23 = np.linalg.norm(eff_R3 - eff_R2)

    # y方向基线（前后方向，正面来波的分辨力）
    dy_12 = abs(eff_R2[1])
    dy_13 = abs(eff_R3[1])

    # 斜率灵敏度
    slope_sensitivity = np.tan(np.radians(tilt))

    # 相对于10°的改善因子
    slope_improve = slope_sensitivity / np.tan(np.radians(10))
    baseline_improve = bl_12 / 1.160  # 相对于10°的等效基线

    print(f"\n--- 倾斜角: {tilt}° ---")
    print(f"  波束偏移:        {offset:.3f}m")
    print(f"  cos(tilt):       {cos_tilt:.4f} (幅度校正因子)")
    print(f"  tan(tilt):       {slope_sensitivity:.4f} (斜率灵敏度)")
    print(f"  等效 R1-R2:      {bl_12:.3f}m")
    print(f"  等效 R1-R3:      {bl_13:.3f}m")
    print(f"  等效 R2-R3:      {bl_23:.3f}m")
    print(f"  y方向基线(R2):   {dy_12:.3f}m (正面来波分辨力)")
    print(f"  相对10°改善:")
    print(f"    斜率灵敏度:    ×{slope_improve:.2f}")
    print(f"    等效基线:      ×{baseline_improve:.2f}")
    print(f"    综合(理论):    ×{slope_improve * baseline_improve:.2f}")

    # 对各波高的预估误差
    # 当前10°的DFTM误差
    errors_10 = {
        '0.3m': [0, 1, 3, 12],  # 去掉14:54异常后 [0, 1, 3]
        '0.2m': [1, 4, 5],
        '0.1m': [4, 6, 9],
    }

    if tilt != 10:
        print(f"\n  预估波向精度 (相对10°按比例缩放):")
        combined_factor = 1.0 / (slope_improve * baseline_improve)
        for h, errs in errors_10.items():
            scaled = [e * combined_factor for e in errs]
            avg_orig = np.mean(errs)
            avg_scaled = np.mean(scaled)
            within3 = sum(1 for e in scaled if e <= 3)
            print(f"    H1/3={h}: 10°平均{avg_orig:.1f}° → {tilt}°预估{avg_scaled:.1f}° "
                  f"(≤3°: {within3}/{len(errs)})")

# 波长参考
print("\n\n--- 波长参考 ---")
g = 9.81
for T in [2, 3, 4]:
    L = g * T**2 / (2 * np.pi)
    k = 2 * np.pi / L
    for tilt in [10, 15]:
        offset = water_dist * np.tan(np.radians(tilt))
        bl_eff = np.sqrt((mount_R2[0] + offset * np.sin(np.radians(300)))**2 +
                         (mount_R2[1] + offset * np.cos(np.radians(300)))**2)
        phase_diff = k * bl_eff  # 最大相位差 (rad)
        print(f"  T={T}s: λ={L:.1f}m, k={k:.3f}  |  "
              f"tilt={tilt}°: 基线={bl_eff:.3f}m, 最大相位差={np.degrees(phase_diff):.1f}°")

print()
