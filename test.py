import akshare as ak
import pandas as pd
import numpy as np

import numpy as np
import matplotlib.pyplot as plt

# ---------------------- 1. 参数设置 ----------------------
R = 0.056       # 球体半径（单位：m）
Q = 7.78e-15    # 球体总电荷（单位：C）
epsilon0 = 8.854e-12  # 真空介电常数（单位：F/m）
inner_coeff = 7.0     # 内区E(r) = inner_coeff * r^2 的系数（近似值，由电荷密度推导）

# ---------------------- 2. 生成数据 ----------------------
# 内部区域（0 ≤ r < R）
r_inner = np.linspace(0, R, 100)  # 生成100个0到R的点
E_inner = inner_coeff * r_inner**2  # 内区E ∝ r²

# 外部区域（r ≥ R）
r_outer = np.linspace(R, 2*R, 100)  # 生成100个R到2R的点
E_outer = Q / (4 * np.pi * epsilon0 * r_outer**2)  # 外区E ∝ 1/r²

# ---------------------- 3. 绘制图像 ----------------------
plt.figure(figsize=(8, 6))  # 设置图的大小

# 绘制内区和外区曲线
plt.plot(r_inner, E_inner, 'b-', label='0 ≤ r < R（内部区域）')
plt.plot(r_outer, E_outer, 'r-', label='r ≥ R（外部区域）')

# 标注关键点（r=R处的电场）
E_at_R = Q / (4 * np.pi * epsilon0 * R**2)
plt.scatter([R], [E_at_R], color='k', s=50, zorder=5, 
            label=f'r=R时，E≈{E_at_R:.2e} N/C')

# 坐标轴与图例
plt.xlabel('径向距离 r (m)')
plt.ylabel('电场大小 E (N/C)')
plt.title('非导电球体的电场分布 E vs. r')
plt.legend()
plt.grid(True)  # 显示网格
plt.show()      # 显示图像