import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import PchipInterpolator  # 改：PCHIP 保形插值（更稳，不易过冲）

# ---------------------- 1. 实验参数定义（源自实验报告）----------------------
N1 = 50               # 励磁线圈匝数
N2 = 150              # 测量线圈匝数
L = 60e-3             # 样品长度（m），60mm转换为国际单位
S = 80e-6             # 样品横截面积（m²），80mm²转换为国际单位
R1 = 10               # 取样电阻（Ω），实验选择0-10Ω档
R2 = 10e3             # 积分电阻（Ω），10kΩ转换为国际单位
C = 10e-6             # 积分电容（F），10μF转换为国际单位

# ---------------------- 2. 基础磁化曲线原始数据（源自实验报告）----------------------
# 按“逐步增加励磁”顺序：Ux 从 0 -> 最大
Ux_data = np.array([0.000, 0.365, 0.512, 0.712, 1.000, 1.350])   # 电压Ux (V)
Uy_data = np.array([0.000, 0.0537, 0.101, 0.147, 0.205, 0.250])  # 电压Uy (V)

# ---------------------- 3. 计算H和B值（根据实验原理公式）----------------------
# H系数：K_H = N1/(L*R1)，单位：A/V
K_H = N1 / (L * R1)
# B系数：K_B = (R2*C)/(N2*S)，单位：T/V
K_B = (R2 * C) / (N2 * S)

# 计算磁场强度H和磁感应强度B
H_data = K_H * Ux_data  # 磁场强度（A/m）
B_data = K_B * Uy_data  # 磁感应强度（T）

# ---------------------- 4. 绘制基础磁化曲线（专业美化）----------------------
plt.rcParams['font.sans-serif'] = ['SimHei']  # 支持中文显示
plt.rcParams['axes.unicode_minus'] = False    # 支持负号显示

# 创建画布，设置尺寸
fig, ax = plt.subplots(figsize=(10, 6))

# 兜底：确保按H升序（理论上此时已是逐步增加）
idx = np.argsort(H_data)
H_plot = H_data[idx]
B_plot = B_data[idx]
Ux_plot = Ux_data[idx]
Uy_plot = Uy_data[idx]

k = min(3, len(H_plot) - 1)
H_smooth = np.linspace(H_plot.min(), H_plot.max(), 800)  # 可加密一点更“顺”
pchip = PchipInterpolator(H_plot, B_plot)               # 改：用PCHIP替代样条
B_smooth = pchip(H_smooth)

# 平滑曲线（先画平滑线）
ax.plot(H_smooth, B_smooth, color='#e74c3c', linewidth=2.5, label='基础磁化曲线（平滑）')

# 原始测量点（再叠加散点）
ax.plot(H_plot, B_plot, linestyle='None', marker='o', markersize=6,
        markerfacecolor='#3498db', markeredgecolor='white', markeredgewidth=1.5,
        label='测量点')

# 设置坐标轴标签（含单位）
ax.set_xlabel('磁场强度 $H$ (A/m)', fontsize=12, fontweight='bold')
ax.set_ylabel('磁感应强度 $B$ (T)', fontsize=12, fontweight='bold')

# 设置标题
ax.set_title('软磁铁氧体材料基础磁化曲线', fontsize=14, fontweight='bold', pad=20)

# 设置网格（虚线，增加可读性）
ax.grid(True, linestyle='--', alpha=0.7, linewidth=0.8)

# 设置坐标轴范围（适当留白，使曲线居中）
ax.set_xlim(H_plot.min() - 5, H_plot.max() + 10)
ax.set_ylim(-0.1, B_plot.max() + 0.2)

# 美化坐标轴刻度
ax.tick_params(axis='both', which='major', labelsize=10, width=1.2, length=5)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_linewidth(1.2)
ax.spines['bottom'].set_linewidth(1.2)

# 添加图例
ax.legend(loc='upper left', fontsize=11, frameon=True, shadow=True, framealpha=0.9)

plt.tight_layout()
plt.savefig('基础磁化曲线.png', dpi=300, bbox_inches='tight')
plt.show()

# ---------------------- 5. 输出计算结果（用于实验报告数据记录）----------------------
print("基础磁化曲线数据计算结果（按逐步增加顺序）：")
print("-" * 50)
print(f"{'序号':<5}{'Ux(V)':<10}{'Uy(V)':<10}{'H(A/m)':<12}{'B(T)':<10}")
print("-" * 50)
for i in range(len(H_plot)):
    print(f"{i+1:<5}{Ux_plot[i]:<10.3f}{Uy_plot[i]:<10.4f}{H_plot[i]:<12.2f}{B_plot[i]:<10.4f}")