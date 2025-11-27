import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
# 启用LaTeX渲染以正确显示下标和上标
plt.rcParams['text.usetex'] = False  # 不使用LaTeX，避免配置问题
plt.rcParams['text.latex.preamble'] = r'\usepackage{amsmath}'  # 如果启用LaTeX时使用

# 实验数据整理（来自实验报告）
# 悬挂点位置x（mm）和对应的共振频率测量值
x_data = np.array([5.30, 15.40, 25.50, 35.50, 45.50, 55.50])  # 位置（mm）

# 每个位置两次测量值；35.50处无测量，用 np.nan 标记
f1 = np.array([764.21, 739.64, 721.12, np.nan, 720.61, 728.65])
f2 = np.array([763.53, 740.31, 722.73, np.nan, 720.94, 728.81])

# 计算平均值，自动忽略 NaN
y_data = np.nanmean(np.vstack([f1, f2]), axis=0)

# 过滤掉缺失点用于拟合
valid_mask = ~np.isnan(y_data)
x_fit_input = x_data[valid_mask]
y_fit_input = y_data[valid_mask]

# 理论节点位置计算
L = 159.8  # 棒长(mm)
node_position = 35.50  # 理论节点位置
print(f"理论节点位置: {node_position:.2f} mm")

# 进行二次多项式拟合 f(x) = ax² + bx + c
def quadratic_func(x, a, b, c):
    return a * x**2 + b * x + c

# 使用curve_fit进行拟合（仅使用有效数据点）
params, covariance = curve_fit(quadratic_func, x_fit_input, y_fit_input)

# 提取拟合参数
a_fit, b_fit, c_fit = params
print(f"\n拟合参数:")
print(f"a = {a_fit:.6e} Hz/mm²")
print(f"b = {b_fit:.4f} Hz/mm")
print(f"c = {c_fit:.4f} Hz")

# 在节点位置计算基频共振频率
f0 = quadratic_func(node_position, a_fit, b_fit, c_fit)
print(f"\n基频共振频率 f₀ = {f0:.2f} Hz")

# 计算拟合优度R²
y_pred = quadratic_func(x_fit_input, a_fit, b_fit, c_fit)
ss_res = np.sum((y_fit_input - y_pred) ** 2)
ss_tot = np.sum((y_fit_input - np.mean(y_fit_input)) ** 2)
r_squared = 1 - (ss_res / ss_tot)
print(f"拟合优度 R² = {r_squared:.4f}")

# 可视化
plt.figure(figsize=(10, 6))

# 绘制原始数据点
plt.scatter(x_data, y_data, color='red', s=80, label='实验数据', zorder=5)

# 绘制拟合曲线
x_fit = np.linspace(np.nanmin(x_data), np.nanmax(x_data), 300)
y_fit = quadratic_func(x_fit, a_fit, b_fit, c_fit)
plt.plot(x_fit, y_fit, color='blue', linewidth=2, label='二次拟合曲线')

# 标注节点位置（使用matplotlib的下标语法）
plt.axvline(x=node_position, color='green', linestyle='--', linewidth=2, 
            label=r'理论节点 $x_0$=%.2fmm' % node_position)
plt.plot(node_position, f0, 'go', markersize=10, label=r'$f_0$=%.2fHz' % f0)

# 图表设置 - 使用数学模式显示下标
plt.xlabel(r'悬挂点位置 $x$ (mm)', fontsize=12)
plt.ylabel(r'共振频率 $f$ (Hz)', fontsize=12)
plt.title(r'共振频率与悬挂点位置关系（外延法）', fontsize=14, fontweight='bold')
plt.legend(loc='best', fontsize=10)
plt.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('resonance_fit.png', dpi=300)
plt.show()

# 输出完整报告 - 使用纯文本表示下标
print("\n" + "="*50)
print("实验数据处理报告")
print("="*50)
print(f"试样长度 L = {L/1000:.4f} m")
print(f"理论节点位置 = {node_position:.2f} mm")
print(f"拟合得到的基频 f0 = {f0:.2f} ± {np.sqrt(covariance[2,2]):.2f} Hz")
print(f"杨氏模量计算公式: Y = 1.6067 × (L^3m/d^4) × f0^2")