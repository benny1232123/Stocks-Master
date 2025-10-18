import akshare as ak
import pandas as pd
import numpy as np
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d

# 实验数据
load_resistances = [100, 200, 300, 390, 510]  # R (Ω)
voltage_original = [2.18, 3.07, 3.55, 3.83, 4.09]  # 原始电路的U_R (V)
voltage_equivalent = [2.14, 3.03, 3.51, 3.79, 4.05]  # 等效电路的U_R (V)
voltage_theoretical = [1.96, 2.91, 3.48, 3.82, 4.14]  # 理论U_R (V)

# 创建更密集的数据点用于平滑曲线
load_resistances_smooth = np.linspace(min(load_resistances), max(load_resistances), 300)

# 使用三次样条插值创建平滑曲线
f_original = interp1d(load_resistances, voltage_original, kind='cubic')
f_equivalent = interp1d(load_resistances, voltage_equivalent, kind='cubic')
f_theoretical = interp1d(load_resistances, voltage_theoretical, kind='cubic')

# 生成平滑的电压值
voltage_original_smooth = f_original(load_resistances_smooth)
voltage_equivalent_smooth = f_equivalent(load_resistances_smooth)
voltage_theoretical_smooth = f_theoretical(load_resistances_smooth)

# 创建图形并绘制平滑曲线
plt.figure(figsize=(10, 6))
plt.plot(load_resistances_smooth, voltage_original_smooth, '-', label='Original Circuit', linewidth=2)
plt.plot(load_resistances_smooth, voltage_equivalent_smooth, '-', label='Equivalent Circuit', linewidth=2)
plt.plot(load_resistances_smooth, voltage_theoretical_smooth, '-', label='Theoretical Value', linewidth=2)

# 仍然显示原始数据点
plt.plot(load_resistances, voltage_original, 'o', markersize=6, color='C0')
plt.plot(load_resistances, voltage_equivalent, 's', markersize=6, color='C1')
plt.plot(load_resistances, voltage_theoretical, '^', markersize=6, color='C2')

# 添加标签、标题、图例和网格
plt.xlabel('Load Resistance $R$ ($\Omega$)')
plt.ylabel('Load Voltage $U_R$ (V)')
plt.title('External Characteristic Curve of Active Two-terminal Network\n(Verification of Thevenin\'s Theorem)')
plt.legend()
plt.grid(True, linestyle='--', alpha=0.7)

# 显示图形
plt.show()