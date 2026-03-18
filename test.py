import numpy as np
import matplotlib.pyplot as plt

beta = 0.4
gamma = 0.1
N = 10000
days = 60

S = [999]
I = [1]
R = [0]

for t in range(days):
    new_infected = beta * S[-1] * I[-1] / N
    new_infected = min(max(new_infected, 0), S[-1])
    
    new_recovered = gamma * I[-1]
    new_recovered = min(max(new_recovered, 0), I[-1])
    
    new_S = S[-1] - new_infected
    new_I = I[-1] + new_infected - new_recovered
    new_R = R[-1] + new_recovered
    
    total = new_S + new_I + new_R
    if abs(total - N) > 1e-10:
        diff = N - total
        new_S += diff
    
    S.append(new_S)
    I.append(new_I)
    R.append(new_R)

R0 = beta/gamma
print(f"基本再生数 R0 = {R0:.2f}")
print(f"峰值感染人数: {max(I):.2f}")
print(f"最终状态 - S: {S[-1]:.2f}, I: {I[-1]:.2f}, R: {R[-1]:.2f}")
print(f"总人口: {S[-1] + I[-1] + R[-1]:.2f}")

plt.figure(figsize=(10,6))
# 修改线条样式：
# S(易感人群) - 粗线 (linewidth=4)
# I(感染人群) - 细线 (linewidth=1.5)
# R(康复人群) - 虚线 (linestyle='--', linewidth=2)
plt.plot(S, label='Susceptible', linewidth=4, color='#1f77b4')
plt.plot(I, label='Infected', linewidth=1.5, color='#ff7f0e')
plt.plot(R, label='Recovered', linestyle='--', linewidth=2, color='#2ca02c')

plt.legend(fontsize=12)
plt.xlabel('Days', fontsize=12)
plt.ylabel('Population', fontsize=12)
plt.title('SIR Model - Negative Information Spread', fontsize=14)
plt.grid(True, alpha=0.3)
plt.show()