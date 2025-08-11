#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from scipy import stats
import seaborn as sns
from pathlib import Path
from datetime import date

# ────────────────── 0. 读数 & 基础设置 ──────────────────
CSV_PATH   = Path("/Users/tauras/Documents/Code/etf_data/etf_score_robust.csv")
PLOT_PATH  = CSV_PATH.with_name(f"etf_score_dist_{date.today()}.png")

# 读取
df       = pd.read_csv(CSV_PATH)
scores   = df['etf_score'].dropna()
n_total  = len(df)
n_valid  = len(scores)
n_missing = n_total - n_valid

print(f"🎯 读入 {n_total:,} 行，缺失 {n_missing:,} 行（{n_missing/n_total:.2%}）")

# ────────────────── 1. 统计指标 ──────────────────
stats_basic = scores.describe(percentiles=[.01, .05, .1, .25, .5, .75, .9, .95, .99])
skewness    = scores.skew()
kurtosis    = scores.kurtosis()

# Shapiro：>5000 采样
sample = scores.sample(5000, random_state=42) if n_valid > 5000 else scores
sw_stat, sw_p = stats.shapiro(sample)

# D'Agostino K²
k2_stat, k2_p = stats.normaltest(sample)

print(f"均值={stats_basic['mean']:.6f}  |  偏度={skewness:.3f}  |  峰度={kurtosis:.3f}")
print(f"Shapiro‑Wilk p={sw_p:.4g} | K² p={k2_p:.4g}")

# IQR 异常值
Q1, Q3 = stats_basic['25%'], stats_basic['75%']
IQR    = Q3 - Q1
lower, upper = Q1 - 1.5*IQR, Q3 + 1.5*IQR
outlier_mask = (scores < lower) | (scores > upper)
n_outlier    = outlier_mask.sum()

# ────────────────── 2. 画图 ──────────────────
plt.rcParams.update({
    "font.sans-serif": ["SimHei", "Arial Unicode MS", "DejaVu Sans"],
    "axes.unicode_minus": False
})

fig, axs = plt.subplots(2, 3, figsize=(18, 10))
fig.suptitle("ETF Score 分布全景图", fontsize=18, weight='bold')

# ① 直方图 + KDE
bin_num = int(np.sqrt(n_valid))                # Rule‑of‑thumb
sns.histplot(scores, bins=bin_num, kde=True, ax=axs[0,0],
             color="steelblue", edgecolor="black", alpha=.7)
axs[0,0].axvline(stats_basic['mean'], color='red', linestyle='--', label=f"均值={stats_basic['mean']:.3f}")
axs[0,0].legend(); axs[0,0].set_title("直方图 + KDE")

# ② 箱线 + 异常点
sns.boxplot(x=scores, ax=axs[0,1], color="skyblue", fliersize=2)
axs[0,1].set_title(f"箱线图（IQR 异常值 {n_outlier} 个）")

# ③ ECDF
sns.ecdfplot(scores, ax=axs[0,2])
axs[0,2].set_title("经验累积分布 (ECDF)")
axs[0,2].yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y:.0%}"))

# ④ Q‑Q
stats.probplot(scores, dist="norm", plot=axs[1,0])
axs[1,0].set_title("Q‑Q 图（正态检验）")

# ⑤ 分位点条形
quantiles = scores.quantile([.01,.05,.1,.25,.5,.75,.9,.95,.99])
axs[1,1].bar(quantiles.index.astype(str), quantiles.values, color="mediumpurple")
axs[1,1].set_title("主要分位点")
axs[1,1].tick_params(axis='x', rotation=45)

# ⑥ 时序热图（可观察漂移）
pivot = df.pivot_table(index='trade_date', columns='ts_code', values='etf_score')
sns.heatmap(pivot.T, cmap='RdBu_r', center=0, ax=axs[1,2], cbar_kws={'label': 'Score'})
axs[1,2].set_title("ETF×日期 热图")
axs[1,2].set_xlabel("Date"); axs[1,2].set_ylabel("ETF")

plt.tight_layout(rect=[0,0,1,0.96])
fig.text(0.01, 0.01,
         f"Shapiro p={sw_p:.3g}  |  K² p={k2_p:.3g}  |  Outlier={n_outlier} ({n_outlier/n_valid:.2%})",
         fontsize=9, color="gray")

fig.savefig(PLOT_PATH, dpi=300)
print(f"📈 已保存到: {PLOT_PATH}")
plt.show()
