import pandas as pd, numpy as np
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:password@localhost:3306/etf_df')

# ① 读 IC（长表）
ic_long = pd.read_sql("SELECT * FROM etf_ts_ic", engine)
ic_long['trade_date'] = pd.to_datetime(ic_long['trade_date'])   # 保守起见再转一次

# ② 转宽表
ic_wide = (ic_long
           .pivot_table(index=['ts_code', 'trade_date'],
                        columns='factor',
                        values='ts_ic')
           .reset_index())

# ③ 读因子原值
factor_val = pd.read_sql('SELECT * FROM etf_factors', engine)
factor_val['trade_date'] = pd.to_datetime(factor_val['trade_date'])  # 关键修正！

# ④ 合并
merged = factor_val.merge(ic_wide,
                          on=['ts_code', 'trade_date'],
                          how='left',
                          suffixes=('', '_ic'))

# ⑤ 计算 z‑score & 加权得分
all_factors = ic_long['factor'].unique().tolist()
score_parts = []

# ⑤ 计算横截面 z‑score 并加权
score_parts = []
for f in all_factors:
    if f not in merged.columns or f'{f}_ic' not in merged.columns:
        print(f'⚠️  跳过因子 {f}，未找到对应列')
        continue
    z = (merged[f] - merged.groupby('trade_date')[f].transform('mean')) \
        / merged.groupby('trade_date')[f].transform('std')
    score_parts.append(z * merged[f'{f}_ic'])

scores       = np.vstack(score_parts)
valid_counts = (~np.isnan(scores)).sum(axis=0)
raw_sum      = np.nansum(scores, axis=0)
merged['etf_score'] = np.where(valid_counts == 0, np.nan, raw_sum)

# ⑥ 写库
merged[['ts_code', 'trade_date', 'etf_score']].to_sql(
    'etf_score_daily', engine, index=False, if_exists='replace')
print('✅ 已更新表 etf_score_daily')


print('✅ 已把每日 ETF 综合得分写入 etf_score_daily')

merged[['ts_code', 'trade_date', 'etf_score']].to_csv("/Users/tauras/Documents/Code/etf_data/etf_score_daily.csv", index=False, encoding='utf-8-sig')