
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, String, Date, Float

# ─────────────────────────────── 0. 参数区 ───────────────────────────────
MYSQL_URI       = 'mysql+pymysql://root:password@localhost:3306/etf_df'
FACTORS_TABLE   = 'etf_factors'          # 已有因子宽表
IC_TABLE        = 'etf_ts_ic'            # 新表名
ROLLING_WINDOW  = 60                     # 滚动窗口长度
DATE_FMT        = '%Y-%m-%d'             # 因子表里的日期格式

# ─────────────────────────────── 1. 读取数据 ──────────────────────────────
engine = create_engine(MYSQL_URI)
print('📥 读取因子表 …')
factors = pd.read_sql(f'SELECT * FROM {FACTORS_TABLE}', engine)

# 日期列转 datetime
factors['trade_date'] = pd.to_datetime(factors['trade_date'], format=DATE_FMT)

# 需要参与 IC 计算的因子列（排除 id / 日期 / 标签列）
meta_cols   = {'ts_code', 'trade_date'}
factor_cols = [c for c in factors.columns if c not in meta_cols]

# 若没有未来收益列，则用现有收益列『prev_return』向前 shift ‑1 得到 next_return
if 'next_return' not in factors.columns:
    factors['next_return'] = (
        factors
        .groupby('ts_code')['prev_return']
        .shift(-1)                      # 下一期收益
    )

# ─────────────────────────────── 2. 计算 TS‑IC ─────────────────────────────
ic_list = []

print('🧮 计算滚动 TS‑IC …')
for fac in factor_cols:
    grp = (
        factors
        .groupby('ts_code', group_keys=False)
        .apply(                       # 对每只 ETF 单独滚动
            lambda g: pd.Series(
                g[fac]                          # 当前因子
                .rolling(ROLLING_WINDOW, min_periods=20)
                .corr(g['next_return']),        # 与 next_return 相关
                index=g.index                   # 保留索引
            )
        )
        .rename('ts_ic')                       # 列名
    )
    tmp = factors.loc[grp.index, ['ts_code', 'trade_date']].copy()
    tmp['factor'] = fac
    tmp['ts_ic']  = grp.values
    ic_list.append(tmp)

ic_df = pd.concat(ic_list, ignore_index=True).dropna()

print(f'✅ 结果行数: {len(ic_df):,}')

# ─────────────────────────────── 3. 写入 MySQL ────────────────────────────
ic_df.to_sql(IC_TABLE, engine, index=False, if_exists='replace',
             dtype={'ts_code': String(12),
                    'trade_date': Date,
                    
                    'factor': String(64),
                    'ts_ic': Float})

print(f'🎉 已写入表 `{IC_TABLE}` （ts_code/trade_date/facator/ts_ic）')
ic_df.to_csv("/Users/tauras/Documents/Code/etf_data/etf_ic.csv", index=False, encoding='utf-8-sig')
