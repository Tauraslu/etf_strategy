import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# 连接MySQL数据库
engine = create_engine('mysql+pymysql://root:password@localhost:3306/etf_df')

# 读取dailyetf原始数据
df = pd.read_sql("SELECT * FROM dailyetf", engine)

# 日期转换
df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')

# 排序：先 ts_code，再 trade_date
df = df.sort_values(by=['ts_code', 'trade_date']).reset_index(drop=True)

# 重命名列，便于统一公式引用
df.rename(columns={
    'close': 'ClosePrice',
    'high': 'HighPrice',
    'low': 'LowPrice',
    'pre_close': 'PrevClosePrice',
    'pct_chg': 'ChangePCT'
}, inplace=True)

# === 1. Volatility Ratio ===
df['volatility_ratio'] = df['ChangePCT'] / (
    (df['HighPrice'] - df['LowPrice']) / df['PrevClosePrice']
)

# === 2. Deviation from 5-day MA (每个 ETF 独立 rolling)
df['deviation_5d'] = df.groupby('ts_code')['ClosePrice'].transform(
    lambda x: (x - x.rolling(window=5).mean()) / x.rolling(window=5).mean()
)

# === 3. Previous Daily Return (每个 ETF 独立 shift)
df['prev_return'] = df.groupby('ts_code')['ClosePrice'].transform(
    lambda x: x / x.shift(1) - 1
)

# Price Momentum（7日）
df['momentum_7d'] = df.groupby('ts_code')['ClosePrice'].transform(
    lambda x: x / x.shift(7) - 1
)

# Volume Momentum（7日）
df['volume_momentum_7d'] = df.groupby('ts_code')['vol'].transform(
    lambda x: x / x.shift(7) - 1
)

df['daily_return'] = df.groupby('ts_code')['ClosePrice'].pct_change()
df['momentum_21d'] = df.groupby('ts_code')['ClosePrice'].transform(lambda x: x / x.shift(21) - 1)
df['ma_10'] = df.groupby('ts_code')['ClosePrice'].transform(lambda x: x.rolling(10).mean())
df['bias_10d'] = (df['ClosePrice'] - df['ma_10']) / df['ma_10']
df['volatility_21d'] = df.groupby('ts_code')['daily_return'].transform(lambda x: x.rolling(21).std())

# 选取因子列
factor_df = df[['ts_code', 'trade_date', 'volatility_ratio', 'deviation_5d', 'prev_return', 'momentum_7d', 'volume_momentum_7d', 'momentum_21d', 'volatility_21d', 'bias_10d']].copy()

# 替换 inf
factor_df = factor_df.replace([np.inf, -np.inf], np.nan)

# 写入 MySQL
factor_df.to_sql('etf_factors', engine, index=False, if_exists='replace')

print("✅ Factors written to table: etf_factors")
print(factor_df.head(50))