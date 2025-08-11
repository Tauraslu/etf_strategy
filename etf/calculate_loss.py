import pandas as pd
from sqlalchemy import create_engine

# 建立连接
engine = create_engine('mysql+pymysql://root:password@localhost:3306/etf_df')

# 读取成分股日行情 + ETF 映射信息
df_sw = pd.read_sql("SELECT ts_code, trade_date, pct_chg, etf_code FROM stock_daily_sw_sorted", engine)

# 去掉没有对应 ETF 的成分股（可能是缺失或不匹配）
df_sw = df_sw.dropna(subset=['etf_code'])

# 转换日期（检查是否已经是 datetime 类型）
if not pd.api.types.is_datetime64_any_dtype(df_sw['trade_date']):
    df_sw['trade_date'] = pd.to_datetime(df_sw['trade_date'], format='%Y-%m-%d' if '-' in str(df_sw['trade_date'].iloc[0]) else '%Y%m%d')

# 亏损标志（跌幅 < 0）
df_sw['is_loss'] = df_sw['pct_chg'] < 0

# 按 etf_code + trade_date 分组：计算亏损比例
loss_ratio_df = (
    df_sw.groupby(['etf_code', 'trade_date'])
    .agg(
        total_stock=('ts_code', 'count'),
        loss_count=('is_loss', 'sum')
    )
    .assign(loss_ratio=lambda x: x['loss_count'] / x['total_stock'])
    .reset_index()[['etf_code', 'trade_date', 'loss_ratio']]
)

# 检查 etf_factors 表的列名
df2 = pd.read_sql("SELECT * FROM etf_factors LIMIT 1", engine)
print("etf_factors 表的列名：", df2.columns.tolist())

# 如果 etf_factors 表有 etf_code 列，则进行筛选
if 'etf_code' in df2.columns:
    df2_full = pd.read_sql("SELECT * FROM etf_factors", engine)
    df2_filtered = df2_full[df2_full['etf_code'] == df2_full['ts_code']]
    print(f"✅ 在 df2 中找到 {len(df2_filtered)} 条 etf_code 等于 ts_code 的记录")
    print(df2_filtered.head())
else:
    print("❌ etf_factors 表中没有 etf_code 列")

# 写入数据库
loss_ratio_df.to_sql('etf_loss_ratio', engine, index=False, if_exists='replace')

print("✅ ETF 成分股亏损比例计算完成，已写入表 etf_loss_ratio")
