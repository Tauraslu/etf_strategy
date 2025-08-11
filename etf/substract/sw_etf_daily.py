import os
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
import tushare as ts
from tqdm import tqdm

# ──────────────────── 0. 参数区 ────────────────────
TOKEN      = "2876ea85cb005fb5fa17c809a98174f2d5aae8b1f830110a5ead6211"
MYSQL_URI  = "mysql+pymysql://root:password@localhost:3306/etf_df"
YEARS_BACK = 3                    # 回溯 N 年
CSV_SW     = "/Users/tauras/Documents/Code/etf_data/sw一级行业指数.csv"
CSV_ETF    = "/Users/tauras/Documents/Code/etf_data/etfcode.csv"

TABLE_SW   = "dailysw"
TABLE_ETF  = "dailyetf"

# ──────────────────── 1. 初始化 ────────────────────
ts.set_token(TOKEN)
pro     = ts.pro_api()
engine  = create_engine(MYSQL_URI)

today       = datetime.today().strftime("%Y%m%d")
start_date  = (datetime.today() - pd.DateOffset(years=YEARS_BACK)).strftime("%Y%m%d")

# ──────────────────── 2. 获得代码列表 ────────────────────
sw_codes  = pd.read_csv(CSV_SW )["ts_code"].dropna().unique().tolist()
etf_codes = pd.read_csv(CSV_ETF)["etf_code"].dropna().unique().tolist()

# ──────────────────── 3. 辅助函数：查已存在最大日期 ────────────────────
def get_latest_date(table_name: str) -> str | None:
    with engine.connect() as conn:
        res = conn.execute(text(
            f"SELECT MAX(trade_date) FROM {table_name}"
        )).fetchone()[0]
    return None if res is None else str(res)

latest_sw  = get_latest_date(TABLE_SW)
latest_etf = get_latest_date(TABLE_ETF)

print(f"📌 {TABLE_SW} 最新日期：{latest_sw or '空表'}")
print(f"📌 {TABLE_ETF} 最新日期：{latest_etf or '空表'}")

# 若空表则使用 start_date；否则用最新日期的次日
def next_day(yyyymmdd):             # 20250731 -> 20250801
    return (pd.to_datetime(yyyymmdd) + pd.Timedelta(days=1)).strftime("%Y%m%d")

sw_start  = next_day(latest_sw)  if latest_sw  else start_date
etf_start = next_day(latest_etf) if latest_etf else start_date

# ──────────────────── 4. 抓申万指数 ────────────────────
sw_frames = []
print(f"\n🟢 更新申万指数：{sw_start} ~ {today}")
for code in tqdm(sw_codes, ncols=90, desc="sw_daily"):
    try:
        df = pro.sw_daily(ts_code=code, start_date=sw_start, end_date=today)
        if not df.empty:
            sw_frames.append(df)
    except Exception as e:
        print(f"⚠️  {code} 失败：{e}")

if sw_frames:
    sw_df = pd.concat(sw_frames, ignore_index=True).drop_duplicates(subset=["ts_code", "trade_date"])
    sw_df.to_sql(TABLE_SW, engine, index=False, if_exists="append")
    print(f"✅ 申万指数新增 {len(sw_df):,} 行，已写入 {TABLE_SW}")
else:
    print("ℹ️  无新增申万指数数据")

# ──────────────────── 5. 抓 ETF 日行情 ────────────────────
etf_frames = []
print(f"\n🟢 更新 ETF 行情：{etf_start} ~ {today}")
for code in tqdm(etf_codes, ncols=90, desc="fund_daily"):
    try:
        df = pro.fund_daily(ts_code=code, start_date=etf_start, end_date=today)
        if not df.empty:
            etf_frames.append(df)
    except Exception as e:
        print(f"⚠️  {code} 失败：{e}")

if etf_frames:
    etf_df = (
        pd.concat(etf_frames, ignore_index=True)
        .drop_duplicates(subset=["ts_code", "trade_date"])
    )
    etf_df.to_sql(TABLE_ETF, engine, index=False, if_exists="append")
    print(f"✅ ETF 行情新增 {len(etf_df):,} 行，已写入 {TABLE_ETF}")
else:
    print("ℹ️  无新增 ETF 数据")

print("\n🎉 更新完毕！")
