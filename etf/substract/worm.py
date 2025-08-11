import time, pandas as pd, tushare as ts
from datetime import datetime, timedelta
from sqlalchemy import create_engine, exc
from tqdm import tqdm

# ── 0. 参数 ──────────────────────────────────────────
TOKENS      = [
    "2876ea85cb005fb5fa17c809a98174f2d5aae8b1f830110a5ead6211",
]
tok_i       = 0                       # 当前 token 下标
REQ_PER_MIN = 800                     # 双 token 共 1600/min，保险再留余量
YEARS_BACK  = 1
MYSQL_URI   = "mysql+pymysql://root:password@localhost:3306/etf_df"
MAP_CSV     = "/Users/tauras/Documents/Code/etf_data/sw一级行业成分股_当前_带etf.csv"
TABLE_OUT   = "stock_daily_sw_sorted"

# ── 1. 初始化 Tushare & MySQL ────────────────────────
def init_pro(idx: int):
    ts.set_token(TOKENS[idx])
    return ts.pro_api()

pro     = init_pro(tok_i)
engine  = create_engine(MYSQL_URI)
today   = datetime.today().strftime("%Y%m%d")

# ── 2. 代码 & 最新日期字典 ────────────────────────────
map_df = (pd.read_csv(MAP_CSV, dtype=str)[["con_code","etf_code"]]
          .dropna().rename(columns={"con_code":"ts_code"}))
codes  = map_df["ts_code"].unique().tolist()

q = f"SELECT ts_code, MAX(trade_date) AS max_date FROM {TABLE_OUT} GROUP BY ts_code"
max_dates = (pd.read_sql(q, engine)
             .dropna(subset=["max_date"])
             .assign(max_date=lambda d: d["max_date"].dt.strftime("%Y%m%d"))
             .set_index("ts_code")["max_date"].to_dict())

# ── 3. 主循环 ────────────────────────────────────────
rows_new, req_cnt, t0 = 0, 0, time.time()

def rate_sleep():
    """软限速：每 REQ_PER_MIN 次补 sleep"""
    global req_cnt, t0
    if req_cnt and req_cnt % REQ_PER_MIN == 0:
        gap = 60 - (time.time() - t0)
        if gap > 0:
            time.sleep(gap + 0.5)
        t0 = time.time()

def fetch_daily(code, start, end):
    """带 token 轮换的安全调用"""
    global tok_i, pro
    while True:
        try:
            df = pro.daily(ts_code=code, start_date=start, end_date=end)
            return df
        except Exception as e:
            msg = str(e)
            # 判断是否触发限速
            if "最多访问该接口" in msg or "抱歉" in msg:
                # 切 token
                tok_i = 1 - tok_i
                print(f"⏳ Token 触顶，切换到 token[{tok_i}] …")
                pro = init_pro(tok_i)
                # 若两 token 在一分钟内都用过，再强制 sleep
                continue
            else:
                # 其他错误直接抛回上层记录
                raise

for code in tqdm(codes, ncols=100, desc="增量拉取"):
    # 计算起始日
    start_dt = (datetime.strptime(max_dates[code], "%Y%m%d")+timedelta(days=1)) \
               if code in max_dates else datetime.today()-timedelta(days=365*YEARS_BACK)
    if start_dt.strftime("%Y%m%d") > today:
        continue

    # 限速
    rate_sleep()

    # ---- 调 Tushare，带轮换 ----
    try:
        df = fetch_daily(code, start_dt.strftime("%Y%m%d"), today)
        req_cnt += 1
    except Exception as e:
        print(f"⚠️ {code} 请求失败：{e}")
        continue

    if df.empty:
        continue

    # 清洗 & 写库
    df = (df.merge(map_df, on="ts_code", how="left")
            .loc[:, ["ts_code","trade_date","open","high","low","close",
                     "pre_close","change","pct_chg","vol","amount","etf_code"]]
            .drop_duplicates(["ts_code","trade_date"])
            .sort_values(["ts_code","trade_date"]))

    try:
        df.to_sql(TABLE_OUT, engine, index=False, if_exists="append", method="multi")
        rows_new += len(df)
    except exc.IntegrityError:
        pass  # 唯一索引过滤重复

print(f"\n✅ 完成！本次新增 {rows_new:,} 行 → `{TABLE_OUT}`")
