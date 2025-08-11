

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, String, Date, Float

# ────────────────── 0. 参数区 ──────────────────
MYSQL_URI   = "mysql+pymysql://root:password@localhost:3306/etf_df"
SRC_TABLE   = "etf_score_daily"      # 原始打分
OUT_TABLE   = "etf_score_robust"     # 合并后目标表
CLIP_Q      = [0.01, 0.99]           # 1%–99% 截尾
THRESH_Z    = 0.8                    # robust‑z 阈值
# ────────────────── 1. 读取原表 ──────────────────
engine = create_engine(MYSQL_URI)
df = pd.read_sql(f"SELECT * FROM {SRC_TABLE}", engine)
df["trade_date"] = pd.to_datetime(df["trade_date"])

# ────────────────── 2. clip → robust‑z → 分位数 ──────────────────
def _robust(group: pd.DataFrame) -> pd.DataFrame:
    # ① 截尾
    lo, hi = group["etf_score"].quantile(CLIP_Q)
    clipped = group["etf_score"].clip(lo, hi)

    # ② robust‑z
    med = clipped.median()
    mad = np.median(np.abs(clipped - med))
    scale = 1.4826 * mad if mad else np.nan
    group["robust_z"] = (clipped - med) / scale

    # ③ 分位映射到 [‑1, 1]
    pct = clipped.rank(pct=True)
    group["score_rank"] = (pct - 0.5) * 2
    return group

df = df.groupby("trade_date", as_index=False, group_keys=False).apply(_robust)

# ────────────────── 3. 生成持仓信号 ──────────────────
long_cond   = (df["robust_z"] >=  THRESH_Z) & (df["score_rank"] >= 0.8)
empty_cond  = (df["robust_z"] <= -THRESH_Z) & (df["score_rank"] <= -0.8)

df["signal"] = 0               # 默认空仓
df.loc[long_cond,  "signal"] = 1
df.loc[empty_cond, "signal"] = 0   # 如需对冲可改为 -1

# ────────────────── 4. 一次性落库 ──────────────────
print("📤 写入 etf_score_robust …")
df[["ts_code", "trade_date",
    "etf_score", "robust_z", "score_rank", "signal"]
  ].to_sql(
      OUT_TABLE, engine,
      index=False, if_exists="replace",
      dtype={"ts_code": String(12),
             "trade_date": Date,
             "etf_score": Float,
             "robust_z": Float,
             "score_rank": Float,
             "signal": Float}
  )

print(f"✅ 完成！共 {len(df):,} 行已写入 `{OUT_TABLE}`")

df[["ts_code", "trade_date",
    "etf_score", "robust_z", "score_rank", "signal"]
  ].to_csv("/Users/tauras/Documents/Code/etf_data/etf_score_robust.csv", index=False, encoding='utf-8-sig')