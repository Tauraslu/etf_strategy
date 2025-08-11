import pandas as pd
from vectorbt.portfolio import Portfolio  # ✅ 注意正确的 import
import vectorbt as vbt

# 1. 加载数据
prices = pd.read_csv(
    "/Users/tauras/Documents/Code/etf_data/dailyetf.csv",
    parse_dates=["trade_date"]
)
scores = pd.read_csv(
    "/Users/tauras/Documents/Code/etf_data/etf_score_robust.csv",
    parse_dates=["trade_date"]
)

# 2. 转成宽表格式z
price_ts = prices.pivot(index="trade_date", columns="ts_code", values="close")
rank_ts = scores.pivot(index="trade_date", columns="ts_code", values="score_rank")

# 3. 每日选 top N
N = 10
is_top_n = rank_ts.rank(axis=1, ascending=False) <= N

# 4. 构建 entry / exit 信号矩阵（布尔型）
entries = is_top_n
exits = ~is_top_n

# 4.1 对齐所有 DataFrame 的索引和列（防止 shape mismatch）
common_index = price_ts.index.intersection(entries.index).intersection(exits.index)
common_columns = price_ts.columns.intersection(entries.columns).intersection(exits.columns)

# 只保留共有的行和列
price_ts = price_ts.loc[common_index, common_columns]
entries = entries.loc[common_index, common_columns]
exits = exits.loc[common_index, common_columns]


# 5. 运行回测（from_signals）
pf = Portfolio.from_signals(
    close=price_ts,
    entries=entries,
    exits=exits,
    size_type='value',          # 按资金价值等权分配
    init_cash=1_000_000,
    fees=0.0001,
    freq="1D"
)

# 6. 输出回测指标
print("📈 Total return:     ", pf.total_return().mean().round(4))
print("📈 Annual return:    ", pf.annualized_return().mean().round(4))
print("📉 Max drawdown:     ", pf.max_drawdown().mean().round(4))
print("⚖️  Sharpe ratio:     ", pf.sharpe_ratio().mean().round(4))

# 7. 绘图
pf.cumulative_returns().vbt.plot(title="组合累计收益率").show()
