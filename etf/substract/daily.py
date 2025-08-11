import pandas as pd
import tushare as ts
from datetime import datetime
from sqlalchemy import create_engine

# 设置 Tushare token
ts.set_token("f4b30b0b2f4ea3f5ae7daedb1c280f0d4e380599ef16647df5e80ac8")
pro = ts.pro_api()

# 读取指数列表
ts_df = pd.read_csv('/Users/tauras/Documents/Code/etf_data/sw一级行业指数.csv')
ts_codes = ts_df['ts_code'].dropna().unique()

end_date = datetime.today().strftime('%Y%m%d')
start_date = (datetime.today() - pd.DateOffset(years=3)).strftime('%Y%m%d')

# 创建 MySQL 引擎
engine = create_engine('mysql+pymysql://root:password@localhost:3306/etf_df')

# 合并结果
all_data = []

for code in ts_codes:
    try:
        df = pro.sw_daily(ts_code=code, start_date=start_date, end_date=end_date)
        if not df.empty:
            all_data.append(df)
            print(f"✅ {code} 获取成功")
    except Exception as e:
        print(f"❌ {code} 失败: {e}")

# 合并并写入数据库
if all_data:
    result = pd.concat(all_data, ignore_index=True)
    result.to_sql('dailysw', engine, index=False, if_exists='replace')  # 可改为 'append' 或 'replace'
    print("✅ 数据已写入 MySQL 表 dailysw")
else:
    print("❌ 没有获取到任何数据")
