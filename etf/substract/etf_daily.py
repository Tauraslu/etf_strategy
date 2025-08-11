import pandas as pd
import tushare as ts
from datetime import datetime
from sqlalchemy import create_engine

# 设置 Tushare token
ts.set_token("2876ea85cb005fb5fa17c809a98174f2d5aae8b1f830110a5ead6211")
pro = ts.pro_api()

# 读取 ETF 列表
etf_df = pd.read_csv('/Users/tauras/Documents/Code/etf_data/etfcode.csv')
print("✅ 列名为:", etf_df.columns.tolist())
etf_codes = etf_df['etf_code'].dropna().unique()

# 设置日期范围（三年）
end_date = datetime.today().strftime('%Y%m%d')
start_date = (datetime.today() - pd.DateOffset(years=3)).strftime('%Y%m%d')

# 建立数据库连接
engine = create_engine('mysql+pymysql://root:password@localhost:3306/etf_df')

all_data = []

for code in etf_codes:
    try:
        df = pro.fund_daily(ts_code=code, start_date=start_date, end_date=end_date)
        if not df.empty:
            if 'ts_code' not in df.columns:
                df['ts_code'] = code
            all_data.append(df)
            print(f"✅ {code} 获取成功")
    except Exception as e:
        print(f"❌ {code} 获取失败: {e}")

# 合并写入数据库
if all_data:
    result = pd.concat(all_data, ignore_index=True)
    result.to_sql('dailyetf', engine, index=False, if_exists='replace')
    print("✅ 已写入数据库 dailyetf 表")
else:
    print("❌ 未获取到任何数据")
