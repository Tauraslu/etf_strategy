import tushare as ts
import pandas as pd

# 设置 token
pro = ts.pro_api('2876ea85cb005fb5fa17c809a98174f2d5aae8b1f830110a5ead6211')

# 读取行业列表 CSV
sw_path = "/Users/tauras/Documents/Code/etf_data/sw一级行业指数.csv"
sw_df = pd.read_csv(sw_path)

# 初始化列表保存所有行业成分股
all_members = []

# 遍历每个行业指数代码，抓取成分股
for code in sw_df['ts_code']:
    try:
        df = pro.index_member_all(index_code=code)
        df['industry_code'] = code
        df['industry_name'] = sw_df.loc[sw_df['ts_code'] == code, 'name'].values[0]
        all_members.append(df)
        print(f"✅ 成功获取：{code}")
    except Exception as e:
        print(f"❌ 获取失败：{code}，错误：{e}")

# 合并所有结果
full_df = pd.concat(all_members, ignore_index=True)

# 保存
output_path = "/Users/tauras/Documents/Code/etf_data/sw一级行业成分股.csv"
full_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"✅ 已保存所有行业成分股到：{output_path}")
