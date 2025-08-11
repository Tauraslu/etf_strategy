import pandas as pd
from sqlalchemy import create_engine, text

# 1. 创建数据库连接
engine = create_engine('mysql+pymysql://root:password@localhost:3306/etf_df')
# 方法 1：使用 engine.inspect 获取所有表名
from sqlalchemy import inspect

inspector = inspect(engine)
table_names = inspector.get_table_names()
print("所有表名：", table_names)

df = pd.read_sql("SELECT * FROM etf_factors_with_loss limit 200", engine)
print(df)
