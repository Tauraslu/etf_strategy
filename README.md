# ETF Strategy

This project implements an ETF strategy based on multi-factor analysis and Information Coefficient (IC) weighting.  
It covers the complete workflow including data retrieval, factor calculation, score evaluation, and portfolio rebalancing/backtesting.  
The system supports extracting ETF historical data from a database, calculating scores by industry classification, and generating portfolio adjustments based on trading signals.

---

## 📂 File Description

### Core Factors & Scoring
- **factor.py**  
  Defines and calculates various ETF strategy factors (e.g., momentum, bias, high volatility ratio).
- **calculate_loss.py**  
  Calculates the proportion of loss-making components based on ETF constituent stock prices.
- **ic.py**  
  Calculates the IC (Information Coefficient) of each factor to evaluate predictive power.
- **etf_score.py**  
  Generates composite ETF scores based on IC-weighted aggregation.

### Factor Analysis & Validation
- **analyze_score_distribution.py**  
  Analyzes the distribution of factor scores to assist in selecting effective factors and thresholds.
- **check_score.py**  
  Checks the validity and stability of factor scores.

### Rebalancing & Backtesting
- **rebalance.py**  
  Generates portfolio adjustment plans based on composite scores and rebalancing rules.

### Data Retrieval & Update
- **sql.py**  
  Database connection and SQL query utility for retrieving ETF historical market data and constituents.
- **daily.py**  
  Fetches daily market data from the data source and updates the database.
- **etf_daily.py**  
  Retrieves and stores all ETF daily market data.
- **sw_etf_daily.py**  
  Retrieves daily market data for ETFs classified under Shenwan industry categories.
- **worm.py**  
  Data retrieval scheduler for executing multiple data update tasks in batch.

---

## 🚀 How to Use

1. **Install dependencies**
   ```bash
   pip install pandas numpy sqlalchemy requests tqdm tushare


---

本项目实现了基于多因子分析与信息系数(IC)加权的 ETF 策略，涵盖数据抓取、因子计算、得分评估、调仓回测等完整流程。  
项目支持从数据库中提取 ETF 历史数据、按行业分类计算得分，并根据调仓信号生成持仓方案。

---

## 📂 文件说明

### 核心因子与打分
- **factor.py**  
  定义并计算各类 ETF 策略因子（如动量、偏离度、高波赔率）。
- **calculate_loss.py**  
  依赖etf成分股行情计算亏损成分比例
- **ic.py**  
  计算各因子的 IC（信息系数），用于评估预测能力。
- **etf_score.py**  
  基于 IC 加权合成 ETF 综合得分。


### 因子分析与验证
- **analyze_score_distribution.py**  
  分析因子得分的分布情况，辅助筛选有效因子及阈值。
- **check_score.py**  
  检查因子得分的有效性与稳定性。


### 调仓与回测
- **rebalance.py**  
  根据综合得分和调仓规则生成持仓调整方案。

### 数据获取与更新
- **sql.py**  
  数据库连接与 SQL 查询工具，用于获取 ETF 历史行情与成分数据。
- **daily.py**  
  从数据源抓取每日行情数据并更新到数据库。
- **etf_daily.py**  
  获取并存储所有 ETF 日线行情数据。
- **sw_etf_daily.py**  
  获取申万行业分类的 ETF 日线行情数据。
- **worm.py**  
  数据抓取调度脚本，可批量执行数据更新任务。

---

## 使用方法

1. **安装依赖**
   ```bash
   pip install pandas numpy sqlalchemy requests tqdm tushare



