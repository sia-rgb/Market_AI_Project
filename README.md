# Market AI / 市场AI数据库

从 Excel 市场数据库读取多资产指标，进行 Z-Score 与单日波动率异动检测，并调用 DeepSeek 大模型生成宏观战略推演报告。面向战略研究岗、宏观与市场分析场景。

## 工作流程

```mermaid
flowchart LR
    Excel[市场AI数据库.xlsx] --> Interpreter[data_interpreter.py]
    Interpreter --> Alerts[daily_alerts.json]
    Alerts --> Generator[insight_generator.py]
    Generator --> Report[Market_Insight_Report_日期.md]
```

## 项目结构

| 文件 | 说明 |
|------|------|
| `data_interpreter.py` | 数据清洗与异动检测 |
| `insight_generator.py` | DeepSeek API 调用与报告生成 |
| `市场AI数据库.xlsx` | 数据源（需自行放置） |
| `.env` | API Key 配置（需自行创建） |
| `daily_alerts.json` | 异动清单（自动生成） |
| `Market_Insight_Report_*.md` | 战略报告（自动生成） |

## 环境要求

- Python 3.10+
- 依赖：pandas, numpy, openai, python-dotenv, openpyxl

## 安装

```bash
# 克隆或下载项目后
cd Market_AI_Project

# 创建虚拟环境（推荐）
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate  # Linux/macOS

# 安装依赖
pip install -r requirements.txt
```

## 配置

1. **API Key**：复制 `.env.example` 为 `.env`，填入 DeepSeek API Key：
   ```
   DEEPSEEK_API_KEY=sk-your-api-key-here
   ```
   在 [DeepSeek 开放平台](https://platform.deepseek.com/) 创建并获取 Key。

2. **数据源**：将 `市场AI数据库.xlsx` 置于项目根目录。

## 使用方法

```bash
# 步骤 1：扫描数据并生成异动清单
python data_interpreter.py

# 步骤 2：调用 DeepSeek 生成战略推演报告
python insight_generator.py
```

## 数据源说明

支持的 Excel Sheet：

- 债券指数、DR001收盘价、VIX、债券收益率
- 石油价格、黄金价格
- 同业拆借利率、美元指数&人民币汇率
- 融资融券余额及买入占比、散户情绪资金流向、A股交易量

数据需包含日期列与数值列，具体结构见 `data_interpreter.py` 中各 `_clean_*` 函数。

## 输出示例

**daily_alerts.json**：异动类型（极值偏离预警、单日波动预警）、日期、指标、描述。

**Market_Insight_Report_*.md**：核心异动事实、跨资产交叉验证、战略研判指向。

## 注意事项

- `.env` 已加入 .gitignore，切勿提交 API Key。
- Excel 文件需持续更新以保持检测有效性。
