import json
import os
from openai import OpenAI
from datetime import datetime
from dotenv import load_dotenv

# 加载本地 .env 文件中的环境变量
load_dotenv()

class MarketInsightGenerator:
    def __init__(self, api_key):
        self.client = OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com"
        )
        
    def load_alerts(self, filepath='daily_alerts.json'):
        if not os.path.exists(filepath):
            print("未找到 daily_alerts.json，请先运行数据解释脚本。")
            return []
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)

    def generate_insights(self, alerts):
        if not alerts:
            return "今日无显著核心数据异动，市场处于均值回归区间。"

        alerts_text = "\n".join([
            f"- {a['date']} | {a['metric']} ({a['type']}): {a['description']}" 
            for a in alerts
        ])

        system_prompt = """你是一个服务于战略研究岗的资深宏观与市场分析Agent。你的语言风格必须极其简洁、客观、中立、冷静。禁止使用任何主观情绪化词汇、冗长的开场白或免责声明。你的任务是基于输入的数据异动，提取周期性特征，并利用基础金融逻辑进行跨资产、跨周期的交叉验证与趋势归因。只输出核心观点和逻辑推演路径。"""

        user_prompt = f"""
【今日客观异动数据】
{alerts_text}

【分析指令】
请基于上述数据执行逻辑推演，严格按以下三部分输出（使用Markdown排版）：
### 1. 核心异动事实
（用最简练的语言归纳触发阈值的数据集现象）
### 2. 跨资产交叉验证
（指出上述多项异动指标之间，是支持同一宏观逻辑形成共振，还是存在逻辑背离。重点提取核心矛盾）
### 3. 战略研判指向
（评估该异动组合对未来1个月及1个季度的宏观流动性或资产定价的潜在影响）
"""

        response = self.client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.2 
        )
        
        return response.choices[0].message.content

    def export_report(self, content, output_dir="."):
        date_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"Market_Insight_Report_{date_str}.md"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"# 宏观市场异动与战略推演报告 ({date_str})\n\n")
            f.write(content)
            
        print(f"执行完毕。战略洞察报告已生成：{filepath}")

if __name__ == "__main__":
    # 通过环境变量安全读取 API Key
    API_KEY = os.getenv("DEEPSEEK_API_KEY")
    
    if not API_KEY:
        raise ValueError("未检测到 API Key，请确认已在 .env 文件中正确配置 DEEPSEEK_API_KEY")
    
    generator = MarketInsightGenerator(api_key=API_KEY)
    alerts_data = generator.load_alerts()
    
    print("正在调用 DeepSeek API 进行逻辑推演与交叉验证，请稍候...")
    insight_content = generator.generate_insights(alerts_data)
    generator.export_report(insight_content)