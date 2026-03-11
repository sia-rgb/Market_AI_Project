import json
import os
from pathlib import Path
from openai import OpenAI
from datetime import datetime
from dotenv import load_dotenv

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
            return "1. 今日无显著核心数据异动，各监测资产均处于历史均值回归区间内。\n2. 宏观流动性与估值水位维持现状。\n3. 战略配置建议维持现有头寸，无需进行战术性调仓。"

        alerts_text = "\n".join([
            f"- {a['metric']} ({a['type']}): {a['description']}"
            for a in alerts
        ])

        system_prompt = """你是一个服务于战略研究岗的资深宏观与市场分析Agent。
你的语言风格必须极其简洁、客观、中立、冷静。
绝对禁止使用任何主观情绪化或夸张的词汇（例如"暴涨"、"暴跌"、"急剧"、"恐慌"等），必须使用"显著上行"、"偏离均值"、"快速回落"等中性学术表达。
你的任务是基于输入的数据异动，提取周期性特征并进行交叉验证。"""

        user_prompt = f"""
【今日客观异动数据】
{alerts_text}

【分析指令】
请基于上述数据执行逻辑推演，直接输出3条高度浓缩的战略洞察结论（无需分类标题或开头结尾）。
每条结论需在80字以内，直击核心矛盾和对未来1个季度的宏观定价影响。
格式严格如下：
1. [结论一]
2. [结论二]
3. [结论三]
"""

        response = self.client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,
        )
        return response.choices[0].message.content

    def export_report(self, content, alerts, output_dir="."):
        date_str = datetime.now().strftime("%Y-%m-%d")
        output_path = Path(output_dir)
        md_path = output_path / f"Market_Insight_Report_{date_str}.md"
        pdf_path = output_path / f"Market_Insight_Report_{date_str}.pdf"

        md_content = f"# 宏观市场异动推演 ({date_str})\n\n### 核心洞察\n\n{content}\n\n### 异动数据追溯\n\n"

        chart_paths = list({a.get("chart_path") for a in alerts if a.get("chart_path")})
        for chart in chart_paths:
            img_src = Path(chart).as_uri() if Path(chart).is_absolute() else chart
            md_content += f"![]({img_src})\n\n"

        with open(md_path, "w", encoding="utf-8") as f:
            f.write(md_content)

        import markdown
        html_body = markdown.markdown(md_content)
        html_content = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
body {{ font-family: sans-serif; color: #333; line-height: 1.6; max-width: 800px; margin: 0 auto; padding: 20px; }}
h1 {{ font-size: 20px; border-bottom: 1px solid #eee; padding-bottom: 10px; }}
h3 {{ font-size: 16px; margin-top: 20px; }}
img {{ max-width: 100%; height: auto; margin-bottom: 15px; border: 1px solid #f0f0f0; }}
</style></head><body>{html_body}</body></html>"""

        pdf_ok = False
        try:
            import shutil
            _wk = shutil.which("wkhtmltopdf") or (r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe" if Path(r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe").exists() else None) or (r"C:\Program Files (x86)\wkhtmltopdf\bin\wkhtmltopdf.exe" if Path(r"C:\Program Files (x86)\wkhtmltopdf\bin\wkhtmltopdf.exe").exists() else None)
            if _wk:
                import pdfkit
                _config = pdfkit.configuration(wkhtmltopdf=_wk)
                pdfkit.from_string(html_content, str(pdf_path), configuration=_config, options={"enable-local-file-access": ""})
                pdf_ok = True
        except Exception:
            pass

        if not pdf_ok:
            try:
                from xhtml2pdf import pisa
                with open(pdf_path, "w+b") as f:
                    if not pisa.CreatePDF(html_content, dest=f, encoding="utf-8").err:
                        pdf_ok = True
            except Exception:
                pass

        if pdf_ok:
            print(f"执行完毕。单页PDF报告已生成：{pdf_path}")
        else:
            print(f"PDF生成失败（可安装 wkhtmltopdf 或 xhtml2pdf 以获取 PDF）。已保存为 Markdown：{md_path}")

if __name__ == "__main__":
    # 通过环境变量安全读取 API Key
    API_KEY = os.getenv("DEEPSEEK_API_KEY")
    
    if not API_KEY:
        raise ValueError("未检测到 API Key，请确认已在 .env 文件中正确配置 DEEPSEEK_API_KEY")
    
    generator = MarketInsightGenerator(api_key=API_KEY)
    alerts_data = generator.load_alerts()
    
    print("正在调用 DeepSeek API 进行逻辑推演与交叉验证，请稍候...")
    insight_content = generator.generate_insights(alerts_data)
    generator.export_report(insight_content, alerts_data)