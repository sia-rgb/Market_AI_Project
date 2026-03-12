import json
import os
from pathlib import Path
from openai import OpenAI
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

REPORT_OUTPUT_DIR = Path(r"C:\Users\xiayi\OneDrive\Desktop\Market_AI_Project\每周市场速览")
EXCEL_PATH = Path(__file__).parent / "市场AI数据库.xlsx"
CHARTS_DIR = Path(__file__).parent / "charts"


class MarketInsightGenerator:
    def __init__(self, api_key):
        self.client = OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com"
        )
        
    def load_weekly_report(self, filepath="weekly_report_data.json"):
        """加载周度报告数据"""
        if not os.path.exists(filepath):
            print("未找到 weekly_report_data.json，请先运行 data_interpreter.py。")
            return {"market_base_level": [], "weekly_anomalies": [], "baseline_chart_paths": []}
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def generate_insights(self, report_data: dict):
        """基于市场水位与本周边际异动生成周度洞察"""
        base_level = report_data.get("market_base_level", [])
        anomalies = report_data.get("weekly_anomalies", [])

        base_text = "\n".join([
            f"- {m['metric']}: {m['value']}{m.get('unit','')}（处于过去3年 {m['percentile']}% 分位段）"
            if m.get("percentile") is not None
            else f"- {m['metric']}: {m['value']}{m.get('unit','')}"
            for m in base_level
        ]) if base_level else "（暂无数据）"
        anomalies_text = (
            "\n".join([f"- {a['metric']} ({a['type']}): {a['description']}" for a in anomalies])
            if anomalies
            else "本周无异动"
        )

        system_prompt = """你是一个服务于战略研究岗的资深宏观与市场分析Agent。
你的语言风格必须极其简洁、客观、中立、冷静。
绝对禁止使用任何主观情绪化或夸张的词汇（例如"暴涨"、"暴跌"、"急剧"、"恐慌"等），必须使用"显著上行"、"偏离均值"、"快速回落"等中性学术表达。
你的任务是基于输入的市场水位与异动数据，进行跨资产交叉验证与周期定位。"""

        user_prompt = f"""
【输入数据】
1. 市场基础水位（核心指标最新值及过去三年分位数）：
{base_text}

2. 本周边际异动：
{anomalies_text}

【分析指令】
请按以下结构输出周度洞察：
1. 宏观坐标判断：基于分位数数据，用一句话客观定性当前的流动性宽裕程度与市场情绪热度。
2. 边际变化推演：若有异动，评估该异动是否打破了当前的宏观坐标状态；若无异动，评估当前状态继续维持将最利好哪类资产。
3. 战略研判：输出两条针对下周的资产配置或风险防范结论。
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

    def export_report(self, content, report_data: dict, output_dir=None):
        """导出周度报告（含核心洞察与固定观测图），默认保存至 每周市场速览 目录"""
        output_path = Path(output_dir) if output_dir else REPORT_OUTPUT_DIR
        output_path.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now().strftime("%Y-%m-%d")
        md_path = output_path / f"Market_Insight_Report_{date_str}.md"
        pdf_path = output_path / f"Market_Insight_Report_{date_str}.pdf"

        md_content = f"# 宏观市场周度异动推演 ({date_str})\n\n### 核心洞察\n\n{content}\n\n### 异动数据追溯\n\n"

        table_html = ""
        if EXCEL_PATH.exists():
            try:
                from data_interpreter import extract_index_summary_table, generate_index_summary_html
                df_index = extract_index_summary_table(EXCEL_PATH)
                if df_index is not None and not df_index.empty:
                    table_html = generate_index_summary_html(df_index) or ""
            except Exception as e:
                print(f"全球股指一览表生成失败: {e}")

        chart_paths = list({a.get("chart_path") for a in report_data.get("weekly_anomalies", []) if a.get("chart_path")})
        for p in report_data.get("baseline_chart_paths", []):
            chart_paths.append(p)
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
</style></head><body>
{table_html}
{html_body}
</body></html>"""

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
            print(f"执行完毕。PDF 已保存至：{pdf_path.resolve()}")
        else:
            print(f"PDF生成失败。Markdown 已保存至：{md_path.resolve()}")

if __name__ == "__main__":
    API_KEY = os.getenv("DEEPSEEK_API_KEY")
    if not API_KEY:
        raise ValueError("未检测到 API Key，请确认已在 .env 文件中正确配置 DEEPSEEK_API_KEY")

    generator = MarketInsightGenerator(api_key=API_KEY)
    report_data = generator.load_weekly_report()

    print("正在调用 DeepSeek API 进行周度逻辑推演与交叉验证，请稍候...")
    insight_content = generator.generate_insights(report_data)
    generator.export_report(insight_content, report_data, output_dir=REPORT_OUTPUT_DIR)