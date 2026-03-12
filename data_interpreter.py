import os
import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path
from typing import Optional

plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False

EXCEL_PATH = Path(__file__).parent / "市场AI数据库.xlsx"
CHARTS_DIR = Path(__file__).parent / "charts"
WEEKLY_LOOKBACK = 52  # 约一年周度数据
PCTL_WINDOW = 252 * 3  # 三年交易日


def calculate_rolling_percentile(series: pd.Series, window: int = PCTL_WINDOW) -> pd.Series:
    """滚动分位数 (0-100)。日频用 252*3，周频用 52*3"""
    def _pctl(x):
        if len(x) < 2 or pd.isna(x.iloc[-1]):
            return np.nan
        return (x.rank(pct=True).iloc[-1] - 0.5 / len(x)) * 100
    return series.rolling(window, min_periods=min(20, window)).apply(_pctl, raw=False)


def _clean_standard_sheet(df_raw: pd.DataFrame, value_cols: list[int], col_names: list[str]) -> Optional[pd.DataFrame]:
    """Standard sheets: header=3, date col 0, value cols. Returns DataFrame with date index."""
    df = df_raw.iloc[:, [0] + value_cols].copy()
    df.columns = ["date"] + col_names
    df["date"] = pd.to_datetime(df["date"], errors="coerce", format="mixed")
    df = df.dropna(subset=["date"])
    df = df.dropna(how="all", axis=1)
    df = df.set_index("date").dropna(how="all")
    return df if len(df) > 0 else None


def _clean_rzrq(xl: pd.ExcelFile) -> Optional[pd.DataFrame]:
    """融资融券余额及买入占比: header row 7, date col 7, value cols 8, 13."""
    df = pd.read_excel(xl, sheet_name="融资融券余额及买入占比", header=7)
    df = df.iloc[:, [7, 8, 13]].copy()
    df.columns = ["date", "total_balance", "buy_ratio"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce", format="mixed")
    df = df.dropna(subset=["date"])
    df = df.dropna(how="all", axis=1)
    df = df.set_index("date").dropna(how="all")
    return df if len(df) > 0 else None


def _clean_retail_sentiment(xl: pd.ExcelFile) -> Optional[pd.DataFrame]:
    """散户情绪资金流向: data from row 8, date col 0, value cols 1,2,3."""
    df = pd.read_excel(xl, sheet_name="散户情绪资金流向", header=6)
    df = df.iloc[:, :4].copy()
    df.columns = ["date", "largeBillInflowMoney", "middleBillInflowMoney", "smallBillInflowMoney"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce", format="mixed")
    df = df.dropna(subset=["date"])
    df = df.dropna(how="all", axis=1)
    df = df.set_index("date").dropna(how="all")
    return df if len(df) > 0 else None


def _clean_astock_volume(xl: pd.ExcelFile) -> Optional[pd.DataFrame]:
    """A股交易量: header row 4, date col 0, value cols 1,2,4."""
    df = pd.read_excel(xl, sheet_name="A股交易量", header=4)
    df = df.iloc[:, [0, 1, 2, 4]].copy()
    df.columns = ["date", "shangzheng", "shenzheng", "a_share_amount"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce", format="mixed")
    df = df.dropna(subset=["date"])
    df = df.dropna(how="all", axis=1)
    df = df.set_index("date").dropna(how="all")
    return df if len(df) > 0 else None


def _clean_dual_table(df_raw: pd.DataFrame, date_col1: int, val_col1: int, date_col2: int, val_col2: int,
                      name1: str, name2: str, thresh1: float = 0.15, thresh2: float = 0.15
                      ) -> list[tuple[pd.DataFrame, str, str, float]]:
    """Split dual-table sheet into two DataFrames. Returns list of (df, col_name, metric_name, threshold_pct)."""
    result = []
    for (dc, vc, name, thresh) in [(date_col1, val_col1, name1, thresh1), (date_col2, val_col2, name2, thresh2)]:
        df = df_raw.iloc[:, [dc, vc]].copy()
        df.columns = ["date", "value"]
        df["date"] = pd.to_datetime(df["date"], errors="coerce", format="mixed")
        df = df.dropna(subset=["date"])
        df = df.dropna(subset=["value"])
        df = df.set_index("date")
        if len(df) > 0:
            result.append((df, "value", name, thresh))
    return result


# 核心观测指标（纳入分位数与双轴图）
CORE_METRICS = ["DR001收盘价", "中债10年期收益率", "美债10年期收益率", "人民币汇率", "融资买入占比", "散户小单净流入"]

# 三张固定双轴图配置: (左轴指标, 右轴指标, 标题)
BASELINE_CHART_CONFIG = [
    ("DR001收盘价", "中债10年期收益率", "内部流动性与资产定价"),
    ("美债10年期收益率", "人民币汇率", "外部压力与汇率锚"),
    ("融资买入占比", "散户小单净流入", "微观情绪与杠杆动能"),
]


class MarketDataInterpreter:
    def __init__(self, lookback_weeks=52):
        self.lookback_weeks = lookback_weeks
        self.alerts = []
        self.weekly_registry: dict[str, tuple[pd.DataFrame, str]] = {}
        self.market_base_level: list[dict] = []
        self.baseline_chart_paths: list[str] = []

    def _resample_weekly(self, df: pd.DataFrame, col_agg: dict[str, str]) -> pd.DataFrame:
        """按周五聚合，col_agg: {col: 'last'|'mean'|'sum'}"""
        agg_dict = {c: ("mean" if a == "mean" else ("sum" if a == "sum" else "last")) for c, a in col_agg.items()}
        return df.resample("W-FRI").agg(agg_dict).dropna(how="all")

    def _generate_chart(self, df: pd.DataFrame, col_name: str, metric_name: str, latest_date: str) -> str:
        """生成并保存最近52周走势图"""
        CHARTS_DIR.mkdir(parents=True, exist_ok=True)
        recent = df.tail(52)
        plt.figure(figsize=(8, 3))
        plt.plot(recent.index, recent[col_name], color="#2c3e50", linewidth=1.5)
        plt.scatter(recent.index[-1], recent[col_name].iloc[-1], color="#e74c3c", zorder=5)
        plt.title(f"{metric_name} (近52周走势)", fontsize=10)
        plt.grid(True, linestyle="--", alpha=0.4)
        plt.xticks(rotation=0, fontsize=8)
        plt.yticks(fontsize=8)
        plt.tight_layout()
        safe_name = metric_name.replace("/", "_").replace("&", "_").replace(" ", "_")
        chart_path = CHARTS_DIR / f"{safe_name}_{latest_date}.png"
        plt.savefig(chart_path, dpi=150)
        plt.close()
        return str(chart_path)

    def _generate_baseline_chart(self, left_metric: str, right_metric: str, title: str, date_str: str) -> Optional[str]:
        """双轴对比图"""
        if left_metric not in self.weekly_registry or right_metric not in self.weekly_registry:
            return None
        df_l, col_l = self.weekly_registry[left_metric]
        df_r, col_r = self.weekly_registry[right_metric]
        common_idx = df_l.index.intersection(df_r.index).sort_values()
        if len(common_idx) < 10:
            return None
        recent = common_idx[-52:] if len(common_idx) >= 52 else common_idx
        left_vals = df_l.loc[recent, col_l].reindex(recent).ffill().bfill()
        right_vals = df_r.loc[recent, col_r].reindex(recent).ffill().bfill()
        if left_vals.isna().all() or right_vals.isna().all():
            return None
        CHARTS_DIR.mkdir(parents=True, exist_ok=True)
        fig, ax1 = plt.subplots(figsize=(8, 3.5))
        ax1.plot(recent, left_vals, color="#2c3e50", linewidth=1.5, label=left_metric)
        ax1.set_ylabel(left_metric, color="#2c3e50", fontsize=9)
        ax1.tick_params(axis="y", labelcolor="#2c3e50")
        ax2 = ax1.twinx()
        ax2.plot(recent, right_vals, color="#e74c3c", linewidth=1.5, alpha=0.8, label=right_metric)
        ax2.set_ylabel(right_metric, color="#e74c3c", fontsize=9)
        ax2.tick_params(axis="y", labelcolor="#e74c3c")
        ax1.set_title(title, fontsize=10)
        ax1.grid(True, linestyle="--", alpha=0.4)
        ax1.legend(loc="upper left", fontsize=8)
        ax2.legend(loc="upper right", fontsize=8)
        plt.xticks(rotation=0, fontsize=8)
        plt.tight_layout()
        idx = next((i for i, (l, r, t) in enumerate(BASELINE_CHART_CONFIG) if t == title), 0)
        path = CHARTS_DIR / f"baseline_{idx+1}_{date_str}.png"
        plt.savefig(path, dpi=150)
        plt.close()
        return str(path)

    def _check_z_score_anomaly(self, df: pd.DataFrame, col_name: str, metric_name: str, threshold: float = 2.0):
        """周度 Z-Score 异动"""
        if len(df) < self.lookback_weeks:
            return
        recent = df.tail(self.lookback_weeks)
        mean = recent[col_name].mean()
        std = recent[col_name].std()
        latest_val = df[col_name].iloc[-1]
        latest_date = df.index[-1].strftime("%Y-%m-%d")
        z_score = (latest_val - mean) / std if std > 0 else 0
        if abs(z_score) > threshold:
            direction = "上行" if z_score > 0 else "下探"
            chart_path = self._generate_chart(df, col_name, metric_name, latest_date)
            self.alerts.append({
                "date": latest_date,
                "metric": metric_name,
                "type": "极值偏离",
                "description": f"{metric_name}呈现{direction}偏离，当前值 {latest_val:.4f}，偏离一年均值 {abs(z_score):.1f}σ。",
                "z_score": round(z_score, 2),
                "chart_path": chart_path,
            })

    def _check_volatility_anomaly(self, df: pd.DataFrame, col_name: str, metric_name: str, threshold_pct: float = 0.05):
        """周度波动异动"""
        if len(df) < 2:
            return
        latest_val = df[col_name].iloc[-1]
        prev_val = df[col_name].iloc[-2]
        latest_date = df.index[-1].strftime("%Y-%m-%d")
        pct_change = (latest_val - prev_val) / prev_val if prev_val != 0 else 0
        if abs(pct_change) > threshold_pct:
            direction = "上行" if pct_change > 0 else "下行"
            chart_path = self._generate_chart(df, col_name, metric_name, latest_date)
            self.alerts.append({
                "date": latest_date,
                "metric": metric_name,
                "type": "单日波动",
                "description": f"{metric_name}周度{direction} {abs(pct_change)*100:.2f}%，当前值 {latest_val:.4f}。",
                "pct_change": round(pct_change, 4),
                "chart_path": chart_path,
            })

    def _load_and_register_weekly(self, xl: pd.ExcelFile) -> None:
        """加载各数据源，周度聚合后写入 weekly_registry"""
        metric_configs = [
            ("债券指数", [1, 2], ["cba00203", "cba20103"], ["中债综合指数", "中债投资级中资美元债指数"], {"cba00203": "last", "cba20103": "last"}, 0.03),
            ("DR001收盘价", [1], ["close"], ["DR001收盘价"], {"close": "last"}, 0.15),
            ("VIX", [1], ["close"], ["VIX波动率"], {"close": "last"}, 0.10),
            ("债券收益率", [1, 2], ["cn_10y", "us_10y"], ["中债10年期收益率", "美债10年期收益率"], {"cn_10y": "last", "us_10y": "last"}, 0.05),
            ("石油价格", [1], ["close"], ["石油价格"], {"close": "last"}, 0.05),
            ("黄金价格", [1], ["close"], ["黄金价格"], {"close": "last"}, 0.05),
        ]
        for sheet_name, value_cols, col_names, metric_names, col_agg, thresh in metric_configs:
            if sheet_name not in xl.sheet_names:
                continue
            try:
                df_raw = pd.read_excel(xl, sheet_name=sheet_name, header=3)
                df = _clean_standard_sheet(df_raw, value_cols, col_names)
                if df is None:
                    continue
                df = df.dropna()
                df_weekly = self._resample_weekly(df, col_agg)
                for col, metric in zip(col_names, metric_names):
                    if col not in df_weekly.columns:
                        continue
                    self.weekly_registry[metric] = (df_weekly[[col]].dropna(), col)
                    self._check_z_score_anomaly(df_weekly, col, metric, 2.0)
                    self._check_volatility_anomaly(df_weekly, col, metric, thresh)
            except Exception as e:
                print(f"Sheet [{sheet_name}] 解析失败: {e}")

        if "同业拆借利率" in xl.sheet_names:
            try:
                df_raw = pd.read_excel(xl, sheet_name="同业拆借利率", header=3)
                duals = _clean_dual_table(df_raw, 0, 1, 3, 4, "LIBOR隔夜", "SHIBOR隔夜", 0.15, 0.15)
                for df, col, metric, thresh in duals:
                    df = df.dropna()
                    df_weekly = self._resample_weekly(df, col_agg={col: "last"})
                    self.weekly_registry[metric] = (df_weekly, col)
                    self._check_z_score_anomaly(df_weekly, col, metric, 2.0)
                    self._check_volatility_anomaly(df_weekly, col, metric, thresh)
            except Exception as e:
                print(f"Sheet [同业拆借利率] 解析失败: {e}")

        if "美元指数&人民币汇率" in xl.sheet_names:
            try:
                df_raw = pd.read_excel(xl, sheet_name="美元指数&人民币汇率", header=3)
                duals = _clean_dual_table(df_raw, 0, 1, 3, 4, "美元指数", "人民币汇率", 0.02, 0.02)
                for df, col, metric, thresh in duals:
                    df = df.dropna()
                    df_weekly = self._resample_weekly(df, col_agg={col: "last"})
                    self.weekly_registry[metric] = (df_weekly, col)
                    self._check_z_score_anomaly(df_weekly, col, metric, 2.0)
                    self._check_volatility_anomaly(df_weekly, col, metric, thresh)
            except Exception as e:
                print(f"Sheet [美元指数&人民币汇率] 解析失败: {e}")

        if "融资融券余额及买入占比" in xl.sheet_names:
            try:
                df = _clean_rzrq(xl)
                if df is not None:
                    df = df.dropna()
                    for col, metric, thresh in [("total_balance", "融资融券余额", 0.05), ("buy_ratio", "融资买入占比", 0.05)]:
                        if col in df.columns:
                            df_weekly = self._resample_weekly(df[[col]], {col: "last"})
                            self.weekly_registry[metric] = (df_weekly, col)
                            self._check_z_score_anomaly(df_weekly, col, metric, 2.0)
                            self._check_volatility_anomaly(df_weekly, col, metric, thresh)
            except Exception as e:
                print(f"Sheet [融资融券余额及买入占比] 解析失败: {e}")

        if "散户情绪资金流向" in xl.sheet_names:
            try:
                df = _clean_retail_sentiment(xl)
                if df is not None:
                    df = df.dropna()
                    for col, metric in [("smallBillInflowMoney", "散户小单净流入"), ("largeBillInflowMoney", "大单净流入")]:
                        if col in df.columns:
                            df_weekly = self._resample_weekly(df[[col]], {col: "mean"})
                            self.weekly_registry[metric] = (df_weekly, col)
                            self._check_z_score_anomaly(df_weekly, col, metric, 2.0)
                            self._check_volatility_anomaly(df_weekly, col, metric, 0.20)
            except Exception as e:
                print(f"Sheet [散户情绪资金流向] 解析失败: {e}")

        if "A股交易量" in xl.sheet_names:
            try:
                df = _clean_astock_volume(xl)
                if df is not None:
                    df = df.dropna()
                    for col, metric in [("a_share_amount", "A股成交金额"), ("shangzheng", "上证成交额")]:
                        if col in df.columns:
                            df_weekly = self._resample_weekly(df[[col]], {col: "sum"})
                            self.weekly_registry[metric] = (df_weekly, col)
                            self._check_z_score_anomaly(df_weekly, col, metric, 2.0)
                            self._check_volatility_anomaly(df_weekly, col, metric, 0.10)
            except Exception as e:
                print(f"Sheet [A股交易量] 解析失败: {e}")

    def _build_market_base_level(self, report_date: str) -> None:
        """构建市场基础水位（含三年分位数）"""
        pctl_window_weeks = 52 * 3
        for metric in CORE_METRICS:
            if metric not in self.weekly_registry:
                continue
            df, col = self.weekly_registry[metric]
            if len(df) < 2:
                continue
            latest_val = df[col].iloc[-1]
            pctl_series = calculate_rolling_percentile(df[col], window=pctl_window_weeks)
            pctl = round(float(pctl_series.iloc[-1]), 1) if not pd.isna(pctl_series.iloc[-1]) else None
            unit = "%" if "%" in metric or "收益率" in metric or "占比" in metric or "汇率" in metric else ""
            self.market_base_level.append({
                "metric": metric,
                "value": round(float(latest_val), 4),
                "percentile": pctl,
                "unit": unit,
            })

    def _generate_baseline_charts(self, date_str: str) -> None:
        """生成三张固定双轴图"""
        for left, right, title in BASELINE_CHART_CONFIG:
            path = self._generate_baseline_chart(left, right, title, date_str)
            if path:
                self.baseline_chart_paths.append(path)

    def run_pipeline(self):
        """执行周度数据扫描与报告生成"""
        if not EXCEL_PATH.exists():
            print(f"Excel 文件不存在: {EXCEL_PATH}")
            return

        try:
            xl = pd.ExcelFile(EXCEL_PATH)
        except Exception as e:
            print(f"无法打开 Excel 文件: {e}")
            return

        self._load_and_register_weekly(xl)
        report_date = datetime.now().strftime("%Y-%m-%d")
        self._build_market_base_level(report_date)
        self._generate_baseline_charts(report_date)

    def export_weekly_report(self, output_path: str = "weekly_report_data.json"):
        """输出周度报告数据"""
        report_date = datetime.now().strftime("%Y-%m-%d")
        payload = {
            "report_date": report_date,
            "market_base_level": self.market_base_level,
            "weekly_anomalies": self.alerts,
            "baseline_chart_paths": self.baseline_chart_paths,
        }
        out_file = Path(output_path).resolve()
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"扫描完成，市场水位 {len(self.market_base_level)} 项，异动 {len(self.alerts)} 条，已输出至 {out_file}")


INDEX_NAMES_ORDER = ["标普500", "道琼斯", "富时100", "恒生指数", "日经225", "上证综指"]
INDEX_NAME_ALIASES = {
    "标普500": ["标普500", "SPX"],
    "道琼斯": ["道琼斯", "DJI", "道指"],
    "富时100": ["富时100", "FTSE", "英国富时100"],
    "恒生指数": ["恒生指数", "HSI"],
    "日经225": ["日经225", "N225", "日经"],
    "上证综指": ["上证综指", "上证指数", "000001"],
}


def extract_index_summary_table(excel_path) -> Optional[pd.DataFrame]:
    """从股指 sheet 提取 6 个指数的汇总表，列：指数名、最新收盘价、最近1周、最近1月、2026年至今、2025年全年"""
    try:
        df_raw = pd.read_excel(excel_path, sheet_name="股指", header=2)
    except Exception as e:
        print(f"读取股指 sheet 失败: {e}")
        return None
    if len(df_raw) == 0 or len(df_raw.columns) < 7:
        return None
    # 列位置：0=Code, 1=Name, 2=最新收盘价, 3=最近1周, 4=最近1月, 5=2026年至今, 6=2025年全年
    name_idx, close_idx = 1, 2
    pct_idxs = [3, 4, 5, 6]
    result_rows = []
    for target_name in INDEX_NAMES_ORDER:
        aliases = INDEX_NAME_ALIASES.get(target_name, [target_name])
        row = None
        for _, r in df_raw.iterrows():
            val = r.iloc[name_idx] if name_idx < len(r) else ""
            if pd.isna(val):
                continue
            val_str = str(val).strip()
            if any(a in val_str or val_str in a for a in aliases):
                row = r
                break
        if row is None:
            continue
        close_val = row.iloc[close_idx] if close_idx < len(row) else np.nan
        pct_vals = [row.iloc[i] if i < len(row) else np.nan for i in pct_idxs]
        result_rows.append({
            "指数名": target_name,
            "最新收盘价": close_val,
            "最近1周": pct_vals[0],
            "最近1月": pct_vals[1],
            "2026年至今": pct_vals[2],
            "2025年全年": pct_vals[3],
        })
    if not result_rows:
        return None
    return pd.DataFrame(result_rows)


def _fmt_pct(val) -> str:
    """格式化涨跌幅：正数红色显示，负数绿色加括号"""
    if pd.isna(val):
        return ""
    try:
        v = float(val)
        pct = v * 100
        if pct >= 0:
            return f"{pct:.1f}%"
        return f"({-pct:.1f}%)"
    except (TypeError, ValueError):
        return str(val)


def generate_index_summary_html(df: pd.DataFrame) -> Optional[str]:
    """生成全球主要股票指数一览 HTML 表格，两行表头（涨跌幅% 跨 4 列），涨红跌绿"""
    if df is None or df.empty:
        return None
    from datetime import datetime
    y = datetime.now().year
    pct_cols = ["最近1周", "最近1月", "2026年至今", "2025年全年"]
    rows = []
    rows.append(
        '<tr style="background:#0f5c8a; color:white">'
        '<td colspan="6" style="text-align:center; font-family:KaiTi,serif; font-weight:bold; padding:8px">全球主要股票指数一览</td>'
        "</tr>"
    )
    rows.append(
        '<tr style="background:#F0F0F0; font-family:KaiTi,serif; font-weight:bold">'
        '<td style="padding:6px 10px; border:1px solid #ddd"></td>'
        '<td style="padding:6px 10px; border:1px solid #ddd">最新收盘价</td>'
        '<td colspan="4" style="padding:6px 10px; border:1px solid #ddd; text-align:center">涨跌幅%</td>'
        "</tr>"
    )
    rows.append(
        '<tr style="background:#F0F0F0; font-family:KaiTi,serif; font-weight:bold">'
        '<td style="padding:6px 10px; border:1px solid #ddd"></td>'
        '<td style="padding:6px 10px; border:1px solid #ddd"></td>'
        f'<td style="padding:6px 10px; border:1px solid #ddd">最近1周</td>'
        f'<td style="padding:6px 10px; border:1px solid #ddd">最近1月</td>'
        f'<td style="padding:6px 10px; border:1px solid #ddd">{y}年至今</td>'
        f'<td style="padding:6px 10px; border:1px solid #ddd">{y-1}年全年</td>'
        "</tr>"
    )
    for _, r in df.iterrows():
        close = r.get("最新收盘价", np.nan)
        close_str = f"{float(close):,.0f}" if not pd.isna(close) and str(close) != "" else ""
        cells = [
            f'<td style="padding:6px 10px; border:1px solid #ddd; font-family:KaiTi,serif">{r.get("指数名", "")}</td>',
            f'<td style="padding:6px 10px; border:1px solid #ddd; font-family:Times New Roman,serif">{close_str}</td>',
        ]
        for col in pct_cols:
            val = r.get(col, np.nan)
            txt = _fmt_pct(val)
            color = "black"
            if not pd.isna(val):
                try:
                    color = "#FF0000" if float(val) >= 0 else "#00A000"
                except (TypeError, ValueError):
                    pass
            cells.append(f'<td style="padding:6px 10px; border:1px solid #ddd; font-family:Times New Roman,serif; color:{color}">{txt}</td>')
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return (
        '<div style="width:50%; max-width:50%; margin-bottom:20px">'
        '<table style="width:100%; border-collapse:collapse; font-size:10pt">'
        + "".join(rows)
        + "</table></div>"
    )


if __name__ == "__main__":
    interpreter = MarketDataInterpreter()
    interpreter.run_pipeline()
    interpreter.export_weekly_report()