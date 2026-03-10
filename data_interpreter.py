import pandas as pd
import numpy as np
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

EXCEL_PATH = Path(__file__).parent / "市场AI数据库.xlsx"


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


class MarketDataInterpreter:
    def __init__(self, lookback_window=252): 
        # 默认回溯一年(约252个交易日)进行历史分位与标准差计算
        self.lookback_window = lookback_window
        self.alerts = []

    def check_z_score_anomaly(self, df, col_name, metric_name, threshold=2.0):
        """规则1：Z-Score 绝对偏离度监控（突破近一年均值 ±2倍标准差）"""
        if len(df) < self.lookback_window:
            return
        
        recent_data = df.tail(self.lookback_window)
        mean = recent_data[col_name].mean()
        std = recent_data[col_name].std()
        latest_val = df[col_name].iloc[-1]
        latest_date = df.index[-1].strftime('%Y-%m-%d')
        
        z_score = (latest_val - mean) / std if std > 0 else 0
        
        if abs(z_score) > threshold:
            direction = "向上" if z_score > 0 else "向下"
            self.alerts.append({
                "date": latest_date,
                "metric": metric_name,
                "type": "极值偏离预警",
                "description": f"{metric_name}触发{direction}偏离，当前值 {latest_val:.4f}，偏离过去一年均值 {abs(z_score):.1f} 个标准差。",
                "z_score": round(z_score, 2)
            })

    def check_volatility_anomaly(self, df, col_name, metric_name, threshold_pct=0.05):
        """规则2：单日波动率异常监控"""
        if len(df) < 2:
            return
            
        latest_val = df[col_name].iloc[-1]
        prev_val = df[col_name].iloc[-2]
        latest_date = df.index[-1].strftime('%Y-%m-%d')
        
        pct_change = (latest_val - prev_val) / prev_val if prev_val != 0 else 0
        
        if abs(pct_change) > threshold_pct:
            direction = "飙升" if pct_change > 0 else "暴跌"
            self.alerts.append({
                "date": latest_date,
                "metric": metric_name,
                "type": "单日波动预警",
                "description": f"{metric_name}单日{direction} {abs(pct_change)*100:.2f}%，当前值 {latest_val:.4f}。",
                "pct_change": round(pct_change, 4)
            })

    def run_pipeline(self):
        """执行所有数据源的扫描"""
        if not EXCEL_PATH.exists():
            print(f"Excel 文件不存在: {EXCEL_PATH}")
            return

        try:
            xl = pd.ExcelFile(EXCEL_PATH)
        except Exception as e:
            print(f"无法打开 Excel 文件: {e}")
            return

        # 标准型 Sheet 配置: (sheet_name, value_col_indices, col_names, metric_names, threshold_pct)
        standard_configs = [
            ("债券指数", [1, 2], ["cba00203", "cba20103"], ["中债综合指数", "中债投资级中资美元债指数"], 0.03),
            ("DR001收盘价", [1], ["close"], ["DR001收盘价"], 0.15),
            ("VIX", [1], ["close"], ["VIX波动率"], 0.10),
            ("债券收益率", [1, 2], ["cn_10y", "us_10y"], ["中债10年期收益率", "美债10年期收益率"], 0.05),
            ("石油价格", [1], ["close"], ["石油价格"], 0.05),
            ("黄金价格", [1], ["close"], ["黄金价格"], 0.05),
        ]

        for sheet_name, value_cols, col_names, metric_names, threshold_pct in standard_configs:
            if sheet_name not in xl.sheet_names:
                continue
            try:
                df_raw = pd.read_excel(xl, sheet_name=sheet_name, header=3)
                df = _clean_standard_sheet(df_raw, value_cols, col_names)
                if df is None:
                    continue
                df = df.dropna()
                for col, metric in zip(col_names, metric_names):
                    if col not in df.columns:
                        continue
                    self.check_z_score_anomaly(df, col, metric)
                    self.check_volatility_anomaly(df, col, metric, threshold_pct=threshold_pct)
            except Exception as e:
                print(f"Sheet [{sheet_name}] 解析失败: {e}")

        # 双表型: 同业拆借利率
        if "同业拆借利率" in xl.sheet_names:
            try:
                df_raw = pd.read_excel(xl, sheet_name="同业拆借利率", header=3)
                duals = _clean_dual_table(df_raw, 0, 1, 3, 4, "LIBOR隔夜", "SHIBOR隔夜", 0.15, 0.15)
                for df, col, metric, thresh in duals:
                    df = df.dropna()
                    self.check_z_score_anomaly(df, col, metric)
                    self.check_volatility_anomaly(df, col, metric, threshold_pct=thresh)
            except Exception as e:
                print(f"Sheet [同业拆借利率] 解析失败: {e}")

        # 双表型: 美元指数&人民币汇率
        if "美元指数&人民币汇率" in xl.sheet_names:
            try:
                df_raw = pd.read_excel(xl, sheet_name="美元指数&人民币汇率", header=3)
                duals = _clean_dual_table(df_raw, 0, 1, 3, 4, "美元指数", "人民币汇率", 0.02, 0.02)
                for df, col, metric, thresh in duals:
                    df = df.dropna()
                    self.check_z_score_anomaly(df, col, metric)
                    self.check_volatility_anomaly(df, col, metric, threshold_pct=thresh)
            except Exception as e:
                print(f"Sheet [美元指数&人民币汇率] 解析失败: {e}")

        # 融资融券余额及买入占比
        if "融资融券余额及买入占比" in xl.sheet_names:
            try:
                df = _clean_rzrq(xl)
                if df is not None:
                    df = df.dropna()
                    for col, metric, thresh in [("total_balance", "融资融券余额", 0.05), ("buy_ratio", "融资买入占比", 0.05)]:
                        if col in df.columns:
                            self.check_z_score_anomaly(df, col, metric)
                            self.check_volatility_anomaly(df, col, metric, threshold_pct=thresh)
            except Exception as e:
                print(f"Sheet [融资融券余额及买入占比] 解析失败: {e}")

        # 散户情绪资金流向
        if "散户情绪资金流向" in xl.sheet_names:
            try:
                df = _clean_retail_sentiment(xl)
                if df is not None:
                    df = df.dropna()
                    for col, metric in zip(
                        ["smallBillInflowMoney", "largeBillInflowMoney"],
                        ["散户小单净流入", "大单净流入"],
                    ):
                        if col in df.columns:
                            self.check_z_score_anomaly(df, col, metric)
                            self.check_volatility_anomaly(df, col, metric, threshold_pct=0.20)
            except Exception as e:
                print(f"Sheet [散户情绪资金流向] 解析失败: {e}")

        # A股交易量
        if "A股交易量" in xl.sheet_names:
            try:
                df = _clean_astock_volume(xl)
                if df is not None:
                    df = df.dropna()
                    for col, metric in zip(
                        ["a_share_amount", "shangzheng"],
                        ["A股成交金额", "上证成交额"],
                    ):
                        if col in df.columns:
                            self.check_z_score_anomaly(df, col, metric)
                            self.check_volatility_anomaly(df, col, metric, threshold_pct=0.10)
            except Exception as e:
                print(f"Sheet [A股交易量] 解析失败: {e}")

        # 股指: 周度快照，非日频，跳过
        if "股指" in xl.sheet_names:
            print("Sheet [股指] 为周度快照结构，暂不纳入日频检测，已跳过。")

    def export_alerts(self, output_path='daily_alerts.json'):
        """输出异动清单供大模型读取"""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.alerts, f, ensure_ascii=False, indent=2)
        print(f"扫描完成，共产生 {len(self.alerts)} 条客观异动，已输出至 {output_path}")

if __name__ == "__main__":
    interpreter = MarketDataInterpreter()
    interpreter.run_pipeline()
    interpreter.export_alerts()