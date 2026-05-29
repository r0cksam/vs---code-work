"""Smart chart recommendation helpers."""
from __future__ import annotations
import pandas as pd


def infer_chart_type(series: pd.Series, cardinality_limit: int = 20) -> str:
    """Return a good default chart type for a column."""
    if series is None or len(series) == 0:
        return "Bar graph"
    s = series.dropna()
    if s.empty:
        return "Bar graph"
    numeric_s = pd.to_numeric(s, errors="coerce")
    if numeric_s.notna().mean() > 0.75:
        return "Histogram"
    dt = pd.to_datetime(s, errors="coerce")
    if dt.notna().mean() > 0.75:
        return "Time series"
    cardinality = s.astype(str).nunique(dropna=True)
    if cardinality <= cardinality_limit:
        return "Pie chart"
    return "Bar graph"


def value_count_df(series: pd.Series, col_name: str, top_n: int = 20, include_other: bool = True) -> pd.DataFrame:
    vc = series.fillna("(null)").astype(str).replace("", "(blank)").value_counts()
    out = vc.head(int(top_n)).reset_index()
    out.columns = [col_name, "count"]
    if include_other and len(vc) > int(top_n):
        out = pd.concat([out, pd.DataFrame([{col_name: "Other", "count": int(vc.iloc[int(top_n):].sum())}])], ignore_index=True)
    return out
