"""コホートデータの変換・加工.

BigQueryから取得した生データを、表示用のピボットテーブルや
継続率テーブルに変換する。発送日目安の計算も含む。
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from src.config_loader import get_product_cycle
from src.constants import MAX_RETENTION_MONTHS


def build_retention_table(df: pd.DataFrame) -> pd.DataFrame:
    """通常コホートの継続率テーブルを構築.

    Args:
        df: BigQuery結果 (cohort_month, total_users, retained_1..retained_12)

    Returns:
        行=コホート月, 列=回数, 値=継続率(%)のDataFrame
    """
    if df.empty:
        return pd.DataFrame()

    result = pd.DataFrame()
    result["コホート月"] = df["cohort_month"]
    result["新規顧客数"] = df["total_users"].astype(int)

    for i in range(1, MAX_RETENTION_MONTHS + 1):
        col = f"retained_{i}"
        if col in df.columns:
            total = df["total_users"].astype(float)
            retained = pd.to_numeric(df[col], errors="coerce").fillna(0)
            result[f"{i}回目"] = retained.astype(int)
            result[f"{i}回目(%)"] = (retained / total * 100).round(1)

    return result


def build_retention_rate_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """ヒートマップ用の継続率マトリクス (行=月, 列=回数, 値=%).

    Args:
        df: BigQuery結果 (cohort_month, total_users, retained_1..retained_12)

    Returns:
        行=コホート月, 列="1回目"〜"12回目", 値=継続率(%)
    """
    if df.empty:
        return pd.DataFrame()

    matrix = pd.DataFrame(index=df["cohort_month"])
    total = df["total_users"].astype(float).values

    for i in range(1, MAX_RETENTION_MONTHS + 1):
        col = f"retained_{i}"
        if col in df.columns:
            retained = pd.to_numeric(df[col], errors="coerce").fillna(0).values
            matrix[f"{i}回目"] = (retained / total * 100).round(1)

    matrix.index.name = "コホート月"
    return matrix


def build_drilldown_retention_table(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """ドリルダウン結果をグループごとの継続率テーブルに変換.

    Args:
        df: BigQuery結果 (dimension_col, cohort_month, total_users, retained_1..12)

    Returns:
        {グループ名: 継続率DataFrame} の辞書
    """
    if df.empty:
        return {}

    result = {}
    for group_name, group_df in df.groupby("dimension_col"):
        group_df = group_df.reset_index(drop=True)
        table = build_retention_table(group_df)
        result[str(group_name)] = table

    return result


def build_drilldown_rate_matrices(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """ドリルダウン結果をグループごとのヒートマップ用マトリクスに変換."""
    if df.empty:
        return {}

    result = {}
    for group_name, group_df in df.groupby("dimension_col"):
        group_df = group_df.reset_index(drop=True)
        matrix = build_retention_rate_matrix(group_df)
        result[str(group_name)] = matrix

    return result


def build_shipping_schedule(
    cohort_months: list[str],
    company_key: str,
    product_name: str | None = None,
) -> pd.DataFrame:
    """発送日目安テーブルを構築.

    GASの interleaveShippingDates ロジックを移植。
    起点: コホート月の翌月1日
    2回目: 起点 + cycle1日
    3回目以降: 前回 + cycle2日
    """
    if not cohort_months:
        return pd.DataFrame()

    cycle1, cycle2 = get_product_cycle(company_key, product_name or "")

    rows = []
    for month_str in cohort_months:
        parts = month_str.split("-")
        if len(parts) != 2:
            continue
        year, month = int(parts[0]), int(parts[1])

        # 起点: 翌月1日
        if month == 12:
            base_date = date(year + 1, 1, 1)
        else:
            base_date = date(year, month + 1, 1)

        row = {"コホート月": month_str}
        calc_date = base_date

        # 1回目: 翌月1日
        row["1回目"] = calc_date.strftime("%Y/%m/%d")
        # 2回目: +cycle1
        calc_date = calc_date + timedelta(days=cycle1)
        row["2回目"] = calc_date.strftime("%Y/%m/%d")
        # 3回目以降: +cycle2
        for i in range(3, MAX_RETENTION_MONTHS + 1):
            calc_date = calc_date + timedelta(days=cycle2)
            row[f"{i}回目"] = calc_date.strftime("%Y/%m/%d")

        rows.append(row)

    return pd.DataFrame(rows)


def compute_summary_metrics(df: pd.DataFrame) -> dict:
    """KPIサマリー指標を計算.

    Args:
        df: BigQuery結果 (cohort_month, total_users, retained_1..12)

    Returns:
        {"total_new_users": ..., "avg_retention_2": ..., "latest_12m_retention": ...}
    """
    if df.empty:
        return {
            "total_new_users": 0,
            "avg_retention_2": 0.0,
            "latest_12m_retention": 0.0,
        }

    total = df["total_users"].astype(float)
    total_new = int(total.sum())

    # 2回目平均継続率
    if "retained_2" in df.columns:
        r2 = pd.to_numeric(df["retained_2"], errors="coerce").fillna(0)
        avg_r2 = (r2 / total * 100).mean()
    else:
        avg_r2 = 0.0

    # 最新月の12回目残存率
    if "retained_12" in df.columns and len(df) > 0:
        last_row = df.iloc[-1]
        t = float(last_row["total_users"])
        r12 = float(pd.to_numeric(last_row.get("retained_12", 0), errors="coerce") or 0)
        latest_12m = (r12 / t * 100) if t > 0 else 0.0
    else:
        latest_12m = 0.0

    return {
        "total_new_users": total_new,
        "avg_retention_2": round(avg_r2, 1),
        "latest_12m_retention": round(latest_12m, 1),
    }


# =====================================================================
# 通算コホート分析 (全月合算)
# =====================================================================


def build_aggregate_table(df: pd.DataFrame) -> pd.DataFrame:
    """通算コホートの継続率・残存率・LTVテーブルを構築.

    Args:
        df: BigQuery結果 (1行: total_users, retained_1..12, revenue_1..12)

    Returns:
        行=定期回数(1〜12), 列=各指標 のDataFrame
    """
    if df.empty:
        return pd.DataFrame()

    row = df.iloc[0]
    total = float(row["total_users"])
    if total == 0:
        return pd.DataFrame()

    rows = []
    cumulative_revenue = 0.0

    for i in range(1, MAX_RETENTION_MONTHS + 1):
        ret_col = f"retained_{i}"
        rev_col = f"revenue_{i}"

        retained = float(pd.to_numeric(row.get(ret_col, 0), errors="coerce") or 0)
        revenue = float(pd.to_numeric(row.get(rev_col, 0), errors="coerce") or 0)

        # 継続率: N回目の成功者数 / 初回全体 (= 残存率と同義、通算では月をまたがないため)
        retention_rate = (retained / total * 100) if total > 0 else 0.0

        # 平均単価: N回目の合計売上 / N回目の人数
        avg_price = (revenue / retained) if retained > 0 else 0.0

        # 累積売上
        cumulative_revenue += revenue

        # LTV: 累積売上 / 初回全体人数
        ltv = cumulative_revenue / total if total > 0 else 0.0

        rows.append({
            "定期回数": f"{i}回目",
            "継続人数": int(retained),
            "継続率(%)": round(retention_rate, 1),
            "平均単価(円)": int(round(avg_price)),
            "回次売上(円)": int(revenue),
            "累積売上(円)": int(cumulative_revenue),
            "LTV(円)": int(round(ltv)),
        })

    return pd.DataFrame(rows)


def compute_aggregate_metrics(df: pd.DataFrame) -> dict:
    """通算コホートのKPIサマリー指標を計算."""
    if df.empty:
        return {
            "total_new_users": 0,
            "retention_2": 0.0,
            "ltv_12": 0,
        }

    row = df.iloc[0]
    total = float(row["total_users"])

    # 2回目継続率
    r2 = float(pd.to_numeric(row.get("retained_2", 0), errors="coerce") or 0)
    retention_2 = (r2 / total * 100) if total > 0 else 0.0

    # 12回目までのLTV
    cumulative = 0.0
    for i in range(1, MAX_RETENTION_MONTHS + 1):
        rev = float(pd.to_numeric(row.get(f"revenue_{i}", 0), errors="coerce") or 0)
        cumulative += rev
    ltv_12 = int(cumulative / total) if total > 0 else 0

    return {
        "total_new_users": int(total),
        "retention_2": round(retention_2, 1),
        "ltv_12": ltv_12,
    }
