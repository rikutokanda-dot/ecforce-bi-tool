"""コホートデータの変換・加工.

BigQueryから取得した生データを、表示用のピボットテーブルや
継続率テーブルに変換する。発送日目安の計算も含む。
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from src.config_loader import get_product_cycle
from src.constants import LTV_PERIOD_DAYS, MAX_RETENTION_MONTHS


# =====================================================================
# 月別コホート
# =====================================================================


def build_retention_table(df: pd.DataFrame) -> pd.DataFrame:
    """通常コホートの継続率テーブルを構築."""
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
    """ヒートマップ用の継続率マトリクス (行=月, 列=回数, 値=%)."""
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
    """ドリルダウン結果をグループごとの継続率テーブルに変換."""
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
    product_name: str | None = None,
) -> pd.DataFrame:
    """発送日目安テーブルを構築."""
    if not cohort_months:
        return pd.DataFrame()

    cycle1, cycle2 = get_product_cycle(product_name or "")

    rows = []
    for month_str in cohort_months:
        parts = month_str.split("-")
        if len(parts) != 2:
            continue
        year, month = int(parts[0]), int(parts[1])

        if month == 12:
            base_date = date(year + 1, 1, 1)
        else:
            base_date = date(year, month + 1, 1)

        row = {"コホート月": month_str}
        calc_date = base_date

        row["1回目"] = calc_date.strftime("%Y/%m/%d")
        calc_date = calc_date + timedelta(days=cycle1)
        row["2回目"] = calc_date.strftime("%Y/%m/%d")
        for i in range(3, MAX_RETENTION_MONTHS + 1):
            calc_date = calc_date + timedelta(days=cycle2)
            row[f"{i}回目"] = calc_date.strftime("%Y/%m/%d")

        rows.append(row)

    return pd.DataFrame(rows)


def compute_summary_metrics(df: pd.DataFrame) -> dict:
    """KPIサマリー指標を計算."""
    if df.empty:
        return {
            "total_new_users": 0,
            "avg_retention_2": 0.0,
            "latest_12m_retention": 0.0,
        }

    total = df["total_users"].astype(float)
    total_new = int(total.sum())

    if "retained_2" in df.columns:
        r2 = pd.to_numeric(df["retained_2"], errors="coerce").fillna(0)
        avg_r2 = (r2 / total * 100).mean()
    else:
        avg_r2 = 0.0

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
    """通算コホートの継続率・残存率・LTVテーブルを構築."""
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

        retention_rate = (retained / total * 100) if total > 0 else 0.0
        avg_price = (revenue / retained) if retained > 0 else 0.0
        cumulative_revenue += revenue
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
        return {"total_new_users": 0, "retention_2": 0.0, "ltv_12": 0}

    row = df.iloc[0]
    total = float(row["total_users"])

    r2 = float(pd.to_numeric(row.get("retained_2", 0), errors="coerce") or 0)
    retention_2 = (r2 / total * 100) if total > 0 else 0.0

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


# =====================================================================
# 商品名別サマリーテーブル (要件1: デフォルト画面)
# =====================================================================


def build_product_summary_table(
    df: pd.DataFrame,
    product_name: str,
) -> pd.DataFrame:
    """商品名ごとの転置サマリーテーブルを構築.

    Returns:
        行=指標(継続率/残存率/残存数), 列=1回目〜N回目
    """
    group = df[df["dimension_col"] == product_name]
    if group.empty:
        return pd.DataFrame()

    total_users = group["total_users"].astype(float).sum()
    if total_users == 0:
        return pd.DataFrame()

    continuation_row = {"指標": "継続率"}
    survival_row = {"指標": "残存率"}
    count_row = {"指標": "残存数"}

    prev_retained = total_users

    for i in range(1, MAX_RETENTION_MONTHS + 1):
        col = f"retained_{i}"
        if col not in group.columns:
            break
        retained = float(pd.to_numeric(group[col], errors="coerce").fillna(0).sum())
        if retained == 0 and i > 1:
            break

        survival_rate = round(retained / total_users * 100, 1) if total_users > 0 else 0.0
        continuation_rate = round(retained / prev_retained * 100, 1) if prev_retained > 0 else 0.0

        label = f"{i}回目"
        continuation_row[label] = f"{continuation_rate}%"
        survival_row[label] = f"{survival_rate}%"
        count_row[label] = f"{int(retained)}件"

        prev_retained = retained

    return pd.DataFrame([continuation_row, survival_row, count_row])


# =====================================================================
# 発送待ちマスク (要件2: 半端なデータ除外)
# =====================================================================


def compute_data_completeness_mask(
    cohort_months: list[str],
    product_name: str,
    data_cutoff_date: date,
) -> dict[str, list[bool]]:
    """各コホート月×回数のデータ完全性マスクを計算.

    Returns:
        {cohort_month: [True/False per subscription count]}
        True = データが存在すべき, False = 発送待ち
    """
    cycle1, cycle2 = get_product_cycle(product_name)
    mask = {}

    for month_str in cohort_months:
        parts = month_str.split("-")
        if len(parts) != 2:
            continue
        year, month = int(parts[0]), int(parts[1])

        if month == 12:
            base_date = date(year + 1, 1, 1)
        else:
            base_date = date(year, month + 1, 1)

        completeness = []
        calc_date = base_date

        # 1回目
        completeness.append(calc_date <= data_cutoff_date)
        # 2回目
        calc_date = calc_date + timedelta(days=cycle1)
        completeness.append(calc_date <= data_cutoff_date)
        # 3回目以降
        for _ in range(3, MAX_RETENTION_MONTHS + 1):
            calc_date = calc_date + timedelta(days=cycle2)
            completeness.append(calc_date <= data_cutoff_date)

        mask[month_str] = completeness

    return mask


def apply_completeness_mask_to_summary(
    summary_df: pd.DataFrame,
    cohort_months: list[str],
    product_name: str,
    data_cutoff_date: date,
) -> pd.DataFrame:
    """サマリーテーブルに発送待ちマスクを適用.

    全コホート月を統合して、最新コホート月の発送待ちで判定。
    """
    if summary_df.empty or not cohort_months:
        return summary_df

    # 最新コホート月の発送待ちで判定
    latest_month = max(cohort_months)
    mask = compute_data_completeness_mask([latest_month], product_name, data_cutoff_date)
    completeness = mask.get(latest_month, [])

    result = summary_df.copy()
    for i, complete in enumerate(completeness):
        label = f"{i + 1}回目"
        if label in result.columns and not complete:
            result[label] = "-"

    return result


# =====================================================================
# アップセル率 (要件4)
# =====================================================================


def compute_upsell_rate(upsell_df: pd.DataFrame) -> float:
    """アップセル率を計算."""
    if upsell_df.empty:
        return 0.0
    row = upsell_df.iloc[0]
    from_count = float(row.get("from_count", 0))
    upsell_count = float(row.get("upsell_count", 0))
    return round(upsell_count / from_count * 100, 1) if from_count > 0 else 0.0


# =====================================================================
# 1年LTV (要件5)
# =====================================================================


def compute_max_orders_in_period(
    cycle1: int, cycle2: int, period_days: int = LTV_PERIOD_DAYS
) -> int:
    """指定期間内に入る注文回数を計算."""
    if period_days <= 0:
        return 0
    days_used = 0
    count = 1  # 1回目 (day 0)
    days_used += cycle1
    if days_used > period_days:
        return count
    count += 1  # 2回目
    while True:
        days_used += cycle2
        if days_used > period_days:
            break
        count += 1
    return count


def build_1year_ltv_table(
    agg_df: pd.DataFrame,
    cycle1: int,
    cycle2: int,
    projected_rates: dict[int, float] | None = None,
    projected_amounts: dict[int, float] | None = None,
) -> pd.DataFrame:
    """1年LTVテーブルを構築（予測値含む）.

    Args:
        agg_df: aggregate query result (1 row)
        cycle1, cycle2: product shipping cycle
        projected_rates: {回数: 予測継続率(%)} 編集可能な値
        projected_amounts: {回数: 予測平均単価} 編集可能な値

    Returns:
        DataFrame with columns: 回数, 継続人数, 残存率(%), 継続率(%), 平均単価, LTV, 予測
    """
    if agg_df.empty:
        return pd.DataFrame()

    max_orders = compute_max_orders_in_period(cycle1, cycle2)
    row = agg_df.iloc[0]
    total = float(row["total_users"])
    if total == 0:
        return pd.DataFrame()

    rows = []
    cumulative_ltv = 0.0
    prev_retained = total
    last_known_rate = 85.0
    last_known_price = 0.0

    for i in range(1, max_orders + 1):
        ret_col = f"retained_{i}"
        rev_col = f"revenue_{i}"

        actual_retained = float(pd.to_numeric(row.get(ret_col, 0), errors="coerce") or 0)
        actual_revenue = float(pd.to_numeric(row.get(rev_col, 0), errors="coerce") or 0)

        has_actual = actual_retained > 0 or i == 1
        is_projected = not has_actual and i > 1

        if has_actual:
            retained = actual_retained
            revenue = actual_revenue
            avg_price = revenue / retained if retained > 0 else 0
            continuation_rate = (retained / prev_retained * 100) if prev_retained > 0 else 0
            last_known_rate = continuation_rate
            if avg_price > 0:
                last_known_price = avg_price
        else:
            # 予測値を使用
            if projected_rates and i in projected_rates:
                continuation_rate = projected_rates[i]
            else:
                continuation_rate = last_known_rate

            retained = prev_retained * continuation_rate / 100

            if projected_amounts and i in projected_amounts:
                avg_price = projected_amounts[i]
            else:
                avg_price = last_known_price

            revenue = retained * avg_price

        survival_rate = (retained / total * 100) if total > 0 else 0
        cumulative_ltv += revenue / total if total > 0 else 0

        rows.append({
            "定期回数": f"{i}回目",
            "継続人数": int(retained),
            "残存率(%)": round(survival_rate, 1),
            "継続率(%)": round(continuation_rate, 1),
            "平均単価(円)": int(round(avg_price)),
            "LTV(円)": int(round(cumulative_ltv)),
            "予測": is_projected,
        })
        prev_retained = retained

    return pd.DataFrame(rows)
