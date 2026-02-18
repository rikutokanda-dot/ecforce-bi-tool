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


def build_retention_table(
    df: pd.DataFrame,
    data_cutoff_date: date | None = None,
    product_name: str | None = None,
) -> pd.DataFrame:
    """通常コホートの継続率テーブルを構築.

    data_cutoff_date と product_name が指定されている場合、
    各コホート月×回数の不完全データを「-」でマスクする。
    """
    if df.empty:
        return pd.DataFrame()

    # 各コホート月のマスク上限を計算
    month_max_count = {}
    if data_cutoff_date is not None and product_name is not None:
        for cm in df["cohort_month"]:
            if cm not in month_max_count:
                month_max_count[cm] = compute_month_end_mask(cm, product_name, data_cutoff_date)

    result = pd.DataFrame()
    result["コホート月"] = df["cohort_month"]
    result["新規顧客数"] = df["total_users"].astype(int)

    for i in range(1, MAX_RETENTION_MONTHS + 1):
        col = f"retained_{i}"
        if col not in df.columns:
            break
        total = df["total_users"].astype(float)
        retained = pd.to_numeric(df[col], errors="coerce").fillna(0)

        counts = retained.astype(int).tolist()
        rates = (retained / total * 100).round(1).tolist()

        # マスク適用: コホート月ごとに判定
        if month_max_count:
            for idx, cm in enumerate(df["cohort_month"]):
                max_n = month_max_count.get(cm, MAX_RETENTION_MONTHS)
                if i > max_n:
                    counts[idx] = "-"
                    rates[idx] = "-"

        result[f"{i}回目"] = counts
        result[f"{i}回目(%)"] = rates

    return result


def build_retention_rate_matrix(
    df: pd.DataFrame,
    data_cutoff_date: date | None = None,
    product_name: str | None = None,
) -> pd.DataFrame:
    """ヒートマップ用の継続率マトリクス (行=月, 列=回数, 値=%).

    data_cutoff_date と product_name が指定されている場合、
    不完全データのセルを None にする。
    """
    if df.empty:
        return pd.DataFrame()

    # 各コホート月のマスク上限を計算
    month_max_count = {}
    if data_cutoff_date is not None and product_name is not None:
        for cm in df["cohort_month"]:
            if cm not in month_max_count:
                month_max_count[cm] = compute_month_end_mask(cm, product_name, data_cutoff_date)

    matrix = pd.DataFrame(index=df["cohort_month"])
    total = df["total_users"].astype(float).values

    for i in range(1, MAX_RETENTION_MONTHS + 1):
        col = f"retained_{i}"
        if col not in df.columns:
            break
        retained = pd.to_numeric(df[col], errors="coerce").fillna(0).values
        rates = (retained / total * 100).round(1)

        # マスク適用
        if month_max_count:
            rates_list = rates.tolist()
            for idx, cm in enumerate(df["cohort_month"]):
                max_n = month_max_count.get(cm, MAX_RETENTION_MONTHS)
                if i > max_n:
                    rates_list[idx] = None
            matrix[f"{i}回目"] = rates_list
        else:
            matrix[f"{i}回目"] = rates

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


def build_aggregate_table(
    df: pd.DataFrame,
    drilldown_df: pd.DataFrame | None = None,
    product_name: str | None = None,
    data_cutoff_date: date | None = None,
) -> pd.DataFrame:
    """通算コホートの継続率・残存率・LTVテーブルを構築.

    drilldown_df, product_name, data_cutoff_date が全て指定された場合、
    各回数iについてデータが揃っているコホート月のみを合算する。
    """
    if df.empty:
        return pd.DataFrame()

    # --- マスク付き合算（ドリルダウンデータから月別にフィルタ） ---
    if drilldown_df is not None and product_name and data_cutoff_date:
        return _build_aggregate_table_filtered(
            drilldown_df, product_name, data_cutoff_date
        )

    # --- 従来の合算（通算SQL結果そのまま） ---
    row = df.iloc[0]
    total = float(row["total_users"])
    if total == 0:
        return pd.DataFrame()

    rows = []
    cumulative_revenue = 0.0
    prev_retained = total

    for i in range(1, MAX_RETENTION_MONTHS + 1):
        ret_col = f"retained_{i}"
        rev_col = f"revenue_{i}"

        retained = float(pd.to_numeric(row.get(ret_col, 0), errors="coerce") or 0)
        revenue = float(pd.to_numeric(row.get(rev_col, 0), errors="coerce") or 0)

        survival_rate = (retained / total * 100) if total > 0 else 0.0
        continuation_rate = (retained / prev_retained * 100) if prev_retained > 0 else 0.0
        avg_price = (revenue / retained) if retained > 0 else 0.0
        cumulative_revenue += revenue
        ltv = cumulative_revenue / total if total > 0 else 0.0

        rows.append({
            "定期回数": f"{i}回目",
            "継続人数": int(retained),
            "残存率(%)": round(survival_rate, 1),
            "継続率(%)": round(continuation_rate, 1),
            "平均単価(円)": int(round(avg_price)),
            "回次売上(円)": int(revenue),
            "累積売上(円)": int(cumulative_revenue),
            "LTV(円)": int(round(ltv)),
        })
        prev_retained = retained

    return pd.DataFrame(rows)


def _build_aggregate_table_filtered(
    drilldown_df: pd.DataFrame,
    product_name: str,
    data_cutoff_date: date,
) -> pd.DataFrame:
    """ドリルダウンデータから、月ごとにフィルタして通算テーブルを構築.

    各回数iについて、データが揃っている同一のコホート月集合から
    残存率・継続率(前回比)を計算する。
    """
    group = drilldown_df[drilldown_df["dimension_col"] == product_name]
    if group.empty:
        return pd.DataFrame()

    # 各コホート月のマスク上限
    month_max = {}
    for cm in group["cohort_month"].unique():
        month_max[cm] = compute_month_end_mask(cm, product_name, data_cutoff_date)

    rows = []
    cumulative_revenue = 0.0

    for i in range(1, MAX_RETENTION_MONTHS + 1):
        ret_col = f"retained_{i}"
        rev_col = f"revenue_{i}"
        if ret_col not in group.columns:
            break

        # i回目のデータが揃っている月のみ
        eligible = group[group["cohort_month"].map(lambda cm: month_max.get(cm, 0) >= i)]
        if eligible.empty:
            break

        eligible_total = eligible["total_users"].astype(float).sum()
        retained = float(pd.to_numeric(eligible[ret_col], errors="coerce").fillna(0).sum())

        if retained == 0 and i > 1:
            break

        # 同じ eligible 月の前回retained を取得して継続率(前回比)を計算
        if i == 1:
            prev_retained_same_months = eligible_total
        else:
            prev_ret_col = f"retained_{i - 1}"
            if prev_ret_col in eligible.columns:
                prev_retained_same_months = float(
                    pd.to_numeric(eligible[prev_ret_col], errors="coerce").fillna(0).sum()
                )
            else:
                prev_retained_same_months = eligible_total

        revenue = 0.0
        if rev_col in eligible.columns:
            revenue = float(pd.to_numeric(eligible[rev_col], errors="coerce").fillna(0).sum())

        # 残存率 = retained / eligible_total (同じ月集合の初回購入者ベース)
        survival_rate = (retained / eligible_total * 100) if eligible_total > 0 else 0.0
        # 継続率 = retained_i / retained_{i-1} (同じ月集合の前回ベース)
        continuation_rate = (
            (retained / prev_retained_same_months * 100) if prev_retained_same_months > 0 else 0.0
        )
        avg_price = (revenue / retained) if retained > 0 else 0.0
        cumulative_revenue += revenue
        ltv = cumulative_revenue / eligible_total if eligible_total > 0 else 0.0

        rows.append({
            "定期回数": f"{i}回目",
            "継続人数": int(retained),
            "残存率(%)": round(survival_rate, 1),
            "継続率(%)": round(continuation_rate, 1),
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
    data_cutoff_date: date | None = None,
) -> pd.DataFrame:
    """商品名ごとの転置サマリーテーブルを構築.

    各回数iについて、データが揃っているコホート月のみを合算する。
    data_cutoff_date が指定されている場合、コホート月末購入者が
    i回目の出荷予定日を迎えているかで判定する。

    Returns:
        行=指標(継続率/残存率/残存数), 列=1回目〜N回目
    """
    import calendar

    group = df[df["dimension_col"] == product_name]
    if group.empty:
        return pd.DataFrame()

    # 各コホート月ごとに「何回目までデータが揃っているか」を計算
    month_max_count = {}
    if data_cutoff_date is not None:
        for _, row in group.iterrows():
            cm = row["cohort_month"]
            max_n = compute_month_end_mask(cm, product_name, data_cutoff_date)
            month_max_count[cm] = max_n
    else:
        # cutoffなし → 全月全回数OK
        for _, row in group.iterrows():
            month_max_count[row["cohort_month"]] = MAX_RETENTION_MONTHS

    continuation_row = {"指標": "継続率"}
    survival_row = {"指標": "残存率"}
    count_row = {"指標": "残存数"}

    for i in range(1, MAX_RETENTION_MONTHS + 1):
        col = f"retained_{i}"
        if col not in group.columns:
            break

        # i回目のデータが揃っている月だけをフィルタ
        eligible_rows = group[
            group["cohort_month"].map(lambda cm: month_max_count.get(cm, 0) >= i)
        ]

        if eligible_rows.empty:
            break

        eligible_total = eligible_rows["total_users"].astype(float).sum()
        retained = float(pd.to_numeric(eligible_rows[col], errors="coerce").fillna(0).sum())

        if retained == 0 and i > 1:
            break
        if eligible_total == 0:
            break

        survival_rate = round(retained / eligible_total * 100, 1)

        # 継続率(前回比): 同じeligible月のi-1回目retainedをベースにする
        if i == 1:
            continuation_rate = survival_rate
        else:
            prev_col = f"retained_{i - 1}"
            if prev_col in eligible_rows.columns:
                prev_retained_same = float(
                    pd.to_numeric(eligible_rows[prev_col], errors="coerce").fillna(0).sum()
                )
            else:
                prev_retained_same = eligible_total
            continuation_rate = round(retained / prev_retained_same * 100, 1) if prev_retained_same > 0 else 0.0

        label = f"{i}回目"
        continuation_row[label] = f"{continuation_rate}%"
        survival_row[label] = f"{survival_rate}%"
        count_row[label] = f"{int(retained)}件"

    if len(continuation_row) <= 1:
        return pd.DataFrame()

    return pd.DataFrame([continuation_row, survival_row, count_row])


def build_dimension_summary_table(
    df: pd.DataFrame,
    dimension_value: str,
) -> pd.DataFrame:
    """広告グループ・商品カテゴリなど、任意のドリルダウン軸の通算サマリーテーブルを構築.

    商品名別サマリーと同じ形式 (行=指標, 列=N回目) を返す。
    全コホート月を合算して通算の継続率/残存率/残存数を出す。
    """
    group = df[df["dimension_col"] == dimension_value]
    if group.empty:
        return pd.DataFrame()

    total_users = group["total_users"].astype(float).sum()
    if total_users == 0:
        return pd.DataFrame()

    continuation_row = {"指標": "継続率"}
    survival_row = {"指標": "残存率"}
    count_row = {"指標": "残存数"}

    for i in range(1, MAX_RETENTION_MONTHS + 1):
        col = f"retained_{i}"
        if col not in group.columns:
            break
        retained = float(pd.to_numeric(group[col], errors="coerce").fillna(0).sum())
        if retained == 0 and i > 1:
            break

        survival_rate = round(retained / total_users * 100, 1) if total_users > 0 else 0.0

        # 継続率(前回比): i-1回目のretainedをベースにする
        if i == 1:
            continuation_rate = round(retained / total_users * 100, 1) if total_users > 0 else 0.0
        else:
            prev_col = f"retained_{i - 1}"
            if prev_col in group.columns:
                prev_retained = float(pd.to_numeric(group[prev_col], errors="coerce").fillna(0).sum())
            else:
                prev_retained = total_users
            continuation_rate = round(retained / prev_retained * 100, 1) if prev_retained > 0 else 0.0

        label = f"{i}回目"
        continuation_row[label] = f"{continuation_rate}%"
        survival_row[label] = f"{survival_rate}%"
        count_row[label] = f"{int(retained)}件"

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


def compute_month_end_mask(
    cohort_month: str,
    product_name: str,
    data_cutoff_date: date,
) -> int:
    """コホート月末日基準でデータが揃っている最大回数を計算.

    コホート月の最終日(例: 12/31)に購入した顧客が
    N回目の出荷予定日を迎えているかを判定する。
    月末購入者が到達していれば、そのコホート月の全員分のデータが揃っている。

    1回目 = 購入日(月末日) に出荷 → コホート月内
    2回目 = 購入日 + cycle1 日後
    3回目 = 2回目 + cycle2 日後
    ...

    Returns:
        データが完全に揃っている最大の回数N。0ならデータなし。
    """
    import calendar

    cycle1, cycle2 = get_product_cycle(product_name)

    parts = cohort_month.split("-")
    if len(parts) != 2:
        return 0
    year, month = int(parts[0]), int(parts[1])

    # コホート月の最終日 (例: 12月 → 12/31)
    last_day = calendar.monthrange(year, month)[1]
    purchase_date = date(year, month, last_day)

    # cutoff日をその月の末日に切り上げ
    # (例: cutoff=12/30 → 12/31とみなす。同月内なら誤差は無視)
    cutoff_last_day = calendar.monthrange(
        data_cutoff_date.year, data_cutoff_date.month
    )[1]
    effective_cutoff = date(
        data_cutoff_date.year, data_cutoff_date.month, cutoff_last_day
    )

    # 1回目 = 購入日に出荷（コホート月内）
    if purchase_date <= effective_cutoff:
        max_count = 1
    else:
        return 0

    # 2回目 = 購入日 + cycle1 日後
    ship_date = purchase_date + timedelta(days=cycle1)
    if ship_date <= effective_cutoff:
        max_count = 2
    else:
        return max_count

    # 3回目以降
    for i in range(3, MAX_RETENTION_MONTHS + 1):
        ship_date = ship_date + timedelta(days=cycle2)
        if ship_date <= effective_cutoff:
            max_count = i
        else:
            break

    return max_count


def apply_completeness_mask_to_summary(
    summary_df: pd.DataFrame,
    cohort_months: list[str],
    product_name: str,
    data_cutoff_date: date,
    drilldown_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """サマリーテーブルに発送待ちマスクを適用.

    最新コホート月の月末購入者基準で、全員分のデータが揃っている
    回数までを表示し、それ以降をマスクする。
    """
    if summary_df.empty or not cohort_months:
        return summary_df

    result = summary_df.copy()

    latest_month = max(cohort_months)
    max_complete = compute_month_end_mask(latest_month, product_name, data_cutoff_date)

    if max_complete > 0:
        for i in range(max_complete + 1, MAX_RETENTION_MONTHS + 1):
            label = f"{i}回目"
            if label in result.columns:
                result[label] = "-"
    else:
        # データが全く揃っていない → 全カラムをマスク
        for i in range(1, MAX_RETENTION_MONTHS + 1):
            label = f"{i}回目"
            if label in result.columns:
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
    filtered_agg_table: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """1年LTVテーブルを構築（予測値含む）.

    周期 (cycle1, cycle2) から1年間に何回注文が入るかを計算し、
    実績データが足りない回数は予測で補完する。
    予測のデフォルト継続率・平均単価は実績最終行の値を使用。

    LTV計算: 残存率チェーンに基づく
      survival_1 = 残存率(1回目)  (= 継続率(1回目) = retained_1/total)
      survival_i = survival_{i-1} * 継続率(i回目) / 100
      LTV = Σ (survival_i / 100 * avg_price_i)
    """
    if agg_df.empty:
        return pd.DataFrame()

    max_orders = compute_max_orders_in_period(cycle1, cycle2)
    row = agg_df.iloc[0]
    total = float(row["total_users"])
    if total == 0:
        return pd.DataFrame()

    # --- マスク付きテーブルから実績の継続率・平均単価を取得 ---
    filtered_rates: dict[int, float] = {}      # 回数 → 継続率(前回比)
    filtered_prices: dict[int, float] = {}     # 回数 → 平均単価
    filtered_survivals: dict[int, float] = {}  # 回数 → 残存率
    filtered_retained: dict[int, int] = {}     # 回数 → 継続人数
    max_actual_order = 0
    last_actual_rate = 85.0
    last_actual_price = 0.0

    if filtered_agg_table is not None and not filtered_agg_table.empty:
        for _, frow in filtered_agg_table.iterrows():
            order_num = int(frow["定期回数"].replace("回目", ""))
            filtered_rates[order_num] = float(frow["継続率(%)"])
            filtered_prices[order_num] = float(frow["平均単価(円)"])
            filtered_survivals[order_num] = float(frow["残存率(%)"])
            filtered_retained[order_num] = int(frow["継続人数"])
            if order_num > max_actual_order:
                max_actual_order = order_num
        # 実績最終行のデフォルト予測値
        last_actual_rate = filtered_rates.get(max_actual_order, 85.0)
        if last_actual_rate <= 0:
            last_actual_rate = 85.0
        last_actual_price = filtered_prices.get(max_actual_order, 0.0)

    # --- 1年LTVを残存率チェーンで構築 ---
    rows = []
    cumulative_ltv = 0.0
    prev_survival = 100.0  # %

    for i in range(1, max_orders + 1):
        is_projected = i > max_actual_order and max_actual_order > 0

        # 継続率(前回比)を決定
        if not is_projected and i in filtered_rates:
            # 実績値
            continuation_rate = filtered_rates[i]
            avg_price = filtered_prices[i]
        elif not is_projected and max_actual_order == 0:
            # フィルタなし → 従来ロジック（raw agg_df）
            ret_col = f"retained_{i}"
            rev_col = f"revenue_{i}"
            actual_retained = float(pd.to_numeric(row.get(ret_col, 0), errors="coerce") or 0)
            actual_revenue = float(pd.to_numeric(row.get(rev_col, 0), errors="coerce") or 0)

            if actual_retained > 0 or i == 1:
                continuation_rate = (actual_retained / (total if i == 1 else
                    float(pd.to_numeric(row.get(f"retained_{i-1}", 0), errors="coerce") or total))
                    * 100)
                avg_price = actual_revenue / actual_retained if actual_retained > 0 else 0
                if continuation_rate > 0:
                    last_actual_rate = continuation_rate
                if avg_price > 0:
                    last_actual_price = avg_price
            else:
                is_projected = True
                continuation_rate = projected_rates[i] if projected_rates and i in projected_rates else last_actual_rate
                avg_price = projected_amounts[i] if projected_amounts and i in projected_amounts else last_actual_price
        else:
            # 予測値
            if projected_rates and i in projected_rates:
                continuation_rate = projected_rates[i]
            else:
                continuation_rate = last_actual_rate
            if projected_amounts and i in projected_amounts:
                avg_price = projected_amounts[i]
            else:
                avg_price = last_actual_price

        # 残存率チェーン
        if i == 1:
            survival_rate = continuation_rate  # 1回目残存率 = 継続率
        else:
            survival_rate = prev_survival * continuation_rate / 100

        # 人数 (表示用)
        retained_count = int(total * survival_rate / 100)

        # LTV加算
        cumulative_ltv += survival_rate / 100 * avg_price

        rows.append({
            "定期回数": f"{i}回目",
            "継続人数": retained_count,
            "残存率(%)": round(survival_rate, 1),
            "継続率(%)": round(continuation_rate, 1),
            "平均単価(円)": int(round(avg_price)),
            "LTV(円)": int(round(cumulative_ltv)),
            "予測": is_projected,
        })
        prev_survival = survival_rate

    return pd.DataFrame(rows)
