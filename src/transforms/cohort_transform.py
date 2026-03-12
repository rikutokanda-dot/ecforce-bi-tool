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
        retained = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # 残存率の分母: 1回目=total_users, N≥2=surv_denom_N
        if i == 1:
            surv_denom = df["total_users"].astype(float)
        else:
            sd_col = f"surv_denom_{i}"
            if sd_col in df.columns:
                surv_denom = pd.to_numeric(df[sd_col], errors="coerce").fillna(0)
            else:
                surv_denom = df["total_users"].astype(float)

        counts = retained.astype(int).tolist()
        rates = []
        for idx in range(len(retained)):
            if surv_denom.iloc[idx] > 0:
                rates.append(round(float(retained.iloc[idx] / surv_denom.iloc[idx] * 100), 1))
            else:
                rates.append(0.0)

        # 未定人数 (残存): 時間適格でない人数
        # 1回目は未定なし、2回目以降: total_users - surv_denom_N
        pending = [0] * len(counts)
        if i >= 2:
            sd_col = f"surv_denom_{i}"
            if sd_col in df.columns:
                total_f = df["total_users"].astype(float)
                sd_vals = pd.to_numeric(df[sd_col], errors="coerce").fillna(0)
                pending = (total_f - sd_vals).clip(lower=0).astype(int).tolist()

        # マスク適用: コホート月ごとに判定
        if month_max_count:
            for idx, cm in enumerate(df["cohort_month"]):
                max_n = month_max_count.get(cm, MAX_RETENTION_MONTHS)
                if i > max_n:
                    counts[idx] = "-"
                    rates[idx] = "-"
                    pending[idx] = "-"

        result[f"{i}回目"] = counts
        result[f"{i}回目(%)"] = rates
        if i >= 2:
            result[f"{i}回目(未定)"] = pending

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

        # 残存率の分母: 1回目=total_users, N≥2=surv_denom_N
        if i == 1:
            surv_denom = total.copy()
        else:
            sd_col = f"surv_denom_{i}"
            if sd_col in df.columns:
                surv_denom = pd.to_numeric(df[sd_col], errors="coerce").fillna(0).values
            else:
                surv_denom = total.copy()

        rates = []
        for idx in range(len(retained)):
            if surv_denom[idx] > 0:
                rates.append(round(retained[idx] / surv_denom[idx] * 100, 1))
            else:
                rates.append(0.0)

        # マスク適用
        if month_max_count:
            for idx, cm in enumerate(df["cohort_month"]):
                max_n = month_max_count.get(cm, MAX_RETENTION_MONTHS)
                if i > max_n:
                    rates[idx] = None
        matrix[f"{i}回目"] = rates

    matrix.index.name = "コホート月"
    return matrix


def build_continuation_rate_matrix(
    df: pd.DataFrame,
    data_cutoff_date: date | None = None,
    product_name: str | None = None,
) -> pd.DataFrame:
    """ヒートマップ用の継続率マトリクス (行=月, 列=回数, 値=%).

    継続率 = retained_N / retained_{N-1} * 100
    1回目は retained_1 / total_users * 100。

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
        # 継続率の分子: 1回目=retained_1, N≥2=cont_num_N
        if i == 1:
            num_col = f"retained_{i}"
        else:
            num_col = f"cont_num_{i}" if f"cont_num_{i}" in df.columns else f"retained_{i}"
        if num_col not in df.columns:
            break
        numerator = pd.to_numeric(df[num_col], errors="coerce").fillna(0).values

        # 継続率の分母を決定
        if i == 1:
            denom = total.copy()
        else:
            denom_col = f"denom_{i}"
            if denom_col in df.columns:
                denom = pd.to_numeric(df[denom_col], errors="coerce").fillna(0).values
            else:
                prev_col = f"retained_{i - 1}"
                denom = pd.to_numeric(df[prev_col], errors="coerce").fillna(0).values if prev_col in df.columns else total.copy()

        # 継続率: cont_num_i / denom_i * 100
        rates = []
        for idx in range(len(numerator)):
            if denom[idx] > 0:
                rates.append(round(numerator[idx] / denom[idx] * 100, 1))
            else:
                rates.append(0.0)

        # マスク適用
        if month_max_count:
            for idx, cm in enumerate(df["cohort_month"]):
                max_n = month_max_count.get(cm, MAX_RETENTION_MONTHS)
                if i > max_n:
                    rates[idx] = None
        matrix[f"{i}回目"] = rates

    matrix.index.name = "コホート月"
    return matrix


def build_drilldown_continuation_matrices(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """ドリルダウン結果をグループごとの継続率マトリクスに変換."""
    if df.empty:
        return {}

    result = {}
    for group_name, group_df in df.groupby("dimension_col"):
        group_df = group_df.reset_index(drop=True)
        matrix = build_continuation_rate_matrix(group_df)
        result[str(group_name)] = matrix

    return result


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

    for i in range(1, MAX_RETENTION_MONTHS + 1):
        ret_col = f"retained_{i}"
        rev_col = f"revenue_{i}"

        retained = float(pd.to_numeric(row.get(ret_col, 0), errors="coerce") or 0)
        revenue = float(pd.to_numeric(row.get(rev_col, 0), errors="coerce") or 0)

        # 残存率の分母: 1回目=total, N≥2=surv_denom_N
        if i == 1:
            surv_denom = total
        else:
            sd_col = f"surv_denom_{i}"
            surv_denom = float(pd.to_numeric(row.get(sd_col, 0), errors="coerce") or 0)
            if surv_denom == 0:
                surv_denom = total

        # 継続率の分子/分母: 1回目=retained_1/total, N≥2=cont_num_N/denom_N
        if i == 1:
            cont_num = retained
            denom = total
        else:
            cn_col = f"cont_num_{i}"
            cont_num = float(pd.to_numeric(row.get(cn_col, 0), errors="coerce") or 0)
            denom_col = f"denom_{i}"
            denom = float(pd.to_numeric(row.get(denom_col, 0), errors="coerce") or 0)
            if denom == 0:
                denom = total

        survival_rate = (retained / surv_denom * 100) if surv_denom > 0 else 0.0
        continuation_rate = (cont_num / denom * 100) if denom > 0 else 0.0
        avg_price = (revenue / retained) if retained > 0 else 0.0
        cumulative_revenue += revenue
        ltv = cumulative_revenue / total if total > 0 else 0.0

        rows.append({
            "定期回数": f"{i}回目",
            "継続人数": int(retained),
            "残存分母": int(surv_denom),
            "継続分母": int(denom),
            "残存率(%)": round(survival_rate, 1),
            "継続率(%)": round(continuation_rate, 1),
            "平均単価(円)": int(round(avg_price)),
            "回次売上(円)": int(revenue),
            "累積売上(円)": int(cumulative_revenue),
            "LTV(円)": int(round(ltv)),
        })

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

        # 残存率の分母: 1回目=eligible_total, N≥2=surv_denom_N
        if i == 1:
            surv_denom = eligible_total
        else:
            sd_col = f"surv_denom_{i}"
            if sd_col in eligible.columns:
                surv_denom = float(pd.to_numeric(eligible[sd_col], errors="coerce").fillna(0).sum())
            else:
                surv_denom = eligible_total

        # 継続率の分子/分母: 1回目=retained/eligible_total, N≥2=cont_num_N/denom_N
        if i == 1:
            cont_num = retained
            denom = eligible_total
        else:
            cn_col = f"cont_num_{i}"
            if cn_col in eligible.columns:
                cont_num = float(pd.to_numeric(eligible[cn_col], errors="coerce").fillna(0).sum())
            else:
                cont_num = retained
            denom_col = f"denom_{i}"
            if denom_col in eligible.columns:
                denom = float(pd.to_numeric(eligible[denom_col], errors="coerce").fillna(0).sum())
            else:
                denom = eligible_total

        revenue = 0.0
        if rev_col in eligible.columns:
            revenue = float(pd.to_numeric(eligible[rev_col], errors="coerce").fillna(0).sum())

        survival_rate = (retained / surv_denom * 100) if surv_denom > 0 else 0.0
        continuation_rate = (cont_num / denom * 100) if denom > 0 else 0.0
        avg_price = (revenue / retained) if retained > 0 else 0.0
        cumulative_revenue += revenue
        ltv = cumulative_revenue / eligible_total if eligible_total > 0 else 0.0

        rows.append({
            "定期回数": f"{i}回目",
            "継続人数": int(retained),
            "残存分母": int(surv_denom),
            "継続分母": int(denom),
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

        # i回目のデータが揃っている月のみ
        eligible = group[
            group["cohort_month"].map(lambda cm: month_max_count.get(cm, 0) >= i)
        ]
        if eligible.empty:
            break

        eligible_total = eligible["total_users"].astype(float).sum()
        retained = float(pd.to_numeric(eligible[col], errors="coerce").fillna(0).sum())

        if retained == 0 and i > 1:
            break

        # 残存率: 1回目=retained/eligible_total, N≥2=retained/surv_denom_N
        if i == 1:
            surv_denom = eligible_total
        else:
            sd_col = f"surv_denom_{i}"
            if sd_col in eligible.columns:
                surv_denom = float(pd.to_numeric(eligible[sd_col], errors="coerce").fillna(0).sum())
            else:
                surv_denom = eligible_total
        survival_rate = round(retained / surv_denom * 100, 1) if surv_denom > 0 else 0.0

        # 継続率: 1回目=retained/eligible_total, N≥2=cont_num_N/denom_N
        if i == 1:
            cont_num = retained
            denom = eligible_total
        else:
            cn_col = f"cont_num_{i}"
            if cn_col in eligible.columns:
                cont_num = float(pd.to_numeric(eligible[cn_col], errors="coerce").fillna(0).sum())
            else:
                cont_num = retained
            denom_col = f"denom_{i}"
            if denom_col in eligible.columns:
                denom = float(pd.to_numeric(eligible[denom_col], errors="coerce").fillna(0).sum())
            else:
                denom = eligible_total
        continuation_rate = round(cont_num / denom * 100, 1) if denom > 0 else 0.0

        label = f"{i}回目"
        continuation_row[label] = f"{continuation_rate}%\n({int(cont_num):,}/{int(denom):,})"
        survival_row[label] = f"{survival_rate}%\n({int(retained):,}/{int(surv_denom):,})"
        count_row[label] = f"{int(retained):,}件"

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

        # 残存率: 1回目=retained/total_users, N≥2=retained/surv_denom_N
        if i == 1:
            surv_denom = total_users
        else:
            sd_col = f"surv_denom_{i}"
            if sd_col in group.columns:
                surv_denom = float(pd.to_numeric(group[sd_col], errors="coerce").fillna(0).sum())
            else:
                surv_denom = total_users
        survival_rate = round(retained / surv_denom * 100, 1) if surv_denom > 0 else 0.0

        # 継続率: 1回目=retained/total_users, N≥2=cont_num_N/denom_N
        if i == 1:
            cont_num = retained
            denom = total_users
        else:
            cn_col = f"cont_num_{i}"
            if cn_col in group.columns:
                cont_num = float(pd.to_numeric(group[cn_col], errors="coerce").fillna(0).sum())
            else:
                cont_num = retained
            denom_col = f"denom_{i}"
            if denom_col in group.columns:
                denom = float(pd.to_numeric(group[denom_col], errors="coerce").fillna(0).sum())
            else:
                denom = total_users
        continuation_rate = round(cont_num / denom * 100, 1) if denom > 0 else 0.0

        label = f"{i}回目"
        continuation_row[label] = f"{continuation_rate}%\n({int(cont_num):,}/{int(denom):,})"
        survival_row[label] = f"{survival_rate}%\n({int(retained):,}/{int(surv_denom):,})"
        count_row[label] = f"{int(retained):,}件"

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
    """コホート月のデータが揃っている最大回数を計算.

    各N回目について「月初日の顧客がN回目データを持っているか」で判定する。
    月初の顧客がN回目到達していれば、少なくとも一部の顧客のデータが揃っている。

    eligible_before = cutoff - PROCESSING_BUFFER_DAYS (10日)
    N回目のeligible閾値 = eligible_before - Σcycles(1..N-1)
    表示条件: month_start <= eligible閾値

    例 (cutoff=2/28, buffer=10, cycle=30):
      eligible_before = 2/18
      1回目: month_start <= 2/18 → 2月(2/1 ≤ 2/18 ✓)
      2回目: month_start <= 2/18-30 = 1/19 → 1月(1/1 ≤ 1/19 ✓), 2月(2/1 > 1/19 ✗)
      3回目: month_start <= 2/18-60 = 12/20 → 12月(12/1 ≤ 12/20 ✓), 1月(1/1 > 12/20 ✗)

    Returns:
        データが（少なくとも一部）揃っている最大の回数N。0ならデータなし。
    """
    from src.constants import PROCESSING_BUFFER_DAYS

    cycle1, cycle2 = get_product_cycle(product_name)

    parts = cohort_month.split("-")
    if len(parts) != 2:
        return 0
    year, month = int(parts[0]), int(parts[1])

    # pd.Timestamp / datetime → datetime.date に統一（比較エラー防止）
    if hasattr(data_cutoff_date, "date") and callable(data_cutoff_date.date):
        effective_cutoff = data_cutoff_date.date()
    else:
        effective_cutoff = data_cutoff_date

    # eligible_before: SQL側でこの日以前の作成日のみを対象にしている
    eligible_before = effective_cutoff - timedelta(days=PROCESSING_BUFFER_DAYS)

    month_start = date(year, month, 1)

    # 1回目: month_start <= eligible_before
    if month_start > eligible_before:
        return 0
    max_count = 1

    # 2回目: month_start <= eligible_before - cycle1
    threshold = eligible_before - timedelta(days=cycle1)
    if month_start <= threshold:
        max_count = 2
    else:
        return max_count

    # 3回目以降: eligible_before - (cycle1 + cycle2*(N-2))
    cumulative_cycle = cycle1
    for i in range(3, MAX_RETENTION_MONTHS + 1):
        cumulative_cycle += cycle2
        threshold = eligible_before - timedelta(days=cumulative_cycle)
        if month_start <= threshold:
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
            rev_col = f"revenue_{i}"
            actual_revenue = float(pd.to_numeric(row.get(rev_col, 0), errors="coerce") or 0)

            # 継続率の分子: 1回目=retained_1, N≥2=cont_num_N
            if i == 1:
                cn_col = f"retained_{i}"
            else:
                cn_col = f"cont_num_{i}" if f"cont_num_{i}" in row.index else f"retained_{i}"
            actual_cont_num = float(pd.to_numeric(row.get(cn_col, 0), errors="coerce") or 0)
            actual_retained = float(pd.to_numeric(row.get(f"retained_{i}", 0), errors="coerce") or 0)

            if actual_retained > 0 or i == 1:
                if i == 1:
                    denom_val = total
                else:
                    denom_col = f"denom_{i}"
                    denom_val = float(pd.to_numeric(row.get(denom_col, 0), errors="coerce") or 0)
                    if denom_val == 0:
                        denom_val = total
                continuation_rate = (actual_cont_num / denom_val * 100)
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
