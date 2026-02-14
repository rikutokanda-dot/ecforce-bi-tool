"""コホート分析用SQLクエリビルダー.

GASコードのビジネスロジックを忠実に移植:
- cohort_base: 受注_定期回数=1 の顧客を月別に集計
- 論理連番2(再処理)を持つ顧客は除外
- 論理連番1またはNULL(失敗含む)のデータがある顧客を対象
- retained_N: shipped & completed の成功数のみカウント
"""

from __future__ import annotations

from src.constants import Col, LogicalSeq, MAX_RETENTION_MONTHS, Status

# ---------------------------------------------------------------------------
# 通算コホート分析SQL (全月を合算して回数別の継続率・残存率・平均売上を算出)
# ---------------------------------------------------------------------------
from src.queries.common import build_filter_clause, get_table_ref


def build_cohort_sql(
    company_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
) -> str:
    """通常コホート分析SQL."""
    table = get_table_ref(company_key)
    filters = build_filter_clause(
        date_from=date_from,
        date_to=date_to,
        product_categories=product_categories,
        ad_groups=ad_groups,
        product_names=product_names,
    )

    retained_columns = ",\n      ".join(
        f"COUNT(DISTINCT IF(t2.`{Col.ORDER_SUBSCRIPTION_COUNT}` = {i}, t1.customer_id, NULL)) AS retained_{i}"
        for i in range(1, MAX_RETENTION_MONTHS + 1)
    )

    return f"""
    WITH
    cohort_base AS (
      SELECT
        `{Col.CUSTOMER_ID}` AS customer_id,
        FORMAT_DATE('%Y-%m', `{Col.SUBSCRIPTION_CREATED_AT}`) AS cohort_month,
        MAX(IF(`{Col.ORDER_LOGICAL_SEQ}` = {LogicalSeq.REPROCESS}, 1, 0)) AS has_logic_2,
        MAX(IF(`{Col.ORDER_LOGICAL_SEQ}` = {LogicalSeq.FIRST} OR `{Col.ORDER_LOGICAL_SEQ}` IS NULL, 1, 0)) AS has_entry_data
      FROM {table}
      WHERE `{Col.ORDER_SUBSCRIPTION_COUNT}` = 1
      {filters}
      GROUP BY customer_id, cohort_month
      HAVING has_entry_data = 1 AND has_logic_2 = 0
    )
    SELECT
      t1.cohort_month,
      COUNT(DISTINCT t1.customer_id) AS total_users,
      {retained_columns}
    FROM
      cohort_base AS t1
    LEFT JOIN
      {table} AS t2
      ON t1.customer_id = t2.`{Col.CUSTOMER_ID}`
      AND t2.`{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
      AND t2.`{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}'
    GROUP BY cohort_month
    ORDER BY cohort_month
    """


def build_drilldown_sql(
    company_key: str,
    drilldown_column: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
) -> str:
    """ドリルダウン分析SQL (商品名別、広告グループ別、商品カテゴリ別)."""
    table = get_table_ref(company_key)
    filters = build_filter_clause(
        date_from=date_from,
        date_to=date_to,
        product_categories=product_categories,
        ad_groups=ad_groups,
        product_names=product_names,
    )

    retained_columns = ",\n      ".join(
        f"COUNT(DISTINCT IF(t2.`{Col.ORDER_SUBSCRIPTION_COUNT}` = {i}, t1.customer_id, NULL)) AS retained_{i}"
        for i in range(1, MAX_RETENTION_MONTHS + 1)
    )

    return f"""
    WITH
    cohort_base AS (
      SELECT
        `{Col.CUSTOMER_ID}` AS customer_id,
        `{drilldown_column}` AS dimension_col,
        FORMAT_DATE('%Y-%m', `{Col.SUBSCRIPTION_CREATED_AT}`) AS cohort_month,
        MAX(IF(`{Col.ORDER_LOGICAL_SEQ}` = {LogicalSeq.REPROCESS}, 1, 0)) AS has_logic_2,
        MAX(IF(`{Col.ORDER_LOGICAL_SEQ}` = {LogicalSeq.FIRST} OR `{Col.ORDER_LOGICAL_SEQ}` IS NULL, 1, 0)) AS has_entry_data
      FROM {table}
      WHERE `{Col.ORDER_SUBSCRIPTION_COUNT}` = 1
      {filters}
      AND `{drilldown_column}` IS NOT NULL
      AND `{drilldown_column}` != ''
      GROUP BY customer_id, dimension_col, cohort_month
      HAVING has_entry_data = 1 AND has_logic_2 = 0
    )
    SELECT
      t1.dimension_col,
      t1.cohort_month,
      COUNT(DISTINCT t1.customer_id) AS total_users,
      {retained_columns}
    FROM
      cohort_base AS t1
    LEFT JOIN
      {table} AS t2
      ON t1.customer_id = t2.`{Col.CUSTOMER_ID}`
      AND t2.`{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
      AND t2.`{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}'
    GROUP BY 1, 2
    ORDER BY 1, 2
    """


def build_aggregate_cohort_sql(
    company_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
) -> str:
    """通算コホート分析SQL.

    全コホート月を合算し、定期回数ごとの
    継続人数・継続率・残存率・平均決済金額を算出する。
    """
    table = get_table_ref(company_key)
    filters = build_filter_clause(
        date_from=date_from,
        date_to=date_to,
        product_categories=product_categories,
        ad_groups=ad_groups,
        product_names=product_names,
    )

    # 回数ごとの継続人数と合計金額
    retained_cols = ",\n      ".join(
        f"COUNT(DISTINCT IF(t2.`{Col.ORDER_SUBSCRIPTION_COUNT}` = {i}, t1.customer_id, NULL)) AS retained_{i},\n"
        f"      SUM(IF(t2.`{Col.ORDER_SUBSCRIPTION_COUNT}` = {i}, t2.`{Col.PAYMENT_AMOUNT}`, 0)) AS revenue_{i}"
        for i in range(1, MAX_RETENTION_MONTHS + 1)
    )

    return f"""
    WITH
    cohort_base AS (
      SELECT
        `{Col.CUSTOMER_ID}` AS customer_id,
        MAX(IF(`{Col.ORDER_LOGICAL_SEQ}` = {LogicalSeq.REPROCESS}, 1, 0)) AS has_logic_2,
        MAX(IF(`{Col.ORDER_LOGICAL_SEQ}` = {LogicalSeq.FIRST} OR `{Col.ORDER_LOGICAL_SEQ}` IS NULL, 1, 0)) AS has_entry_data
      FROM {table}
      WHERE `{Col.ORDER_SUBSCRIPTION_COUNT}` = 1
      {filters}
      GROUP BY customer_id
      HAVING has_entry_data = 1 AND has_logic_2 = 0
    )
    SELECT
      COUNT(DISTINCT t1.customer_id) AS total_users,
      {retained_cols}
    FROM
      cohort_base AS t1
    LEFT JOIN
      {table} AS t2
      ON t1.customer_id = t2.`{Col.CUSTOMER_ID}`
      AND t2.`{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
      AND t2.`{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}'
    """
