"""Tier分析用SQLクエリビルダー.

顧客ごとの通算LTV（累計受注金額）をTierに分け、
各Tierの定期ステータス別の人数・割合を算出する。
"""

from __future__ import annotations

from src.config_loader import load_tier_boundaries
from src.constants import Col, LogicalSeq, Status
from src.queries.common import build_filter_clause, get_table_ref

# STRING型カラムの安全なCAST式
_SUB_COUNT = f"SAFE_CAST(`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64)"
_LOGIC_SEQ = f"SAFE_CAST(`{Col.ORDER_LOGICAL_SEQ}` AS INT64)"
_PAY_AMOUNT = f"SAFE_CAST(`{Col.PAYMENT_AMOUNT}` AS FLOAT64)"
_TS = f"SAFE_CAST(`{Col.SUBSCRIPTION_CREATED_AT}` AS TIMESTAMP)"
_SALES_TS = f"SAFE_CAST(`{Col.SALES_DATE}` AS TIMESTAMP)"


def _get_tier_boundaries() -> list[int]:
    """マスタからTier境界値を動的に読み込む."""
    return load_tier_boundaries()


def _tier_case_expr(boundaries: list[int] | None = None) -> str:
    """LTV金額からTierラベルを返すCASE式を生成."""
    boundaries = boundaries or _get_tier_boundaries()
    parts = []
    prev = 0
    for boundary in boundaries:
        parts.append(
            f"WHEN total_ltv <= {boundary} THEN '{prev:,}~{boundary:,}円'"
        )
        prev = boundary + 1
    parts.append(f"ELSE '{prev:,}円~'")
    return "CASE\n        " + "\n        ".join(parts) + "\n      END"


def _tier_order_expr(boundaries: list[int] | None = None) -> str:
    """Tierのソート用数値を返すCASE式を生成."""
    boundaries = boundaries or _get_tier_boundaries()
    parts = []
    for idx, boundary in enumerate(boundaries):
        parts.append(f"WHEN total_ltv <= {boundary} THEN {idx}")
    parts.append(f"ELSE {len(boundaries)}")
    return "CASE\n        " + "\n        ".join(parts) + "\n      END"


def build_tier_sql(
    company_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
    ad_url_params: list[str] | None = None,
) -> str:
    """Tier分析SQL.

    1. cohort_base: 初回購入者を特定（再処理除外）
    2. 顧客ごとの通算LTV（shipped&completedの累計決済金額）を計算
    3. 顧客の最新の定期ステータスを取得
    4. LTVをTierに分けて、ステータス別に集計
    """
    table = get_table_ref(company_key)
    filters = build_filter_clause(
        date_from=date_from,
        date_to=date_to,
        product_categories=product_categories,
        ad_groups=ad_groups,
        product_names=product_names,
        ad_url_params=ad_url_params,
    )

    tier_case = _tier_case_expr()
    tier_order = _tier_order_expr()

    return f"""
    WITH
    cohort_base AS (
      SELECT
        `{Col.CUSTOMER_ID}` AS customer_id,
        `{Col.SUBSCRIPTION_PRODUCT_NAME}` AS first_product_name,
        MAX(IF({_LOGIC_SEQ} = {LogicalSeq.REPROCESS}, 1, 0)) AS has_logic_2,
        MAX(IF({_LOGIC_SEQ} = {LogicalSeq.FIRST} OR `{Col.ORDER_LOGICAL_SEQ}` IS NULL, 1, 0)) AS has_entry_data
      FROM {table}
      WHERE {_SUB_COUNT} = 1
      {filters}
      GROUP BY customer_id, first_product_name
      HAVING has_entry_data = 1 AND has_logic_2 = 0
    ),
    -- 顧客ごとの通算LTV (shipped&completedの累計)
    customer_ltv AS (
      SELECT
        t1.customer_id,
        IFNULL(SUM(
          IF(t2.`{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
             AND t2.`{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}',
             {_PAY_AMOUNT}, 0)
        ), 0) AS total_ltv
      FROM cohort_base AS t1
      LEFT JOIN {table} AS t2
        ON t1.customer_id = t2.`{Col.CUSTOMER_ID}`
        AND t2.`{Col.SUBSCRIPTION_PRODUCT_NAME}` = t1.first_product_name
      GROUP BY t1.customer_id
    ),
    -- 顧客の定期ステータス（最新）
    customer_status AS (
      SELECT
        t1.customer_id,
        -- 最新のステータスを取得（NULLでないもの）
        ARRAY_AGG(
          t2.`{Col.SUBSCRIPTION_STATUS}`
          IGNORE NULLS
          ORDER BY SAFE_CAST(t2.`{Col.SUBSCRIPTION_CREATED_AT}` AS TIMESTAMP) DESC
          LIMIT 1
        )[SAFE_OFFSET(0)] AS subscription_status
      FROM cohort_base AS t1
      LEFT JOIN {table} AS t2
        ON t1.customer_id = t2.`{Col.CUSTOMER_ID}`
        AND t2.`{Col.SUBSCRIPTION_PRODUCT_NAME}` = t1.first_product_name
      GROUP BY t1.customer_id
    ),
    -- Tier振り分け
    tiered AS (
      SELECT
        cl.customer_id,
        cl.total_ltv,
        {tier_case} AS tier_label,
        {tier_order} AS tier_sort,
        IFNULL(cs.subscription_status, '不明') AS subscription_status
      FROM customer_ltv cl
      LEFT JOIN customer_status cs ON cl.customer_id = cs.customer_id
    )
    SELECT
      tier_label,
      tier_sort,
      subscription_status,
      COUNT(*) AS customer_count
    FROM tiered
    GROUP BY tier_label, tier_sort, subscription_status
    ORDER BY tier_sort, subscription_status
    """


def build_tier_by_order_count_sql(
    company_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
    ad_url_params: list[str] | None = None,
) -> str:
    """定期回数別Tier分析SQL.

    顧客ごとのLTV Tierと最大定期回数(shipped&completed)を取得し、
    Tier × 定期回数 の顧客数を集計する。
    """
    table = get_table_ref(company_key)
    filters = build_filter_clause(
        date_from=date_from, date_to=date_to,
        product_categories=product_categories, ad_groups=ad_groups,
        product_names=product_names, ad_url_params=ad_url_params,
    )
    boundaries = _get_tier_boundaries()
    tier_case = _tier_case_expr(boundaries)
    tier_order = _tier_order_expr(boundaries)

    return f"""
    WITH
    cohort_base AS (
      SELECT
        `{Col.CUSTOMER_ID}` AS customer_id,
        `{Col.SUBSCRIPTION_PRODUCT_NAME}` AS first_product_name,
        MAX(IF({_LOGIC_SEQ} = {LogicalSeq.REPROCESS}, 1, 0)) AS has_logic_2,
        MAX(IF({_LOGIC_SEQ} = {LogicalSeq.FIRST} OR `{Col.ORDER_LOGICAL_SEQ}` IS NULL, 1, 0)) AS has_entry_data
      FROM {table}
      WHERE {_SUB_COUNT} = 1
      {filters}
      GROUP BY customer_id, first_product_name
      HAVING has_entry_data = 1 AND has_logic_2 = 0
    ),
    customer_ltv AS (
      SELECT
        t1.customer_id,
        IFNULL(SUM(
          IF(t2.`{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
             AND t2.`{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}',
             {_PAY_AMOUNT}, 0)
        ), 0) AS total_ltv,
        MAX(IF(
          t2.`{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
          AND t2.`{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}',
          SAFE_CAST(t2.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64),
          NULL
        )) AS max_order_count
      FROM cohort_base AS t1
      LEFT JOIN {table} AS t2
        ON t1.customer_id = t2.`{Col.CUSTOMER_ID}`
        AND t2.`{Col.SUBSCRIPTION_PRODUCT_NAME}` = t1.first_product_name
      GROUP BY t1.customer_id
    ),
    tiered AS (
      SELECT
        cl.customer_id,
        {tier_case} AS tier_label,
        {tier_order} AS tier_sort,
        IFNULL(cl.max_order_count, 0) AS order_count
      FROM customer_ltv cl
    )
    SELECT
      tier_label,
      tier_sort,
      order_count,
      COUNT(*) AS customer_count
    FROM tiered
    GROUP BY tier_label, tier_sort, order_count
    ORDER BY tier_sort, order_count
    """


def build_revenue_proportion_sql(
    company_key: str,
    group_by_column: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
    ad_url_params: list[str] | None = None,
    cohort_date_from: str | None = None,
    cohort_date_to: str | None = None,
) -> str:
    """売上比率SQL.

    指定された軸(商品カテゴリ/広告グループ/定期商品名/定期回数)で売上を集計。
    date_from/date_to: 売上完了日（受注_売上日時）で絞り込む。
    cohort_date_from/cohort_date_to: 定期受注_作成日時でcohort_baseを絞り込む（サイドバー日付）。
    """
    table = get_table_ref(company_key)
    # cohort_base用: サイドバー日付 + その他フィルタ
    cohort_filters = build_filter_clause(
        date_from=cohort_date_from, date_to=cohort_date_to,
        product_categories=product_categories, ad_groups=ad_groups,
        product_names=product_names, ad_url_params=ad_url_params,
    )

    # 売上日時フィルタ（t2側に適用）
    sales_date_clauses = []
    _sales_ts_t2 = f"SAFE_CAST(t2.`{Col.SALES_DATE}` AS TIMESTAMP)"
    if date_from:
        sales_date_clauses.append(f"AND {_sales_ts_t2} >= '{date_from}'")
    if date_to:
        sales_date_clauses.append(f"AND {_sales_ts_t2} <= '{date_to} 23:59:59'")
    sales_date_filter = "\n      ".join(sales_date_clauses)

    # 定期回数の場合は特別な処理
    if group_by_column == "__order_count__":
        group_expr = f"CAST(SAFE_CAST(t2.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) AS STRING)"
    else:
        group_expr = f"t2.`{group_by_column}`"

    return f"""
    WITH
    cohort_base AS (
      SELECT
        `{Col.CUSTOMER_ID}` AS customer_id,
        `{Col.SUBSCRIPTION_PRODUCT_NAME}` AS first_product_name,
        MAX(IF({_LOGIC_SEQ} = {LogicalSeq.REPROCESS}, 1, 0)) AS has_logic_2,
        MAX(IF({_LOGIC_SEQ} = {LogicalSeq.FIRST} OR `{Col.ORDER_LOGICAL_SEQ}` IS NULL, 1, 0)) AS has_entry_data
      FROM {table}
      WHERE {_SUB_COUNT} = 1
      {cohort_filters}
      GROUP BY customer_id, first_product_name
      HAVING has_entry_data = 1 AND has_logic_2 = 0
    )
    SELECT
      {group_expr} AS group_value,
      SUM(IF(
        t2.`{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
        AND t2.`{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}',
        SAFE_CAST(t2.`{Col.PAYMENT_AMOUNT}` AS FLOAT64), 0
      )) AS total_revenue,
      COUNT(DISTINCT t1.customer_id) AS customer_count
    FROM cohort_base AS t1
    LEFT JOIN {table} AS t2
      ON t1.customer_id = t2.`{Col.CUSTOMER_ID}`
      AND t2.`{Col.SUBSCRIPTION_PRODUCT_NAME}` = t1.first_product_name
    WHERE {group_expr} IS NOT NULL AND {group_expr} != ''
      AND t2.`{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
      AND t2.`{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}'
      {sales_date_filter}
    GROUP BY group_value
    ORDER BY total_revenue DESC
    """
