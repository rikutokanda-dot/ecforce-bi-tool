"""Tier分析用SQLクエリビルダー.

顧客ごとの通算LTV（累計受注金額）をTierに分け、
各Tierの定期ステータス別の人数・割合を算出する。
"""

from __future__ import annotations

from src.constants import Col, LogicalSeq, Status
from src.queries.common import build_filter_clause, get_table_ref

# STRING型カラムの安全なCAST式
_SUB_COUNT = f"SAFE_CAST(`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64)"
_LOGIC_SEQ = f"SAFE_CAST(`{Col.ORDER_LOGICAL_SEQ}` AS INT64)"
_PAY_AMOUNT = f"SAFE_CAST(`{Col.PAYMENT_AMOUNT}` AS FLOAT64)"

# Tierの境界値（上限）
TIER_BOUNDARIES = [
    5000, 10000, 20000, 30000, 40000, 50000,
    60000, 70000, 80000, 90000, 100000,
]


def _tier_case_expr() -> str:
    """LTV金額からTierラベルを返すCASE式を生成."""
    parts = []
    prev = 0
    for boundary in TIER_BOUNDARIES:
        parts.append(
            f"WHEN total_ltv <= {boundary} THEN '{prev:,}~{boundary:,}円'"
        )
        prev = boundary + 1
    parts.append(f"ELSE '{prev:,}円~'")
    return "CASE\n        " + "\n        ".join(parts) + "\n      END"


def _tier_order_expr() -> str:
    """Tierのソート用数値を返すCASE式を生成."""
    parts = []
    for idx, boundary in enumerate(TIER_BOUNDARIES):
        parts.append(f"WHEN total_ltv <= {boundary} THEN {idx}")
    parts.append(f"ELSE {len(TIER_BOUNDARIES)}")
    return "CASE\n        " + "\n        ".join(parts) + "\n      END"


def build_tier_sql(
    company_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
    ad_urls: list[str] | None = None,
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
        ad_urls=ad_urls,
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
