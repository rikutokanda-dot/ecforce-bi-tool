"""解約分析用SQLクエリビルダー.

キャンセル理由の集計、定期回数別キャンセル理由の集計を行う。
"""

from __future__ import annotations

from src.constants import Col, LogicalSeq, Status
from src.queries.common import build_filter_clause, get_table_ref

# STRING型カラムの安全なCAST式
_TS = f"SAFE_CAST(`{Col.SUBSCRIPTION_CREATED_AT}` AS TIMESTAMP)"
_SUB_COUNT = f"SAFE_CAST(`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64)"
_LOGIC_SEQ = f"SAFE_CAST(`{Col.ORDER_LOGICAL_SEQ}` AS INT64)"


def build_churn_reason_sql(
    company_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
    ad_urls: list[str] | None = None,
) -> str:
    """キャンセル理由別の集計SQL.

    キャンセル理由がある顧客を理由別にカウント。
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
    cancelled AS (
      SELECT DISTINCT
        t1.customer_id,
        t2.`{Col.CANCEL_REASON}` AS cancel_reason
      FROM cohort_base AS t1
      INNER JOIN {table} AS t2
        ON t1.customer_id = t2.`{Col.CUSTOMER_ID}`
        AND t2.`{Col.SUBSCRIPTION_PRODUCT_NAME}` = t1.first_product_name
      WHERE t2.`{Col.CANCEL_REASON}` IS NOT NULL
        AND t2.`{Col.CANCEL_REASON}` != ''
    )
    SELECT
      cancel_reason,
      COUNT(*) AS cancel_count
    FROM cancelled
    GROUP BY cancel_reason
    ORDER BY cancel_count DESC
    """


def build_churn_by_order_reason_sql(
    company_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
    ad_urls: list[str] | None = None,
) -> str:
    """定期回数別キャンセル理由SQL.

    各顧客の「最後にshipped&completedだった定期回数」を求め、
    その回数別にキャンセル理由を集計する。
    例: 2回目まで出荷完了して解約 → last_completed_order = 2
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
    -- 顧客ごとの最後のshipped&completed回数
    last_completed AS (
      SELECT
        t1.customer_id,
        MAX(IF(
          t2.`{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
          AND t2.`{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}',
          {_SUB_COUNT},
          NULL
        )) AS last_completed_order
      FROM cohort_base AS t1
      LEFT JOIN {table} AS t2
        ON t1.customer_id = t2.`{Col.CUSTOMER_ID}`
        AND t2.`{Col.SUBSCRIPTION_PRODUCT_NAME}` = t1.first_product_name
      GROUP BY t1.customer_id
    ),
    -- 顧客ごとのキャンセル理由（重複排除）
    cancel_info AS (
      SELECT DISTINCT
        t1.customer_id,
        t2.`{Col.CANCEL_REASON}` AS cancel_reason
      FROM cohort_base AS t1
      INNER JOIN {table} AS t2
        ON t1.customer_id = t2.`{Col.CUSTOMER_ID}`
        AND t2.`{Col.SUBSCRIPTION_PRODUCT_NAME}` = t1.first_product_name
      WHERE t2.`{Col.CANCEL_REASON}` IS NOT NULL
        AND t2.`{Col.CANCEL_REASON}` != ''
    )
    SELECT
      lc.last_completed_order,
      ci.cancel_reason,
      COUNT(DISTINCT lc.customer_id) AS cancel_count
    FROM last_completed lc
    INNER JOIN cancel_info ci ON lc.customer_id = ci.customer_id
    WHERE lc.last_completed_order IS NOT NULL
    GROUP BY lc.last_completed_order, ci.cancel_reason
    ORDER BY lc.last_completed_order, cancel_count DESC
    """
