"""解約分析用SQLクエリビルダー.

キャンセル理由の集計、定期回数別キャンセル理由の集計、返品率の集計を行う。
解約分析ではステータスによる絞り込みを行わず、全ステータスを対象とする。
"""

from __future__ import annotations

from src.constants import Col, Status
from src.queries.common import build_filter_clause, build_sales_date_clause, get_table_ref

# STRING型カラムの安全なCAST式
_TS = f"SAFE_CAST(`{Col.SUBSCRIPTION_CREATED_AT}` AS TIMESTAMP)"
_SUB_COUNT = f"SAFE_CAST(`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64)"

# 返品率分析用: IN句の文字列
_TOTAL_SHIPPED_IN = ", ".join(f"'{s}'" for s in Status.TOTAL_SHIPPED_STATUSES)
_RETURN_IN = ", ".join(f"'{s}'" for s in Status.RETURN_STATUSES)


def build_churn_reason_sql(
    company_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
    ad_url_params: list[str] | None = None,
    sales_date_from: str | None = None,
    sales_date_to: str | None = None,
) -> str:
    """キャンセル理由別の集計SQL.

    キャンセル理由がある顧客を理由別にカウント。
    全ステータスを対象とする。
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
    sales_filter = build_sales_date_clause(sales_date_from, sales_date_to)

    return f"""
    WITH
    cohort_base AS (
      SELECT DISTINCT
        `{Col.CUSTOMER_ID}` AS customer_id
      FROM {table}
      WHERE {_SUB_COUNT} = 1
      {filters}
    ),
    cancelled AS (
      SELECT DISTINCT
        t1.customer_id,
        t2.`{Col.CANCEL_REASON}` AS cancel_reason
      FROM cohort_base AS t1
      INNER JOIN {table} AS t2
        ON t1.customer_id = t2.`{Col.CUSTOMER_ID}`
      WHERE t2.`{Col.CANCEL_REASON}` IS NOT NULL
        AND t2.`{Col.CANCEL_REASON}` != ''
        {sales_filter}
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
    ad_url_params: list[str] | None = None,
    sales_date_from: str | None = None,
    sales_date_to: str | None = None,
) -> str:
    """定期回数別キャンセル理由SQL.

    各顧客の「最後にshipped&completedだった定期回数」を求め、
    その回数別にキャンセル理由を集計する。
    全ステータスを対象とする。
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
    sales_filter = build_sales_date_clause(sales_date_from, sales_date_to)

    return f"""
    WITH
    cohort_base AS (
      SELECT DISTINCT
        `{Col.CUSTOMER_ID}` AS customer_id
      FROM {table}
      WHERE {_SUB_COUNT} = 1
      {filters}
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
      WHERE 1=1
        {sales_filter}
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
      WHERE t2.`{Col.CANCEL_REASON}` IS NOT NULL
        AND t2.`{Col.CANCEL_REASON}` != ''
    )
    SELECT
      IFNULL(lc.last_completed_order, 0) AS last_completed_order,
      ci.cancel_reason,
      COUNT(DISTINCT lc.customer_id) AS cancel_count
    FROM last_completed lc
    INNER JOIN cancel_info ci ON lc.customer_id = ci.customer_id
    GROUP BY last_completed_order, ci.cancel_reason
    ORDER BY last_completed_order, cancel_count DESC
    """


def build_return_rate_sql(
    company_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
    ad_url_params: list[str] | None = None,
    sales_date_from: str | None = None,
    sales_date_to: str | None = None,
) -> str:
    """返品率SQL.

    定期回数ごとに、総件数と返品件数を集計して返品率を算出する。
    全ステータスを対象（ステータスフィルタなし）。
    売上日フィルタのみ使用（定期受注作成日の日付フィルタは適用しない）。
    """
    table = get_table_ref(company_key)
    # 日付フィルタを除外し、商品・カテゴリ等のフィルタのみ適用
    filters = build_filter_clause(
        product_categories=product_categories,
        ad_groups=ad_groups,
        product_names=product_names,
        ad_url_params=ad_url_params,
    )
    sales_filter = build_sales_date_clause(sales_date_from, sales_date_to, alias="")

    return f"""
    SELECT
      {_SUB_COUNT} AS sub_count,
      COUNT(*) AS shipped_count,
      COUNTIF(
        `{Col.ORDER_STATUS}` IN ({_RETURN_IN})
      ) AS return_count
    FROM {table}
    WHERE {_SUB_COUNT} IS NOT NULL
      {filters}
      {sales_filter}
    GROUP BY sub_count
    ORDER BY sub_count
    """


def build_return_cancel_reason_sql(
    company_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
    ad_url_params: list[str] | None = None,
    sales_date_from: str | None = None,
    sales_date_to: str | None = None,
) -> str:
    """返品者のキャンセル理由SQL.

    返品ステータスの全注文から顧客を特定し、キャンセル理由を集計。
    売上日フィルタのみ使用。
    """
    table = get_table_ref(company_key)
    filters = build_filter_clause(
        product_categories=product_categories, ad_groups=ad_groups,
        product_names=product_names, ad_url_params=ad_url_params,
    )
    sales_filter = build_sales_date_clause(sales_date_from, sales_date_to, alias="")

    return f"""
    WITH
    return_customers AS (
      SELECT DISTINCT `{Col.CUSTOMER_ID}` AS customer_id
      FROM {table}
      WHERE `{Col.ORDER_STATUS}` IN ({_RETURN_IN})
        {filters}
        {sales_filter}
    ),
    cancel_info AS (
      SELECT DISTINCT
        rc.customer_id,
        t2.`{Col.CANCEL_REASON}` AS cancel_reason
      FROM return_customers rc
      INNER JOIN {table} AS t2
        ON rc.customer_id = t2.`{Col.CUSTOMER_ID}`
      WHERE t2.`{Col.CANCEL_REASON}` IS NOT NULL
        AND t2.`{Col.CANCEL_REASON}` != ''
    )
    SELECT
      IFNULL(cancel_reason, '理由なし') AS cancel_reason,
      COUNT(*) AS cancel_count
    FROM cancel_info
    GROUP BY cancel_reason
    ORDER BY cancel_count DESC
    """


def build_return_by_order_cancel_reason_sql(
    company_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
    ad_url_params: list[str] | None = None,
    sales_date_from: str | None = None,
    sales_date_to: str | None = None,
) -> str:
    """定期回数別・返品者のキャンセル理由SQL.

    返品ステータスの全注文から定期回数別にキャンセル理由を集計。
    売上日フィルタのみ使用。
    """
    table = get_table_ref(company_key)
    filters = build_filter_clause(
        product_categories=product_categories, ad_groups=ad_groups,
        product_names=product_names, ad_url_params=ad_url_params,
    )
    sales_filter = build_sales_date_clause(sales_date_from, sales_date_to, alias="")

    return f"""
    WITH
    return_orders AS (
      SELECT
        `{Col.CUSTOMER_ID}` AS customer_id,
        {_SUB_COUNT} AS sub_count
      FROM {table}
      WHERE `{Col.ORDER_STATUS}` IN ({_RETURN_IN})
        AND {_SUB_COUNT} IS NOT NULL
        {filters}
        {sales_filter}
    ),
    cancel_info AS (
      SELECT DISTINCT
        ro.customer_id,
        ro.sub_count,
        t2.`{Col.CANCEL_REASON}` AS cancel_reason
      FROM return_orders ro
      INNER JOIN {table} AS t2
        ON ro.customer_id = t2.`{Col.CUSTOMER_ID}`
      WHERE t2.`{Col.CANCEL_REASON}` IS NOT NULL
        AND t2.`{Col.CANCEL_REASON}` != ''
    )
    SELECT
      sub_count,
      cancel_reason,
      COUNT(*) AS cancel_count
    FROM cancel_info
    GROUP BY sub_count, cancel_reason
    ORDER BY sub_count, cancel_count DESC
    """


def build_shipped_order_ids_sql(
    company_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
    ad_url_params: list[str] | None = None,
    sales_date_from: str | None = None,
    sales_date_to: str | None = None,
    sub_count_filter: int | None = None,
) -> str:
    """受注IDリスト取得SQL.

    全ステータスの受注_id, 顧客_id, 定期回数, 売上日時を返す。
    売上日フィルタのみ使用。
    """
    table = get_table_ref(company_key)
    filters = build_filter_clause(
        product_categories=product_categories, ad_groups=ad_groups,
        product_names=product_names, ad_url_params=ad_url_params,
    )
    sales_filter = build_sales_date_clause(sales_date_from, sales_date_to, alias="")

    sub_count_clause = ""
    if sub_count_filter is not None:
        sub_count_clause = f"AND {_SUB_COUNT} = {sub_count_filter}"

    return f"""
    SELECT
      `{Col.ORDER_ID}` AS order_id,
      `{Col.CUSTOMER_ID}` AS customer_id,
      {_SUB_COUNT} AS sub_count,
      `{Col.ORDER_STATUS}` AS order_status,
      `{Col.PAYMENT_STATUS}` AS payment_status,
      `{Col.SALES_DATE}` AS sales_date,
      `{Col.SUBSCRIPTION_PRODUCT_NAME}` AS product_name
    FROM {table}
    WHERE 1=1
      {filters}
      {sub_count_clause}
      {sales_filter}
    ORDER BY {_SUB_COUNT}, `{Col.ORDER_ID}`
    """
