"""コホート分析用SQLクエリビルダー.

GASコードのビジネスロジックを忠実に移植:
- cohort_base: 受注_定期回数=1 の顧客を月別に集計
- 論理連番2(再処理)を持つ顧客は除外
- 論理連番1またはNULL(失敗含む)のデータがある顧客を対象
- retained_N: shipped & completed の成功数のみカウント
- 商品切替者除外: 1回目の定期商品名と同じ商品のみ継続としてカウント
"""

from __future__ import annotations

from src.constants import Col, LogicalSeq, MAX_RETENTION_MONTHS, Status
from src.queries.common import build_filter_clause, get_table_ref


def build_cohort_sql(
    company_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
) -> str:
    """通常コホート分析SQL (月別)."""
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
        `{Col.SUBSCRIPTION_PRODUCT_NAME}` AS first_product_name,
        FORMAT_DATE('%Y-%m', `{Col.SUBSCRIPTION_CREATED_AT}`) AS cohort_month,
        MAX(IF(`{Col.ORDER_LOGICAL_SEQ}` = {LogicalSeq.REPROCESS}, 1, 0)) AS has_logic_2,
        MAX(IF(`{Col.ORDER_LOGICAL_SEQ}` = {LogicalSeq.FIRST} OR `{Col.ORDER_LOGICAL_SEQ}` IS NULL, 1, 0)) AS has_entry_data
      FROM {table}
      WHERE `{Col.ORDER_SUBSCRIPTION_COUNT}` = 1
      {filters}
      GROUP BY customer_id, first_product_name, cohort_month
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
      AND t2.`{Col.SUBSCRIPTION_PRODUCT_NAME}` = t1.first_product_name
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
    """ドリルダウン分析SQL (商品名別、広告グループ別、商品カテゴリ別).

    定期商品名ドリルダウン時は revenue も取得する。
    """
    table = get_table_ref(company_key)
    filters = build_filter_clause(
        date_from=date_from,
        date_to=date_to,
        product_categories=product_categories,
        ad_groups=ad_groups,
        product_names=product_names,
    )

    # 定期商品名の場合 revenue も取得
    is_product_drilldown = drilldown_column == Col.SUBSCRIPTION_PRODUCT_NAME

    if is_product_drilldown:
        retained_columns = ",\n      ".join(
            f"COUNT(DISTINCT IF(t2.`{Col.ORDER_SUBSCRIPTION_COUNT}` = {i}, t1.customer_id, NULL)) AS retained_{i},\n"
            f"      SUM(IF(t2.`{Col.ORDER_SUBSCRIPTION_COUNT}` = {i}, t2.`{Col.PAYMENT_AMOUNT}`, 0)) AS revenue_{i}"
            for i in range(1, MAX_RETENTION_MONTHS + 1)
        )
    else:
        retained_columns = ",\n      ".join(
            f"COUNT(DISTINCT IF(t2.`{Col.ORDER_SUBSCRIPTION_COUNT}` = {i}, t1.customer_id, NULL)) AS retained_{i}"
            for i in range(1, MAX_RETENTION_MONTHS + 1)
        )

    return f"""
    WITH
    cohort_base AS (
      SELECT
        `{Col.CUSTOMER_ID}` AS customer_id,
        `{Col.SUBSCRIPTION_PRODUCT_NAME}` AS first_product_name,
        `{drilldown_column}` AS dimension_col,
        FORMAT_DATE('%Y-%m', `{Col.SUBSCRIPTION_CREATED_AT}`) AS cohort_month,
        MAX(IF(`{Col.ORDER_LOGICAL_SEQ}` = {LogicalSeq.REPROCESS}, 1, 0)) AS has_logic_2,
        MAX(IF(`{Col.ORDER_LOGICAL_SEQ}` = {LogicalSeq.FIRST} OR `{Col.ORDER_LOGICAL_SEQ}` IS NULL, 1, 0)) AS has_entry_data
      FROM {table}
      WHERE `{Col.ORDER_SUBSCRIPTION_COUNT}` = 1
      {filters}
      AND `{drilldown_column}` IS NOT NULL
      AND `{drilldown_column}` != ''
      GROUP BY customer_id, first_product_name, dimension_col, cohort_month
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
      AND t2.`{Col.SUBSCRIPTION_PRODUCT_NAME}` = t1.first_product_name
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
        `{Col.SUBSCRIPTION_PRODUCT_NAME}` AS first_product_name,
        MAX(IF(`{Col.ORDER_LOGICAL_SEQ}` = {LogicalSeq.REPROCESS}, 1, 0)) AS has_logic_2,
        MAX(IF(`{Col.ORDER_LOGICAL_SEQ}` = {LogicalSeq.FIRST} OR `{Col.ORDER_LOGICAL_SEQ}` IS NULL, 1, 0)) AS has_entry_data
      FROM {table}
      WHERE `{Col.ORDER_SUBSCRIPTION_COUNT}` = 1
      {filters}
      GROUP BY customer_id, first_product_name
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
      AND t2.`{Col.SUBSCRIPTION_PRODUCT_NAME}` = t1.first_product_name
      AND t2.`{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
      AND t2.`{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}'
    """


def build_max_date_sql(company_key: str) -> str:
    """データの最終日を取得するSQL."""
    table = get_table_ref(company_key)
    return f"""
    SELECT MAX(`{Col.SUBSCRIPTION_CREATED_AT}`) AS max_date
    FROM {table}
    """


def build_upsell_sql(
    company_key: str,
    from_product_name: str,
    to_product_name: str,
    date_from: str | None = None,
    date_to: str | None = None,
) -> str:
    """アップセル率計算SQL.

    from_product_name の顧客のうち、to_product_name も購入した顧客の割合を算出。
    """
    table = get_table_ref(company_key)
    date_filter = ""
    if date_from:
        date_filter += f"\n        AND `{Col.SUBSCRIPTION_CREATED_AT}` >= '{date_from}'"
    if date_to:
        date_filter += f"\n        AND `{Col.SUBSCRIPTION_CREATED_AT}` <= '{date_to}'"

    return f"""
    WITH
    from_customers AS (
      SELECT DISTINCT `{Col.CUSTOMER_ID}` AS customer_id
      FROM {table}
      WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` = '{from_product_name}'
        AND `{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
        AND `{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}'
        {date_filter}
    ),
    to_customers AS (
      SELECT DISTINCT `{Col.CUSTOMER_ID}` AS customer_id
      FROM {table}
      WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` = '{to_product_name}'
        AND `{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
        AND `{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}'
    )
    SELECT
      COUNT(DISTINCT f.customer_id) AS from_count,
      COUNT(DISTINCT t.customer_id) AS upsell_count
    FROM from_customers f
    LEFT JOIN to_customers t ON f.customer_id = t.customer_id
    """
