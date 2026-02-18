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
    """出荷済み受注データの最終日を取得するSQL.

    定期受注_作成日時のMAXだと未出荷の定期受注も含むため
    実際のデータカットオフ日にならない。
    shipped & completed の受注の最新日をカットオフとして使用する。
    """
    table = get_table_ref(company_key)
    return f"""
    SELECT MAX(`{Col.SUBSCRIPTION_CREATED_AT}`) AS max_date
    FROM {table}
    WHERE `{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
      AND `{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}'
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


def build_upsell_rate_sql(
    company_key: str,
    normal_product_names: str | list[str],
    upsell_product_name: str,
    date_from: str | None = None,
    date_to: str | None = None,
) -> str:
    """アップセル率計算SQL.

    アップセル商品が実際に購入されている期間を自動検出し、
    その期間中の通常商品とアップセル商品の1回目購入数から
    アップセル切替率を算出する。

    normal_product_names は単一文字列またはリストを受け付ける。

    アップセル率 = upsell_1回目購入数 / (normal_1回目購入数 + upsell_1回目購入数)
    """
    table = get_table_ref(company_key)

    # normal_product_names をリスト化
    if isinstance(normal_product_names, str):
        normal_product_names = [normal_product_names]
    normal_in = ", ".join(f"'{n}'" for n in normal_product_names)

    # ユーザー日付フィルタとの交差期間計算
    period_start_expr = "p.period_start"
    period_end_expr = "p.period_end"
    if date_from:
        period_start_expr = f"GREATEST(p.period_start, '{date_from}')"
    if date_to:
        period_end_expr = f"LEAST(p.period_end, '{date_to}')"

    return f"""
    WITH
    upsell_period AS (
      SELECT
        MIN(`{Col.SUBSCRIPTION_CREATED_AT}`) AS period_start,
        MAX(`{Col.SUBSCRIPTION_CREATED_AT}`) AS period_end
      FROM {table}
      WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` = '{upsell_product_name}'
        AND `{Col.ORDER_SUBSCRIPTION_COUNT}` = 1
        AND `{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
        AND `{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}'
    ),
    effective_period AS (
      SELECT
        {period_start_expr} AS eff_start,
        {period_end_expr} AS eff_end
      FROM upsell_period p
      WHERE p.period_start IS NOT NULL
    ),
    normal_first AS (
      SELECT COUNT(DISTINCT `{Col.CUSTOMER_ID}`) AS normal_count
      FROM {table}
      CROSS JOIN effective_period ep
      WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` IN ({normal_in})
        AND `{Col.ORDER_SUBSCRIPTION_COUNT}` = 1
        AND `{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
        AND `{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}'
        AND `{Col.SUBSCRIPTION_CREATED_AT}` >= ep.eff_start
        AND `{Col.SUBSCRIPTION_CREATED_AT}` <= ep.eff_end
    ),
    upsell_first AS (
      SELECT COUNT(DISTINCT `{Col.CUSTOMER_ID}`) AS upsell_count
      FROM {table}
      CROSS JOIN effective_period ep
      WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` = '{upsell_product_name}'
        AND `{Col.ORDER_SUBSCRIPTION_COUNT}` = 1
        AND `{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
        AND `{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}'
        AND `{Col.SUBSCRIPTION_CREATED_AT}` >= ep.eff_start
        AND `{Col.SUBSCRIPTION_CREATED_AT}` <= ep.eff_end
    )
    SELECT
      n.normal_count,
      u.upsell_count,
      ep.eff_start AS period_start,
      ep.eff_end AS period_end,
      SAFE_DIVIDE(u.upsell_count, n.normal_count + u.upsell_count) * 100 AS upsell_rate
    FROM normal_first n
    CROSS JOIN upsell_first u
    CROSS JOIN effective_period ep
    """


def build_upsell_rate_monthly_sql(
    company_key: str,
    normal_product_names: str | list[str],
    upsell_product_name: str,
    date_from: str | None = None,
    date_to: str | None = None,
) -> str:
    """月別アップセル率計算SQL.

    アップセル商品が購入されている期間内で、月ごとの
    通常商品とアップセル商品の1回目購入数からアップセル率を算出。
    normal_product_names は単一文字列またはリストを受け付ける。
    """
    table = get_table_ref(company_key)

    if isinstance(normal_product_names, str):
        normal_product_names = [normal_product_names]
    normal_in = ", ".join(f"'{n}'" for n in normal_product_names)

    period_start_expr = "p.period_start"
    period_end_expr = "p.period_end"
    if date_from:
        period_start_expr = f"GREATEST(p.period_start, '{date_from}')"
    if date_to:
        period_end_expr = f"LEAST(p.period_end, '{date_to}')"

    return f"""
    WITH
    upsell_period AS (
      SELECT
        MIN(`{Col.SUBSCRIPTION_CREATED_AT}`) AS period_start,
        MAX(`{Col.SUBSCRIPTION_CREATED_AT}`) AS period_end
      FROM {table}
      WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` = '{upsell_product_name}'
        AND `{Col.ORDER_SUBSCRIPTION_COUNT}` = 1
        AND `{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
        AND `{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}'
    ),
    effective_period AS (
      SELECT
        {period_start_expr} AS eff_start,
        {period_end_expr} AS eff_end
      FROM upsell_period p
      WHERE p.period_start IS NOT NULL
    ),
    monthly_normal AS (
      SELECT
        FORMAT_DATE('%Y-%m', `{Col.SUBSCRIPTION_CREATED_AT}`) AS cohort_month,
        COUNT(DISTINCT `{Col.CUSTOMER_ID}`) AS normal_count
      FROM {table}
      CROSS JOIN effective_period ep
      WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` IN ({normal_in})
        AND `{Col.ORDER_SUBSCRIPTION_COUNT}` = 1
        AND `{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
        AND `{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}'
        AND `{Col.SUBSCRIPTION_CREATED_AT}` >= ep.eff_start
        AND `{Col.SUBSCRIPTION_CREATED_AT}` <= ep.eff_end
      GROUP BY cohort_month
    ),
    monthly_upsell AS (
      SELECT
        FORMAT_DATE('%Y-%m', `{Col.SUBSCRIPTION_CREATED_AT}`) AS cohort_month,
        COUNT(DISTINCT `{Col.CUSTOMER_ID}`) AS upsell_count
      FROM {table}
      CROSS JOIN effective_period ep
      WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` = '{upsell_product_name}'
        AND `{Col.ORDER_SUBSCRIPTION_COUNT}` = 1
        AND `{Col.ORDER_STATUS}` = '{Status.SHIPPED}'
        AND `{Col.PAYMENT_STATUS}` = '{Status.COMPLETED}'
        AND `{Col.SUBSCRIPTION_CREATED_AT}` >= ep.eff_start
        AND `{Col.SUBSCRIPTION_CREATED_AT}` <= ep.eff_end
      GROUP BY cohort_month
    )
    SELECT
      COALESCE(n.cohort_month, u.cohort_month) AS cohort_month,
      IFNULL(n.normal_count, 0) AS normal_count,
      IFNULL(u.upsell_count, 0) AS upsell_count,
      SAFE_DIVIDE(
        IFNULL(u.upsell_count, 0),
        IFNULL(n.normal_count, 0) + IFNULL(u.upsell_count, 0)
      ) * 100 AS upsell_rate
    FROM monthly_normal n
    FULL OUTER JOIN monthly_upsell u ON n.cohort_month = u.cohort_month
    ORDER BY cohort_month
    """
