"""メールアップセル分析用SQLクエリビルダー.

マスタ管理で定義した分母/分子商品のマッピングに基づき、
all_integrated テーブルから商品切替を検出して
アップセル率（F別）と切替後継続率を算出する。
"""

from __future__ import annotations

from datetime import date

from src.constants import Col, PROJECT_ID
from src.queries.chirashi import _product_cycles_cte


def _integrated_ref(company_key: str) -> str:
    """all_integrated テーブルの参照."""
    dataset = f"{company_key}_ecforce_raw_data"
    table = f"{company_key}_all_integrated"
    return f"`{PROJECT_ID}.{dataset}.{table}`"


def _in_clause(names: list[str]) -> str:
    """商品名リストをSQL IN句用文字列に変換."""
    escaped = [n.replace("'", "''") for n in names]
    return ", ".join(f"'{n}'" for n in escaped)


def _date_filter(
    date_from: str | None = None,
    date_to: str | None = None,
    sales_date_from: str | None = None,
    sales_date_to: str | None = None,
    order_date_from: date | None = None,
    order_date_to: date | None = None,
    alias: str = "",
) -> str:
    """全日付フィルタを結合したAND句を生成.

    サイドバーで有効な日付フィルタを全て適用する。
    """
    prefix = f"{alias}." if alias else ""
    parts = []

    # 定期受注_作成日時
    _sub_ts = f"SAFE_CAST({prefix}`{Col.SUBSCRIPTION_CREATED_AT}` AS TIMESTAMP)"
    if date_from:
        parts.append(f"{_sub_ts} >= '{date_from}'")
    if date_to:
        parts.append(f"{_sub_ts} <= '{date_to}'")

    # 受注_売上日時
    _sales_ts = f"SAFE_CAST({prefix}`{Col.SALES_DATE}` AS TIMESTAMP)"
    if sales_date_from:
        parts.append(f"{_sales_ts} >= '{sales_date_from}'")
    if sales_date_to:
        parts.append(f"{_sales_ts} <= '{sales_date_to} 23:59:59'")

    # 受注_作成日時_yyyymmdd
    if order_date_from:
        parts.append(
            f"{prefix}`{Col.ORDER_CREATED_DATE}` >= '{order_date_from:%Y%m%d}'"
        )
    if order_date_to:
        parts.append(
            f"{prefix}`{Col.ORDER_CREATED_DATE}` <= '{order_date_to:%Y%m%d}'"
        )

    return ("\n    AND " + "\n    AND ".join(parts)) if parts else ""


# ------------------------------------------------------------------
# 期間自動検出
# ------------------------------------------------------------------


def build_email_upsell_period_sql(
    company_key: str,
    period_ref_names: list[str],
) -> str:
    """period_ref_names商品の定期開始日範囲を取得するSQL.

    Returns:
        SQL文字列。結果カラム: min_date, max_date
    """
    table = _integrated_ref(company_key)
    ref_in = _in_clause(period_ref_names)

    return f"""
SELECT
  MIN(SAFE_CAST(`{Col.SUBSCRIPTION_CREATED_AT}` AS TIMESTAMP)) AS min_date,
  MAX(SAFE_CAST(`{Col.SUBSCRIPTION_CREATED_AT}` AS TIMESTAMP)) AS max_date
FROM {table}
WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` IN ({ref_in})
  AND SAFE_CAST(`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) = 1
  AND `{Col.ORDER_STATUS}` = 'shipped'
"""


# ------------------------------------------------------------------
# 顧客IDリスト（CSV出力用）
# ------------------------------------------------------------------


def build_email_denominator_ids_sql(
    company_key: str,
    denominator_names: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
    sales_date_from: str | None = None,
    sales_date_to: str | None = None,
    order_date_from: date | None = None,
    order_date_to: date | None = None,
) -> str:
    """分母（対象者）の顧客IDリストSQL."""
    table = _integrated_ref(company_key)
    den_in = _in_clause(denominator_names)
    df = _date_filter(
        date_from, date_to, sales_date_from, sales_date_to,
        order_date_from, order_date_to,
    )

    return f"""
SELECT
  customer_id,
  STRING_AGG(DISTINCT product_name, ' / ' ORDER BY product_name) AS product_name
FROM (
  SELECT DISTINCT
    `{Col.CUSTOMER_ID}` AS customer_id,
    `{Col.SUBSCRIPTION_PRODUCT_NAME}` AS product_name
  FROM {table}
  WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` IN ({den_in})
    AND SAFE_CAST(`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) = 1
    AND `{Col.ORDER_STATUS}` = 'shipped'
    AND `{Col.PAYMENT_STATUS}` = 'completed'{df}
)
GROUP BY customer_id
ORDER BY customer_id
"""


def build_email_numerator_ids_sql(
    company_key: str,
    numerator_names: list[str],
    denominator_names: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
    sales_date_from: str | None = None,
    sales_date_to: str | None = None,
    order_date_from: date | None = None,
    order_date_to: date | None = None,
) -> str:
    """分子（切替者）の顧客IDリストSQL."""
    table = _integrated_ref(company_key)
    den_in = _in_clause(denominator_names)
    num_in = _in_clause(numerator_names)
    df = _date_filter(
        date_from, date_to, sales_date_from, sales_date_to,
        order_date_from, order_date_to,
    )

    return f"""
WITH denominator_customers AS (
  SELECT DISTINCT `{Col.CUSTOMER_ID}` AS customer_id
  FROM {table}
  WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` IN ({den_in})
    AND SAFE_CAST(`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) = 1
    AND `{Col.ORDER_STATUS}` = 'shipped'
    AND `{Col.PAYMENT_STATUS}` = 'completed'{df}
),

first_switch AS (
  SELECT
    dc.customer_id,
    MIN(SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64)) AS switch_order_count,
    ANY_VALUE(a.`{Col.SUBSCRIPTION_PRODUCT_NAME}`) AS switched_product_name
  FROM denominator_customers dc
  JOIN {table} a
    ON dc.customer_id = a.`{Col.CUSTOMER_ID}`
  WHERE a.`{Col.SUBSCRIPTION_PRODUCT_NAME}` IN ({num_in})
    AND SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) > 1
    AND a.`{Col.ORDER_STATUS}` = 'shipped'
  GROUP BY 1
)

SELECT
  fs.customer_id,
  fs.switch_order_count,
  fs.switched_product_name
FROM first_switch fs
ORDER BY fs.switch_order_count, fs.customer_id
"""


# ------------------------------------------------------------------
# 全体サマリー
# ------------------------------------------------------------------


def build_email_upsell_overall_sql(
    company_key: str,
    numerator_names: list[str],
    denominator_names: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
    sales_date_from: str | None = None,
    sales_date_to: str | None = None,
    order_date_from: date | None = None,
    order_date_to: date | None = None,
) -> str:
    """メールアップセルの全体サマリーSQL.

    Returns:
        SQL文字列。結果カラム: total_denominator, total_switched, upsell_rate
    """
    table = _integrated_ref(company_key)
    den_in = _in_clause(denominator_names)
    num_in = _in_clause(numerator_names)
    df = _date_filter(
        date_from, date_to, sales_date_from, sales_date_to,
        order_date_from, order_date_to,
    )

    return f"""
WITH denominator_customers AS (
  SELECT DISTINCT `{Col.CUSTOMER_ID}` AS customer_id
  FROM {table}
  WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` IN ({den_in})
    AND SAFE_CAST(`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) = 1
    AND `{Col.ORDER_STATUS}` = 'shipped'
    AND `{Col.PAYMENT_STATUS}` = 'completed'{df}
),

first_switch AS (
  SELECT
    dc.customer_id,
    MIN(SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64)) AS switch_order_count
  FROM denominator_customers dc
  JOIN {table} a
    ON dc.customer_id = a.`{Col.CUSTOMER_ID}`
  WHERE a.`{Col.SUBSCRIPTION_PRODUCT_NAME}` IN ({num_in})
    AND SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) > 1
    AND a.`{Col.ORDER_STATUS}` = 'shipped'
  GROUP BY 1
)

SELECT
  (SELECT COUNT(*) FROM denominator_customers) AS total_denominator,
  COUNT(DISTINCT fs.customer_id) AS total_switched,
  ROUND(
    SAFE_DIVIDE(
      COUNT(DISTINCT fs.customer_id),
      (SELECT COUNT(*) FROM denominator_customers)
    ) * 100, 1
  ) AS upsell_rate
FROM first_switch fs
"""


# ------------------------------------------------------------------
# F別転換率
# ------------------------------------------------------------------


def build_email_upsell_rate_sql(
    company_key: str,
    numerator_names: list[str],
    denominator_names: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
    sales_date_from: str | None = None,
    sales_date_to: str | None = None,
    order_date_from: date | None = None,
    order_date_to: date | None = None,
) -> str:
    """F(回数)別の転換率SQL.

    Returns:
        SQL文字列。結果カラム:
        switch_order_count, switched_at_n, total_denominator, conversion_rate
    """
    table = _integrated_ref(company_key)
    den_in = _in_clause(denominator_names)
    num_in = _in_clause(numerator_names)
    df = _date_filter(
        date_from, date_to, sales_date_from, sales_date_to,
        order_date_from, order_date_to,
    )

    return f"""
WITH denominator_customers AS (
  SELECT DISTINCT `{Col.CUSTOMER_ID}` AS customer_id
  FROM {table}
  WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` IN ({den_in})
    AND SAFE_CAST(`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) = 1
    AND `{Col.ORDER_STATUS}` = 'shipped'
    AND `{Col.PAYMENT_STATUS}` = 'completed'{df}
),

first_switch AS (
  SELECT
    dc.customer_id,
    MIN(SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64)) AS switch_order_count
  FROM denominator_customers dc
  JOIN {table} a
    ON dc.customer_id = a.`{Col.CUSTOMER_ID}`
  WHERE a.`{Col.SUBSCRIPTION_PRODUCT_NAME}` IN ({num_in})
    AND SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) > 1
    AND a.`{Col.ORDER_STATUS}` = 'shipped'
  GROUP BY 1
)

SELECT
  fs.switch_order_count,
  COUNT(DISTINCT fs.customer_id) AS switched_at_n,
  (SELECT COUNT(*) FROM denominator_customers) AS total_denominator,
  ROUND(
    SAFE_DIVIDE(
      COUNT(DISTINCT fs.customer_id),
      (SELECT COUNT(*) FROM denominator_customers)
    ) * 100, 1
  ) AS conversion_rate
FROM first_switch fs
GROUP BY 1
ORDER BY 1
"""


# ------------------------------------------------------------------
# 切替後継続率
# ------------------------------------------------------------------


def build_email_upsell_retention_sql(
    company_key: str,
    numerator_names: list[str],
    denominator_names: list[str],
    max_days: int = 365,
    date_from: str | None = None,
    date_to: str | None = None,
    sales_date_from: str | None = None,
    sales_date_to: str | None = None,
    order_date_from: date | None = None,
    order_date_to: date | None = None,
    product_cycles: dict | None = None,
) -> str:
    """切り替えタイミング別継続率SQL.

    母体: 分子商品に切り替えた顧客
    行:  切り替えた回数（switch_order_count）
    列:  各定期回数での継続率

    N回目で切り替えた人 → 1〜N回目は定義上100%

    Returns:
        SQL文字列。結果カラム:
        switch_order_count, total_switched, original_product_name,
        switched_product_name, retained_1..max_n, eligible_1..max_n,
        cont_denom_1..max_n
    """
    table = _integrated_ref(company_key)
    den_in = _in_clause(denominator_names)
    num_in = _in_clause(numerator_names)
    df = _date_filter(
        date_from, date_to, sales_date_from, sales_date_to,
        order_date_from, order_date_to,
    )

    pc_cte, default_cycle2 = _product_cycles_cte(product_cycles)

    # max_days から max_n を自動算出
    min_cycle2 = default_cycle2
    if product_cycles:
        products = product_cycles.get("products", [])
        cycles = [
            p["cycle2"]
            for p in products
            if isinstance(p.get("cycle2"), (int, float)) and p["cycle2"] > 0
        ]
        if cycles:
            min_cycle2 = min(cycles)
    max_n = max_days // max(min_cycle2, 1) + 20
    max_n = min(max_n, 48)

    retained_parts = []
    for i in range(1, max_n + 1):
        retained_parts.append(
            f"COUNT(DISTINCT CASE WHEN max_shipped >= {i} AND expected_max >= {i} THEN customer_id END) AS retained_{i}"
        )
        retained_parts.append(
            f"COUNT(DISTINCT CASE WHEN expected_max >= {i} THEN customer_id END) AS eligible_{i}"
        )
        prev = i - 1
        if prev == 0:
            retained_parts.append(
                f"COUNT(DISTINCT CASE WHEN expected_max >= {i} THEN customer_id END) AS cont_denom_{i}"
            )
        else:
            retained_parts.append(
                f"COUNT(DISTINCT CASE WHEN max_shipped >= {prev} AND expected_max >= {i} THEN customer_id END) AS cont_denom_{i}"
            )
    retained_cols = ",\n    ".join(retained_parts)

    pc_prefix = f"{pc_cte},\n\n" if pc_cte else ""

    return f"""
WITH {pc_prefix}denominator_customers AS (
  SELECT DISTINCT `{Col.CUSTOMER_ID}` AS customer_id
  FROM {table}
  WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` IN ({den_in})
    AND SAFE_CAST(`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) = 1
    AND `{Col.ORDER_STATUS}` = 'shipped'
    AND `{Col.PAYMENT_STATUS}` = 'completed'{df}
),

customer_orders AS (
  SELECT
    dc.customer_id,
    SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) AS order_count,
    a.`{Col.SUBSCRIPTION_PRODUCT_NAME}` AS sub_product_name,
    a.`{Col.PRODUCT_NAME}` AS order_product_name,
    SAFE_CAST(a.`{Col.SALES_DATE}` AS TIMESTAMP) AS sales_date
  FROM denominator_customers dc
  JOIN {table} a
    ON dc.customer_id = a.`{Col.CUSTOMER_ID}`
  WHERE a.`{Col.ORDER_STATUS}` = 'shipped'
),

switched_with_timing AS (
  SELECT
    customer_id,
    MIN(order_count) AS switch_order_count
  FROM customer_orders
  WHERE sub_product_name IN ({num_in})
    AND order_count > 1
  GROUP BY 1
),

switched_max AS (
  SELECT
    s.customer_id,
    s.switch_order_count,
    MAX(co.order_count) AS max_shipped,
    MIN(CASE WHEN co.order_count = s.switch_order_count THEN co.sales_date END) AS switch_date,
    MIN(co.sales_date) AS first_order_date,
    ANY_VALUE(CASE WHEN co.order_count = s.switch_order_count THEN co.sub_product_name END) AS switched_product_name,
    ANY_VALUE(CASE WHEN co.order_count = s.switch_order_count - 1 THEN co.order_product_name END) AS original_product_name
  FROM switched_with_timing s
  JOIN customer_orders co
    ON s.customer_id = co.customer_id
  GROUP BY 1, 2
),

with_eligible AS (
  SELECT
    sm.*,
    CASE
      WHEN COALESCE(pc.cycle2, 0) > 0 THEN
        sm.switch_order_count + CAST(
          FLOOR(
            SAFE_DIVIDE(
              LEAST(
                DATE_DIFF(CURRENT_DATE(), DATE(sm.switch_date), DAY),
                GREATEST({max_days} - DATE_DIFF(DATE(sm.switch_date), DATE(sm.first_order_date), DAY), 0)
              ),
              pc.cycle2
            )
          ) AS INT64
        )
      ELSE sm.max_shipped
    END AS expected_max
  FROM switched_max sm
  LEFT JOIN {"product_cycles" if pc_cte else "(SELECT CAST(NULL AS STRING) AS name, CAST(NULL AS INT64) AS cycle1, CAST(NULL AS INT64) AS cycle2)"} pc
    ON sm.switched_product_name = pc.name
)

SELECT
  switch_order_count,
  COUNT(DISTINCT customer_id) AS total_switched,
  APPROX_TOP_COUNT(original_product_name, 1)[OFFSET(0)].value AS original_product_name,
  APPROX_TOP_COUNT(switched_product_name, 1)[OFFSET(0)].value AS switched_product_name,
  {retained_cols}
FROM with_eligible
GROUP BY 1
ORDER BY 1
"""


# ------------------------------------------------------------------
# 未登録商品検出
# ------------------------------------------------------------------


def build_email_upsell_unmatched_products_sql(
    company_key: str,
    numerator_names: list[str],
    denominator_names: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
    sales_date_from: str | None = None,
    sales_date_to: str | None = None,
    order_date_from: date | None = None,
    order_date_to: date | None = None,
    product_cycles: dict | None = None,
) -> str:
    """商品マスタに未登録の切替先商品名を検出するSQL.

    Returns:
        SQL文字列。結果カラム: switched_product_name, customer_count
    """
    table = _integrated_ref(company_key)
    den_in = _in_clause(denominator_names)
    num_in = _in_clause(numerator_names)
    df = _date_filter(
        date_from, date_to, sales_date_from, sales_date_to,
        order_date_from, order_date_to,
    )

    pc_cte, _ = _product_cycles_cte(product_cycles)
    pc_prefix = f"{pc_cte},\n\n" if pc_cte else ""

    return f"""
WITH {pc_prefix}denominator_customers AS (
  SELECT DISTINCT `{Col.CUSTOMER_ID}` AS customer_id
  FROM {table}
  WHERE `{Col.SUBSCRIPTION_PRODUCT_NAME}` IN ({den_in})
    AND SAFE_CAST(`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) = 1
    AND `{Col.ORDER_STATUS}` = 'shipped'
    AND `{Col.PAYMENT_STATUS}` = 'completed'{df}
),

switched_products AS (
  SELECT DISTINCT
    a.`{Col.SUBSCRIPTION_PRODUCT_NAME}` AS switched_product_name,
    dc.customer_id
  FROM denominator_customers dc
  JOIN {table} a
    ON dc.customer_id = a.`{Col.CUSTOMER_ID}`
  WHERE a.`{Col.SUBSCRIPTION_PRODUCT_NAME}` IN ({num_in})
    AND SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) > 1
    AND a.`{Col.ORDER_STATUS}` = 'shipped'
)

SELECT
  sp.switched_product_name,
  COUNT(DISTINCT sp.customer_id) AS customer_count
FROM switched_products sp
LEFT JOIN {"product_cycles" if pc_cte else "(SELECT CAST(NULL AS STRING) AS name)"} pc
  ON sp.switched_product_name = pc.name
WHERE pc.name IS NULL
GROUP BY 1
ORDER BY 2 DESC
"""
