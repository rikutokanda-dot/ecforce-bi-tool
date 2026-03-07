"""チラシ分析用SQLクエリビルダー.

チラシ同梱受注 × chirashi_config(設定) × all_integrated を結合し、
アップセル率 / 切り替えタイミング別継続率を算出する。

データフロー:
  chirashi_master (VIEW, スプシ自動同期) = 受注番号一覧
  chirashi_config (外部テーブル, スプシ自動同期) = チラシ名→切替先商品マッピング
  all_integrated = 全受注データ
"""

from __future__ import annotations

from src.constants import Col, PROJECT_ID


def _chirashi_ref(company_key: str) -> str:
    """chirashi_master VIEW のテーブル参照."""
    dataset = f"{company_key}_ecforce_raw_data"
    return f"`{PROJECT_ID}.{dataset}.chirashi_master`"


def _config_ref(company_key: str) -> str:
    """chirashi_config 外部テーブルの参照."""
    dataset = f"{company_key}_ecforce_raw_data"
    return f"`{PROJECT_ID}.{dataset}.chirashi_config`"


def _integrated_ref(company_key: str) -> str:
    """all_integrated テーブルの参照."""
    dataset = f"{company_key}_ecforce_raw_data"
    table = f"{company_key}_all_integrated"
    return f"`{PROJECT_ID}.{dataset}.{table}`"


def build_chirashi_upsell_rate_sql(company_key: str) -> str:
    """チラシ別アップセル率を算出するSQL.

    分母: チラシを送った顧客数（顧客_id DISTINCT）
    分子: そのうちチラシ同梱回以降にターゲット商品に切り替えた顧客数

    Returns:
        SQL文字列。結果カラム:
        chirashi_name, total_recipients, switched_count, upsell_rate
    """
    chirashi = _chirashi_ref(company_key)
    config = _config_ref(company_key)
    integrated = _integrated_ref(company_key)

    return f"""
WITH chirashi_recipients AS (
  -- チラシを送った顧客（顧客ID × チラシ名で重複除外）
  SELECT
    c.chirashi_name,
    a.`{Col.CUSTOMER_ID}` AS customer_id,
    MIN(SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64)) AS chirashi_order_count,
    ANY_VALUE(cfg.target_product) AS target_product
  FROM {chirashi} c
  JOIN {config} cfg
    ON c.chirashi_name = cfg.chirashi_name
    AND cfg.company = '{company_key}'
  JOIN {integrated} a
    ON c.chirashi_order_number = a.`受注_受注番号`
  WHERE cfg.target_product IS NOT NULL
    AND TRIM(cfg.target_product) != ''
    AND a.`{Col.ORDER_STATUS}` = 'shipped'
  GROUP BY 1, 2
),

switched AS (
  -- チラシ同梱回より後の回でターゲット商品に切り替えた顧客
  SELECT DISTINCT
    cr.chirashi_name,
    cr.customer_id
  FROM chirashi_recipients cr
  JOIN {integrated} a
    ON cr.customer_id = a.`{Col.CUSTOMER_ID}`
  WHERE EXISTS (
    SELECT 1 FROM UNNEST(SPLIT(cr.target_product, ',')) AS tp
    WHERE STRPOS(a.`{Col.SUBSCRIPTION_PRODUCT_NAME}`, TRIM(tp)) > 0
  )
  AND SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) > cr.chirashi_order_count
  AND a.`{Col.ORDER_STATUS}` = 'shipped'
)

SELECT
  cr.chirashi_name,
  COUNT(DISTINCT cr.customer_id) AS total_recipients,
  COUNT(DISTINCT s.customer_id)  AS switched_count,
  ROUND(
    SAFE_DIVIDE(
      COUNT(DISTINCT s.customer_id),
      COUNT(DISTINCT cr.customer_id)
    ) * 100, 1
  ) AS upsell_rate
FROM chirashi_recipients cr
LEFT JOIN switched s
  ON cr.chirashi_name = s.chirashi_name
  AND cr.customer_id = s.customer_id
GROUP BY 1
ORDER BY 1
"""


def build_chirashi_retention_sql(
    company_key: str,
    chirashi_name: str | None = None,
    max_n: int = 24,
) -> str:
    """切り替えタイミング別継続率を算出するSQL.

    母体: アップセル商品に切り替えた顧客
    行:  切り替えた回数（何回目の注文で切り替えたか）
    列:  各定期回数での継続率

    3回目で切り替えた人 → 1〜3回目は自動的に100%

    Args:
        company_key: 会社キー
        chirashi_name: フィルタするチラシ名（Noneなら全チラシ）
        max_n: 最大定期回数

    Returns:
        SQL文字列。結果カラム:
        chirashi_name, switch_order_count, total_switched, retained_1..max_n
    """
    chirashi = _chirashi_ref(company_key)
    config = _config_ref(company_key)
    integrated = _integrated_ref(company_key)

    chirashi_filter = ""
    if chirashi_name:
        chirashi_filter = f"AND c.chirashi_name = '{chirashi_name}'"

    retained_cols = ",\n    ".join(
        f"COUNT(DISTINCT CASE WHEN max_shipped >= {i} THEN customer_id END) AS retained_{i},\n"
        f"    COUNT(DISTINCT CASE WHEN expected_max >= {i} THEN customer_id END) AS eligible_{i}"
        for i in range(1, max_n + 1)
    )

    return f"""
WITH chirashi_recipients AS (
  SELECT
    c.chirashi_name,
    a.`{Col.CUSTOMER_ID}` AS customer_id,
    MIN(SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64)) AS chirashi_order_count,
    ANY_VALUE(cfg.target_product) AS target_product
  FROM {chirashi} c
  JOIN {config} cfg
    ON c.chirashi_name = cfg.chirashi_name
    AND cfg.company = '{company_key}'
  JOIN {integrated} a
    ON c.chirashi_order_number = a.`受注_受注番号`
  WHERE cfg.target_product IS NOT NULL
    AND TRIM(cfg.target_product) != ''
    AND a.`{Col.ORDER_STATUS}` = 'shipped'
    {chirashi_filter}
  GROUP BY 1, 2
),

switched_with_timing AS (
  SELECT
    cr.chirashi_name,
    cr.customer_id,
    MIN(SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64)) AS switch_order_count
  FROM chirashi_recipients cr
  JOIN {integrated} a
    ON cr.customer_id = a.`{Col.CUSTOMER_ID}`
  WHERE EXISTS (
    SELECT 1 FROM UNNEST(SPLIT(cr.target_product, ',')) AS tp
    WHERE STRPOS(a.`{Col.SUBSCRIPTION_PRODUCT_NAME}`, TRIM(tp)) > 0
  )
  AND SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) > cr.chirashi_order_count
  AND a.`{Col.ORDER_STATUS}` = 'shipped'
  GROUP BY 1, 2
),

switched_max AS (
  SELECT
    s.chirashi_name,
    s.customer_id,
    s.switch_order_count,
    MAX(SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64)) AS max_shipped,
    MIN(SAFE_CAST(a.`{Col.SUBSCRIPTION_CREATED_AT}` AS TIMESTAMP)) AS sub_created,
    MAX(SAFE_CAST(a.`{Col.SALES_DATE}` AS TIMESTAMP)) AS last_order_date
  FROM switched_with_timing s
  JOIN {integrated} a
    ON s.customer_id = a.`{Col.CUSTOMER_ID}`
  WHERE a.`{Col.ORDER_STATUS}` = 'shipped'
  GROUP BY 1, 2, 3
),

with_eligible AS (
  SELECT
    *,
    CAST(
      FLOOR(
        SAFE_DIVIDE(
          DATE_DIFF(CURRENT_DATE(), DATE(sub_created), DAY),
          GREATEST(
            SAFE_DIVIDE(
              DATE_DIFF(DATE(last_order_date), DATE(sub_created), DAY),
              GREATEST(max_shipped - 1, 1)
            ),
            1
          )
        )
      ) + 1 AS INT64
    ) AS expected_max
  FROM switched_max
)

SELECT
  chirashi_name,
  switch_order_count,
  COUNT(DISTINCT customer_id) AS total_switched,
  {retained_cols}
FROM with_eligible
GROUP BY 1, 2
ORDER BY 1, 2
"""


def build_chirashi_list_sql(company_key: str) -> str:
    """ターゲット商品が設定されたチラシ名の一覧を取得するSQL.

    Returns:
        SQL文字列。結果カラム: chirashi_name
    """
    config = _config_ref(company_key)

    return f"""
SELECT DISTINCT chirashi_name
FROM {config}
WHERE company = '{company_key}'
  AND target_product IS NOT NULL
  AND TRIM(target_product) != ''
ORDER BY chirashi_name
"""


def build_chirashi_config_sql(company_key: str) -> str:
    """チラシ設定（チラシ名→ターゲット商品）の一覧を取得するSQL.

    Returns:
        SQL文字列。結果カラム: chirashi_name, target_product
    """
    config = _config_ref(company_key)

    return f"""
SELECT DISTINCT chirashi_name, target_product
FROM {config}
WHERE company = '{company_key}'
  AND target_product IS NOT NULL
  AND TRIM(target_product) != ''
ORDER BY chirashi_name
"""
