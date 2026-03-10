"""チラシ分析用SQLクエリビルダー.

チラシ同梱受注 × chirashi_config(設定) × all_integrated を結合し、
アップセル率 / 切り替えタイミング別継続率を算出する。

データフロー:
  chirashi_master (VIEW, スプシ自動同期) = 受注番号一覧
  chirashi_config (外部テーブル, スプシ自動同期) = チラシ名→切替先商品マッピング
  all_integrated = 全受注データ
"""

from __future__ import annotations

import math
from datetime import date, timedelta

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


def _date_filter(
    date_from: date | None = None,
    date_to: date | None = None,
    alias: str = "a",
) -> str:
    """受注日（受注_作成日時_yyyymmdd）の日付フィルタ句を生成."""
    parts = []
    if date_from:
        parts.append(
            f"{alias}.`{Col.ORDER_CREATED_DATE}` >= '{date_from:%Y%m%d}'"
        )
    if date_to:
        parts.append(
            f"{alias}.`{Col.ORDER_CREATED_DATE}` <= '{date_to:%Y%m%d}'"
        )
    return ("\n    AND " + "\n    AND ".join(parts)) if parts else ""


def build_chirashi_upsell_rate_sql(
    company_key: str,
    date_from: date | None = None,
    date_to: date | None = None,
) -> str:
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
    df = _date_filter(date_from, date_to)

    return f"""
WITH chirashi_all AS (
  -- チラシ投函受注（重複排除前の全レコード）
  SELECT
    c.chirashi_name,
    a.`{Col.CUSTOMER_ID}` AS customer_id,
    SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) AS order_count,
    cfg.target_product
  FROM {chirashi} c
  JOIN {config} cfg
    ON c.chirashi_name = cfg.chirashi_name
    AND cfg.company = '{company_key}'
  JOIN {integrated} a
    ON c.chirashi_order_number = a.`受注_受注番号`
  WHERE cfg.target_product IS NOT NULL
    AND TRIM(cfg.target_product) != ''
    AND a.`{Col.ORDER_STATUS}` = 'shipped'{df}
),

chirashi_recipients AS (
  -- 顧客ごと重複除外
  SELECT
    chirashi_name,
    customer_id,
    MIN(order_count) AS chirashi_order_count,
    ANY_VALUE(target_product) AS target_product
  FROM chirashi_all
  GROUP BY 1, 2
),

delivery_counts AS (
  -- チラシ名ごとの投函数（重複含む全配布数）
  SELECT chirashi_name, COUNT(*) AS total_deliveries
  FROM chirashi_all
  GROUP BY 1
),

switched AS (
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
  ANY_VALUE(dc.total_deliveries) AS total_deliveries,
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
LEFT JOIN delivery_counts dc
  ON cr.chirashi_name = dc.chirashi_name
GROUP BY 1
ORDER BY 1
"""


def build_chirashi_frequency_rate_sql(
    company_key: str,
    date_from: date | None = None,
    date_to: date | None = None,
) -> str:
    """F(回数)別転換率を算出するSQL.

    各顧客を1回だけカウント（最初のチラシ基準、最初の切替回数で分類）。
    継続率タブと同じ定義を使用し、人数が一致する。

    Returns:
        SQL文字列。結果カラム:
        chirashi_name, order_count, total_at_n, switched_at_next, conversion_rate
    """
    chirashi = _chirashi_ref(company_key)
    config = _config_ref(company_key)
    integrated = _integrated_ref(company_key)
    df = _date_filter(date_from, date_to)

    return f"""
WITH chirashi_orders AS (
  -- チラシ投函受注（投函回ごと、重複排除しない → 投函数の母体）
  SELECT
    c.chirashi_name,
    a.`{Col.CUSTOMER_ID}` AS customer_id,
    SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) AS order_count,
    cfg.target_product
  FROM {chirashi} c
  JOIN {config} cfg
    ON c.chirashi_name = cfg.chirashi_name
    AND cfg.company = '{company_key}'
  JOIN {integrated} a
    ON c.chirashi_order_number = a.`受注_受注番号`
  WHERE cfg.target_product IS NOT NULL
    AND TRIM(cfg.target_product) != ''
    AND a.`{Col.ORDER_STATUS}` = 'shipped'{df}
),

-- 顧客ごと最初のチラシ（切替判定の基準、継続率タブと同じ定義）
chirashi_recipients AS (
  SELECT
    chirashi_name,
    customer_id,
    MIN(order_count) AS chirashi_order_count,
    ANY_VALUE(target_product) AS target_product
  FROM chirashi_orders
  GROUP BY 1, 2
),

customer_orders AS (
  SELECT
    cr.chirashi_name,
    cr.customer_id,
    cr.chirashi_order_count,
    cr.target_product,
    SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) AS order_count,
    a.`{Col.SUBSCRIPTION_PRODUCT_NAME}` AS sub_product_name
  FROM chirashi_recipients cr
  JOIN {integrated} a
    ON cr.customer_id = a.`{Col.CUSTOMER_ID}`
  WHERE a.`{Col.ORDER_STATUS}` = 'shipped'
),

-- 各顧客の最初の切替回数（継続率タブと同じ定義）
first_switch AS (
  SELECT
    chirashi_name,
    customer_id,
    MIN(order_count) AS switch_order_count
  FROM customer_orders
  WHERE order_count > chirashi_order_count
    AND EXISTS (
      SELECT 1 FROM UNNEST(SPLIT(target_product, ',')) AS tp
      WHERE STRPOS(sub_product_name, TRIM(tp)) > 0
    )
  GROUP BY 1, 2
),

-- 各切替を直前のチラシ投函に紐付け（最も近い投函回）
attributed AS (
  SELECT
    co.chirashi_name,
    fs.customer_id,
    MAX(co.order_count) AS attributed_chirashi_order
  FROM chirashi_orders co
  JOIN first_switch fs
    ON co.chirashi_name = fs.chirashi_name
    AND co.customer_id = fs.customer_id
    AND co.order_count < fs.switch_order_count
  GROUP BY 1, 2
)

SELECT
  co.chirashi_name,
  co.order_count,
  COUNT(DISTINCT co.customer_id) AS total_at_n,
  COUNT(DISTINCT a.customer_id) AS switched_at_next,
  ROUND(
    SAFE_DIVIDE(
      COUNT(DISTINCT a.customer_id),
      COUNT(DISTINCT co.customer_id)
    ) * 100, 1
  ) AS conversion_rate
FROM chirashi_orders co
LEFT JOIN attributed a
  ON co.chirashi_name = a.chirashi_name
  AND co.order_count = a.attributed_chirashi_order
GROUP BY 1, 2
ORDER BY 1, 2
"""


def _product_cycles_cte(product_cycles: dict | None) -> tuple[str, int]:
    """商品サイクルマスタをSQL CTEとして生成.

    Returns:
        (CTE SQL文字列, デフォルトcycle2)
    """
    if not product_cycles:
        return "", 30

    products = product_cycles.get("products", [])
    defaults = product_cycles.get("defaults", {})
    dc2 = defaults.get("cycle2", 30)

    if not products:
        return "", dc2

    rows = []
    for p in products:
        # cycle2キーが存在しない / None / NaN → CTEに含めない（NULLマッチ→実績ベース）
        c2_val = p.get("cycle2")
        if c2_val is None or (isinstance(c2_val, float) and math.isnan(c2_val)):
            continue
        c1_val = p.get("cycle1")
        if c1_val is None or (isinstance(c1_val, float) and math.isnan(c1_val)):
            c1_val = c2_val  # cycle1未設定ならcycle2をフォールバック
        name = p["name"].replace("'", "''")
        c1 = int(c1_val)
        c2 = int(c2_val)
        rows.append(
            f"STRUCT('{name}' AS name, {c1} AS cycle1, {c2} AS cycle2)"
        )
    if not rows:
        return "", dc2

    cte = "product_cycles AS (\n  SELECT * FROM UNNEST([\n    "
    cte += ",\n    ".join(rows)
    cte += "\n  ])\n)"
    return cte, dc2


def build_chirashi_unmatched_products_sql(
    company_key: str,
    chirashi_name: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    product_cycles: dict | None = None,
) -> str:
    """商品マスタに未登録の切替先商品名を検出するSQL.

    Returns:
        SQL文字列。結果カラム: switched_product_name, customer_count
    """
    chirashi = _chirashi_ref(company_key)
    config = _config_ref(company_key)
    integrated = _integrated_ref(company_key)
    df = _date_filter(date_from, date_to)

    chirashi_filter = ""
    if chirashi_name:
        chirashi_filter = f"AND c.chirashi_name = '{chirashi_name}'"

    pc_cte, _ = _product_cycles_cte(product_cycles)
    pc_prefix = f"{pc_cte},\n\n" if pc_cte else ""

    return f"""
WITH {pc_prefix}chirashi_recipients AS (
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
    AND a.`{Col.ORDER_STATUS}` = 'shipped'{df}
    {chirashi_filter}
  GROUP BY 1, 2
),
switched_products AS (
  SELECT DISTINCT
    a.`{Col.SUBSCRIPTION_PRODUCT_NAME}` AS switched_product_name,
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
  sp.switched_product_name,
  COUNT(DISTINCT sp.customer_id) AS customer_count
FROM switched_products sp
LEFT JOIN product_cycles pc
  ON sp.switched_product_name = pc.name
WHERE pc.name IS NULL
GROUP BY 1
ORDER BY 2 DESC
"""


def build_chirashi_retention_sql(
    company_key: str,
    chirashi_name: str | None = None,
    max_days: int = 365,
    date_from: date | None = None,
    date_to: date | None = None,
    product_cycles: dict | None = None,
) -> str:
    """切り替えタイミング別継続率を算出するSQL.

    母体: アップセル商品に切り替えた顧客
    行:  切り替えた回数（何回目の注文で切り替えたか）
    列:  各定期回数での継続率

    3回目で切り替えた人 → 1〜3回目は自動的に100%

    eligible判定は商品マスタのcycle2を使用（初回起算の累計日数ベース）:
      残り日数 = max_days - (切替日 - 初回日)
      expected_max = switch_order_count + FLOOR(残り日数 / cycle2)

    Args:
        company_key: 会社キー
        chirashi_name: フィルタするチラシ名（Noneなら全チラシ）
        max_days: 期間（日数）。この期間内で到達可能な回数まで表示。
        date_from: 受注日フィルタ開始
        date_to: 受注日フィルタ終了
        product_cycles: 商品サイクルマスタ (load_product_cycles()の戻り値)

    Returns:
        SQL文字列。結果カラム:
        chirashi_name, switch_order_count, total_switched, retained_1..max_n
    """
    chirashi = _chirashi_ref(company_key)
    config = _config_ref(company_key)
    integrated = _integrated_ref(company_key)
    df = _date_filter(date_from, date_to)

    chirashi_filter = ""
    if chirashi_name:
        chirashi_filter = f"AND c.chirashi_name = '{chirashi_name}'"

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
    max_n = max_days // max(min_cycle2, 1) + 20  # +20 for switch_order_count offset
    max_n = min(max_n, 48)

    retained_parts = []
    for i in range(1, max_n + 1):
        retained_parts.append(
            f"COUNT(DISTINCT CASE WHEN max_shipped >= {i} AND expected_max >= {i} THEN customer_id END) AS retained_{i}"
        )
        retained_parts.append(
            f"COUNT(DISTINCT CASE WHEN expected_max >= {i} THEN customer_id END) AS eligible_{i}"
        )
        # 継続率の分母: N-1回到達済み かつ N回目が発送され得る人
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

    # product_cycles CTE がある場合は先頭に追加
    pc_prefix = f"{pc_cte},\n\n" if pc_cte else ""

    return f"""
WITH {pc_prefix}chirashi_recipients AS (
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
    AND a.`{Col.ORDER_STATUS}` = 'shipped'{df}
    {chirashi_filter}
  GROUP BY 1, 2
),

-- all_integratedへのJOINは1回だけ: 必要カラムのみ取得してCTEに保持
customer_orders AS (
  SELECT
    cr.chirashi_name,
    cr.customer_id,
    cr.chirashi_order_count,
    cr.target_product,
    SAFE_CAST(a.`{Col.ORDER_SUBSCRIPTION_COUNT}` AS INT64) AS order_count,
    a.`{Col.SUBSCRIPTION_PRODUCT_NAME}` AS sub_product_name,
    a.`{Col.PRODUCT_NAME}` AS order_product_name,
    SAFE_CAST(a.`{Col.SALES_DATE}` AS TIMESTAMP) AS sales_date
  FROM chirashi_recipients cr
  JOIN {integrated} a
    ON cr.customer_id = a.`{Col.CUSTOMER_ID}`
  WHERE a.`{Col.ORDER_STATUS}` = 'shipped'
),

switched_with_timing AS (
  SELECT
    chirashi_name,
    customer_id,
    MIN(order_count) AS switch_order_count
  FROM customer_orders
  WHERE order_count > chirashi_order_count
    AND EXISTS (
      SELECT 1 FROM UNNEST(SPLIT(target_product, ',')) AS tp
      WHERE STRPOS(sub_product_name, TRIM(tp)) > 0
    )
  GROUP BY 1, 2
),

switched_max AS (
  SELECT
    s.chirashi_name,
    s.customer_id,
    s.switch_order_count,
    MAX(co.order_count) AS max_shipped,
    MIN(CASE WHEN co.order_count = s.switch_order_count THEN co.sales_date END) AS switch_date,
    MIN(co.sales_date) AS first_order_date,
    ANY_VALUE(CASE WHEN co.order_count = s.switch_order_count THEN co.sub_product_name END) AS switched_product_name,
    ANY_VALUE(CASE WHEN co.order_count = s.switch_order_count - 1 THEN co.order_product_name END) AS original_product_name
  FROM switched_with_timing s
  JOIN customer_orders co
    ON s.chirashi_name = co.chirashi_name
    AND s.customer_id = co.customer_id
  GROUP BY 1, 2, 3
),

with_eligible AS (
  SELECT
    sm.*,
    COALESCE(pc_pre.cycle1, 0) AS pre_cycle1,
    COALESCE(pc_pre.cycle2, 0) AS pre_cycle2,
    COALESCE(pc.cycle1, 0) AS post_cycle1,
    COALESCE(pc.cycle2, 0) AS rep_cycle2,
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
  LEFT JOIN product_cycles pc
    ON sm.switched_product_name = pc.name
  LEFT JOIN product_cycles pc_pre
    ON sm.original_product_name = pc_pre.name
)

SELECT
  chirashi_name,
  switch_order_count,
  COUNT(DISTINCT customer_id) AS total_switched,
  APPROX_TOP_COUNT(original_product_name, 1)[OFFSET(0)].value AS original_product_name,
  APPROX_TOP_COUNT(switched_product_name, 1)[OFFSET(0)].value AS switched_product_name,
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
