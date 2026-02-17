"""共通クエリヘルパー: テーブル参照、WHERE句生成."""

from __future__ import annotations

from src.config_loader import get_company_keys
from src.constants import Col, PROJECT_ID


def get_table_ref(company_key: str) -> str:
    """テーブル参照文字列を生成. ホワイトリスト検証付き."""
    allowed = get_company_keys()
    if company_key not in allowed:
        raise ValueError(f"不明な会社キー: {company_key}")
    dataset = f"{company_key}_ecforce_raw_data"
    table = f"{company_key}_all_integrated"
    return f"`{PROJECT_ID}.{dataset}.{table}`"


def build_filter_clause(
    date_from: str | None = None,
    date_to: str | None = None,
    product_categories: list[str] | None = None,
    ad_groups: list[str] | None = None,
    product_names: list[str] | None = None,
) -> str:
    """共通のWHERE句フィルタを構築.

    Returns:
        "AND ..." 形式のフィルタ文字列。呼び出し側でWHEREの後に結合する。
    """
    clauses = []

    if date_from:
        clauses.append(
            f"AND `{Col.SUBSCRIPTION_CREATED_AT}` >= '{date_from}'"
        )
    if date_to:
        clauses.append(
            f"AND `{Col.SUBSCRIPTION_CREATED_AT}` <= '{date_to}'"
        )

    if product_categories:
        conditions = " OR ".join(
            f"`{Col.PRODUCT_CATEGORY}` = '{c}'" for c in product_categories
        )
        clauses.append(f"AND ({conditions})")

    if ad_groups:
        conditions = " OR ".join(
            f"`{Col.AD_GROUP}` = '{g}'" for g in ad_groups
        )
        clauses.append(f"AND ({conditions})")

    if product_names:
        conditions = " OR ".join(
            f"`{Col.SUBSCRIPTION_PRODUCT_NAME}` = '{n}'" for n in product_names
        )
        clauses.append(f"AND ({conditions})")

    return "\n      ".join(clauses)
