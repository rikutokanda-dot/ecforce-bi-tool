"""コホート分析用フィルタUI.

カスケードフィルタ: 親フィルタの選択に応じて子フィルタの候補を絞り込む。
階層: 商品カテゴリ → 広告グループ → 定期商品名
"""

from __future__ import annotations

import streamlit as st

from src.bigquery_client import fetch_filter_options, fetch_filtered_options, get_bigquery_client
from src.constants import Col, DRILLDOWN_OPTIONS
from src.queries.common import get_table_ref


def render_cohort_filters(company_key: str) -> dict:
    """コホート分析用のカスケードフィルタUIを描画し、選択値を返す."""
    client = get_bigquery_client()
    table_ref = get_table_ref(company_key)

    st.subheader("フィルタ")

    # Level 1: 商品カテゴリ (親なし)
    categories = fetch_filter_options(client, table_ref, Col.PRODUCT_CATEGORY)
    selected_categories = st.multiselect(
        "商品カテゴリ", categories, key="filter_categories"
    )

    # Level 2: 広告グループ (カテゴリで絞り込み)
    parent_l2 = {}
    if selected_categories:
        parent_l2[Col.PRODUCT_CATEGORY] = selected_categories
    ad_groups = fetch_filtered_options(
        client, table_ref, Col.AD_GROUP, parent_l2 or None
    )
    selected_ad_groups = st.multiselect(
        "広告グループ", ad_groups, key="filter_ad_groups"
    )

    # Level 3: 定期商品名 (カテゴリ + 広告グループで絞り込み)
    parent_l3 = {}
    if selected_categories:
        parent_l3[Col.PRODUCT_CATEGORY] = selected_categories
    if selected_ad_groups:
        parent_l3[Col.AD_GROUP] = selected_ad_groups
    product_names = fetch_filtered_options(
        client, table_ref, Col.SUBSCRIPTION_PRODUCT_NAME, parent_l3 or None
    )
    selected_product_names = st.multiselect(
        "定期商品名", product_names, key="filter_product_names"
    )

    st.divider()

    # ドリルダウン軸 (デフォルト: 定期商品名 = 先頭)
    drilldown_label = st.radio(
        "ドリルダウン軸",
        list(DRILLDOWN_OPTIONS.keys()),
        horizontal=True,
        key="filter_drilldown",
    )
    drilldown_column = DRILLDOWN_OPTIONS[drilldown_label]

    return {
        "product_categories": selected_categories or None,
        "ad_groups": selected_ad_groups or None,
        "product_names": selected_product_names or None,
        "drilldown_column": drilldown_column,
    }
