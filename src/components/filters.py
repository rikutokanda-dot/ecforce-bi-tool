"""コホート分析用フィルタUI.

カスケードフィルタ: 親フィルタの選択に応じて子フィルタの候補を絞り込む。
階層: 商品カテゴリ → 広告グループ → 広告URLパラメータ → 定期商品名
"""

from __future__ import annotations

import streamlit as st

from src.bigquery_client import fetch_filter_options, fetch_filtered_options, get_bigquery_client
from src.constants import Col
from src.queries.common import get_table_ref
from src.session import SessionKey


def render_cohort_filters(company_key: str) -> dict:
    """コホート分析用のカスケードフィルタUIを描画し、選択値を返す."""
    client = get_bigquery_client()
    table_ref = get_table_ref(company_key)

    st.subheader("フィルタ")

    # 永続化された選択値を読み込み
    persisted_cats = st.session_state.get(SessionKey.FILTER_CATEGORIES, [])
    persisted_ad_groups = st.session_state.get(SessionKey.FILTER_AD_GROUPS, [])
    persisted_ad_url_params = st.session_state.get(SessionKey.FILTER_AD_URLS, [])
    persisted_products = st.session_state.get(SessionKey.FILTER_PRODUCT_NAMES, [])

    # Level 1: 商品カテゴリ (親なし)
    categories = fetch_filter_options(client, table_ref, Col.PRODUCT_CATEGORY)
    default_cats = [c for c in persisted_cats if c in categories]
    selected_categories = st.multiselect(
        "商品カテゴリ", categories, default=default_cats, key="filter_categories"
    )
    st.session_state[SessionKey.FILTER_CATEGORIES] = selected_categories

    # Level 2: 広告グループ (カテゴリで絞り込み)
    parent_l2 = {}
    if selected_categories:
        parent_l2[Col.PRODUCT_CATEGORY] = selected_categories
    ad_groups = fetch_filtered_options(
        client, table_ref, Col.AD_GROUP, parent_l2 or None
    )
    default_ag = [g for g in persisted_ad_groups if g in ad_groups]
    selected_ad_groups = st.multiselect(
        "広告グループ", ad_groups, default=default_ag, key="filter_ad_groups"
    )
    st.session_state[SessionKey.FILTER_AD_GROUPS] = selected_ad_groups

    # Level 3: 広告URLパラメータ (カテゴリ + 広告グループで絞り込み)
    parent_l3 = {}
    if selected_categories:
        parent_l3[Col.PRODUCT_CATEGORY] = selected_categories
    if selected_ad_groups:
        parent_l3[Col.AD_GROUP] = selected_ad_groups
    ad_url_params: list[str] = fetch_filtered_options(
        client, table_ref, Col.AD_URL_PARAM, parent_l3 or None
    )

    default_ad_urls = [p for p in persisted_ad_url_params if p in ad_url_params]
    selected_ad_url_params = st.multiselect(
        "広告URLパラメータ", ad_url_params, default=default_ad_urls, key="filter_ad_urls"
    )
    st.session_state[SessionKey.FILTER_AD_URLS] = selected_ad_url_params

    # Level 4: 定期商品名 (カテゴリ + 広告グループ + 広告URLパラメータで絞り込み)
    parent_l4 = {}
    if selected_categories:
        parent_l4[Col.PRODUCT_CATEGORY] = selected_categories
    if selected_ad_groups:
        parent_l4[Col.AD_GROUP] = selected_ad_groups
    if selected_ad_url_params:
        parent_l4[Col.AD_URL_PARAM] = selected_ad_url_params
    product_names = fetch_filtered_options(
        client, table_ref, Col.SUBSCRIPTION_PRODUCT_NAME, parent_l4 or None
    )
    default_pn = [p for p in persisted_products if p in product_names]
    selected_product_names = st.multiselect(
        "定期商品名", product_names, default=default_pn, key="filter_product_names"
    )
    st.session_state[SessionKey.FILTER_PRODUCT_NAMES] = selected_product_names

    return {
        "product_categories": selected_categories or None,
        "ad_groups": selected_ad_groups or None,
        "ad_url_params": selected_ad_url_params or None,
        "product_names": selected_product_names or None,
    }
