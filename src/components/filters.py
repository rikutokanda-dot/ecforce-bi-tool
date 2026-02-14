"""分析ページ共通のフィルタコンポーネント."""

import streamlit as st

from src.bigquery_client import fetch_filter_options, get_bigquery_client
from src.constants import Col, DRILLDOWN_OPTIONS
from src.queries.common import get_table_ref


def render_cohort_filters(company_key: str) -> dict:
    """コホート分析用のフィルタUIを描画し、選択値を返す."""
    client = get_bigquery_client()
    table_ref = get_table_ref(company_key)

    st.subheader("フィルタ")

    # 商品カテゴリ
    categories = fetch_filter_options(client, table_ref, Col.PRODUCT_CATEGORY)
    selected_categories = st.multiselect(
        "商品カテゴリ", categories, key="filter_categories"
    )

    # 広告グループ
    ad_groups = fetch_filter_options(client, table_ref, Col.AD_GROUP)
    selected_ad_groups = st.multiselect(
        "広告グループ", ad_groups, key="filter_ad_groups"
    )

    # 商品名
    product_names = fetch_filter_options(client, table_ref, Col.PRODUCT_NAME)
    selected_product_names = st.multiselect(
        "定期商品名", product_names, key="filter_product_names"
    )

    st.divider()

    # ドリルダウン軸
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
