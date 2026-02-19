"""コホート分析用フィルタUI.

カスケードフィルタ: 親フィルタの選択に応じて子フィルタの候補を絞り込む。
階層: 商品カテゴリ → 広告グループ → 広告URL → 定期商品名
"""

from __future__ import annotations

import streamlit as st

from src.bigquery_client import fetch_filter_options, fetch_filtered_options, get_bigquery_client
from src.config_loader import get_ad_url_display_map
from src.constants import Col
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

    # Level 3: 広告URL (カテゴリ + 広告グループで絞り込み)
    # マスタの表示名があれば「名前 (ID)」形式で表示、なければIDのまま
    parent_l3 = {}
    if selected_categories:
        parent_l3[Col.PRODUCT_CATEGORY] = selected_categories
    if selected_ad_groups:
        parent_l3[Col.AD_GROUP] = selected_ad_groups
    raw_ad_url_ids: list[str] = fetch_filtered_options(
        client, table_ref, Col.AD_URL, parent_l3 or None
    )

    # マスタから表示名マップ取得
    display_map = get_ad_url_display_map()  # {id: name}

    # 表示ラベルを作成: 名前あり→「名前 (ID)」、なし→ID
    display_labels: list[str] = []
    label_to_id: dict[str, str] = {}
    for aid in raw_ad_url_ids:
        name = display_map.get(aid)
        if name:
            label = f"{name} ({aid})"
        else:
            label = aid
        display_labels.append(label)
        label_to_id[label] = aid

    selected_ad_url_labels = st.multiselect(
        "広告URL(代理店)", display_labels, key="filter_ad_urls"
    )

    # 選択されたラベルを実際のIDに変換
    selected_ad_url_ids = [label_to_id[lbl] for lbl in selected_ad_url_labels]

    # Level 4: 定期商品名 (カテゴリ + 広告グループ + 広告URLで絞り込み)
    parent_l4 = {}
    if selected_categories:
        parent_l4[Col.PRODUCT_CATEGORY] = selected_categories
    if selected_ad_groups:
        parent_l4[Col.AD_GROUP] = selected_ad_groups
    if selected_ad_url_ids:
        parent_l4[Col.AD_URL] = selected_ad_url_ids
    product_names = fetch_filtered_options(
        client, table_ref, Col.SUBSCRIPTION_PRODUCT_NAME, parent_l4 or None
    )
    selected_product_names = st.multiselect(
        "定期商品名", product_names, key="filter_product_names"
    )

    return {
        "product_categories": selected_categories or None,
        "ad_groups": selected_ad_groups or None,
        "ad_urls": selected_ad_url_ids or None,
        "product_names": selected_product_names or None,
    }
