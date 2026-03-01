"""session_state管理ヘルパー."""

from __future__ import annotations

from typing import Optional

import streamlit as st


class SessionKey:
    SELECTED_COMPANY = "selected_company"
    DATE_FROM = "date_from"
    DATE_TO = "date_to"
    DATE_ENABLED = "date_enabled"
    SALES_DATE_ENABLED = "sales_date_enabled"
    SALES_DATE_FROM = "sales_date_from"
    SALES_DATE_TO = "sales_date_to"
    AUTHENTICATED = "authenticated"
    # 永続フィルタ（ページ遷移時も保持）
    FILTER_CATEGORIES = "persist_filter_categories"
    FILTER_AD_GROUPS = "persist_filter_ad_groups"
    FILTER_AD_URLS = "persist_filter_ad_urls"
    FILTER_PRODUCT_NAMES = "persist_filter_product_names"


def get_selected_company() -> Optional[dict]:
    """選択中の会社情報を取得."""
    return st.session_state.get(SessionKey.SELECTED_COMPANY)


def get_selected_company_key() -> Optional[str]:
    """選択中の会社キーを取得."""
    company = get_selected_company()
    return company["key"] if company else None
