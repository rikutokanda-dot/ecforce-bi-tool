"""session_state管理ヘルパー."""

from __future__ import annotations

from typing import Optional

import streamlit as st


class SessionKey:
    SELECTED_COMPANY = "selected_company"
    DATE_FROM = "date_from"
    DATE_TO = "date_to"
    AUTHENTICATED = "authenticated"


def get_selected_company() -> Optional[dict]:
    """選択中の会社情報を取得."""
    return st.session_state.get(SessionKey.SELECTED_COMPANY)


def get_selected_company_key() -> Optional[str]:
    """選択中の会社キーを取得."""
    company = get_selected_company()
    return company["key"] if company else None
