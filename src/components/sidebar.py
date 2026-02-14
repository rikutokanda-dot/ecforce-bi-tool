"""共通サイドバーコンポーネント."""

from datetime import date, timedelta

import streamlit as st

from src.config_loader import load_companies
from src.session import SessionKey


def render_sidebar():
    """会社選択と日付範囲を含むサイドバーを描画."""
    with st.sidebar:
        st.header("設定")

        # 会社選択
        companies = load_companies()
        company_names = [c["display_name"] for c in companies]
        selected_idx = st.selectbox(
            "会社",
            range(len(companies)),
            format_func=lambda i: company_names[i],
            key="company_selector",
        )
        st.session_state[SessionKey.SELECTED_COMPANY] = companies[selected_idx]

        st.divider()

        # 日付範囲
        today = date.today()
        default_start = today - timedelta(days=365)

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "開始日",
                value=default_start,
                key="sidebar_start_date",
            )
        with col2:
            end_date = st.date_input(
                "終了日",
                value=today,
                key="sidebar_end_date",
            )

        st.session_state[SessionKey.DATE_FROM] = start_date
        st.session_state[SessionKey.DATE_TO] = end_date

        st.divider()

        # キャッシュクリア
        if st.button("データ更新", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
