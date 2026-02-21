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
        # 会社変更検知 → 永続フィルタをクリア
        prev_company = st.session_state.get("_prev_company_key")
        new_company = companies[selected_idx]["key"]
        if prev_company and prev_company != new_company:
            for k in [
                SessionKey.FILTER_CATEGORIES,
                SessionKey.FILTER_AD_GROUPS,
                SessionKey.FILTER_AD_URLS,
                SessionKey.FILTER_PRODUCT_NAMES,
            ]:
                st.session_state.pop(k, None)
        st.session_state["_prev_company_key"] = new_company
        st.session_state[SessionKey.SELECTED_COMPANY] = companies[selected_idx]

        st.divider()

        # 日付範囲（デフォルト: 1年前の先月初日 ～ 先月末日）
        today = date.today()
        first_of_this_month = today.replace(day=1)
        last_month_end = first_of_this_month - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        default_start = last_month_start.replace(year=last_month_start.year - 1)
        default_end = last_month_end

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
                value=default_end,
                key="sidebar_end_date",
            )

        st.session_state[SessionKey.DATE_FROM] = start_date
        st.session_state[SessionKey.DATE_TO] = end_date

        st.divider()

        # キャッシュクリア
        if st.button("データ更新", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        # ログアウト
        _email = st.user.get("email", "") if st.user.is_logged_in else ""
        if _email:
            st.caption(f"👤 {_email}")
        st.button("ログアウト", use_container_width=True, on_click=st.logout)
