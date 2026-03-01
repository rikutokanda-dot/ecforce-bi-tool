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

        # デフォルト日付（1年前の先月初日 ～ 先月末日）
        today = date.today()
        first_of_this_month = today.replace(day=1)
        last_month_end = first_of_this_month - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        default_start = last_month_start.replace(year=last_month_start.year - 1)
        default_end = last_month_end

        # 定期作成日フィルタ（任意）
        date_enabled = st.checkbox(
            "定期作成日でフィルタ",
            value=st.session_state.get(SessionKey.DATE_ENABLED, True),
            key="sidebar_date_enabled",
        )
        st.session_state[SessionKey.DATE_ENABLED] = date_enabled

        if date_enabled:
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
        else:
            st.session_state[SessionKey.DATE_FROM] = None
            st.session_state[SessionKey.DATE_TO] = None

        st.divider()

        # 売上日フィルタ（任意）
        sales_enabled = st.checkbox(
            "売上日でフィルタ",
            value=st.session_state.get(SessionKey.SALES_DATE_ENABLED, False),
            key="sidebar_sales_date_enabled",
        )
        st.session_state[SessionKey.SALES_DATE_ENABLED] = sales_enabled

        if sales_enabled:
            sc1, sc2 = st.columns(2)
            with sc1:
                sales_start = st.date_input(
                    "売上日 開始",
                    value=st.session_state.get(
                        SessionKey.SALES_DATE_FROM, last_month_start
                    ),
                    key="sidebar_sales_start_date",
                )
            with sc2:
                sales_end = st.date_input(
                    "売上日 終了",
                    value=st.session_state.get(
                        SessionKey.SALES_DATE_TO, last_month_end
                    ),
                    key="sidebar_sales_end_date",
                )
            st.session_state[SessionKey.SALES_DATE_FROM] = sales_start
            st.session_state[SessionKey.SALES_DATE_TO] = sales_end
        else:
            st.session_state[SessionKey.SALES_DATE_FROM] = None
            st.session_state[SessionKey.SALES_DATE_TO] = None

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
