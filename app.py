"""ECforce BI - エントリーポイント."""

import streamlit as st

from src.auth import check_auth, show_login
from src.components.sidebar import render_sidebar

st.set_page_config(
    page_title="ECforce BI",
    page_icon=":material/analytics:",
    layout="wide",
)

# 認証チェック
if not check_auth():
    show_login()
    st.stop()

# サイドバー (会社選択・日付範囲)
render_sidebar()

# ナビゲーション
pg = st.navigation(
    {
        "分析": [
            st.Page("pages/01_cohort.py", title="コホート分析", icon=":material/group:"),
            st.Page("pages/02_sales.py", title="売上分析", icon=":material/trending_up:"),
            st.Page("pages/03_ad_performance.py", title="広告効果", icon=":material/campaign:"),
            st.Page("pages/04_churn.py", title="解約分析", icon=":material/person_remove:"),
        ],
        "設定": [
            st.Page("pages/05_master.py", title="マスタ管理", icon=":material/settings:"),
        ],
    }
)
pg.run()
