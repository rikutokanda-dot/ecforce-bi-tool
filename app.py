"""ECforce BI - エントリーポイント."""

import streamlit as st

from src.auth import check_auth, show_login
from src.components.sidebar import render_sidebar

st.set_page_config(
    page_title="ECforce BI",
    page_icon=":material/analytics:",
    layout="wide",
)

# multiselect: ドロップダウン一覧 & 選択済みタグを全文表示（省略しない）
st.markdown(
    """
    <style>
    /* ドロップダウン展開時のリスト項目を全文表示 */
    div[data-baseweb="popover"] li,
    div[data-baseweb="popover"] li span,
    ul[role="listbox"] li,
    ul[role="listbox"] li span {
        white-space: normal !important;
        word-break: break-all !important;
        overflow: visible !important;
        text-overflow: unset !important;
        max-width: none !important;
    }
    /* サイドバー内のポップオーバーは親幅に収める */
    [data-testid="stSidebar"] div[data-baseweb="popover"] {
        min-width: unset !important;
        max-width: 100% !important;
    }
    /* メインコンテンツ内のポップオーバーは広め */
    div[data-baseweb="popover"] ul {
        max-width: none !important;
    }
    /* 選択済みタグ: コンパクト表示 */
    span[data-baseweb="tag"] {
        max-width: 180px !important;
        white-space: nowrap !important;
        height: auto !important;
    }
    span[data-baseweb="tag"] > span:first-child {
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        max-width: 140px !important;
        display: inline-block !important;
    }
    /* サイドバー内のタグは幅制限きつめ */
    [data-testid="stSidebar"] span[data-baseweb="tag"] {
        max-width: 160px !important;
    }
    [data-testid="stSidebar"] span[data-baseweb="tag"] > span:first-child {
        max-width: 120px !important;
    }
    /* multiselect入力エリアの高さを拡張（全タグ表示） */
    div[data-baseweb="select"] > div:first-child {
        max-height: none !important;
        flex-wrap: wrap !important;
    }
    /* サイドバー: ホバー時に画面半分まで拡張 */
    [data-testid="stSidebar"] {
        transition: width 0.3s ease, min-width 0.3s ease !important;
    }
    [data-testid="stSidebar"]:hover {
        width: 50vw !important;
        min-width: 50vw !important;
        max-width: 50vw !important;
    }
    /* サイドバー内のドロップダウンが開いている間もサイドバーを広げたままにする
       aria-expanded="true" はサイドバー内のselectが開いている時のみ付く */
    [data-testid="stSidebar"]:has([aria-expanded="true"]) {
        width: 50vw !important;
        min-width: 50vw !important;
        max-width: 50vw !important;
    }
    /* サイドバー内のコンテンツも広がる */
    [data-testid="stSidebar"]:hover [data-testid="stSidebarContent"],
    [data-testid="stSidebar"]:has([aria-expanded="true"]) [data-testid="stSidebarContent"] {
        width: 100% !important;
    }
    /* サイドバー拡張時はタグも全文表示 */
    [data-testid="stSidebar"]:hover span[data-baseweb="tag"],
    [data-testid="stSidebar"]:has([aria-expanded="true"]) span[data-baseweb="tag"] {
        max-width: none !important;
    }
    [data-testid="stSidebar"]:hover span[data-baseweb="tag"] > span:first-child,
    [data-testid="stSidebar"]:has([aria-expanded="true"]) span[data-baseweb="tag"] > span:first-child {
        max-width: none !important;
        overflow: visible !important;
        text-overflow: unset !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
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
            st.Page("pages/01_cohort.py", title="分析", icon=":material/group:"),
            st.Page("pages/02_sales.py", title="Tier", icon=":material/trending_up:"),
            st.Page("pages/03_ad_performance.py", title="広告効果", icon=":material/campaign:"),
            st.Page("pages/04_churn.py", title="解約分析", icon=":material/person_remove:"),
        ],
        "設定": [
            st.Page("pages/05_master.py", title="マスタ管理", icon=":material/settings:"),
        ],
    }
)
pg.run()
