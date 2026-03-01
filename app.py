"""ECforce BI - エントリーポイント."""

import streamlit as st
from streamlit_js_eval import streamlit_js_eval

from src.components.sidebar import render_sidebar

# 許可ドメイン
_ALLOWED_DOMAIN = "organic-gr.com"

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
    /* サイドバー: CSS変数でベース幅を管理（JSからドラッグで変更可能） */
    :root { --sb-base-w: 380px; }
    [data-testid="stSidebar"] {
        width: var(--sb-base-w) !important;
        min-width: var(--sb-base-w) !important;
        max-width: var(--sb-base-w) !important;
        transition: width 0.3s ease, min-width 0.3s ease, max-width 0.3s ease !important;
    }
    /* ドラッグ中はtransition無効化 */
    [data-testid="stSidebar"].sidebar-dragging {
        transition: none !important;
    }
    /* hover拡張 */
    [data-testid="stSidebar"]:hover {
        width: 50vw !important;
        min-width: 50vw !important;
        max-width: 50vw !important;
    }
    /* サイドバー内のドロップダウンが開いている間もサイドバーを広げたままにする */
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

# サイドバー幅ドラッグリサイズ（streamlit_js_eval でJS実行）
streamlit_js_eval(
    js_expressions="""
(function() {
    setFrameHeight(0);
    var doc = parent.document;
    var win = parent.window;
    if (win._sidebarResizeReady) return 'already';
    win._sidebarResizeReady = true;

    var sidebar = doc.querySelector('[data-testid="stSidebar"]');
    if (!sidebar) return 'no-sidebar';

    // ハンドルをDOM挿入
    var handle = doc.createElement('div');
    handle.id = 'sb-drag';
    handle.style.cssText = 'position:fixed;top:0;height:60px;width:20px;cursor:col-resize;z-index:999999;'
        + 'display:flex;align-items:center;justify-content:center;background:transparent;';
    handle.innerHTML = '<div style="width:4px;height:28px;border-radius:3px;background:rgba(74,144,217,0.45);'
        + 'pointer-events:none;"></div>';

    handle.addEventListener('mouseenter', function() {
        handle.style.background = 'rgba(74,144,217,0.12)';
        handle.firstChild.style.background = 'rgba(74,144,217,0.9)';
    });
    handle.addEventListener('mouseleave', function() {
        if (!dragging) {
            handle.style.background = 'transparent';
            handle.firstChild.style.background = 'rgba(74,144,217,0.45)';
        }
    });

    doc.body.appendChild(handle);

    var dragging = false;
    var curW = sidebar.getBoundingClientRect().width || 380;
    handle.style.left = (curW - 10) + 'px';

    // MutationObserverでサイドバー幅変化を追跡
    new MutationObserver(function() {
        if (!dragging) {
            var r = sidebar.getBoundingClientRect();
            handle.style.left = (r.width - 10) + 'px';
        }
    }).observe(sidebar, { attributes: true, attributeFilter: ['style'] });

    // ダブルクリックでデフォルト幅(380px)にリセット
    handle.addEventListener('dblclick', function(e) {
        e.preventDefault(); e.stopPropagation();
        doc.documentElement.style.setProperty('--sb-base-w', '380px');
        curW = 380;
        handle.style.left = (380 - 10) + 'px';
    });

    handle.addEventListener('mousedown', function(e) {
        e.preventDefault(); e.stopPropagation();
        dragging = true;
        sidebar.classList.add('sidebar-dragging');
        handle.style.background = 'rgba(74,144,217,0.18)';
        handle.firstChild.style.background = 'rgba(74,144,217,1)';
        doc.body.style.cursor = 'col-resize';
        doc.body.style.userSelect = 'none';
    });

    doc.addEventListener('mousemove', function(e) {
        if (!dragging) return;
        e.preventDefault();
        var nw = Math.max(280, Math.min(e.clientX, win.innerWidth * 0.7));
        curW = nw;
        // CSS変数を更新 → サイドバーのベース幅が変わる
        doc.documentElement.style.setProperty('--sb-base-w', nw + 'px');
        handle.style.left = (nw - 10) + 'px';
    });

    doc.addEventListener('mouseup', function() {
        if (!dragging) return;
        dragging = false;
        doc.body.style.cursor = '';
        doc.body.style.userSelect = '';
        handle.style.background = 'transparent';
        handle.firstChild.style.background = 'rgba(74,144,217,0.45)';
        sidebar.classList.remove('sidebar-dragging');
    });

    return 'ok';
})()
""",
    key="sidebar_resize_js",
)

# 認証チェック (Google OAuth)
if not st.user.is_logged_in:
    st.title("ECforce BI")
    st.caption("分析ダッシュボードにアクセスするにはGoogleアカウントでログインしてください。")
    st.caption(f"※ {_ALLOWED_DOMAIN} ドメインのアカウントのみアクセスできます。")
    st.button("Googleでログイン", type="primary", on_click=st.login)
    st.stop()

# ドメインチェック
_user_email = st.user.get("email", "")
if not _user_email.endswith(f"@{_ALLOWED_DOMAIN}"):
    st.title("アクセス拒否")
    st.error(f"このアプリは {_ALLOWED_DOMAIN} ドメインのアカウントのみ利用できます。")
    st.caption(f"現在のログイン: {_user_email}")
    st.button("ログアウト", on_click=st.logout)
    st.stop()

# サイドバー (会社選択・日付範囲)
render_sidebar()

# ナビゲーション
pg = st.navigation(
    {
        "分析": [
            st.Page("pages/01_cohort.py", title="継続分析", icon=":material/group:"),
            st.Page("pages/02_sales.py", title="顧客分析", icon=":material/trending_up:"),
            st.Page("pages/03_ad_performance.py", title="広告効果", icon=":material/campaign:"),
            st.Page("pages/04_churn.py", title="解約分析", icon=":material/person_remove:"),
        ],
        "設定": [
            st.Page("pages/05_master.py", title="マスタ管理", icon=":material/settings:"),
        ],
    }
)
pg.run()
