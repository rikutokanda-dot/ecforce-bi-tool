"""認証ロジック (Phase1: 簡易パスワード認証 + クッキー永続化)."""

import hashlib
import hmac

import streamlit as st

from src.session import SessionKey

# クッキーの有効期間 (30日)
_COOKIE_MAX_DAYS = 30
_COOKIE_KEY = "ecforce_bi_auth"


def _make_token(password: str) -> str:
    """パスワードからHMACトークンを生成."""
    return hmac.new(
        password.encode(), b"ecforce-bi-auth", hashlib.sha256,
    ).hexdigest()[:32]


def check_auth() -> bool:
    """認証済みかチェック (session_state → クッキー の順)."""
    # 1. session_stateにあればOK
    if st.session_state.get(SessionKey.AUTHENTICATED, False):
        return True

    # 2. クッキーにトークンがあれば検証
    try:
        from streamlit_js_eval import streamlit_js_eval
        cookie_val = streamlit_js_eval(
            js_expressions=f'document.cookie.split("; ").find(c => c.startsWith("{_COOKIE_KEY}="))?.split("=")[1] || ""',
            key=f"cookie_read_{id(st)}",
        )
    except ImportError:
        cookie_val = None

    if cookie_val:
        expected = _make_token(st.secrets.get("app_password", ""))
        if cookie_val == expected:
            st.session_state[SessionKey.AUTHENTICATED] = True
            return True

    # 3. query_params にトークンがあれば検証 (フォールバック)
    params = st.query_params
    token = params.get("auth", "")
    if token:
        expected = _make_token(st.secrets.get("app_password", ""))
        if token == expected:
            st.session_state[SessionKey.AUTHENTICATED] = True
            return True

    return False


def _set_auth_cookie():
    """認証トークンをクッキーに保存."""
    try:
        from streamlit_js_eval import streamlit_js_eval
        token = _make_token(st.secrets.get("app_password", ""))
        max_age = _COOKIE_MAX_DAYS * 86400
        streamlit_js_eval(
            js_expressions=f'document.cookie = "{_COOKIE_KEY}={token}; path=/; max-age={max_age}; SameSite=Lax"',
            key=f"cookie_write_{id(st)}",
        )
    except ImportError:
        pass


def show_login():
    """ログイン画面を表示."""
    st.title("ECforce BI")
    st.caption("分析ダッシュボードにアクセスするにはパスワードを入力してください。")

    password = st.text_input("パスワード", type="password", key="login_password")
    if st.button("ログイン", type="primary"):
        if password == st.secrets.get("app_password", ""):
            st.session_state[SessionKey.AUTHENTICATED] = True
            _set_auth_cookie()
            st.rerun()
        else:
            st.error("パスワードが正しくありません。")
