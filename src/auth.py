"""認証ロジック (Phase1: 簡易パスワード認証 + URLトークン永続化)."""

import hashlib
import hmac

import streamlit as st

from src.session import SessionKey


def _make_token(password: str) -> str:
    """パスワードからHMACトークンを生成."""
    return hmac.new(
        password.encode(), b"ecforce-bi-auth", hashlib.sha256,
    ).hexdigest()[:32]


def check_auth() -> bool:
    """認証済みかチェック (session_state → URL query_params の順)."""
    # 1. session_stateにあればOK
    if st.session_state.get(SessionKey.AUTHENTICATED, False):
        return True

    # 2. URLの query_params にトークンがあれば検証
    token = st.query_params.get("auth", "")
    if token:
        expected = _make_token(st.secrets.get("app_password", ""))
        if token == expected:
            st.session_state[SessionKey.AUTHENTICATED] = True
            return True

    return False


def show_login():
    """ログイン画面を表示."""
    st.title("ECforce BI")
    st.caption("分析ダッシュボードにアクセスするにはパスワードを入力してください。")

    password = st.text_input("パスワード", type="password", key="login_password")
    if st.button("ログイン", type="primary"):
        if password == st.secrets.get("app_password", ""):
            st.session_state[SessionKey.AUTHENTICATED] = True
            # URLにトークンを埋め込み → リロードしても認証維持
            token = _make_token(password)
            st.query_params["auth"] = token
            st.rerun()
        else:
            st.error("パスワードが正しくありません。")
