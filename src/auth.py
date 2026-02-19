"""認証ロジック (Phase1: 簡易パスワード認証)."""

import streamlit as st

from src.session import SessionKey


def check_auth() -> bool:
    """認証済みかチェック."""
    return st.session_state.get(SessionKey.AUTHENTICATED, False)


def show_login():
    """ログイン画面を表示."""
    st.title("ECforce BI")
    st.caption("分析ダッシュボードにアクセスするにはパスワードを入力してください。")

    password = st.text_input("パスワード", type="password", key="login_password")
    if st.button("ログイン", type="primary"):
        if password == st.secrets.get("app_password", ""):
            st.session_state[SessionKey.AUTHENTICATED] = True
            st.rerun()
        else:
            st.error("パスワードが正しくありません。")
