"""認証ロジック (パスワード認証 + Cookie永続化)."""

from __future__ import annotations

import hashlib
import hmac
import os
import time

import streamlit as st
from streamlit_js_eval import streamlit_js_eval

from src.session import SessionKey

# =====================================================================
# Cookie設定
# =====================================================================
_COOKIE_NAME = "ecforce_bi_auth"
_COOKIE_MAX_AGE_DAYS = 7
_COOKIE_MAX_AGE_SECONDS = _COOKIE_MAX_AGE_DAYS * 86400


# =====================================================================
# トークン生成・検証
# =====================================================================

def _get_signing_key() -> str:
    """署名キーを取得。app_passwordを再利用。"""
    return st.secrets.get("app_password", "")


def _generate_token() -> str:
    """HMAC-SHA256署名付きトークンを生成。

    Format: {timestamp}:{hmac_hex}
    """
    key = _get_signing_key()
    timestamp = str(int(time.time()))
    signature = hmac.new(
        key.encode("utf-8"),
        timestamp.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{timestamp}:{signature}"


def _verify_token(token: str) -> bool:
    """トークンの署名と有効期限を検証。"""
    if not token or ":" not in token:
        return False
    try:
        timestamp_str, signature = token.split(":", 1)
        timestamp = int(timestamp_str)
    except (ValueError, TypeError):
        return False

    # 有効期限チェック
    now = int(time.time())
    if now - timestamp > _COOKIE_MAX_AGE_SECONDS:
        return False

    # HMAC検証
    key = _get_signing_key()
    expected = hmac.new(
        key.encode("utf-8"),
        timestamp_str.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(signature, expected)


# =====================================================================
# Cookie読み書き (streamlit_js_eval)
# =====================================================================

def _secure_flag() -> str:
    """Cloud Run (HTTPS) ではSecureフラグを付ける。ローカルでは外す。"""
    return "; Secure" if os.environ.get("K_SERVICE") else ""


def _set_auth_cookie(token: str) -> None:
    """ブラウザにCookieを設定。"""
    max_age = _COOKIE_MAX_AGE_SECONDS
    secure = _secure_flag()
    js_code = (
        f'document.cookie = "{_COOKIE_NAME}={token}'
        f"; path=/; max-age={max_age}; SameSite=Lax{secure}"
        '";'
    )
    streamlit_js_eval(js_expressions=js_code, key="set_auth_cookie")


def _get_auth_cookie() -> str | None:
    """ブラウザからCookie値を読み取る。

    Returns:
        Cookie値文字列。未取得/JS未実行時はNone。
    """
    js_code = f"""
    (function() {{
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {{
            var cookie = cookies[i].trim();
            if (cookie.startsWith('{_COOKIE_NAME}=')) {{
                return cookie.substring({len(_COOKIE_NAME) + 1});
            }}
        }}
        return '';
    }})()
    """
    result = streamlit_js_eval(js_expressions=js_code, key="get_auth_cookie")
    if result is None or result == 0:
        return None  # JS未実行
    return result if result else None


def clear_auth_cookie() -> None:
    """認証Cookieを削除。"""
    secure = _secure_flag()
    js_code = (
        f'document.cookie = "{_COOKIE_NAME}='
        f"; path=/; max-age=0; SameSite=Lax{secure}"
        '";'
    )
    streamlit_js_eval(js_expressions=js_code, key="clear_auth_cookie")


# =====================================================================
# 認証フロー
# =====================================================================

def check_auth() -> bool:
    """認証済みかチェック。session_stateを優先し、なければCookieを確認。"""
    # 1. session_stateで認証済み
    if st.session_state.get(SessionKey.AUTHENTICATED, False):
        return True

    # 2. Cookie確認（JSが返していなければNone）
    cookie_token = _get_auth_cookie()
    if cookie_token is None:
        return False

    # 3. トークン検証
    if _verify_token(cookie_token):
        st.session_state[SessionKey.AUTHENTICATED] = True
        return True

    return False


def show_login():
    """ログイン画面を表示。"""
    st.title("ECforce BI")
    st.caption("分析ダッシュボードにアクセスするにはパスワードを入力してください。")

    password = st.text_input("パスワード", type="password", key="login_password")
    if st.button("ログイン", type="primary"):
        if password == st.secrets.get("app_password", ""):
            st.session_state[SessionKey.AUTHENTICATED] = True
            token = _generate_token()
            _set_auth_cookie(token)
            st.rerun()
        else:
            st.error("パスワードが正しくありません。")
