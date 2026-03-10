"""チラシ分析ページ.

チラシ同梱受注のアップセル効果を2つの軸で分析:
1. アップセル率: チラシを送った顧客のうち、ターゲット商品に切り替えた割合
2. 切り替えタイミング別継続率: 切り替えた回数ごとの、その後の継続率
"""

import math

import altair as alt
import pandas as pd
import streamlit as st

from src.bigquery_client import execute_query, get_bigquery_client
from src.components.retention_table import build_grouped_retention_html
from src.config_loader import load_product_cycles, save_product_cycles
from src.queries.chirashi import (
    build_chirashi_config_sql,
    build_chirashi_frequency_rate_sql,
    build_chirashi_list_sql,
    build_chirashi_retention_sql,
    build_chirashi_unmatched_products_sql,
    build_chirashi_upsell_rate_sql,
)
from src.session import SessionKey, get_selected_company_key


# ---------------------------------------------------------------------------
# ヘッダー
# ---------------------------------------------------------------------------
st.header("チラシ分析")

company_key = get_selected_company_key()
if not company_key:
    st.warning("サイドバーで会社を選択してください。")
    st.stop()

client = get_bigquery_client()

# サイドバーの受注日フィルタを取得
order_date_from = st.session_state.get(SessionKey.ORDER_DATE_FROM)
order_date_to = st.session_state.get(SessionKey.ORDER_DATE_TO)

if not order_date_from and not order_date_to:
    st.info("サイドバーで「受注日でフィルタ」を有効にしてください。")
    st.stop()

# ---------------------------------------------------------------------------
# ターゲット商品設定の表示
# ---------------------------------------------------------------------------
try:
    config_sql = build_chirashi_config_sql(company_key)
    config_df = execute_query(client, config_sql)
except Exception:
    config_df = pd.DataFrame()

if not config_df.empty:
    with st.expander("📋 ターゲット商品設定（部分一致キーワード）", expanded=False):
        cfg_display = config_df.copy()
        cfg_display.columns = ["チラシ名", "ターゲット商品（部分一致）"]
        st.dataframe(cfg_display, use_container_width=True, hide_index=True)
        st.caption("※ スプシの「チラシ設定」シートで変更可能。BQに自動同期されます。")

# ---------------------------------------------------------------------------
# タブ
# ---------------------------------------------------------------------------
tab_upsell, tab_retention = st.tabs(["アップセル率", "切り替えタイミング別継続率"])

# ===== タブ1: アップセル率 =====
with tab_upsell:
    st.subheader("チラシ別アップセル率")
    st.caption(
        "分母: チラシを同梱した顧客数（顧客ID重複なし）　"
        "分子: そのうちターゲット商品に切り替えた顧客数"
    )

    try:
        sql = build_chirashi_upsell_rate_sql(company_key, order_date_from, order_date_to)
        df = execute_query(client, sql)
    except Exception as e:
        st.error(f"クエリ実行エラー: {e}")
        df = pd.DataFrame()

    if df.empty:
        st.info(
            "データがありません。スプシの「チラシ設定」シートで"
            "ターゲット商品を設定してください。"
        )

    if not df.empty:
        # 表示用フォーマット
        display_df = df.copy()
        display_df.columns = ["チラシ名", "投函数", "分母（送付者数）", "分子（切替者数）", "アップセル率(%)"]
        display_df["投函数"] = display_df["投函数"].apply(lambda x: f"{int(x):,}")
        display_df["分母（送付者数）"] = display_df["分母（送付者数）"].apply(lambda x: f"{int(x):,}")
        display_df["分子（切替者数）"] = display_df["分子（切替者数）"].apply(lambda x: f"{int(x):,}")
        display_df["アップセル率(%)"] = display_df["アップセル率(%)"].apply(lambda x: f"{x:.1f}%")

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
        )

        # 縦棒グラフ
        chart_df = df[["chirashi_name", "upsell_rate"]].copy()
        chart = (
            alt.Chart(chart_df)
            .mark_bar(color="#4A90D9")
            .encode(
                x=alt.X(
                    "chirashi_name:N",
                    title="チラシ名",
                    sort="-y",
                    axis=alt.Axis(labelAngle=0, labelLimit=400),
                ),
                y=alt.Y("upsell_rate:Q", title="アップセル率(%)"),
                tooltip=[
                    alt.Tooltip("chirashi_name:N", title="チラシ名"),
                    alt.Tooltip("upsell_rate:Q", title="アップセル率(%)", format=".1f"),
                ],
            )
            .properties(height=400)
        )
        st.altair_chart(chart, use_container_width=True)

        # ----- F(回数)別転換率 -----
        st.divider()
        st.subheader("F(回数)別転換率")
        st.caption(
            "チラシ受領者のうち、各回数で初めてターゲット商品に切り替えた顧客の割合（顧客重複なし）"
        )

        try:
            freq_sql = build_chirashi_frequency_rate_sql(
                company_key, order_date_from, order_date_to
            )
            freq_df = execute_query(client, freq_sql)
        except Exception as e:
            st.error(f"F転換率クエリ実行エラー: {e}")
            freq_df = pd.DataFrame()

        if not freq_df.empty:
            for cname in freq_df["chirashi_name"].unique():
                cdf = freq_df[freq_df["chirashi_name"] == cname].copy()
                with st.expander(f"{cname}", expanded=True):
                    disp = cdf[["order_count", "total_at_n", "switched_at_next", "conversion_rate"]].copy()
                    disp.columns = ["切替F(回数)", "投函数", "切替数", "転換率(%)"]
                    disp["切替F(回数)"] = (disp["切替F(回数)"].astype(int) + 1).apply(lambda x: f"{x}回目")
                    disp["投函数"] = disp["投函数"].apply(lambda x: f"{int(x):,}")
                    disp["切替数"] = disp["切替数"].apply(lambda x: f"{int(x):,}")
                    disp["転換率(%)"] = disp["転換率(%)"].apply(lambda x: f"{x:.1f}%")
                    st.dataframe(disp, use_container_width=True, hide_index=True)
        else:
            st.info("F転換率データがありません。")


# ===== タブ2: 切り替えタイミング別継続率 =====
with tab_retention:
    st.subheader("切り替えタイミング別継続率")
    st.caption(
        "アップセル商品に切り替えた顧客が母体。"
        "N回目で切り替えた人の1〜N回目の継続率は定義上100%です。"
    )

    # チラシ名一覧を取得
    try:
        list_sql = build_chirashi_list_sql(company_key)
        chirashi_df = execute_query(client, list_sql)
    except Exception as e:
        st.error(f"チラシ一覧の取得エラー: {e}")
        st.stop()

    if chirashi_df.empty:
        st.info("ターゲット商品が設定されたチラシがありません。")
        st.stop()

    chirashi_names = chirashi_df["chirashi_name"].tolist()
    selected_chirashi = st.selectbox(
        "チラシを選択",
        options=["全チラシ"] + chirashi_names,
        key="chirashi_retention_select",
    )

    chirashi_filter = None if selected_chirashi == "全チラシ" else selected_chirashi

    max_days = st.slider(
        "期間（日数）", min_value=30, max_value=730, value=365, step=30,
        key="chirashi_max_days",
    )

    try:
        pc_data = load_product_cycles()
        ret_sql = build_chirashi_retention_sql(
            company_key, chirashi_filter, max_days,
            order_date_from, order_date_to, pc_data,
        )
        ret_df = execute_query(client, ret_sql)
    except Exception as e:
        st.error(f"継続率クエリ実行エラー: {e}")
        st.stop()

    if ret_df.empty:
        st.info("切り替えた顧客が見つかりません。")
        st.stop()

    # SQL生成列数を検出し、データのある最後の列で自動トリム
    retained_nums = sorted(
        int(c.split("_")[1])
        for c in ret_df.columns
        if c.startswith("retained_")
    )
    effective_max_n = 1
    for n in retained_nums:
        cd_col = f"cont_denom_{n}"
        r_col = f"retained_{n}"
        if (cd_col in ret_df.columns and ret_df[cd_col].sum() > 0) or \
           (r_col in ret_df.columns and ret_df[r_col].sum() > 0):
            effective_max_n = n

    # ----- チラシ名ごとにエキスパンダーで表示 -----
    chirashi_groups = ret_df.groupby("chirashi_name")

    # デフォルトcycle2をマスタから取得
    _defaults = pc_data.get("defaults", {}) if pc_data else {}
    _default_cycle2 = _defaults.get("cycle2", 30)

    for chirashi_name, group_df in chirashi_groups:
        with st.expander(f"{chirashi_name}", expanded=True):
            html = build_grouped_retention_html(
                group_df, effective_max_n, default_cycle2=_default_cycle2,
                max_days=max_days, product_cycles=pc_data,
            )
            st.markdown(html, unsafe_allow_html=True)

    st.caption(
        "※ 切替回数より前の継続率は定義上100%（例: 3回目切替 → 1〜3回目は100%）"
    )

    # ----- 商品マスタ未登録 → 自動追加 + 周期未設定の警告 -----
    try:
        unmatched_sql = build_chirashi_unmatched_products_sql(
            company_key, chirashi_filter,
            order_date_from, order_date_to, pc_data,
        )
        unmatched_df = execute_query(client, unmatched_sql)
    except Exception:
        unmatched_df = pd.DataFrame()

    if not unmatched_df.empty:
        # 未登録商品をマスタに自動追加（cycle2キーなし = 未設定）
        new_names = unmatched_df["switched_product_name"].tolist()
        existing_names = {p["name"] for p in pc_data.get("products", [])}
        added = []
        for name in new_names:
            if name not in existing_names:
                pc_data["products"].append({"name": name})
                added.append(name)
        if added:
            save_product_cycles(pc_data)
            st.cache_data.clear()

    # cycle2キーが存在しない / None / NaN（未設定）商品を警告
    # ※ cycle2: 0 は「単品/周期なし」として正当な設定
    def _is_unconfigured(p: dict) -> bool:
        c2 = p.get("cycle2")
        if c2 is None:
            return True
        if isinstance(c2, float) and math.isnan(c2):
            return True
        return False

    unconfigured = [
        p["name"] for p in pc_data.get("products", [])
        if _is_unconfigured(p)
    ]
    if unconfigured:
        product_list = "\n".join(f"- {name}" for name in unconfigured)
        st.warning(
            "以下の商品の発送周期が未設定です。\n"
            "周期が未設定の商品は実績ベースの計算となり、正確なeligible判定ができません。\n"
            "**マスタ管理ページで周期を設定してください。**\n\n"
            f"{product_list}"
        )
