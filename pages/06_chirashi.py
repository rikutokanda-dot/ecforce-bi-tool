"""チラシ分析ページ.

チラシ同梱受注のアップセル効果を2つの軸で分析:
1. アップセル率: チラシを送った顧客のうち、ターゲット商品に切り替えた割合
2. 切り替えタイミング別継続率: 切り替えた回数ごとの、その後の継続率
"""

import altair as alt
import pandas as pd
import streamlit as st

from src.bigquery_client import execute_query, get_bigquery_client
from src.queries.chirashi import (
    build_chirashi_config_sql,
    build_chirashi_frequency_rate_sql,
    build_chirashi_list_sql,
    build_chirashi_retention_sql,
    build_chirashi_upsell_rate_sql,
)
from src.session import SessionKey, get_selected_company_key

# ---------------------------------------------------------------------------
# HTML描画ヘルパー
# ---------------------------------------------------------------------------


def _build_grouped_retention_html(group_df: pd.DataFrame, max_n: int) -> str:
    """切替回数ごとにグループ化した継続率・残存率テーブルをHTML描画."""

    # ヘッダー
    th_style = (
        'padding:6px 8px;text-align:center;border-bottom:2px solid #ddd;'
        'font-size:12px;white-space:nowrap;'
    )
    header = f'<th style="{th_style}">指標</th>'
    for n in range(1, max_n + 1):
        header += f'<th style="{th_style}">{n}回目</th>'

    rows_html = ""
    sorted_df = group_df.sort_values("switch_order_count")

    for i, (_, row) in enumerate(sorted_df.iterrows()):
        switch_n = int(row["switch_order_count"])
        total = int(row["total_switched"])

        # retained / eligible を事前に取得
        retained = {}
        eligible = {}
        for n in range(1, max_n + 1):
            r_col = f"retained_{n}"
            e_col = f"eligible_{n}"
            retained[n] = int(row[r_col]) if r_col in row.index and pd.notna(row[r_col]) else 0
            eligible[n] = int(row[e_col]) if e_col in row.index and pd.notna(row[e_col]) else 0

        # グループ見出し行
        top_border = 'border-top:2px solid #ccc;' if i > 0 else ''
        rows_html += (
            f'<tr><td colspan="{max_n + 1}" style="padding:8px 8px 2px;'
            f'font-weight:700;font-size:13px;{top_border}">'
            f'{switch_n}回目切替（{total:,}人）</td></tr>'
        )

        # 継続率行: retained_N / retained_(N-1)
        cells = (
            '<td style="padding:2px 8px;font-size:12px;color:#555;'
            'white-space:nowrap;">継続率</td>'
        )
        for n in range(1, max_n + 1):
            if eligible[n] == 0:
                cells += (
                    '<td style="text-align:center;padding:2px 6px;'
                    'font-size:13px;color:#ccc;">-</td>'
                )
            elif n <= switch_n:
                cells += (
                    '<td style="text-align:center;padding:2px 6px;white-space:nowrap;">'
                    '<span style="font-size:14px;font-weight:600;">100.0%</span><br>'
                    f'<span style="font-size:10px;color:#888;">'
                    f'({retained[n]:,}/{retained[n]:,})</span></td>'
                )
            else:
                prev = retained[n - 1] if n > 1 else total
                rate = retained[n] / prev * 100 if prev > 0 else 0
                cells += (
                    f'<td style="text-align:center;padding:2px 6px;white-space:nowrap;">'
                    f'<span style="font-size:14px;font-weight:600;">{rate:.1f}%</span><br>'
                    f'<span style="font-size:10px;color:#888;">'
                    f'({retained[n]:,}/{prev:,})</span></td>'
                )
        rows_html += f"<tr>{cells}</tr>"

        # 残存率行: retained_N / eligible_N
        cells = (
            '<td style="padding:2px 8px 6px;font-size:12px;color:#555;'
            'white-space:nowrap;">残存率</td>'
        )
        for n in range(1, max_n + 1):
            if eligible[n] == 0:
                cells += (
                    '<td style="text-align:center;padding:2px 6px 6px;'
                    'font-size:13px;color:#ccc;">-</td>'
                )
            else:
                rate = retained[n] / eligible[n] * 100
                cells += (
                    f'<td style="text-align:center;padding:2px 6px 6px;white-space:nowrap;">'
                    f'<span style="font-size:14px;font-weight:600;">{rate:.1f}%</span><br>'
                    f'<span style="font-size:10px;color:#888;">'
                    f'({retained[n]:,}/{eligible[n]:,})</span></td>'
                )
        rows_html += f"<tr>{cells}</tr>"

    return f"""
    <div style="overflow-x:auto;">
    <table style="width:100%;border-collapse:collapse;">
    <thead><tr>{header}</tr></thead>
    <tbody>{rows_html}</tbody>
    </table>
    </div>
    """


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
        st.stop()

    if df.empty:
        st.info(
            "データがありません。スプシの「チラシ設定」シートで"
            "ターゲット商品を設定してください。"
        )
        st.stop()

    # 表示用フォーマット
    display_df = df.copy()
    display_df.columns = ["チラシ名", "分母（送付者数）", "分子（切替者数）", "アップセル率(%)"]
    display_df["分母（送付者数）"] = display_df["分母（送付者数）"].apply(lambda x: f"{int(x):,}")
    display_df["分子（切替者数）"] = display_df["分子（切替者数）"].apply(lambda x: f"{int(x):,}")
    display_df["アップセル率(%)"] = display_df["アップセル率(%)"].apply(lambda x: f"{x:.1f}%")

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )

    # 縦棒グラフ（チラシ名を横書きで全文表示）
    if len(df) > 0:
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
        "N回目に投函した顧客のうち、N+1回目にターゲット商品に切り替えた割合"
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
                disp.columns = ["投函F(回数)", "投函数", "次回切替数", "転換率(%)"]
                disp["投函F(回数)"] = disp["投函F(回数)"].apply(lambda x: f"{int(x)}回目")
                disp["投函数"] = disp["投函数"].apply(lambda x: f"{int(x):,}")
                disp["次回切替数"] = disp["次回切替数"].apply(lambda x: f"{int(x):,}")
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

    max_n = st.slider("最大定期回数", min_value=6, max_value=24, value=12, key="chirashi_max_n")

    try:
        ret_sql = build_chirashi_retention_sql(
            company_key, chirashi_filter, max_n, order_date_from, order_date_to
        )
        ret_df = execute_query(client, ret_sql)
    except Exception as e:
        st.error(f"継続率クエリ実行エラー: {e}")
        st.stop()

    if ret_df.empty:
        st.info("切り替えた顧客が見つかりません。")
        st.stop()

    # ----- チラシ名ごとにエキスパンダーで表示 -----
    chirashi_groups = ret_df.groupby("chirashi_name")

    for chirashi_name, group_df in chirashi_groups:
        with st.expander(f"{chirashi_name}", expanded=True):
            html = _build_grouped_retention_html(group_df, max_n)
            st.markdown(html, unsafe_allow_html=True)

    st.caption(
        "※ 切替回数より前の継続率は定義上100%（例: 3回目切替 → 1〜3回目は100%）"
    )
