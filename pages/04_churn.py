"""解約分析ページ - キャンセル理由・定期回数別キャンセル理由."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.bigquery_client import execute_query, get_bigquery_client
from src.components.filters import render_cohort_filters
from src.queries.churn import build_churn_reason_sql, build_churn_by_order_reason_sql
from src.session import SessionKey, get_selected_company_key

st.header("解約分析")
st.caption("キャンセル理由、定期回数別のキャンセル理由を分析します。")

company_key = get_selected_company_key()
if not company_key:
    st.warning("サイドバーから会社を選択してください。")
    st.stop()

date_from = st.session_state.get(SessionKey.DATE_FROM)
date_to = st.session_state.get(SessionKey.DATE_TO)

with st.sidebar:
    filters = render_cohort_filters(company_key)

client = get_bigquery_client()

date_from_str = date_from.strftime("%Y-%m-%d") if date_from else None
date_to_str = date_to.strftime("%Y-%m-%d") if date_to else None

filter_params = dict(
    company_key=company_key,
    date_from=date_from_str,
    date_to=date_to_str,
    product_categories=filters["product_categories"],
    ad_groups=filters["ad_groups"],
    product_names=filters["product_names"],
    ad_urls=filters.get("ad_urls"),
)

# ========== メインコンテンツ ==========
reason_tab, order_reason_tab = st.tabs(["キャンセル理由", "定期回数別キャンセル理由"])

# ---------- キャンセル理由 ----------
with reason_tab:
    if not filters["product_names"]:
        st.info("正確なデータ表示のため、サイドバーから「定期商品名」を選択してください。")
    elif not st.button("表示する", key="btn_reason", type="primary"):
        st.info("フィルタを設定して「表示する」を押してください。")
    else:
        try:
            sql = build_churn_reason_sql(**filter_params)
            df = execute_query(client, sql)
        except Exception as e:
            st.error(f"BigQueryクエリ実行エラー: {e}")
            df = pd.DataFrame()

        if df.empty:
            st.info("キャンセル理由のデータが見つかりませんでした。")
        else:
            total = df["cancel_count"].sum()

            st.metric("総キャンセル人数", f"{total:,}人")
            st.markdown("---")

            # 割合を追加
            df["割合(%)"] = (df["cancel_count"] / total * 100).round(1)
            df.columns = ["キャンセル理由", "件数", "割合(%)"]

            # グラフ
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df["件数"],
                y=df["キャンセル理由"],
                orientation="h",
                text=df.apply(lambda r: f"{int(r['件数']):,}件 ({r['割合(%)']}%)", axis=1),
                textposition="outside",
                marker_color="rgba(239, 83, 80, 0.7)",
            ))
            fig.update_layout(
                title="キャンセル理由別件数",
                xaxis_title="件数",
                height=max(300, len(df) * 35 + 100),
                margin=dict(t=50, b=50, l=250),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig, use_container_width=True)

            # テーブル
            display_df = df.copy()
            display_df["件数"] = display_df["件数"].apply(lambda v: f"{int(v):,}")
            display_df["割合(%)"] = display_df["割合(%)"].apply(lambda v: f"{v}%")
            st.dataframe(display_df, use_container_width=True, hide_index=True)

# ---------- 定期回数別キャンセル理由 ----------
with order_reason_tab:
    if not filters["product_names"]:
        st.info("正確なデータ表示のため、サイドバーから「定期商品名」を選択してください。")
    elif not st.button("表示する", key="btn_order_reason", type="primary"):
        st.info("フィルタを設定して「表示する」を押してください。")
    else:
        try:
            sql = build_churn_by_order_reason_sql(**filter_params)
            df = execute_query(client, sql)
        except Exception as e:
            st.error(f"BigQueryクエリ実行エラー: {e}")
            df = pd.DataFrame()

        if df.empty:
            st.info("該当するデータが見つかりませんでした。")
        else:
            st.caption("顧客ごとの「最後に出荷完了・売上完了した定期回数」別にキャンセル理由を集計しています。")

            # 回数を整数に変換してソート
            df["last_completed_order"] = df["last_completed_order"].astype(int)
            df = df.sort_values(["last_completed_order", "cancel_count"], ascending=[True, False])

            # 回数ごとにアコーディオン
            order_numbers = sorted(df["last_completed_order"].unique())

            # まずサマリー: 回数別キャンセル人数
            summary_rows = []
            for order_num in order_numbers:
                group = df[df["last_completed_order"] == order_num]
                total_in_group = group["cancel_count"].sum()
                summary_rows.append({
                    "最終出荷完了回数": f"{order_num}回目",
                    "キャンセル人数": int(total_in_group),
                })
            summary_df = pd.DataFrame(summary_rows)

            # サマリーグラフ
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=summary_df["最終出荷完了回数"],
                y=summary_df["キャンセル人数"],
                text=summary_df["キャンセル人数"].apply(lambda v: f"{v:,}人"),
                textposition="outside",
                marker_color="rgba(239, 83, 80, 0.7)",
            ))
            fig.update_layout(
                title="最終出荷完了回数別キャンセル人数",
                xaxis_title="最終出荷完了回数",
                yaxis_title="キャンセル人数",
                height=400,
                margin=dict(t=50, b=50),
            )
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("---")

            # 回数ごとの詳細アコーディオン
            for order_num in order_numbers:
                group = df[df["last_completed_order"] == order_num].copy()
                total_in_group = group["cancel_count"].sum()
                group["割合(%)"] = (group["cancel_count"] / total_in_group * 100).round(1)

                with st.expander(f"{order_num}回目で離脱 — {total_in_group:,}人", expanded=(order_num <= 3)):
                    display = group[["cancel_reason", "cancel_count", "割合(%)"]].copy()
                    display.columns = ["キャンセル理由", "件数", "割合(%)"]
                    display["件数"] = display["件数"].apply(lambda v: f"{int(v):,}")
                    display["割合(%)"] = display["割合(%)"].apply(lambda v: f"{v}%")
                    st.dataframe(display, use_container_width=True, hide_index=True)
