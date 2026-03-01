"""解約分析ページ - キャンセル理由・定期回数別キャンセル理由."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.bigquery_client import execute_query, get_bigquery_client
from src.components.download_button import render_download_buttons
from src.components.filters import render_cohort_filters
from src.queries.churn import (
    build_churn_reason_sql,
    build_churn_by_order_reason_sql,
    build_return_cancel_reason_sql,
    build_return_by_order_cancel_reason_sql,
    build_return_rate_sql,
    build_shipped_order_ids_sql,
)
from src.session import SessionKey, get_selected_company_key

st.header("解約分析")
st.caption("キャンセル理由、定期回数別のキャンセル理由を分析します。")

company_key = get_selected_company_key()
if not company_key:
    st.warning("サイドバーから会社を選択してください。")
    st.stop()

date_from = st.session_state.get(SessionKey.DATE_FROM)
date_to = st.session_state.get(SessionKey.DATE_TO)
sales_date_from = st.session_state.get(SessionKey.SALES_DATE_FROM)
sales_date_to = st.session_state.get(SessionKey.SALES_DATE_TO)

with st.sidebar:
    filters = render_cohort_filters(company_key)

client = get_bigquery_client()

date_from_str = date_from.strftime("%Y-%m-%d") if date_from else None
date_to_str = date_to.strftime("%Y-%m-%d") if date_to else None
sales_from_str = sales_date_from.strftime("%Y-%m-%d") if sales_date_from else None
sales_to_str = sales_date_to.strftime("%Y-%m-%d") if sales_date_to else None

filter_params = dict(
    company_key=company_key,
    date_from=date_from_str,
    date_to=date_to_str,
    product_categories=filters["product_categories"],
    ad_groups=filters["ad_groups"],
    product_names=filters["product_names"],
    ad_url_params=filters.get("ad_url_params"),
    sales_date_from=sales_from_str,
    sales_date_to=sales_to_str,
)

# ========== メインコンテンツ ==========
reason_tab, order_reason_tab, return_tab = st.tabs(["キャンセル理由", "定期回数別キャンセル理由", "返品率"])

# ---------- キャンセル理由 ----------
with reason_tab:
    if not st.button("表示する", key="btn_reason", type="primary"):
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
    if not st.button("表示する", key="btn_order_reason", type="primary"):
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

            # ========== サマリーテーブル ==========
            st.markdown("---")
            st.markdown("##### 定期回数別キャンセル理由 サマリー")

            # クロス集計: rows=キャンセル理由, columns=N回目
            pivot_churn = df.pivot_table(
                index="cancel_reason",
                columns="last_completed_order",
                values="cancel_count",
                aggfunc="sum",
                fill_value=0,
            )
            pivot_churn.columns = [f"{int(c)}回目" for c in pivot_churn.columns]
            pivot_churn["合計"] = pivot_churn.sum(axis=1)
            pivot_churn = pivot_churn.sort_values("合計", ascending=False)
            pivot_churn = pivot_churn.reset_index()
            pivot_churn = pivot_churn.rename(columns={"cancel_reason": "キャンセル理由"})

            st.dataframe(pivot_churn, use_container_width=True, hide_index=True)
            render_download_buttons(pivot_churn, f"churn_by_order_{company_key}")

# ---------- 返品率 ----------
with return_tab:
    if st.button("表示する", key="btn_return_rate", type="primary"):
        st.session_state["return_tab_show"] = True

    if not st.session_state.get("return_tab_show"):
        st.info("フィルタを設定して「表示する」を押してください。")
    else:
        try:
            sql = build_return_rate_sql(**filter_params)
            df_ret = execute_query(client, sql)
        except Exception as e:
            st.error(f"BigQueryクエリ実行エラー: {e}")
            df_ret = pd.DataFrame()

        if df_ret.empty:
            st.info("返品データが見つかりませんでした。")
        else:
            df_ret["sub_count"] = df_ret["sub_count"].astype(int)
            df_ret["shipped_count"] = df_ret["shipped_count"].astype(int)
            df_ret["return_count"] = df_ret["return_count"].astype(int)
            df_ret = df_ret.sort_values("sub_count")

            # shipped_count = 総出荷(9ステータス), return_count = 返品(7ステータス)
            df_ret["total_shipped"] = df_ret["shipped_count"]
            df_ret["return_rate"] = (
                df_ret["return_count"] / df_ret["total_shipped"] * 100
            ).round(1).fillna(0)

            # === 合計の返品率 ===
            total_shipped_all = df_ret["total_shipped"].sum()
            total_return_all = df_ret["return_count"].sum()
            total_rate = round(total_return_all / total_shipped_all * 100, 1) if total_shipped_all > 0 else 0.0

            st.markdown("##### 合計")
            cols = st.columns(3)
            cols[0].metric("総出荷件数", f"{total_shipped_all:,}件")
            cols[1].metric("返品件数", f"{total_return_all:,}件")
            cols[2].metric("返品率", f"{total_rate}%")

            st.markdown("---")

            # === 定期回数別の返品率 ===
            st.markdown("##### 定期回数別 返品率")

            display_ret = df_ret[["sub_count", "total_shipped", "return_count", "return_rate"]].copy()
            display_ret.columns = ["定期回数", "出荷件数", "返品件数", "返品率(%)"]
            display_ret["定期回数"] = display_ret["定期回数"].apply(lambda v: f"{v}回目")

            # グラフ
            fig_ret = go.Figure()
            fig_ret.add_trace(go.Bar(
                x=display_ret["定期回数"],
                y=display_ret["返品率(%)"],
                text=display_ret["返品率(%)"].apply(lambda v: f"{v}%"),
                textposition="outside",
                marker_color="rgba(255, 152, 0, 0.7)",
            ))
            fig_ret.update_layout(
                title="定期回数別 返品率",
                xaxis_title="定期回数",
                yaxis_title="返品率(%)",
                height=400,
                margin=dict(t=50, b=50),
                yaxis=dict(rangemode="tozero"),
            )
            st.plotly_chart(fig_ret, use_container_width=True)

            # テーブル
            table_ret = display_ret.copy()
            table_ret["出荷件数"] = table_ret["出荷件数"].apply(lambda v: f"{int(v):,}")
            table_ret["返品件数"] = table_ret["返品件数"].apply(lambda v: f"{int(v):,}")
            table_ret["返品率(%)"] = table_ret["返品率(%)"].apply(lambda v: f"{v}%")
            st.dataframe(table_ret, use_container_width=True, hide_index=True)
            render_download_buttons(display_ret, f"return_rate_{company_key}")

            # === 出荷件数の受注IDダウンロード ===
            st.markdown("---")
            st.markdown("##### 出荷完了 受注IDダウンロード")
            if st.button("出荷完了の受注IDを取得", key="btn_shipped_ids"):
                try:
                    sql_shipped = build_shipped_order_ids_sql(**filter_params)
                    df_shipped = execute_query(client, sql_shipped)
                    if df_shipped.empty:
                        st.session_state["shipped_csv_data"] = None
                        st.session_state["shipped_csv_count"] = 0
                    else:
                        st.session_state["shipped_csv_data"] = df_shipped.to_csv(index=False).encode("utf-8-sig")
                        st.session_state["shipped_csv_count"] = len(df_shipped)
                except Exception as e:
                    st.error(f"クエリ実行エラー: {e}")
                    st.session_state.pop("shipped_csv_data", None)

            if st.session_state.get("shipped_csv_data") is not None:
                cnt = st.session_state["shipped_csv_count"]
                st.success(f"出荷完了: {cnt:,}件")
                st.download_button(
                    label=f"受注ID CSVダウンロード ({cnt:,}件)",
                    data=st.session_state["shipped_csv_data"],
                    file_name=f"shipped_orders_{company_key}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            elif st.session_state.get("shipped_csv_count") == 0 and "shipped_csv_data" in st.session_state:
                st.warning("出荷完了データが見つかりませんでした。")

            # === 返品者のキャンセル理由 ===
            st.markdown("---")
            st.markdown("##### 返品者のキャンセル理由")

            try:
                sql_rc = build_return_cancel_reason_sql(**filter_params)
                df_rc = execute_query(client, sql_rc)
            except Exception as e:
                st.error(f"クエリ実行エラー: {e}")
                df_rc = pd.DataFrame()

            if df_rc.empty:
                st.info("返品者のキャンセル理由データが見つかりませんでした。")
            else:
                total_rc = df_rc["cancel_count"].sum()
                df_rc["割合(%)"] = (df_rc["cancel_count"] / total_rc * 100).round(1)

                fig_rc = go.Figure()
                fig_rc.add_trace(go.Bar(
                    x=df_rc["cancel_count"],
                    y=df_rc["cancel_reason"],
                    orientation="h",
                    text=df_rc.apply(
                        lambda r: f"{int(r['cancel_count']):,}件 ({r['割合(%)']}%)", axis=1
                    ),
                    textposition="outside",
                    marker_color="rgba(255, 152, 0, 0.7)",
                ))
                fig_rc.update_layout(
                    title="返品者のキャンセル理由",
                    xaxis_title="件数",
                    height=max(300, len(df_rc) * 35 + 100),
                    margin=dict(t=50, b=50, l=250),
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(fig_rc, use_container_width=True)

                disp_rc = df_rc.copy()
                disp_rc.columns = ["キャンセル理由", "件数", "割合(%)"]
                disp_rc["件数"] = disp_rc["件数"].apply(lambda v: f"{int(v):,}")
                disp_rc["割合(%)"] = disp_rc["割合(%)"].apply(lambda v: f"{v}%")
                st.dataframe(disp_rc, use_container_width=True, hide_index=True)

            # === 定期回数別・返品者のキャンセル理由 ===
            st.markdown("---")
            st.markdown("##### 定期回数別 返品者のキャンセル理由")

            try:
                sql_rco = build_return_by_order_cancel_reason_sql(**filter_params)
                df_rco = execute_query(client, sql_rco)
            except Exception as e:
                st.error(f"クエリ実行エラー: {e}")
                df_rco = pd.DataFrame()

            if df_rco.empty:
                st.info("該当するデータが見つかりませんでした。")
            else:
                df_rco["sub_count"] = df_rco["sub_count"].astype(int)
                df_rco = df_rco.sort_values(["sub_count", "cancel_count"], ascending=[True, False])

                order_numbers_r = sorted(df_rco["sub_count"].unique())

                for order_num in order_numbers_r:
                    group = df_rco[df_rco["sub_count"] == order_num].copy()
                    total_in_group = group["cancel_count"].sum()
                    group["割合(%)"] = (group["cancel_count"] / total_in_group * 100).round(1)

                    with st.expander(
                        f"{order_num}回目で返品 — {total_in_group:,}人",
                        expanded=(order_num <= 3),
                    ):
                        display_rco = group[["cancel_reason", "cancel_count", "割合(%)"]].copy()
                        display_rco.columns = ["キャンセル理由", "件数", "割合(%)"]
                        display_rco["件数"] = display_rco["件数"].apply(lambda v: f"{int(v):,}")
                        display_rco["割合(%)"] = display_rco["割合(%)"].apply(lambda v: f"{v}%")
                        st.dataframe(display_rco, use_container_width=True, hide_index=True)

                # クロス集計
                st.markdown("---")
                st.markdown("##### 定期回数別 返品キャンセル理由 サマリー")
                pivot_rco = df_rco.pivot_table(
                    index="cancel_reason",
                    columns="sub_count",
                    values="cancel_count",
                    aggfunc="sum",
                    fill_value=0,
                )
                pivot_rco.columns = [f"{int(c)}回目" for c in pivot_rco.columns]
                pivot_rco["合計"] = pivot_rco.sum(axis=1)
                pivot_rco = pivot_rco.sort_values("合計", ascending=False).reset_index()
                pivot_rco = pivot_rco.rename(columns={"cancel_reason": "キャンセル理由"})
                st.dataframe(pivot_rco, use_container_width=True, hide_index=True)
                render_download_buttons(pivot_rco, f"return_cancel_reason_{company_key}")
