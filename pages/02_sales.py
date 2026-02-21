"""Tier分析ページ - 通算LTVレンジ別の定期ステータス構成."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.bigquery_client import execute_query, get_bigquery_client
from src.components.download_button import render_download_buttons
from src.components.filters import render_cohort_filters
from src.constants import Col
from src.queries.tier import (
    build_revenue_proportion_sql,
    build_tier_by_order_count_sql,
    build_tier_sql,
)
from src.session import SessionKey, get_selected_company_key

st.header("顧客分析")
st.caption("顧客の通算LTV（累計受注金額）をTierに分け、定期ステータス別の構成を分析します。")

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
    ad_url_params=filters.get("ad_url_params"),
)


# =====================================================================
# ヘルパー
# =====================================================================
def _classify_status(s: str) -> str:
    """ステータスを分類（アクティブ / キャンセル / その他）."""
    s_lower = str(s).lower().strip()
    if s_lower in ("active", "アクティブ"):
        return "アクティブ"
    elif "cancel" in s_lower or "キャンセル" in s_lower:
        return "キャンセル"
    else:
        return str(s)


_STATUS_COLOR_MAP = {
    "アクティブ": "rgba(52, 211, 153, 0.8)",
    "キャンセル": "rgba(239, 83, 80, 0.7)",
}
_DEFAULT_COLORS = [
    "rgba(74, 144, 217, 0.7)",
    "rgba(255, 193, 7, 0.7)",
    "rgba(156, 39, 176, 0.7)",
    "rgba(255, 152, 0, 0.7)",
]

# =====================================================================
# メインタブ構成
# =====================================================================
tab_status, tab_order, tab_total, tab_revenue = st.tabs(
    ["ステータス別Tier", "定期回数別Tier", "ステータス", "売上比率"]
)


# =====================================================================
# ステータス別タブ（既存のTier分析）
# =====================================================================
with tab_status:
    if not st.button("表示する", key="btn_tier_status", type="primary"):
        st.info("フィルタを設定して「表示する」を押してください。")
    else:
        try:
            sql = build_tier_sql(**filter_params)
            df = execute_query(client, sql)
        except Exception as e:
            st.error(f"BigQueryクエリ実行エラー: {e}")
            df = pd.DataFrame()

        if df.empty:
            st.info("該当するデータが見つかりませんでした。")
        else:
            df["status_group"] = df["subscription_status"].apply(_classify_status)

            # tier_sort順でtier_labelをソート
            tier_order = (
                df[["tier_label", "tier_sort"]]
                .drop_duplicates()
                .sort_values("tier_sort")
            )
            tier_labels = tier_order["tier_label"].tolist()

            # Tierごとに集計
            pivot = (
                df.groupby(["tier_label", "tier_sort", "status_group"])["customer_count"]
                .sum()
                .reset_index()
            )

            # Tier別の合計人数
            tier_totals = pivot.groupby(["tier_label", "tier_sort"])["customer_count"].sum().reset_index()
            tier_totals.columns = ["tier_label", "tier_sort", "total"]
            tier_totals = tier_totals.sort_values("tier_sort")

            # KPI
            total_customers = int(tier_totals["total"].sum())
            active_total = int(
                pivot[pivot["status_group"] == "アクティブ"]["customer_count"].sum()
            )
            cancel_total = int(
                pivot[pivot["status_group"] == "キャンセル"]["customer_count"].sum()
            )

            kpi1, kpi2, kpi3 = st.columns(3)
            kpi1.metric("総顧客数", f"{total_customers:,}人")
            kpi2.metric("アクティブ", f"{active_total:,}人 ({round(active_total/total_customers*100, 1) if total_customers > 0 else 0}%)")
            kpi3.metric("キャンセル", f"{cancel_total:,}人 ({round(cancel_total/total_customers*100, 1) if total_customers > 0 else 0}%)")

            st.markdown("---")

            # ========== 積み上げ棒グラフ ==========
            status_groups = sorted(pivot["status_group"].unique())

            fig = go.Figure()
            color_idx = 0
            for status in status_groups:
                subset = pivot[pivot["status_group"] == status]
                counts = []
                for tl in tier_labels:
                    row = subset[subset["tier_label"] == tl]
                    counts.append(int(row["customer_count"].sum()) if not row.empty else 0)

                color = _STATUS_COLOR_MAP.get(status)
                if color is None:
                    color = _DEFAULT_COLORS[color_idx % len(_DEFAULT_COLORS)]
                    color_idx += 1

                fig.add_trace(go.Bar(
                    x=tier_labels,
                    y=counts,
                    name=status,
                    marker_color=color,
                    text=[f"{c:,}" for c in counts],
                    textposition="inside",
                ))

            fig.update_layout(
                barmode="stack",
                title="Tier別 定期ステータス構成",
                xaxis_title="LTV Tier",
                yaxis_title="顧客数",
                height=500,
                margin=dict(t=50, b=80),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                xaxis=dict(tickangle=-30),
            )
            st.plotly_chart(fig, use_container_width=True)

            # ========== 詳細テーブル ==========
            st.markdown("##### Tier別詳細")

            table_rows = []
            for tl in tier_labels:
                tier_data = pivot[pivot["tier_label"] == tl]
                tier_total = int(tier_data["customer_count"].sum())
                proportion = round(tier_total / total_customers * 100, 1) if total_customers > 0 else 0.0

                row_data = {"Tier": tl, "合計": f"{tier_total:,}人", "全体比(%)": f"{proportion}%"}
                for status in status_groups:
                    status_data = tier_data[tier_data["status_group"] == status]
                    count = int(status_data["customer_count"].sum()) if not status_data.empty else 0
                    pct = round(count / tier_total * 100, 1) if tier_total > 0 else 0.0
                    row_data[status] = f"{count:,}人 ({pct}%)"

                    # アクティブの全体比
                    if status == "アクティブ":
                        active_pct = round(count / active_total * 100, 1) if active_total > 0 else 0.0
                        row_data["アクティブ全体比"] = f"{active_pct}%"
                table_rows.append(row_data)

            result_df = pd.DataFrame(table_rows)
            st.dataframe(result_df, use_container_width=True, hide_index=True)


# =====================================================================
# 定期回数別タブ
# =====================================================================
with tab_order:
    if not st.button("表示する", key="btn_tier_order", type="primary"):
        st.info("フィルタを設定して「表示する」を押してください。")
    else:
        try:
            sql_order = build_tier_by_order_count_sql(**filter_params)
            df_order = execute_query(client, sql_order)
        except Exception as e:
            st.error(f"BigQueryクエリ実行エラー: {e}")
            df_order = pd.DataFrame()

        if df_order.empty:
            st.info("該当するデータが見つかりませんでした。")
        else:
            # tier_sort順でtier_labelをソート
            tier_order_labels = (
                df_order[["tier_label", "tier_sort"]]
                .drop_duplicates()
                .sort_values("tier_sort")
            )
            tier_labels_o = tier_order_labels["tier_label"].tolist()
            order_counts = sorted(df_order["order_count"].unique())

            # KPI
            total_o = int(df_order["customer_count"].sum())
            st.metric("総顧客数", f"{total_o:,}人")
            st.markdown("---")

            # クロス集計テーブル: rows=Tier, columns=回数
            pivot_o = df_order.pivot_table(
                index=["tier_label", "tier_sort"],
                columns="order_count",
                values="customer_count",
                aggfunc="sum",
                fill_value=0,
            )
            pivot_o = pivot_o.reset_index().sort_values("tier_sort")

            # 表示用テーブル
            display_rows = []
            for _, row in pivot_o.iterrows():
                row_data = {"Tier": row["tier_label"]}
                tier_total = 0
                for oc in order_counts:
                    count = int(row.get(oc, 0))
                    tier_total += count
                    row_data[f"{int(oc)}回目"] = count
                row_data["合計"] = tier_total
                display_rows.append(row_data)

            display_df = pd.DataFrame(display_rows)

            # ヒートマップ風テーブル表示
            st.markdown("##### Tier × 定期回数 クロス集計")
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            render_download_buttons(display_df, f"tier_by_order_{company_key}")

            # 棒グラフ: Tier別の回数分布
            fig_o = go.Figure()
            for tl in tier_labels_o:
                tier_data = df_order[df_order["tier_label"] == tl]
                fig_o.add_trace(go.Bar(
                    x=[f"{int(oc)}回目" for oc in order_counts],
                    y=[
                        int(tier_data[tier_data["order_count"] == oc]["customer_count"].sum())
                        if not tier_data[tier_data["order_count"] == oc].empty else 0
                        for oc in order_counts
                    ],
                    name=tl,
                ))

            fig_o.update_layout(
                barmode="group",
                title="Tier別 定期回数分布",
                xaxis_title="定期回数",
                yaxis_title="顧客数",
                height=500,
                margin=dict(t=50, b=50),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_o, use_container_width=True)


# =====================================================================
# 合計タブ
# =====================================================================
with tab_total:
    if not st.button("表示する", key="btn_tier_total", type="primary"):
        st.info("フィルタを設定して「表示する」を押してください。")
    else:
        try:
            sql_total = build_tier_sql(**filter_params)
            df_total = execute_query(client, sql_total)
        except Exception as e:
            st.error(f"BigQueryクエリ実行エラー: {e}")
            df_total = pd.DataFrame()

        if df_total.empty:
            st.info("該当するデータが見つかりませんでした。")
        else:
            df_total["status_group"] = df_total["subscription_status"].apply(_classify_status)

            # Tier関係なく全体集計
            status_summary = df_total.groupby("status_group")["customer_count"].sum().reset_index()
            total_all = int(status_summary["customer_count"].sum())

            # KPIカード
            kpi_cols = st.columns(len(status_summary) + 1)
            kpi_cols[0].metric("定期有効者数（総計）", f"{total_all:,}人")
            for i, (_, row) in enumerate(status_summary.iterrows()):
                count = int(row["customer_count"])
                pct = round(count / total_all * 100, 1) if total_all > 0 else 0.0
                kpi_cols[i + 1].metric(
                    row["status_group"],
                    f"{count:,}人",
                    delta=f"{pct}%",
                )

            st.markdown("---")

            # 円グラフ
            fig_pie = go.Figure(data=[go.Pie(
                labels=status_summary["status_group"],
                values=status_summary["customer_count"],
                textinfo="label+percent+value",
                marker_colors=[
                    _STATUS_COLOR_MAP.get(s, "rgba(74, 144, 217, 0.7)")
                    for s in status_summary["status_group"]
                ],
            )])
            fig_pie.update_layout(
                title="定期ステータス構成",
                height=400,
            )
            st.plotly_chart(fig_pie, use_container_width=True)

            # 詳細テーブル
            detail_rows = []
            for _, row in status_summary.iterrows():
                count = int(row["customer_count"])
                pct = round(count / total_all * 100, 1) if total_all > 0 else 0.0
                detail_rows.append({
                    "ステータス": row["status_group"],
                    "人数": f"{count:,}人",
                    "割合(%)": f"{pct}%",
                })
            st.dataframe(pd.DataFrame(detail_rows), use_container_width=True, hide_index=True)


# =====================================================================
# 売上比率タブ
# =====================================================================
with tab_revenue:
    st.markdown("##### 売上比率")

    # 集計軸の選択
    REVENUE_AXES = {
        "商品カテゴリ": Col.PRODUCT_CATEGORY,
        "広告グループ": Col.AD_GROUP,
        "定期商品名": Col.SUBSCRIPTION_PRODUCT_NAME,
        "定期回数": "__order_count__",
    }

    col_axis, col_from, col_to = st.columns([2, 1, 1])
    with col_axis:
        selected_axis = st.selectbox(
            "集計軸", list(REVENUE_AXES.keys()), index=0, key="revenue_axis"
        )

    # デフォルト期間: 先月
    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    with col_from:
        rev_from = st.date_input(
            "売上日 開始", value=last_month_start, key="revenue_date_from"
        )
    with col_to:
        rev_to = st.date_input(
            "売上日 終了", value=last_month_end, key="revenue_date_to"
        )

    if not st.button("表示する", key="btn_tier_revenue", type="primary"):
        st.info("集計軸と期間を設定して「表示する」を押してください。")
    else:
        rev_params = dict(
            company_key=company_key,
            group_by_column=REVENUE_AXES[selected_axis],
            date_from=rev_from.strftime("%Y-%m-%d"),
            date_to=rev_to.strftime("%Y-%m-%d"),
            product_categories=filters["product_categories"],
            ad_groups=filters["ad_groups"],
            product_names=filters["product_names"],
            ad_url_params=filters.get("ad_url_params"),
            cohort_date_from=date_from_str,
            cohort_date_to=date_to_str,
        )

        try:
            sql_rev = build_revenue_proportion_sql(**rev_params)
            df_rev = execute_query(client, sql_rev)
        except Exception as e:
            st.error(f"BigQueryクエリ実行エラー: {e}")
            df_rev = pd.DataFrame()

        if df_rev.empty:
            st.info("該当するデータが見つかりませんでした。")
        else:
            total_rev = df_rev["total_revenue"].sum()
            df_rev["売上比率(%)"] = (df_rev["total_revenue"] / total_rev * 100).round(1)

            # 定期回数の場合、ソート順を整数にする
            if selected_axis == "定期回数":
                df_rev["_sort"] = df_rev["group_value"].astype(float).astype(int)
                df_rev = df_rev.sort_values("_sort")
                df_rev["group_value"] = df_rev["_sort"].astype(str) + "回目"
                df_rev = df_rev.drop(columns=["_sort"])

            # ピボットテーブル
            display_rev = df_rev.rename(columns={
                "group_value": selected_axis,
                "total_revenue": "売上金額(円)",
                "customer_count": "顧客数",
            })
            display_rev["売上金額(円)"] = display_rev["売上金額(円)"].apply(lambda v: f"¥{int(v):,}")
            display_rev["顧客数"] = display_rev["顧客数"].apply(lambda v: f"{int(v):,}")
            display_rev["売上比率(%)"] = display_rev["売上比率(%)"].apply(lambda v: f"{v}%")

            st.dataframe(display_rev, use_container_width=True, hide_index=True)
            render_download_buttons(
                df_rev.rename(columns={"group_value": selected_axis}),
                f"revenue_{company_key}",
            )

            # 円グラフ
            fig_rev = go.Figure(data=[go.Pie(
                labels=df_rev["group_value"],
                values=df_rev["total_revenue"],
                textinfo="label+percent",
                hole=0.3,
            )])
            fig_rev.update_layout(
                title=f"売上比率 ({selected_axis}別)",
                height=500,
            )
            st.plotly_chart(fig_rev, use_container_width=True)
