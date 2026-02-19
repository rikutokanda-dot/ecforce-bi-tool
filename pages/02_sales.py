"""Tier分析ページ - 通算LTVレンジ別の定期ステータス構成."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.bigquery_client import execute_query, get_bigquery_client
from src.components.filters import render_cohort_filters
from src.queries.tier import build_tier_sql
from src.session import SessionKey, get_selected_company_key

st.header("Tier分析")
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
    ad_urls=filters.get("ad_urls"),
)

# ========== メインコンテンツ ==========
if not filters["product_names"]:
    st.info("正確なデータ表示のため、サイドバーから「定期商品名」を選択してください。")
elif not st.button("表示する", key="btn_tier", type="primary"):
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
        # ステータスを分類（アクティブ / キャンセル / その他）
        def classify_status(s: str) -> str:
            s_lower = str(s).lower().strip()
            if s_lower in ("active", "アクティブ"):
                return "アクティブ"
            elif "cancel" in s_lower or "キャンセル" in s_lower:
                return "キャンセル"
            else:
                return str(s)

        df["status_group"] = df["subscription_status"].apply(classify_status)

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
        # ステータスグループ一覧
        status_groups = sorted(pivot["status_group"].unique())

        # 色マップ
        color_map = {
            "アクティブ": "rgba(52, 211, 153, 0.8)",
            "キャンセル": "rgba(239, 83, 80, 0.7)",
        }
        default_colors = [
            "rgba(74, 144, 217, 0.7)",
            "rgba(255, 193, 7, 0.7)",
            "rgba(156, 39, 176, 0.7)",
            "rgba(255, 152, 0, 0.7)",
        ]

        fig = go.Figure()
        color_idx = 0
        for status in status_groups:
            subset = pivot[pivot["status_group"] == status]
            # tier_labels順にデータを並べる
            counts = []
            for tl in tier_labels:
                row = subset[subset["tier_label"] == tl]
                counts.append(int(row["customer_count"].sum()) if not row.empty else 0)

            color = color_map.get(status)
            if color is None:
                color = default_colors[color_idx % len(default_colors)]
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

            row_data = {"Tier": tl, "合計": f"{tier_total:,}人"}
            for status in status_groups:
                status_data = tier_data[tier_data["status_group"] == status]
                count = int(status_data["customer_count"].sum()) if not status_data.empty else 0
                pct = round(count / tier_total * 100, 1) if tier_total > 0 else 0.0
                row_data[status] = f"{count:,}人 ({pct}%)"
            table_rows.append(row_data)

        result_df = pd.DataFrame(table_rows)
        st.dataframe(result_df, use_container_width=True, hide_index=True)
