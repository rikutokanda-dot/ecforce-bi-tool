"""コホート分析ページ - 継続率・残存率・LTV."""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from src.bigquery_client import execute_query, get_bigquery_client
from src.components.cohort_heatmap import render_cohort_heatmap, render_retention_line_chart
from src.components.download_button import render_download_buttons
from src.components.filters import render_cohort_filters
from src.components.metrics_row import render_metrics
from src.queries.cohort import build_aggregate_cohort_sql, build_cohort_sql, build_drilldown_sql
from src.session import SessionKey, get_selected_company_key
from src.transforms.cohort_transform import (
    build_aggregate_table,
    build_drilldown_rate_matrices,
    build_drilldown_retention_table,
    build_retention_rate_matrix,
    build_retention_table,
    build_shipping_schedule,
    compute_aggregate_metrics,
    compute_summary_metrics,
)


# =====================================================================
# ヘルパー: 色付きHTMLテーブル (Streamlitはトップダウン実行のため先に定義)
# =====================================================================
def _styled_table(df: pd.DataFrame, value_col: str, color: str = "blue") -> str:
    """値の大きさに応じて色の濃さが変わるHTMLテーブルを生成."""
    if color == "blue":
        bg = "rgba(74, 144, 217, {alpha})"
    elif color == "green":
        bg = "rgba(52, 211, 153, {alpha})"
    else:
        bg = "rgba(74, 144, 217, {alpha})"

    max_val = df[value_col].max() if len(df) > 0 else 100

    rows_html = ""
    for _, row in df.iterrows():
        val = row[value_col]
        alpha = round(val / max_val * 0.6 + 0.05, 2) if max_val > 0 else 0.05
        bg_color = bg.format(alpha=alpha)
        text_color = "#1a1a2e" if alpha < 0.4 else "#ffffff"

        cells = ""
        for col in df.columns:
            v = row[col]
            if col == value_col:
                cells += f'<td style="background:{bg_color};color:{text_color};font-weight:600;text-align:right;padding:4px 8px;">{v}%</td>'
            elif isinstance(v, (int, float)) and col != df.columns[0]:
                cells += f'<td style="text-align:right;padding:4px 8px;">{int(v):,}</td>'
            else:
                cells += f'<td style="padding:4px 8px;">{v}</td>'
        rows_html += f"<tr>{cells}</tr>"

    header = "".join(f'<th style="padding:4px 8px;text-align:center;border-bottom:2px solid #ddd;">{c}</th>' for c in df.columns)

    return f"""
    <div style="max-height:460px;overflow-y:auto;">
    <table style="width:100%;border-collapse:collapse;font-size:13px;">
    <thead><tr>{header}</tr></thead>
    <tbody>{rows_html}</tbody>
    </table>
    </div>
    """


st.header("コホート分析")

company_key = get_selected_company_key()
if not company_key:
    st.warning("サイドバーから会社を選択してください。")
    st.stop()

date_from = st.session_state.get(SessionKey.DATE_FROM)
date_to = st.session_state.get(SessionKey.DATE_TO)

with st.sidebar:
    filters = render_cohort_filters(company_key)

client = get_bigquery_client()
drilldown_col = filters["drilldown_column"]

date_from_str = date_from.strftime("%Y-%m-%d") if date_from else None
date_to_str = date_to.strftime("%Y-%m-%d") if date_to else None

filter_params = dict(
    company_key=company_key,
    date_from=date_from_str,
    date_to=date_to_str,
    product_categories=filters["product_categories"],
    ad_groups=filters["ad_groups"],
    product_names=filters["product_names"],
)

# =====================================================================
# メインタブ: 通算 / 月別 / ドリルダウン
# =====================================================================
main_tab_aggregate, main_tab_monthly, main_tab_drilldown = st.tabs(
    ["通算", "月別コホート", "ドリルダウン"]
)


# =====================================================================
# 通算タブ — 一画面で残存率・継続率・LTVが一瞬でわかるレイアウト
# =====================================================================
with main_tab_aggregate:
    agg_sql = build_aggregate_cohort_sql(**filter_params)
    try:
        agg_df = execute_query(client, agg_sql)
    except Exception as e:
        st.error(f"BigQueryクエリ実行エラー: {e}")
        st.stop()

    if agg_df.empty:
        st.info("該当するデータが見つかりませんでした。")
    else:
        agg_metrics = compute_aggregate_metrics(agg_df)
        agg_table = build_aggregate_table(agg_df)

        if agg_table.empty:
            st.info("データがありません。")
        else:
            # ========== KPIカード ==========
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            kpi1.metric("新規顧客数", f"{agg_metrics['total_new_users']:,}")
            kpi2.metric("2回目残存率", f"{agg_metrics['retention_2']}%")

            # 6回目残存率
            r6 = agg_table.loc[agg_table["定期回数"] == "6回目", "継続率(%)"]
            kpi3.metric("6回目残存率", f"{r6.values[0]}%" if len(r6) > 0 else "-")

            kpi4.metric("12回LTV", f"¥{agg_metrics['ltv_12']:,}")

            st.markdown("")

            # ========== メイン3カラム: 残存率 / 継続率 / LTV ==========
            col_surv, col_cont, col_ltv = st.columns(3)

            # --- 残存率テーブル (N回目の人数 / 初回全体) ---
            with col_surv:
                st.markdown("##### 残存率")
                surv_df = agg_table[["定期回数", "継続人数", "継続率(%)"]].copy()
                surv_df.columns = ["回数", "人数", "残存率(%)"]

                # 色付きHTMLテーブル
                html = _styled_table(surv_df, value_col="残存率(%)", color="blue")
                st.markdown(html, unsafe_allow_html=True)

            # --- 継続率テーブル (N回目 / N-1回目 = 前回比) ---
            with col_cont:
                st.markdown("##### 継続率 (前回比)")
                cont_rows = []
                prev_count = agg_table["継続人数"].iloc[0]
                for _, row in agg_table.iterrows():
                    curr = row["継続人数"]
                    rate = round(curr / prev_count * 100, 1) if prev_count > 0 else 0.0
                    cont_rows.append({
                        "回数": row["定期回数"],
                        "人数": int(curr),
                        "継続率(%)": rate,
                    })
                    prev_count = curr
                cont_df = pd.DataFrame(cont_rows)

                html = _styled_table(cont_df, value_col="継続率(%)", color="green")
                st.markdown(html, unsafe_allow_html=True)

            # --- LTVテーブル ---
            with col_ltv:
                st.markdown("##### LTV")
                ltv_df = agg_table[["定期回数", "平均単価(円)", "累積売上(円)", "LTV(円)"]].copy()
                ltv_df.columns = ["回数", "平均単価", "累積売上", "LTV"]
                ltv_df["平均単価"] = ltv_df["平均単価"].apply(lambda v: f"¥{v:,}")
                ltv_df["累積売上"] = ltv_df["累積売上"].apply(lambda v: f"¥{v:,}")
                ltv_df["LTV"] = ltv_df["LTV"].apply(lambda v: f"¥{v:,}")

                st.dataframe(ltv_df, use_container_width=True, hide_index=True, height=460)

            st.markdown("")

            # ========== グラフ ==========
            fig = make_subplots(specs=[[{"secondary_y": True}]])

            # 残存率バー
            fig.add_trace(
                go.Bar(
                    x=agg_table["定期回数"],
                    y=agg_table["継続率(%)"],
                    name="残存率(%)",
                    marker_color="rgba(74, 144, 217, 0.7)",
                    text=agg_table["継続率(%)"].apply(lambda v: f"{v}%"),
                    textposition="outside",
                    textfont=dict(size=10),
                ),
                secondary_y=False,
            )

            # LTV折れ線
            fig.add_trace(
                go.Scatter(
                    x=agg_table["定期回数"],
                    y=agg_table["LTV(円)"],
                    name="LTV(円)",
                    mode="lines+markers+text",
                    text=[f"¥{v:,}" for v in agg_table["LTV(円)"]],
                    textposition="top center",
                    textfont=dict(size=9),
                    line=dict(color="#E74C3C", width=2.5),
                    marker=dict(size=7),
                ),
                secondary_y=True,
            )

            fig.update_layout(
                title="残存率 & LTV 推移",
                xaxis_title="定期回数",
                height=420,
                margin=dict(l=50, r=50, t=50, b=40),
                legend=dict(orientation="h", y=1.12),
            )
            fig.update_yaxes(title_text="残存率 (%)", range=[0, 110], secondary_y=False)
            fig.update_yaxes(title_text="LTV (円)", secondary_y=True)

            st.plotly_chart(fig, use_container_width=True)

            # ========== ダウンロード ==========
            st.divider()
            render_download_buttons(agg_table, f"aggregate_{company_key}")


# =====================================================================
# 月別コホートタブ
# =====================================================================
with main_tab_monthly:
    monthly_sql = build_cohort_sql(**filter_params)
    try:
        monthly_df = execute_query(client, monthly_sql)
    except Exception as e:
        st.error(f"BigQueryクエリ実行エラー: {e}")
        st.stop()

    if monthly_df.empty:
        st.info("該当するデータが見つかりませんでした。")
    else:
        summary = compute_summary_metrics(monthly_df)
        render_metrics([
            {"label": "新規顧客数 (合計)", "value": f"{summary['total_new_users']:,}"},
            {"label": "2回目平均継続率", "value": f"{summary['avg_retention_2']}%"},
            {"label": "最古月12回目残存率", "value": f"{summary['latest_12m_retention']}%"},
        ])

        st.divider()

        tab_heatmap, tab_line, tab_table, tab_schedule = st.tabs(
            ["ヒートマップ", "折れ線グラフ", "データテーブル", "発送日目安"]
        )

        rate_matrix = build_retention_rate_matrix(monthly_df)
        retention_table = build_retention_table(monthly_df)

        with tab_heatmap:
            render_cohort_heatmap(rate_matrix)

        with tab_line:
            render_retention_line_chart(rate_matrix)

        with tab_table:
            st.dataframe(retention_table, use_container_width=True, hide_index=True)
            render_download_buttons(retention_table, f"cohort_{company_key}")

        with tab_schedule:
            schedule = build_shipping_schedule(
                cohort_months=monthly_df["cohort_month"].tolist(),
                company_key=company_key,
                product_name=filters["product_names"][0] if filters["product_names"] else None,
            )
            if not schedule.empty:
                st.dataframe(schedule, use_container_width=True, hide_index=True)
            else:
                st.info("発送スケジュールを表示するデータがありません。")


# =====================================================================
# ドリルダウンタブ
# =====================================================================
with main_tab_drilldown:
    if not drilldown_col:
        st.info("サイドバーの「ドリルダウン軸」で商品名・広告グループ・商品カテゴリを選択してください。")
    else:
        dd_sql = build_drilldown_sql(drilldown_column=drilldown_col, **filter_params)
        try:
            dd_df = execute_query(client, dd_sql)
        except Exception as e:
            st.error(f"BigQueryクエリ実行エラー: {e}")
            st.stop()

        if dd_df.empty:
            st.info("該当するドリルダウンデータが見つかりませんでした。")
        else:
            drilldown_tables = build_drilldown_retention_table(dd_df)
            drilldown_matrices = build_drilldown_rate_matrices(dd_df)

            st.info(f"{len(drilldown_tables)} グループが見つかりました。")

            for group_name in drilldown_tables:
                with st.expander(f"{group_name}", expanded=True):
                    tab_hm, tab_tbl = st.tabs(["ヒートマップ", "データテーブル"])

                    with tab_hm:
                        if group_name in drilldown_matrices:
                            render_cohort_heatmap(
                                drilldown_matrices[group_name],
                                title=f"{group_name} - 継続率",
                            )

                    with tab_tbl:
                        st.dataframe(
                            drilldown_tables[group_name],
                            use_container_width=True,
                            hide_index=True,
                        )
                        render_download_buttons(
                            drilldown_tables[group_name],
                            f"cohort_{company_key}_{group_name}",
                        )
