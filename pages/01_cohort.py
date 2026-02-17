"""コホート分析ページ - 継続率・残存率・LTV."""

from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from src.bigquery_client import execute_query, get_bigquery_client
from src.components.cohort_heatmap import render_cohort_heatmap, render_retention_line_chart
from src.components.download_button import render_download_buttons
from src.components.filters import render_cohort_filters
from src.components.metrics_row import render_metrics
from src.config_loader import get_product_cycle, get_upsell_target
from src.constants import Col
from src.queries.cohort import (
    build_aggregate_cohort_sql,
    build_cohort_sql,
    build_drilldown_sql,
    build_max_date_sql,
    build_upsell_sql,
)
from src.session import SessionKey, get_selected_company_key
from src.transforms.cohort_transform import (
    apply_completeness_mask_to_summary,
    build_1year_ltv_table,
    build_aggregate_table,
    build_drilldown_rate_matrices,
    build_drilldown_retention_table,
    build_product_summary_table,
    build_retention_rate_matrix,
    build_retention_table,
    build_shipping_schedule,
    compute_aggregate_metrics,
    compute_max_orders_in_period,
    compute_summary_metrics,
    compute_upsell_rate,
)


# =====================================================================
# ヘルパー: 色付きHTMLテーブル
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
        for col_name in df.columns:
            v = row[col_name]
            if col_name == value_col:
                cells += f'<td style="background:{bg_color};color:{text_color};font-weight:600;text-align:right;padding:4px 8px;">{v}%</td>'
            elif isinstance(v, (int, float)) and col_name != df.columns[0]:
                cells += f'<td style="text-align:right;padding:4px 8px;">{int(v):,}</td>'
            else:
                cells += f'<td style="padding:4px 8px;">{v}</td>'
        rows_html += f"<tr>{cells}</tr>"

    header = "".join(
        f'<th style="padding:4px 8px;text-align:center;border-bottom:2px solid #ddd;">{c}</th>'
        for c in df.columns
    )

    return f"""
    <div style="max-height:460px;overflow-y:auto;">
    <table style="width:100%;border-collapse:collapse;font-size:13px;">
    <thead><tr>{header}</tr></thead>
    <tbody>{rows_html}</tbody>
    </table>
    </div>
    """


# =====================================================================
# ページ初期化
# =====================================================================
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

# データ最終日を取得
try:
    max_date_df = execute_query(client, build_max_date_sql(company_key))
    data_cutoff_date = max_date_df["max_date"].iloc[0].date() if not max_date_df.empty else date.today()
except Exception:
    data_cutoff_date = date.today()


# =====================================================================
# メインタブ: 商品別 / 通算 / 月別コホート
# =====================================================================
main_tab_product, main_tab_aggregate, main_tab_monthly = st.tabs(
    ["商品別", "通算", "月別コホート"]
)


# =====================================================================
# 商品別タブ (デフォルト)
# =====================================================================
with main_tab_product:
    dd_col = Col.SUBSCRIPTION_PRODUCT_NAME
    dd_sql = build_drilldown_sql(drilldown_column=dd_col, **filter_params)
    try:
        dd_df = execute_query(client, dd_sql)
    except Exception as e:
        st.error(f"BigQueryクエリ実行エラー: {e}")
        st.stop()

    if dd_df.empty:
        st.info("該当するデータが見つかりませんでした。")
    else:
        product_names_list = sorted(dd_df["dimension_col"].unique())
        st.info(f"{len(product_names_list)} 商品が見つかりました。")

        for pname in product_names_list:
            with st.expander(f"{pname}", expanded=False):
                summary = build_product_summary_table(dd_df, pname)
                if summary.empty:
                    st.info("データがありません。")
                    continue

                cohort_months = dd_df[dd_df["dimension_col"] == pname]["cohort_month"].tolist()
                summary = apply_completeness_mask_to_summary(
                    summary, cohort_months, pname, data_cutoff_date
                )

                st.dataframe(summary, use_container_width=True, hide_index=True)

                # アップセル率
                upsell_target = get_upsell_target(pname)
                if upsell_target:
                    cols = st.columns(2)
                    if upsell_target.get("upsell_name"):
                        us_sql = build_upsell_sql(
                            company_key, pname, upsell_target["upsell_name"],
                            date_from_str, date_to_str,
                        )
                        try:
                            us_df = execute_query(client, us_sql)
                            us_rate = compute_upsell_rate(us_df)
                            cols[0].metric(
                                f"アップセル率 → {upsell_target['upsell_name'][:20]}...",
                                f"{us_rate}%",
                            )
                        except Exception:
                            cols[0].metric("アップセル率", "エラー")

                    if upsell_target.get("upsell_upsell_name"):
                        uu_sql = build_upsell_sql(
                            company_key,
                            upsell_target["upsell_name"],
                            upsell_target["upsell_upsell_name"],
                            date_from_str, date_to_str,
                        )
                        try:
                            uu_df = execute_query(client, uu_sql)
                            uu_rate = compute_upsell_rate(uu_df)
                            cols[1].metric(
                                f"アップセル² → {upsell_target['upsell_upsell_name'][:20]}...",
                                f"{uu_rate}%",
                            )
                        except Exception:
                            cols[1].metric("アップセル²率", "エラー")


# =====================================================================
# 通算タブ — 残存率・継続率・1年LTV
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
            # 1年LTV計算
            selected_pnames = filters.get("product_names")
            if selected_pnames and len(selected_pnames) == 1:
                cycle1, cycle2 = get_product_cycle(selected_pnames[0])
            else:
                cycle1, cycle2 = 30, 30

            proj_rates = st.session_state.get("proj_rates", {})
            proj_amounts = st.session_state.get("proj_amounts", {})

            ltv_table = build_1year_ltv_table(
                agg_df, cycle1, cycle2,
                projected_rates=proj_rates or None,
                projected_amounts=proj_amounts or None,
            )

            # ========== KPIカード ==========
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            kpi1.metric("新規顧客数", f"{agg_metrics['total_new_users']:,}")
            kpi2.metric("2回目残存率", f"{agg_metrics['retention_2']}%")

            r6 = agg_table.loc[agg_table["定期回数"] == "6回目", "継続率(%)"]
            kpi3.metric("6回目残存率", f"{r6.values[0]}%" if len(r6) > 0 else "-")

            if not ltv_table.empty:
                year_ltv = ltv_table["LTV(円)"].iloc[-1]
                kpi4.metric("1年LTV", f"¥{year_ltv:,}")
            else:
                kpi4.metric("1年LTV", "-")

            st.markdown("")

            # ========== メイン3カラム ==========
            col_surv, col_cont, col_ltv = st.columns(3)

            with col_surv:
                st.markdown("##### 残存率")
                surv_df = agg_table[["定期回数", "継続人数", "継続率(%)"]].copy()
                surv_df.columns = ["回数", "人数", "残存率(%)"]
                html = _styled_table(surv_df, value_col="残存率(%)", color="blue")
                st.markdown(html, unsafe_allow_html=True)

            with col_cont:
                st.markdown("##### 継続率 (前回比)")
                cont_rows = []
                prev_count = agg_table["継続人数"].iloc[0]
                for _, r in agg_table.iterrows():
                    curr = r["継続人数"]
                    rate = round(curr / prev_count * 100, 1) if prev_count > 0 else 0.0
                    cont_rows.append({
                        "回数": r["定期回数"],
                        "人数": int(curr),
                        "継続率(%)": rate,
                    })
                    prev_count = curr
                cont_df = pd.DataFrame(cont_rows)
                html = _styled_table(cont_df, value_col="継続率(%)", color="green")
                st.markdown(html, unsafe_allow_html=True)

            with col_ltv:
                st.markdown("##### 1年LTV")
                if not ltv_table.empty:
                    display_ltv = ltv_table[["定期回数", "平均単価(円)", "LTV(円)", "予測"]].copy()
                    display_ltv["平均単価(円)"] = display_ltv["平均単価(円)"].apply(lambda v: f"¥{v:,}")
                    display_ltv["LTV(円)"] = display_ltv["LTV(円)"].apply(lambda v: f"¥{v:,}")
                    display_ltv["予測"] = display_ltv["予測"].apply(lambda v: "予測" if v else "実績")
                    st.dataframe(display_ltv, use_container_width=True, hide_index=True, height=460)

            # ========== 予測値の編集 ==========
            if not ltv_table.empty and ltv_table["予測"].any():
                st.markdown("---")
                st.markdown("##### 予測値の編集")
                st.caption("予測行の継続率・平均単価を編集すると1年LTVが再計算されます")

                proj_rows = ltv_table[ltv_table["予測"]].copy()
                edit_df = proj_rows[["定期回数", "継続率(%)", "平均単価(円)"]].copy()

                edited = st.data_editor(
                    edit_df,
                    key="ltv_editor",
                    disabled=["定期回数"],
                    use_container_width=True,
                )

                if st.button("再計算", key="recalc_ltv"):
                    new_rates = {}
                    new_amounts = {}
                    for _, erow in edited.iterrows():
                        order_num = int(erow["定期回数"].replace("回目", ""))
                        new_rates[order_num] = float(erow["継続率(%)"])
                        new_amounts[order_num] = float(erow["平均単価(円)"])
                    st.session_state["proj_rates"] = new_rates
                    st.session_state["proj_amounts"] = new_amounts
                    st.rerun()

            st.markdown("")

            # ========== グラフ ==========
            fig = make_subplots(specs=[[{"secondary_y": True}]])

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

            if not ltv_table.empty:
                fig.add_trace(
                    go.Scatter(
                        x=ltv_table["定期回数"],
                        y=ltv_table["LTV(円)"],
                        name="1年LTV(円)",
                        mode="lines+markers+text",
                        text=[f"¥{v:,}" for v in ltv_table["LTV(円)"]],
                        textposition="top center",
                        textfont=dict(size=9),
                        line=dict(color="#E74C3C", width=2.5),
                        marker=dict(size=7),
                    ),
                    secondary_y=True,
                )

            fig.update_layout(
                title="残存率 & 1年LTV 推移",
                xaxis_title="定期回数",
                height=420,
                margin=dict(l=50, r=50, t=50, b=40),
                legend=dict(orientation="h", y=1.12),
            )
            fig.update_yaxes(title_text="残存率 (%)", range=[0, 110], secondary_y=False)
            fig.update_yaxes(title_text="LTV (円)", secondary_y=True)

            st.plotly_chart(fig, use_container_width=True)

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
        summary_m = compute_summary_metrics(monthly_df)
        render_metrics([
            {"label": "新規顧客数 (合計)", "value": f"{summary_m['total_new_users']:,}"},
            {"label": "2回目平均継続率", "value": f"{summary_m['avg_retention_2']}%"},
            {"label": "最古月12回目残存率", "value": f"{summary_m['latest_12m_retention']}%"},
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
            selected_pn = filters["product_names"][0] if filters["product_names"] else None
            schedule = build_shipping_schedule(
                cohort_months=monthly_df["cohort_month"].tolist(),
                product_name=selected_pn,
            )
            if not schedule.empty:
                st.dataframe(schedule, use_container_width=True, hide_index=True)
            else:
                st.info("発送スケジュールを表示するデータがありません。")
