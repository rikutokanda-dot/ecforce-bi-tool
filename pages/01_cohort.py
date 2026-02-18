"""コホート分析ページ - 継続率・残存率・LTV・アップセル率."""

from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from src.bigquery_client import execute_query, fetch_filtered_options, get_bigquery_client
from src.components.cohort_heatmap import render_cohort_heatmap, render_retention_line_chart
from src.components.download_button import render_download_buttons
from src.components.filters import render_cohort_filters
from src.components.metrics_row import render_metrics
from src.config_loader import get_product_cycle, get_upsell_target, get_upsell_targets, load_upsell_mappings
from src.constants import Col
from src.queries.common import get_table_ref
from src.queries.cohort import (
    build_aggregate_cohort_sql,
    build_cohort_sql,
    build_drilldown_sql,
    build_max_date_sql,
    build_upsell_rate_monthly_sql,
    build_upsell_rate_sql,
    build_upsell_sql,
)
from src.session import SessionKey, get_selected_company_key
from src.transforms.cohort_transform import (
    build_1year_ltv_table,
    build_aggregate_table,
    build_dimension_summary_table,
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
# ヘルパー: アップセル率表示
# =====================================================================
def _upsell_label_html(title: str, before_name: str, after_name: str) -> str:
    """アップセル率の2段ラベルHTMLを生成."""
    return (
        f"**{title}**\n\n"
        f"US前：{before_name}  \n"
        f"US後：{after_name}"
    )


def _render_upsell_pair(
    client,
    company_key: str,
    normal_names: str | list[str],
    upsell_name: str,
    label_title: str,
    date_from_str: str | None,
    date_to_str: str | None,
    *,
    skip_if_no_normal: bool = False,
    pair_key: str = "",
):
    """1組のアップセル率を表示（初回判定のみ）。skip時はUI自体を出さない。"""
    # skip_if_no_normal の場合、まずデータ有無を確認してからフラグメント描画
    if skip_if_no_normal:
        sql_check = build_upsell_rate_sql(
            company_key, normal_names, upsell_name,
            date_from_str, date_to_str,
        )
        try:
            df_check = execute_query(client, sql_check)
            if df_check.empty or df_check["upsell_rate"].iloc[0] is None:
                return
            if int(df_check.iloc[0]["normal_count"]) == 0:
                return
        except Exception:
            return

    # フラグメントとして描画（日付変更時にここだけ再実行）
    _upsell_pair_fragment(
        client, company_key, normal_names, upsell_name,
        label_title, date_from_str, date_to_str,
        pair_key=pair_key,
    )


@st.fragment
def _upsell_pair_fragment(
    client,
    company_key: str,
    normal_names: str | list[str],
    upsell_name: str,
    label_title: str,
    date_from_str: str | None,
    date_to_str: str | None,
    *,
    pair_key: str = "",
):
    """フラグメント化されたアップセル率表示。日付変更時にこの部分だけ再実行。"""
    # normal_names をリスト化して表示用文字列を作る
    if isinstance(normal_names, str):
        _normal_list = [normal_names]
    else:
        _normal_list = list(normal_names)
    _normal_display = ", ".join(_normal_list)

    _key_base = pair_key or f"{'_'.join(_normal_list)}_{upsell_name}"
    _k_from = f"us_period_from_{_key_base}"
    _k_to = f"us_period_to_{_key_base}"

    # session_state にユーザー指定日付があればそれを使う、なければ自動検出
    has_override = _k_from in st.session_state
    if has_override:
        override_from = st.session_state[_k_from].strftime("%Y-%m-%d")
        override_to = st.session_state[_k_to].strftime("%Y-%m-%d")
        query_from = override_from
        query_to = override_to
    else:
        query_from = date_from_str
        query_to = date_to_str

    sql = build_upsell_rate_sql(
        company_key, _normal_list, upsell_name,
        query_from, query_to,
    )
    try:
        df = execute_query(client, sql)
        if df.empty or df["upsell_rate"].iloc[0] is None:
            st.markdown(f"**{label_title}**　データなし")
            st.markdown(f"<small>US前：{_normal_display}<br>US後：{upsell_name}</small>",
                        unsafe_allow_html=True)
            st.divider()
            return
        row = df.iloc[0]
        rate = round(float(row["upsell_rate"]), 1)
        normal_count = int(row["normal_count"])
        upsell_count = int(row["upsell_count"])
        period_start = str(row["period_start"])[:10]
        period_end = str(row["period_end"])[:10]

        # 1行目: アップセル率 ~~%　通常:-人/アップセル:-人
        st.markdown(
            f"**{label_title}　{rate}%**　　通常: {normal_count:,}人 / アップセル: {upsell_count:,}人"
        )
        # 2行目: US前/US後
        st.markdown(
            f"<small>US前：{_normal_display}<br>US後：{upsell_name}</small>",
            unsafe_allow_html=True,
        )

        # 対象期間を date_input で表示（初回は自動検出値をデフォルトに）
        if not has_override:
            st.session_state[_k_from] = date.fromisoformat(period_start)
            st.session_state[_k_to] = date.fromisoformat(period_end)

        dcols = st.columns([1, 1])
        with dcols[0]:
            st.date_input("対象開始日", key=_k_from)
        with dcols[1]:
            st.date_input("対象終了日", key=_k_to)

        # 仕切り線
        st.divider()
    except Exception as e:
        st.markdown(f"**{label_title}**　エラー")
        st.markdown(
            f"<small>US前：{_normal_display}<br>US後：{upsell_name}</small>",
            unsafe_allow_html=True,
        )
        st.caption(f"({e})")
        st.divider()


def _render_upsell_monthly(
    client,
    company_key: str,
    normal_names: str | list[str],
    upsell_name: str,
    label_title: str,
    date_from_str: str | None,
    date_to_str: str | None,
    *,
    skip_if_no_normal: bool = False,
):
    """月別アップセル率テーブル+グラフを表示."""
    if isinstance(normal_names, str):
        _normal_list = [normal_names]
    else:
        _normal_list = list(normal_names)
    _normal_display = ", ".join(_normal_list)

    sql = build_upsell_rate_monthly_sql(
        company_key, _normal_list, upsell_name,
        date_from_str, date_to_str,
    )
    label_md = _upsell_label_html(label_title, _normal_display, upsell_name)
    try:
        df = execute_query(client, sql)
        if df.empty:
            if not skip_if_no_normal:
                st.markdown(label_md)
                st.info("データなし")
            return

        # 通常商品が全月で0人ならスキップ
        if skip_if_no_normal and df["normal_count"].sum() == 0:
            return

        display_df = df[["cohort_month", "normal_count", "upsell_count", "upsell_rate"]].copy()
        display_df.columns = ["月", "通常商品(人)", "アップセル商品(人)", "アップセル率(%)"]
        display_df["アップセル率(%)"] = display_df["アップセル率(%)"].round(1)

        st.markdown(label_md)
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # 折れ線グラフ
        if len(display_df) > 1:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=display_df["月"],
                y=display_df["アップセル率(%)"],
                mode="lines+markers+text",
                text=display_df["アップセル率(%)"].apply(lambda v: f"{v}%"),
                textposition="top center",
                textfont=dict(size=9),
                line=dict(color="#E74C3C", width=2),
                marker=dict(size=6),
            ))
            fig.update_layout(
                title=f"{label_title} 推移",
                xaxis_title="月",
                yaxis_title="アップセル率 (%)",
                height=350,
                margin=dict(l=50, r=30, t=40, b=30),
            )
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.markdown(label_md)
        st.error(f"エラー ({e})")


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
    if not max_date_df.empty and max_date_df["max_date"].iloc[0] is not None:
        raw_val = max_date_df["max_date"].iloc[0]
        if isinstance(raw_val, date):
            data_cutoff_date = raw_val
        elif hasattr(raw_val, "date"):
            data_cutoff_date = raw_val.date()
        else:
            data_cutoff_date = date.today()
    else:
        data_cutoff_date = date.today()
except Exception as e:
    st.warning(f"データカットオフ日取得エラー: {e}")
    data_cutoff_date = date.today()


# =====================================================================
# メインタブ: ドリルダウン / LTV / 月別 / アップセル率
# =====================================================================
main_tab_drilldown, main_tab_aggregate, main_tab_monthly, main_tab_upsell = st.tabs(
    ["ドリルダウン", "LTV", "月別", "アップセル率"]
)


# =====================================================================
# ドリルダウンタブ — サブタブで軸を切り替え
# =====================================================================
with main_tab_drilldown:
    dd_tab_product, dd_tab_adgroup, dd_tab_category = st.tabs(
        ["定期商品名", "広告グループ", "商品カテゴリ"]
    )

    # ========== 定期商品名 ==========
    with dd_tab_product:
        if st.button("表示する", key="btn_dd_product", type="primary"):
            st.session_state["dd_product_shown"] = True
        if not st.session_state.get("dd_product_shown"):
            st.info("フィルタを設定して「表示する」を押してください。")
        else:
            dd_sql = build_drilldown_sql(
                drilldown_column=Col.SUBSCRIPTION_PRODUCT_NAME, **filter_params
            )
            try:
                dd_df = execute_query(client, dd_sql)
            except Exception as e:
                st.error(f"BigQueryクエリ実行エラー: {e}")
                dd_df = pd.DataFrame()

            if dd_df.empty:
                st.info("該当するデータが見つかりませんでした。")
            else:
                # デフォルト: 文字数少ない順
                dim_raw = list(dd_df["dimension_col"].unique())
                dim_sorted = sorted(dim_raw, key=len)

                # ユーザーが並び替えた順序が保存されていればそれを使う
                _order_key = "dd_product_order"
                if _order_key in st.session_state:
                    saved = st.session_state[_order_key]
                    # 保存済み順序に存在する値のみ残し、新規分を末尾に追加
                    ordered = [v for v in saved if v in set(dim_raw)]
                    new_vals = [v for v in dim_sorted if v not in set(ordered)]
                    dimension_values = ordered + new_vals
                else:
                    dimension_values = dim_sorted

                st.info(f"**定期商品名別**: {len(dimension_values)} 件")
                st.caption(f"データカットオフ日: {data_cutoff_date}")

                # 並び替えUI
                sort_opt = st.radio(
                    "並び順",
                    ["文字数少ない順", "文字数多い順", "名前昇順", "名前降順"],
                    horizontal=True,
                    key="dd_product_sort",
                )
                if sort_opt == "文字数少ない順":
                    dimension_values = sorted(dimension_values, key=len)
                elif sort_opt == "文字数多い順":
                    dimension_values = sorted(dimension_values, key=len, reverse=True)
                elif sort_opt == "名前昇順":
                    dimension_values = sorted(dimension_values)
                elif sort_opt == "名前降順":
                    dimension_values = sorted(dimension_values, reverse=True)

                # 並び順を保存
                st.session_state[_order_key] = list(dimension_values)

                for pname in dimension_values:
                    with st.expander(f"{pname}", expanded=False):
                        summary = build_product_summary_table(dd_df, pname, data_cutoff_date)
                        if summary.empty:
                            st.info("データがありません。")
                            continue
                        st.dataframe(summary, use_container_width=True, hide_index=True)

    # ========== 広告グループ ==========
    with dd_tab_adgroup:
        if st.button("表示する", key="btn_dd_adgroup", type="primary"):
            st.session_state["dd_adgroup_shown"] = True
        if not st.session_state.get("dd_adgroup_shown"):
            st.info("フィルタを設定して「表示する」を押してください。")
        else:
            dd_sql_ag = build_drilldown_sql(
                drilldown_column=Col.AD_GROUP, **filter_params
            )
            try:
                dd_df_ag = execute_query(client, dd_sql_ag)
            except Exception as e:
                st.error(f"BigQueryクエリ実行エラー: {e}")
                dd_df_ag = pd.DataFrame()

            if dd_df_ag.empty:
                st.info("該当するデータが見つかりませんでした。")
            else:
                dim_ag = sorted(dd_df_ag["dimension_col"].unique())
                st.info(f"**広告グループ別**: {len(dim_ag)} 件")
                st.caption(f"データカットオフ日: {data_cutoff_date}")
                for grp_name in dim_ag:
                    with st.expander(f"{grp_name}", expanded=False):
                        summary = build_dimension_summary_table(dd_df_ag, grp_name)
                        if summary.empty:
                            st.info("データがありません。")
                            continue
                        st.dataframe(summary, use_container_width=True, hide_index=True)

    # ========== 商品カテゴリ ==========
    with dd_tab_category:
        if st.button("表示する", key="btn_dd_category", type="primary"):
            st.session_state["dd_category_shown"] = True
        if not st.session_state.get("dd_category_shown"):
            st.info("フィルタを設定して「表示する」を押してください。")
        else:
            dd_sql_cat = build_drilldown_sql(
                drilldown_column=Col.PRODUCT_CATEGORY, **filter_params
            )
            try:
                dd_df_cat = execute_query(client, dd_sql_cat)
            except Exception as e:
                st.error(f"BigQueryクエリ実行エラー: {e}")
                dd_df_cat = pd.DataFrame()

            if dd_df_cat.empty:
                st.info("該当するデータが見つかりませんでした。")
            else:
                # カテゴリごとの定期商品名を取得
                _cat_product_map: dict[str, list[str]] = {}
                _table_ref = get_table_ref(company_key)
                for _cat in dd_df_cat["dimension_col"].unique():
                    try:
                        _pnames = fetch_filtered_options(
                            client, _table_ref, Col.SUBSCRIPTION_PRODUCT_NAME,
                            {Col.PRODUCT_CATEGORY: [_cat]},
                        )
                        _cat_product_map[_cat] = _pnames
                    except Exception:
                        _cat_product_map[_cat] = []

                dim_cat = sorted(dd_df_cat["dimension_col"].unique())
                st.info(f"**商品カテゴリ別**: {len(dim_cat)} 件")
                st.caption(f"データカットオフ日: {data_cutoff_date}")
                for cat_name in dim_cat:
                    with st.expander(f"カテゴリ: {cat_name}", expanded=False):
                        # カテゴリに含まれる定期商品名を小さく表示
                        _pnames_in_cat = _cat_product_map.get(cat_name, [])
                        if _pnames_in_cat:
                            st.caption(f"対象商品: {', '.join(_pnames_in_cat)}")
                        summary = build_dimension_summary_table(dd_df_cat, cat_name)
                        if summary.empty:
                            st.info("データがありません。")
                            continue
                        st.dataframe(summary, use_container_width=True, hide_index=True)


# =====================================================================
# 通算タブ — 残存率・継続率・1年LTV
# =====================================================================
with main_tab_aggregate:
    if not filters["product_names"]:
        st.info("正確なデータ表示のため、サイドバーから「定期商品名」を選択してください。")
    elif not st.button("表示する", key="btn_aggregate", type="primary"):
        st.info("フィルタを設定して「表示する」を押してください。")
    else:
        agg_sql = build_aggregate_cohort_sql(**filter_params)
        try:
            agg_df = execute_query(client, agg_sql)
        except Exception as e:
            st.error(f"BigQueryクエリ実行エラー: {e}")
            agg_df = pd.DataFrame()

        if agg_df.empty:
            st.info("該当するデータが見つかりませんでした。")
        else:
            _agg_pnames = filters.get("product_names")
            agg_metrics = compute_aggregate_metrics(agg_df)

            # 商品名1つ選択時: ドリルダウンデータでマスク付き合算
            _agg_dd_df = None
            _agg_pname = None
            if _agg_pnames and len(_agg_pnames) == 1:
                _agg_pname = _agg_pnames[0]
                try:
                    _agg_dd_sql = build_drilldown_sql(
                        drilldown_column=Col.SUBSCRIPTION_PRODUCT_NAME,
                        **filter_params,
                    )
                    _agg_dd_df = execute_query(client, _agg_dd_sql)
                except Exception:
                    _agg_dd_df = None

            agg_table = build_aggregate_table(
                agg_df,
                drilldown_df=_agg_dd_df,
                product_name=_agg_pname,
                data_cutoff_date=data_cutoff_date,
            )

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
                    filtered_agg_table=agg_table,
                )

                # ========== KPIカード ==========
                kpi1, kpi2, kpi3, kpi4 = st.columns(4)
                kpi1.metric("新規顧客数", f"{agg_metrics['total_new_users']:,}")
                kpi2.metric("2回目残存率", f"{agg_metrics['retention_2']}%")

                r6 = agg_table.loc[agg_table["定期回数"] == "6回目", "残存率(%)"]
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
                    surv_df = agg_table[["定期回数", "継続人数", "残存率(%)"]].copy()
                    surv_df.columns = ["回数", "人数", "残存率(%)"]
                    html = _styled_table(surv_df, value_col="残存率(%)", color="blue")
                    st.markdown(html, unsafe_allow_html=True)

                with col_cont:
                    st.markdown("##### 継続率 (前回比)")
                    cont_df = agg_table[["定期回数", "継続人数", "継続率(%)"]].copy()
                    cont_df.columns = ["回数", "人数", "継続率(%)"]
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
                        y=agg_table["残存率(%)"],
                        name="残存率(%)",
                        marker_color="rgba(74, 144, 217, 0.7)",
                        text=agg_table["残存率(%)"].apply(lambda v: f"{v}%"),
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
    if not filters["product_names"]:
        st.info("正確なデータ表示のため、サイドバーから「定期商品名」を選択してください。")
    elif not st.button("表示する", key="btn_monthly", type="primary"):
        st.info("フィルタを設定して「表示する」を押してください。")
    else:
        monthly_sql = build_cohort_sql(**filter_params)
        try:
            monthly_df = execute_query(client, monthly_sql)
        except Exception as e:
            st.error(f"BigQueryクエリ実行エラー: {e}")
            monthly_df = pd.DataFrame()

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

            # 商品名1つ選択時のみマスク適用
            _monthly_pn = filters["product_names"][0] if filters["product_names"] and len(filters["product_names"]) == 1 else None
            rate_matrix = build_retention_rate_matrix(monthly_df, data_cutoff_date, _monthly_pn)
            retention_table = build_retention_table(monthly_df, data_cutoff_date, _monthly_pn)

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


# =====================================================================
# アップセル率タブ (全マッピング横断、フィルタ適用)
# =====================================================================
with main_tab_upsell:
    _all_mappings_raw = load_upsell_mappings()

    # サイドバーフィルタで対象マッピングを絞り込む
    _upsell_filter_pnames = filters.get("product_names")
    _upsell_filter_cats = filters.get("product_categories")
    if _upsell_filter_pnames:
        # 商品名が選択されていれば from_names に含まれるか
        _pname_set = set(_upsell_filter_pnames)
        all_mappings = [
            m for m in _all_mappings_raw
            if _pname_set & set(m.get("from_names", []))
        ]
    elif _upsell_filter_cats:
        # 商品カテゴリが選択されていれば、そのカテゴリに属する商品名を取得してフィルタ
        _table_ref = get_table_ref(company_key)
        _cat_product_names = fetch_filtered_options(
            client, _table_ref, Col.SUBSCRIPTION_PRODUCT_NAME,
            {Col.PRODUCT_CATEGORY: _upsell_filter_cats},
        )
        _cat_pname_set = set(_cat_product_names)
        all_mappings = [
            m for m in _all_mappings_raw
            if _cat_pname_set & set(m.get("from_names", []))
        ]
    else:
        all_mappings = list(_all_mappings_raw)

    if not _all_mappings_raw:
        st.info("アップセルマッピングが設定されていません。マスタ管理で設定してください。")
    elif not all_mappings:
        st.info("サイドバーで選択中の商品に該当するアップセルマッピングがありません。")
    else:
        if st.button("表示する", key="btn_upsell", type="primary"):
            st.session_state["upsell_tab_shown"] = True
        if not st.session_state.get("upsell_tab_shown"):
            st.info("「表示する」を押すとアップセル率を計算します。")
        else:
            # upsell_name 単位でグループ化（1アコーディオン = 1 US後商品）
            _upsell_items: list[dict] = []
            _seen_upsell: dict[str, int] = {}
            for m in all_mappings:
                fns = m.get("from_names", [])
                un = m.get("upsell_name", "")
                uun = m.get("upsell_upsell_name")
                if not fns or not un:
                    continue
                if un not in _seen_upsell:
                    _seen_upsell[un] = len(_upsell_items)
                    _upsell_items.append({
                        "from_names": list(fns),
                        "upsell_name": un,
                        "upsell_upsell_names": [uun] if uun else [],
                    })
                else:
                    item = _upsell_items[_seen_upsell[un]]
                    for fn in fns:
                        if fn not in item["from_names"]:
                            item["from_names"].append(fn)
                    if uun and uun not in item["upsell_upsell_names"]:
                        item["upsell_upsell_names"].append(uun)

            upsell_sub_agg, upsell_sub_monthly = st.tabs(["通算", "月別"])

            # ---------- 通算アップセル率 ----------
            with upsell_sub_agg:
                for _gi, item in enumerate(_upsell_items):
                    un = item["upsell_name"]
                    with st.expander(f"📦 {un}", expanded=True):
                        # アップセル率: 各 from_names → upsell_name
                        _render_upsell_pair(
                            client, company_key,
                            item["from_names"], un,
                            "アップセル率",
                            date_from_str, date_to_str,
                            pair_key=f"agg_{_gi}",
                        )
                        # アップアップセル率: upsell_name → 各 upsell_upsell_name
                        if item["upsell_upsell_names"]:
                            st.divider()
                            for _uui, uun in enumerate(item["upsell_upsell_names"]):
                                _render_upsell_pair(
                                    client, company_key,
                                    un, uun,
                                    "ｱｯﾌﾟｱｯﾌﾟｾﾙ率",
                                    date_from_str, date_to_str,
                                    skip_if_no_normal=True,
                                    pair_key=f"agg_uu_{_gi}_{_uui}",
                                )

            # ---------- 月別アップセル率 ----------
            with upsell_sub_monthly:
                for _gi, item in enumerate(_upsell_items):
                    un = item["upsell_name"]
                    with st.expander(f"📦 {un}", expanded=True):
                        _render_upsell_monthly(
                            client, company_key,
                            item["from_names"], un,
                            "アップセル率",
                            date_from_str, date_to_str,
                        )
                        if item["upsell_upsell_names"]:
                            st.divider()
                            for uun in item["upsell_upsell_names"]:
                                _render_upsell_monthly(
                                    client, company_key,
                                    un, uun,
                                    "ｱｯﾌﾟｱｯﾌﾟｾﾙ率",
                                    date_from_str, date_to_str,
                                    skip_if_no_normal=True,
                                )
