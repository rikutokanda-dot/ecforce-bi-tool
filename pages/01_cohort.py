"""コホート分析ページ - 継続率・残存率・LTV・アップセル率."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from src.bigquery_client import execute_query, fetch_filtered_options, get_bigquery_client
from src.components.cohort_heatmap import render_cohort_heatmap, render_retention_line_chart
from src.components.download_button import df_to_csv_bytes, render_download_buttons
from src.components.filters import render_cohort_filters
from src.components.metrics_row import render_metrics
from src.config_loader import get_product_cycle, load_upsell_mappings
from src.constants import Col, MAX_RETENTION_MONTHS, PROCESSING_BUFFER_DAYS
from src.queries.common import get_table_ref
from src.queries.cohort import (
    build_aggregate_cohort_sql,
    build_cohort_sql,
    build_drilldown_order_detail_sql,
    build_drilldown_sql,
    build_max_date_sql,
    build_upsell_rate_monthly_sql,
    build_upsell_rate_sql,
)
from src.session import SessionKey, get_selected_company_key
from src.transforms.cohort_transform import (
    build_1year_ltv_table,
    build_aggregate_table,
    build_continuation_rate_matrix,
    build_dimension_summary_table,
    build_drilldown_rate_matrices,
    build_drilldown_retention_table,
    build_product_summary_table,
    build_retention_rate_matrix,
    build_retention_table,
    build_shipping_schedule,
    compute_aggregate_metrics,
    compute_max_orders_in_period,
    compute_month_end_mask,
    compute_summary_metrics,
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
    numerator_names: list[str],
    denominator_names: list[str],
    period_ref_names: list[str],
    label_title: str,
    date_from_str: str | None,
    date_to_str: str | None,
    *,
    pair_key: str = "",
    upsell_filters: dict | None = None,
):
    """1組のアップセル率を表示."""
    _upsell_pair_fragment(
        client, company_key, numerator_names, denominator_names, period_ref_names,
        label_title, date_from_str, date_to_str,
        pair_key=pair_key,
        upsell_filters=upsell_filters,
    )


@st.fragment
def _upsell_pair_fragment(
    client,
    company_key: str,
    numerator_names: list[str],
    denominator_names: list[str],
    period_ref_names: list[str],
    label_title: str,
    date_from_str: str | None,
    date_to_str: str | None,
    *,
    pair_key: str = "",
    upsell_filters: dict | None = None,
):
    """フラグメント化されたアップセル率表示。日付変更時にこの部分だけ再実行。"""
    _num_display = ", ".join(numerator_names)
    _denom_display = ", ".join(denominator_names)

    _key_base = pair_key or f"{'_'.join(numerator_names)}_{'_'.join(denominator_names)}"
    _k_from = f"us_period_from_{_key_base}"
    _k_to = f"us_period_to_{_key_base}"

    has_override = _k_from in st.session_state
    if has_override:
        query_from = st.session_state[_k_from].strftime("%Y-%m-%d")
        query_to = st.session_state[_k_to].strftime("%Y-%m-%d")
    else:
        query_from = date_from_str
        query_to = date_to_str

    _no_period_ref = not period_ref_names
    _uf = upsell_filters or {}
    sql = build_upsell_rate_sql(
        company_key, numerator_names, denominator_names, period_ref_names,
        query_from, query_to,
        product_categories=_uf.get("product_categories"),
        ad_groups=_uf.get("ad_groups"),
        ad_url_params=_uf.get("ad_url_params"),
    )
    try:
        df = execute_query(client, sql)
        if df.empty:
            st.markdown(f"**{label_title}**　データなし")
            st.markdown(f"<small>分母：{_denom_display}<br>分子：{_num_display}</small>",
                        unsafe_allow_html=True)
            st.divider()
            return
        row = df.iloc[0]
        _rate_val = pd.to_numeric(row.get("upsell_rate"), errors="coerce")
        if pd.isna(_rate_val):
            st.markdown(f"**{label_title}**　データなし")
            st.markdown(f"<small>分母：{_denom_display}<br>分子：{_num_display}</small>",
                        unsafe_allow_html=True)
            st.divider()
            return
        if _no_period_ref:
            st.warning("⚠️ デフォルト期間が未設定です。分母商品の期間で代用しています。マスタ管理で「期間デフォルト」を設定してください。")
        rate = round(float(_rate_val), 1)
        normal_count = int(pd.to_numeric(row.get("normal_count", 0), errors="coerce") or 0)
        upsell_count = int(pd.to_numeric(row.get("upsell_count", 0), errors="coerce") or 0)
        period_start = str(row.get("period_start", ""))[:10]
        period_end = str(row.get("period_end", ""))[:10]

        st.markdown(
            f"**{label_title}　{rate}%**　　分母: {normal_count:,}人 / 分子: {upsell_count:,}人"
        )
        st.markdown(
            f"<small>分母：{_denom_display}<br>分子：{_num_display}</small>",
            unsafe_allow_html=True,
        )

        if not has_override and period_start and period_end and len(period_start) == 10:
            try:
                st.session_state[_k_from] = date.fromisoformat(period_start)
                st.session_state[_k_to] = date.fromisoformat(period_end)
            except ValueError:
                pass

        dcols = st.columns([1, 1])
        with dcols[0]:
            st.date_input("対象開始日", key=_k_from)
        with dcols[1]:
            st.date_input("対象終了日", key=_k_to)

        st.divider()
    except Exception as e:
        st.markdown(f"**{label_title}**　エラー")
        st.markdown(
            f"<small>分母：{_denom_display}<br>分子：{_num_display}</small>",
            unsafe_allow_html=True,
        )
        st.caption(f"({e})")
        st.divider()


def _render_upsell_monthly(
    client,
    company_key: str,
    numerator_names: list[str],
    denominator_names: list[str],
    period_ref_names: list[str],
    label_title: str,
    date_from_str: str | None,
    date_to_str: str | None,
    *,
    upsell_filters: dict | None = None,
):
    """月別アップセル率テーブル+グラフを表示."""
    _num_display = ", ".join(numerator_names)
    _denom_display = ", ".join(denominator_names)

    _no_period_ref = not period_ref_names
    _uf = upsell_filters or {}
    sql = build_upsell_rate_monthly_sql(
        company_key, numerator_names, denominator_names, period_ref_names,
        date_from_str, date_to_str,
        product_categories=_uf.get("product_categories"),
        ad_groups=_uf.get("ad_groups"),
        ad_url_params=_uf.get("ad_url_params"),
    )
    label_md = _upsell_label_html(label_title, _denom_display, _num_display)
    try:
        df = execute_query(client, sql)
        if df.empty:
            st.markdown(label_md)
            st.info("データなし")
            return

        if _no_period_ref:
            st.warning("⚠️ デフォルト期間が未設定です。分母商品の期間で代用しています。マスタ管理で「期間デフォルト」を設定してください。")

        display_df = df[["cohort_month", "normal_count", "upsell_count", "upsell_rate"]].copy()
        display_df.columns = ["月", "分母(人)", "分子(人)", "アップセル率(%)"]
        display_df["アップセル率(%)"] = display_df["アップセル率(%)"].round(1)

        st.markdown(label_md)
        st.dataframe(display_df, use_container_width=True, hide_index=True)

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

# 選択商品のサイクル値を取得
_selected_pnames = filters.get("product_names")
if _selected_pnames and len(_selected_pnames) == 1:
    _global_cycle1, _global_cycle2 = get_product_cycle(_selected_pnames[0])
else:
    _global_cycle1, _global_cycle2 = 30, 30

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

# eligible_before: 定期受注作成日がこの日以前の顧客のみ対象
# cutoff - 10日 = 1回目の出荷・解決が完了している作成日の上限
eligible_before_date = data_cutoff_date - timedelta(days=PROCESSING_BUFFER_DAYS)
eligible_before_str = eligible_before_date.strftime("%Y-%m-%d")

filter_params = dict(
    company_key=company_key,
    date_from=date_from_str,
    date_to=date_to_str,
    product_categories=filters["product_categories"],
    ad_groups=filters["ad_groups"],
    product_names=filters["product_names"],
    eligible_before=eligible_before_str,
)

# コホートSQL用の追加パラメータ (継続率の時間適格チェック)
_cutoff_str = data_cutoff_date.strftime("%Y-%m-%d")
cohort_params = {
    **filter_params,
    "cycle1": _global_cycle1,
    "cycle2": _global_cycle2,
    "cutoff_date": _cutoff_str,
}


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
    dd_tab_product, dd_tab_adgroup, dd_tab_adurl, dd_tab_category = st.tabs(
        ["定期商品名", "広告グループ", "広告URLパラメータ", "商品カテゴリ"]
    )

    # ========== 定期商品名 ==========
    with dd_tab_product:
        if st.button("表示する", key="btn_dd_product", type="primary"):
            st.session_state["dd_product_shown"] = True
        if not st.session_state.get("dd_product_shown"):
            st.info("フィルタを設定して「表示する」を押してください。")
        else:
            dd_sql = build_drilldown_sql(
                drilldown_column=Col.SUBSCRIPTION_PRODUCT_NAME, **cohort_params
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
                st.caption(f"データカットオフ日: {data_cutoff_date}　/　eligible上限: {eligible_before_str}")

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

                        # --- 受注番号ダウンロード ---
                        _dl_key = f"dd_order_detail_{pname}"
                        if st.button(
                            "📥 受注番号ダウンロード",
                            key=f"btn_order_dl_{pname}",
                        ):
                            try:
                                _detail_sql = build_drilldown_order_detail_sql(
                                    product_name=pname, **filter_params
                                )
                                st.session_state[_dl_key] = execute_query(
                                    client, _detail_sql
                                )
                            except Exception as _e:
                                st.error(f"受注番号クエリ実行エラー: {_e}")

                        if _dl_key in st.session_state:
                            _detail_df = st.session_state[_dl_key]
                            if _detail_df.empty:
                                st.info("受注番号データがありません。")
                            else:
                                # eligible月を計算
                                _group = dd_df[dd_df["dimension_col"] == pname]
                                _month_max: dict[str, int] = {}
                                if data_cutoff_date is not None:
                                    for _, _r in _group.iterrows():
                                        _cm = _r["cohort_month"]
                                        _month_max[_cm] = compute_month_end_mask(
                                            _cm, pname, data_cutoff_date
                                        )
                                else:
                                    for _, _r in _group.iterrows():
                                        _month_max[_r["cohort_month"]] = MAX_RETENTION_MONTHS

                                # 表示可能な回数を取得
                                _avail_counts = sorted(
                                    {
                                        int(c.replace("回目", ""))
                                        for c in summary.columns
                                        if c.endswith("回目")
                                    }
                                )
                                if _avail_counts:
                                    _sel_n = st.selectbox(
                                        "定期回数を選択",
                                        _avail_counts,
                                        format_func=lambda x: f"{x}回目",
                                        key=f"sel_dl_n_{pname}",
                                    )

                                    # eligible月 (count _sel_n のデータが揃っている月)
                                    _eligible = [
                                        cm
                                        for cm, mx in _month_max.items()
                                        if mx >= _sel_n
                                    ]

                                    # 残存率分母: base (1回目の全受注) × eligible月
                                    _surv_denom = _detail_df[
                                        (_detail_df["record_type"] == "base")
                                        & (_detail_df["cohort_month"].isin(_eligible))
                                    ][["customer_id", "order_id"]].drop_duplicates()

                                    # 残存率分子 = 継続率分子: retained × count=_sel_n × eligible月
                                    _surv_numer = _detail_df[
                                        (_detail_df["record_type"] == "retained")
                                        & (_detail_df["subscription_count"] == _sel_n)
                                        & (_detail_df["cohort_month"].isin(_eligible))
                                    ][["customer_id", "order_id"]].drop_duplicates()

                                    # 継続率分母: i=1→base, i>1→retained at count _sel_n-1 (同eligible月)
                                    if _sel_n == 1:
                                        _cont_denom = _surv_denom
                                    else:
                                        _cont_denom = _detail_df[
                                            (_detail_df["record_type"] == "retained")
                                            & (
                                                _detail_df["subscription_count"]
                                                == _sel_n - 1
                                            )
                                            & (
                                                _detail_df["cohort_month"].isin(
                                                    _eligible
                                                )
                                            )
                                        ][["customer_id", "order_id"]].drop_duplicates()

                                    st.caption(f"**{_sel_n}回目の受注番号CSV**")

                                    # ファイル名のWindows禁止文字を除去
                                    _safe_name = (
                                        pname.replace("/", "_")
                                        .replace("\\", "_")
                                        .replace(":", "_")
                                        .replace("*", "_")
                                        .replace("?", "_")
                                        .replace('"', "_")
                                        .replace("<", "_")
                                        .replace(">", "_")
                                        .replace("|", "_")
                                    )

                                    _hdr = {"header": ["顧客ID", "受注番号"]}
                                    _c1, _c2 = st.columns(2)
                                    with _c1:
                                        st.markdown("**残存率**")
                                        st.download_button(
                                            f"分母 ({len(_surv_denom)}件)",
                                            df_to_csv_bytes(_surv_denom, **_hdr),
                                            f"{_safe_name}_{_sel_n}回目_残存率_分母.csv",
                                            key=f"dl_surv_d_{pname}_{_sel_n}",
                                        )
                                        st.download_button(
                                            f"分子 ({len(_surv_numer)}件)",
                                            df_to_csv_bytes(_surv_numer, **_hdr),
                                            f"{_safe_name}_{_sel_n}回目_残存率_分子.csv",
                                            key=f"dl_surv_n_{pname}_{_sel_n}",
                                        )
                                    with _c2:
                                        st.markdown("**継続率**")
                                        st.download_button(
                                            f"分母 ({len(_cont_denom)}件)",
                                            df_to_csv_bytes(_cont_denom, **_hdr),
                                            f"{_safe_name}_{_sel_n}回目_継続率_分母.csv",
                                            key=f"dl_cont_d_{pname}_{_sel_n}",
                                        )
                                        st.download_button(
                                            f"分子 ({len(_surv_numer)}件)",
                                            df_to_csv_bytes(_surv_numer, **_hdr),
                                            f"{_safe_name}_{_sel_n}回目_継続率_分子.csv",
                                            key=f"dl_cont_n_{pname}_{_sel_n}",
                                        )

    # ========== 広告グループ ==========
    with dd_tab_adgroup:
        if st.button("表示する", key="btn_dd_adgroup", type="primary"):
            st.session_state["dd_adgroup_shown"] = True
        if not st.session_state.get("dd_adgroup_shown"):
            st.info("フィルタを設定して「表示する」を押してください。")
        else:
            dd_sql_ag = build_drilldown_sql(
                drilldown_column=Col.AD_GROUP, **cohort_params
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

    # ========== 広告URLパラメータ ==========
    with dd_tab_adurl:
        if st.button("表示する", key="btn_dd_adurl", type="primary"):
            st.session_state["dd_adurl_shown"] = True
        if not st.session_state.get("dd_adurl_shown"):
            st.info("フィルタを設定して「表示する」を押してください。")
        else:
            dd_sql_au = build_drilldown_sql(
                drilldown_column=Col.AD_URL_PARAM, **cohort_params
            )
            try:
                dd_df_au = execute_query(client, dd_sql_au)
            except Exception as e:
                st.error(f"BigQueryクエリ実行エラー: {e}")
                dd_df_au = pd.DataFrame()

            if dd_df_au.empty:
                st.info("該当するデータが見つかりませんでした。")
            else:
                dim_au = sorted(dd_df_au["dimension_col"].unique())
                st.info(f"**広告URLパラメータ別**: {len(dim_au)} 件")
                st.caption(f"データカットオフ日: {data_cutoff_date}")
                for au_name in dim_au:
                    with st.expander(f"{au_name}", expanded=False):
                        summary = build_dimension_summary_table(dd_df_au, au_name)
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
                drilldown_column=Col.PRODUCT_CATEGORY, **cohort_params
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
        agg_sql = build_aggregate_cohort_sql(**cohort_params)
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
                        **cohort_params,
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
        monthly_sql = build_cohort_sql(**cohort_params)
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

            tab_heatmap, tab_cont_heatmap, tab_line, tab_cont_line, tab_table, tab_schedule = st.tabs(
                ["残存率ヒートマップ", "継続率ヒートマップ", "残存率推移", "継続率推移", "データテーブル", "発送日目安"]
            )

            # 商品名1つ選択時のみマスク適用
            _monthly_pn = filters["product_names"][0] if filters["product_names"] and len(filters["product_names"]) == 1 else None
            rate_matrix = build_retention_rate_matrix(monthly_df, data_cutoff_date, _monthly_pn)
            cont_matrix = build_continuation_rate_matrix(monthly_df, data_cutoff_date, _monthly_pn)
            retention_table = build_retention_table(monthly_df, data_cutoff_date, _monthly_pn)

            with tab_heatmap:
                render_cohort_heatmap(rate_matrix, title="残存率ヒートマップ")

            with tab_cont_heatmap:
                render_cohort_heatmap(cont_matrix, title="継続率ヒートマップ")

            with tab_line:
                render_retention_line_chart(rate_matrix, title="残存率推移")

            with tab_cont_line:
                render_retention_line_chart(cont_matrix, title="継続率推移")

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

    # 会社の商品リストを取得してマッピングをフィルタ
    _upsell_table_ref = get_table_ref(company_key)
    _company_products = set(
        fetch_filtered_options(client, _upsell_table_ref, Col.SUBSCRIPTION_PRODUCT_NAME)
    )

    # 会社フィルタ: 分母商品が会社に存在するマッピングのみ
    _company_mappings = [
        m for m in _all_mappings_raw
        if _company_products & (set(m.get("denominator_names", [])) | set(m.get("numerator_names", [])))
    ]

    # サイドバーフィルタで対象マッピングをさらに絞り込む
    _upsell_filter_pnames = filters.get("product_names")
    _upsell_filter_cats = filters.get("product_categories")
    if _upsell_filter_pnames:
        _pname_set = set(_upsell_filter_pnames)
        all_mappings = [
            m for m in _company_mappings
            if _pname_set & set(m.get("denominator_names", []))
        ]
    elif _upsell_filter_cats:
        _cat_product_names = fetch_filtered_options(
            client, _upsell_table_ref, Col.SUBSCRIPTION_PRODUCT_NAME,
            {Col.PRODUCT_CATEGORY: _upsell_filter_cats},
        )
        _cat_pname_set = set(_cat_product_names)
        all_mappings = [
            m for m in _company_mappings
            if _cat_pname_set & set(m.get("denominator_names", []))
        ]
    else:
        all_mappings = list(_company_mappings)

    # サイドバーフィルタをアップセルSQLに渡す
    _upsell_sql_filters = {
        "product_categories": filters.get("product_categories"),
        "ad_groups": filters.get("ad_groups"),
        "ad_url_params": filters.get("ad_url_params"),
    }

    if not _company_mappings:
        st.info("この会社に該当するアップセルマッピングがありません。マスタ管理で設定してください。")
    elif not all_mappings:
        st.info("サイドバーで選択中の商品に該当するアップセルマッピングがありません。")
    else:
        if st.button("表示する", key="btn_upsell", type="primary"):
            st.session_state["upsell_tab_shown"] = True
        if not st.session_state.get("upsell_tab_shown"):
            st.info("「表示する」を押すとアップセル率を計算します。")
        else:
            upsell_sub_agg, upsell_sub_monthly = st.tabs(["通算", "月別"])

            with upsell_sub_agg:
                for _gi, m in enumerate(all_mappings):
                    label = m.get("label", f"マッピング{_gi+1}")
                    num = m.get("numerator_names", [])
                    denom = m.get("denominator_names", [])
                    pref = m.get("period_ref_names", num)
                    if not num or not denom:
                        continue
                    with st.expander(f"📦 {label}", expanded=True):
                        _render_upsell_pair(
                            client, company_key,
                            num, denom, pref,
                            label,
                            date_from_str, date_to_str,
                            pair_key=f"agg_{_gi}",
                            upsell_filters=_upsell_sql_filters,
                        )

            with upsell_sub_monthly:
                for _gi, m in enumerate(all_mappings):
                    label = m.get("label", f"マッピング{_gi+1}")
                    num = m.get("numerator_names", [])
                    denom = m.get("denominator_names", [])
                    pref = m.get("period_ref_names", num)
                    if not num or not denom:
                        continue
                    with st.expander(f"📦 {label}", expanded=True):
                        _render_upsell_monthly(
                            client, company_key,
                            num, denom, pref,
                            label,
                            date_from_str, date_to_str,
                            upsell_filters=_upsell_sql_filters,
                        )
