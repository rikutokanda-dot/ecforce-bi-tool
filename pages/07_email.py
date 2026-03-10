"""メールアップセル分析ページ.

マスタ管理で定義した分母/分子商品マッピングに基づき、
メール施策によるアップセル効果を分析:
1. アップセル率: 分母商品→分子商品への切替率（F別）
2. 切り替えタイミング別継続率: 切替後の定期回数ごとの継続率
"""

import math

import altair as alt
import pandas as pd
import streamlit as st

from src.bigquery_client import execute_query, get_bigquery_client
from src.components.retention_table import build_grouped_retention_html
from src.config_loader import (
    load_email_upsell_mappings,
    load_product_cycles,
    save_product_cycles,
)
from src.queries.email_upsell import (
    build_email_denominator_ids_sql,
    build_email_numerator_ids_sql,
    build_email_upsell_overall_sql,
    build_email_upsell_period_sql,
    build_email_upsell_rate_sql,
    build_email_upsell_retention_sql,
    build_email_upsell_unmatched_products_sql,
)
from src.session import SessionKey, get_selected_company_key

# ---------------------------------------------------------------------------
# ヘッダー
# ---------------------------------------------------------------------------
st.header("メール分析")

company_key = get_selected_company_key()
if not company_key:
    st.warning("サイドバーで会社を選択してください。")
    st.stop()

client = get_bigquery_client()

# マッピング読み込み
mappings = load_email_upsell_mappings()
if not mappings:
    st.info(
        "メールアップセルマッピングが設定されていません。"
        "マスタ管理ページの「メールアップセルマッピング」タブで設定してください。"
    )
    st.stop()

# マッピング選択
mapping_labels = [m["label"] for m in mappings]
selected_label = st.selectbox(
    "マッピングを選択",
    options=mapping_labels,
    key="email_mapping_select",
)
selected_mapping = next(m for m in mappings if m["label"] == selected_label)

numerator_names = selected_mapping.get("numerator_names", [])
denominator_names = selected_mapping.get("denominator_names", [])

if not numerator_names or not denominator_names:
    st.warning("分子・分母の商品が設定されていません。")
    st.stop()

# ---------------------------------------------------------------------------
# 日付フィルタ取得
# サイドバーで有効なフィルタがあればそれを使用。
# なければマッピングのデフォルト期間（手動日付 or 商品自動検出）を適用。
# ---------------------------------------------------------------------------
date_from = st.session_state.get(SessionKey.DATE_FROM)
date_to = st.session_state.get(SessionKey.DATE_TO)
sales_date_from = st.session_state.get(SessionKey.SALES_DATE_FROM)
sales_date_to = st.session_state.get(SessionKey.SALES_DATE_TO)
order_date_from = st.session_state.get(SessionKey.ORDER_DATE_FROM)
order_date_to = st.session_state.get(SessionKey.ORDER_DATE_TO)

# サイドバーの日付フィルタが一つでもONか
sidebar_has_dates = any([date_from, date_to, sales_date_from, sales_date_to,
                         order_date_from, order_date_to])

_period_source = ""  # UI表示用

if sidebar_has_dates:
    # サイドバーフィルタを使用
    date_from_str = date_from.strftime("%Y-%m-%d") if date_from else None
    date_to_str = date_to.strftime("%Y-%m-%d") if date_to else None
    sales_from_str = sales_date_from.strftime("%Y-%m-%d") if sales_date_from else None
    sales_to_str = sales_date_to.strftime("%Y-%m-%d") if sales_date_to else None
    _period_source = "サイドバーの日付フィルタ"
else:
    # デフォルト期間を適用（定期受注_作成日時でフィルタ）
    date_from_str = None
    date_to_str = None
    sales_from_str = None
    sales_to_str = None
    order_date_from = None
    order_date_to = None

    # 1) 手動日付が設定されていればそれを使用
    manual_from = selected_mapping.get("period_from")
    manual_to = selected_mapping.get("period_to")
    if manual_from or manual_to:
        date_from_str = manual_from
        date_to_str = manual_to
        _period_source = f"定期受注_作成日時フィルタ（手動設定）: {manual_from or '---'} ～ {manual_to or '---'}"
    else:
        # 2) period_ref_namesで自動検出
        period_ref = selected_mapping.get("period_ref_names", [])
        if period_ref:
            try:
                period_sql = build_email_upsell_period_sql(company_key, period_ref)
                period_df = execute_query(client, period_sql)
                if not period_df.empty and pd.notna(period_df.iloc[0]["min_date"]):
                    auto_from = pd.Timestamp(period_df.iloc[0]["min_date"])
                    auto_to = pd.Timestamp(period_df.iloc[0]["max_date"])
                    date_from_str = auto_from.strftime("%Y-%m-%d")
                    date_to_str = auto_to.strftime("%Y-%m-%d")
                    _period_source = (
                        f"定期受注_作成日時フィルタ（商品自動検出）: {date_from_str} ～ {date_to_str}"
                    )
            except Exception:
                pass

    if not date_from_str and not date_to_str:
        _period_source = "期間フィルタなし（全期間）"

# 共通の日付フィルタ引数
_date_kwargs = dict(
    date_from=date_from_str,
    date_to=date_to_str,
    sales_date_from=sales_from_str,
    sales_date_to=sales_to_str,
    order_date_from=order_date_from,
    order_date_to=order_date_to,
)

# 適用中の期間を表示
if _period_source:
    st.caption(f"📅 適用中の期間: {_period_source}")

# ---------------------------------------------------------------------------
# タブ
# ---------------------------------------------------------------------------
tab_upsell, tab_retention = st.tabs(["アップセル率", "切り替えタイミング別継続率"])

# ===== タブ1: アップセル率 =====
with tab_upsell:
    st.subheader("メールアップセル率")
    st.caption(
        "分母: 分母商品で1回目注文(shipped/completed)した顧客数　"
        "分子: そのうち分子商品に切り替えた顧客数"
    )

    # --- 全体サマリー ---
    try:
        overall_sql = build_email_upsell_overall_sql(
            company_key, numerator_names, denominator_names,
            **_date_kwargs,
        )
        overall_df = execute_query(client, overall_sql)
    except Exception as e:
        st.error(f"クエリ実行エラー: {e}")
        overall_df = pd.DataFrame()

    if not overall_df.empty:
        row = overall_df.iloc[0]
        total = int(row["total_denominator"]) if pd.notna(row["total_denominator"]) else 0
        switched = int(row["total_switched"]) if pd.notna(row["total_switched"]) else 0
        rate = float(row["upsell_rate"]) if pd.notna(row["upsell_rate"]) else 0.0

        k1, k2, k3 = st.columns(3)
        k1.metric("分母（対象者数）", f"{total:,}")
        k2.metric("分子（切替者数）", f"{switched:,}")
        k3.metric("アップセル率", f"{rate:.1f}%")

        # --- 顧客IDダウンロード ---
        dl1, dl2, _ = st.columns(3)
        with dl1:
            if st.button("分母 顧客ID CSV", key="dl_den"):
                with st.spinner("取得中..."):
                    den_sql = build_email_denominator_ids_sql(
                        company_key, denominator_names, **_date_kwargs,
                    )
                    den_df = execute_query(client, den_sql)
                st.download_button(
                    label=f"ダウンロード（{len(den_df):,}件）",
                    data=den_df.to_csv(index=False).encode("utf-8-sig"),
                    file_name="email_denominator_ids.csv",
                    mime="text/csv",
                    key="dl_den_csv",
                )
        with dl2:
            if st.button("分子 顧客ID CSV", key="dl_num"):
                with st.spinner("取得中..."):
                    num_sql = build_email_numerator_ids_sql(
                        company_key, numerator_names, denominator_names,
                        **_date_kwargs,
                    )
                    num_df = execute_query(client, num_sql)
                st.download_button(
                    label=f"ダウンロード（{len(num_df):,}件）",
                    data=num_df.to_csv(index=False).encode("utf-8-sig"),
                    file_name="email_numerator_ids.csv",
                    mime="text/csv",
                    key="dl_num_csv",
                )
    else:
        st.info("データがありません。")

    # --- F(回数)別転換率 ---
    st.divider()
    st.subheader("F(回数)別転換率")
    st.caption(
        "分母商品からの顧客のうち、各回数で初めて分子商品に切り替えた顧客の割合"
    )

    try:
        freq_sql = build_email_upsell_rate_sql(
            company_key, numerator_names, denominator_names,
            **_date_kwargs,
        )
        freq_df = execute_query(client, freq_sql)
    except Exception as e:
        st.error(f"F転換率クエリ実行エラー: {e}")
        freq_df = pd.DataFrame()

    if not freq_df.empty:
        disp = freq_df[["switch_order_count", "switched_at_n", "total_denominator", "conversion_rate"]].copy()
        disp.columns = ["切替F(回数)", "切替数", "分母", "転換率(%)"]
        disp["切替F(回数)"] = disp["切替F(回数)"].astype(int).apply(lambda x: f"{x}回目")
        disp["切替数"] = disp["切替数"].apply(lambda x: f"{int(x):,}")
        disp["分母"] = disp["分母"].apply(lambda x: f"{int(x):,}")
        disp["転換率(%)"] = disp["転換率(%)"].apply(lambda x: f"{x:.1f}%")
        st.dataframe(disp, use_container_width=True, hide_index=True)

        # 棒グラフ
        chart_df = freq_df[["switch_order_count", "conversion_rate"]].copy()
        chart = (
            alt.Chart(chart_df)
            .mark_bar(color="#4A90D9")
            .encode(
                x=alt.X(
                    "switch_order_count:O",
                    title="切替回数",
                    axis=alt.Axis(labelAngle=0),
                ),
                y=alt.Y("conversion_rate:Q", title="転換率(%)"),
                tooltip=[
                    alt.Tooltip("switch_order_count:O", title="切替回数"),
                    alt.Tooltip("conversion_rate:Q", title="転換率(%)", format=".1f"),
                ],
            )
            .properties(height=350)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("F転換率データがありません。")


# ===== タブ2: 切り替えタイミング別継続率 =====
with tab_retention:
    st.subheader("切り替えタイミング別継続率")
    st.caption(
        "分子商品に切り替えた顧客が母体。"
        "N回目で切り替えた人の1〜N回目の継続率は定義上100%です。"
    )

    # マッピングの全商品名をチップ表示
    _chip = (
        'display:inline-block;padding:2px 8px;margin:2px 4px;'
        'border-radius:12px;font-size:11px;max-width:100%;word-break:break-all;'
    )
    _before_chips = "".join(
        f'<span style="{_chip}background:#f0f0f0;color:#555;">{n}</span>'
        for n in denominator_names
    )
    _after_chips = "".join(
        f'<span style="{_chip}background:#e3f2fd;color:#1565c0;">{n}</span>'
        for n in numerator_names
    )
    st.markdown(
        f'<div style="margin-bottom:8px;">'
        f'<div><span style="font-size:12px;color:#888;">切替前：</span>{_before_chips}</div>'
        f'<div style="margin-top:4px;"><span style="font-size:12px;color:#888;">切替後：</span>{_after_chips}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    max_days = st.slider(
        "期間（日数）", min_value=30, max_value=730, value=365, step=30,
        key="email_max_days",
    )

    try:
        pc_data = load_product_cycles()
        ret_sql = build_email_upsell_retention_sql(
            company_key, numerator_names, denominator_names,
            max_days,
            **_date_kwargs,
            product_cycles=pc_data,
        )
        ret_df = execute_query(client, ret_sql)
    except Exception as e:
        st.error(f"継続率クエリ実行エラー: {e}")
        ret_df = pd.DataFrame()

    if ret_df.empty:
        st.info("切り替えた顧客が見つかりません。")
    else:
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

        # デフォルトcycle2をマスタから取得
        _defaults = pc_data.get("defaults", {}) if pc_data else {}
        _default_cycle2 = _defaults.get("cycle2", 30)

        # 切替回数ごとにエキスパンダーで表示
        for switch_n, group_df in ret_df.groupby("switch_order_count"):
            total = int(group_df["total_switched"].iloc[0])
            with st.expander(f"{int(switch_n)}回目切替（{total:,}人）", expanded=True):
                html = build_grouped_retention_html(
                    group_df, effective_max_n, default_cycle2=_default_cycle2,
                    max_days=max_days, product_cycles=pc_data,
                    show_product_names=False,
                )
                st.markdown(html, unsafe_allow_html=True)

        st.caption(
            "※ 切替回数より前の継続率は定義上100%（例: 3回目切替 → 1〜3回目は100%）"
        )

    # ----- 商品マスタ未登録 → 自動追加 + 周期未設定の警告 -----
    try:
        pc_data = load_product_cycles()
        unmatched_sql = build_email_upsell_unmatched_products_sql(
            company_key, numerator_names, denominator_names,
            **_date_kwargs,
            product_cycles=pc_data,
        )
        unmatched_df = execute_query(client, unmatched_sql)
    except Exception:
        unmatched_df = pd.DataFrame()

    if not unmatched_df.empty:
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
