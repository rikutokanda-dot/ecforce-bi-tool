"""マスタ管理ページ - 商品サイクル・アップセルマッピング・広告URL IDの閲覧・編集."""

from __future__ import annotations

from difflib import SequenceMatcher

import pandas as pd
import streamlit as st

from src.bigquery_client import fetch_filter_options, fetch_filtered_options, get_bigquery_client
from src.config_loader import (
    load_ad_url_mappings,
    load_product_cycles,
    load_upsell_mappings,
    save_ad_url_mappings,
    save_product_cycles,
    save_upsell_mappings,
)
from src.constants import Col
from src.queries.common import get_table_ref
from src.session import get_selected_company_key

st.header("マスタ管理")

tab_cycles, tab_upsell, tab_ad_url = st.tabs(["商品サイクル", "アップセルマッピング", "広告URL ID"])


# =====================================================================
# ヘルパー: 類似度ソート
# =====================================================================
def _similarity(a: str, b: str) -> float:
    """2文字列の類似度 (0~1)."""
    return SequenceMatcher(None, a, b).ratio()


def _sort_by_similarity(candidates: list[str], reference: str) -> list[str]:
    """reference に類似度が高い順にソート."""
    if not reference:
        return candidates
    return sorted(candidates, key=lambda c: _similarity(reference, c), reverse=True)


# =====================================================================
# 商品名一覧の取得 (BigQuery)
# =====================================================================
@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_all_product_names(company_key: str) -> list[str]:
    """選択中の会社のBigQueryから定期商品名一覧を取得."""
    client = get_bigquery_client()
    table_ref = get_table_ref(company_key)
    return fetch_filter_options(client, table_ref, Col.SUBSCRIPTION_PRODUCT_NAME)


@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_product_categories(company_key: str) -> list[str]:
    """商品カテゴリ一覧を取得."""
    client = get_bigquery_client()
    table_ref = get_table_ref(company_key)
    return fetch_filter_options(client, table_ref, Col.PRODUCT_CATEGORY)


@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_product_names_by_category(
    company_key: str, categories: tuple[str, ...],
) -> list[str]:
    """商品カテゴリで絞り込んだ定期商品名一覧を取得."""
    client = get_bigquery_client()
    table_ref = get_table_ref(company_key)
    return fetch_filtered_options(
        client, table_ref, Col.SUBSCRIPTION_PRODUCT_NAME,
        {Col.PRODUCT_CATEGORY: list(categories)},
    )


# =====================================================================
# 商品サイクルタブ
# =====================================================================
with tab_cycles:
    st.subheader("商品名別 発送サイクル")
    st.caption("商品名ごとの発送間隔を管理します。行の追加・削除も可能です。")

    data = load_product_cycles()
    products = data.get("products", [])
    defaults = data.get("defaults", {"cycle1": 30, "cycle2": 30})

    df = pd.DataFrame(products) if products else pd.DataFrame(columns=["name", "cycle1", "cycle2"])

    # --- 検索フィルタ ---
    cycle_search = st.text_input(
        "商品名で検索",
        placeholder="検索キーワード...",
        key="cycle_search",
    )

    if cycle_search.strip():
        keyword = cycle_search.strip()
        filtered_df = df[df["name"].str.contains(keyword, case=False, na=False)]
        st.info(f"🔍 {len(filtered_df)} / {len(df)} 件がヒット  —  フィルタを解除すると編集可能になります")
        st.dataframe(
            filtered_df,
            column_config={
                "name": st.column_config.TextColumn("商品名", width="large"),
                "cycle1": st.column_config.NumberColumn("初回→2回目 (日)"),
                "cycle2": st.column_config.NumberColumn("2回目以降 (日)"),
            },
            use_container_width=True,
            height=600,
        )
    else:
        edited_df = st.data_editor(
            df,
            num_rows="dynamic",
            column_config={
                "name": st.column_config.TextColumn("商品名", required=True, width="large"),
                "cycle1": st.column_config.NumberColumn("初回→2回目 (日)", min_value=1, default=30),
                "cycle2": st.column_config.NumberColumn("2回目以降 (日)", min_value=1, default=30),
            },
            use_container_width=True,
            height=600,
            key="cycle_editor",
        )

        col_save, col_default = st.columns([1, 2])
        with col_save:
            if st.button("保存", type="primary", key="save_cycles"):
                new_data = {
                    "products": edited_df.dropna(subset=["name"]).to_dict("records"),
                    "defaults": defaults,
                }
                save_product_cycles(new_data)
                st.success(f"{len(new_data['products'])} 件の商品サイクルを保存しました。")
                st.rerun()

        with col_default:
            st.markdown(f"**デフォルト値**: 初回→2回目 = {defaults['cycle1']}日 / 2回目以降 = {defaults['cycle2']}日")


# =====================================================================
# アップセルマッピングタブ
# =====================================================================
with tab_upsell:
    st.subheader("アップセルマッピング")
    st.caption("アップセル率 = 分子(人数) / 分母(人数) × 100")

    # --- 商品名一覧を取得 ---
    company_key = get_selected_company_key()
    if not company_key:
        st.warning("サイドバーから会社を選択してください。")
    else:
        # --- 商品カテゴリフィルタ ---
        categories = _fetch_product_categories(company_key)
        selected_categories = st.multiselect(
            "商品カテゴリで絞り込み",
            categories,
            key="master_upsell_category_filter",
            help="選択すると、分子・分母・期間デフォルトの候補がこのカテゴリの商品のみに絞り込まれます",
        )

        if selected_categories:
            all_product_names: list[str] = _fetch_product_names_by_category(
                company_key, tuple(selected_categories),
            )
        else:
            all_product_names: list[str] = _fetch_all_product_names(company_key)

        mappings = load_upsell_mappings()

        # ========== カード形式の編集UI ==========

        # session_state でマッピングリストを管理
        if "upsell_mappings_edit" not in st.session_state:
            st.session_state["upsell_mappings_edit"] = mappings if mappings else []

        edit_mappings: list[dict] = st.session_state["upsell_mappings_edit"]

        for idx, m in enumerate(edit_mappings):
            with st.container(border=True):
                header_col, del_col = st.columns([10, 1])
                with del_col:
                    if st.button("🗑️", key=f"del_{idx}", help="この行を削除"):
                        edit_mappings.pop(idx)
                        st.session_state["upsell_mappings_edit"] = edit_mappings
                        st.rerun()

                # --- マッピング名 (text_input) ---
                with header_col:
                    current_label = m.get("label", f"マッピング {idx + 1}")
                    sel_label = st.text_input(
                        "マッピング名",
                        value=current_label,
                        key=f"label_{idx}",
                    )
                    m["label"] = sel_label

                # 類似度ソートの基準
                ref_name = (m.get("numerator_names") or [""])[0]
                sorted_candidates = _sort_by_similarity(all_product_names, ref_name)

                # --- 分子 (multiselect) ---
                current_numerators = m.get("numerator_names", [])
                num_options = list(sorted_candidates)
                for cv in current_numerators:
                    if cv and cv not in num_options:
                        num_options.insert(0, cv)

                sel_numerators = st.multiselect(
                    "分子（複数選択可）",
                    num_options,
                    default=current_numerators,
                    key=f"numerator_{idx}",
                )
                m["numerator_names"] = sel_numerators

                # --- 分母 (multiselect) ---
                current_denominators = m.get("denominator_names", [])
                den_options = list(sorted_candidates)
                for cv in current_denominators:
                    if cv and cv not in den_options:
                        den_options.insert(0, cv)

                sel_denominators = st.multiselect(
                    "分母（複数選択可）",
                    den_options,
                    default=current_denominators,
                    key=f"denominator_{idx}",
                )
                m["denominator_names"] = sel_denominators

                # --- 期間デフォルト (multiselect) ---
                current_period_ref = m.get("period_ref_names", [])
                period_ref_name = (current_period_ref or [""])[0] or ref_name
                sorted_period = _sort_by_similarity(all_product_names, period_ref_name)
                period_options = list(sorted_period)
                for cv in current_period_ref:
                    if cv and cv not in period_options:
                        period_options.insert(0, cv)

                sel_period_ref = st.multiselect(
                    "期間デフォルト（この商品の定期開始日の範囲をデフォルト期間にする）",
                    period_options,
                    default=current_period_ref,
                    key=f"period_ref_{idx}",
                )
                m["period_ref_names"] = sel_period_ref

        # --- 行追加ボタン ---
        if st.button("＋ マッピングを追加", key="add_mapping"):
            edit_mappings.append({
                "label": "",
                "numerator_names": [],
                "denominator_names": [],
                "period_ref_names": [],
            })
            st.session_state["upsell_mappings_edit"] = edit_mappings
            st.rerun()

        # --- 保存ボタン ---
        st.markdown("")
        if st.button("保存", type="primary", key="save_upsell"):
            valid_mappings = [
                m for m in edit_mappings
                if m.get("numerator_names") and m.get("denominator_names")
            ]
            save_upsell_mappings(valid_mappings)
            st.session_state["upsell_mappings_edit"] = valid_mappings
            st.success(f"{len(valid_mappings)} 件のマッピングを保存しました。")
            st.rerun()


# =====================================================================
# 広告URL IDタブ
# =====================================================================
@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_all_ad_url_ids(company_key: str) -> list[str]:
    """BigQueryから広告URL ID一覧を取得."""
    client = get_bigquery_client()
    table_ref = get_table_ref(company_key)
    return fetch_filter_options(client, table_ref, Col.AD_URL)


with tab_ad_url:
    st.subheader("広告URL ID → 広告URL名 マッピング")
    st.caption(
        "広告URL IDに表示名を紐付けます。"
        "名前が定義されたIDはフィルタで名前表示されます。"
        "未定義のIDはそのままIDで表示されます。"
    )

    company_key_ad = get_selected_company_key()
    if not company_key_ad:
        st.warning("サイドバーから会社を選択してください。")
    else:
        # BigQueryから全広告URL IDを取得
        all_ad_url_ids = _fetch_all_ad_url_ids(company_key_ad)

        # 既存マッピングを読み込み
        existing_mappings = load_ad_url_mappings()
        existing_map = {m["ad_url_id"]: m.get("ad_url_name", "") for m in existing_mappings}

        # BigQueryにあるIDで既存マッピングにないものを追加（名前は空）
        merged: list[dict] = []
        seen_ids: set[str] = set()

        # 既存マッピング分を先に追加
        for m in existing_mappings:
            aid = m.get("ad_url_id", "")
            if aid:
                merged.append({
                    "ad_url_id": aid,
                    "ad_url_name": m.get("ad_url_name", ""),
                })
                seen_ids.add(aid)

        # BigQueryにあって未登録のIDを追加
        for aid in all_ad_url_ids:
            if aid not in seen_ids:
                merged.append({"ad_url_id": aid, "ad_url_name": ""})
                seen_ids.add(aid)

        # DataFrameに変換
        df_ad = pd.DataFrame(merged, columns=["ad_url_id", "ad_url_name"])

        # --- 検索フィルタ ---
        ad_search = st.text_input(
            "IDまたは名前で検索",
            placeholder="検索キーワード...",
            key="ad_url_search",
        )

        # 検索の有無で表示を分岐（どちらも編集可能）
        if ad_search.strip():
            kw = ad_search.strip()
            mask = (
                df_ad["ad_url_id"].str.contains(kw, case=False, na=False)
                | df_ad["ad_url_name"].str.contains(kw, case=False, na=False)
            )
            filtered_ad = df_ad[mask].copy()
            st.info(f"🔍 {len(filtered_ad)} / {len(df_ad)} 件がヒット")

            edited_ad = st.data_editor(
                filtered_ad,
                column_config={
                    "ad_url_id": st.column_config.TextColumn(
                        "広告URL ID", width="large", disabled=True,
                    ),
                    "ad_url_name": st.column_config.TextColumn(
                        "広告URL名", width="large",
                    ),
                },
                use_container_width=True,
                height=600,
                key="ad_url_editor_filtered",
            )

            if st.button("保存", type="primary", key="save_ad_url"):
                # 編集された行をマージ: 検索結果の編集内容で元データを更新
                edited_map = {
                    r["ad_url_id"]: r.get("ad_url_name", "")
                    for r in edited_ad.to_dict("records")
                    if r.get("ad_url_id", "").strip()
                }
                save_data = []
                for r in df_ad.to_dict("records"):
                    aid = r.get("ad_url_id", "")
                    if not aid.strip():
                        continue
                    if aid in edited_map:
                        r["ad_url_name"] = edited_map[aid]
                    save_data.append(r)
                save_ad_url_mappings(save_data)
                st.success(
                    f"{len(save_data)} 件の広告URL IDマッピングを保存しました。"
                )
                st.rerun()
        else:
            edited_ad = st.data_editor(
                df_ad,
                num_rows="dynamic",
                column_config={
                    "ad_url_id": st.column_config.TextColumn(
                        "広告URL ID", required=True, width="large",
                    ),
                    "ad_url_name": st.column_config.TextColumn(
                        "広告URL名", width="large",
                    ),
                },
                use_container_width=True,
                height=600,
                key="ad_url_editor",
            )

            if st.button("保存", type="primary", key="save_ad_url_all"):
                save_data = (
                    edited_ad.dropna(subset=["ad_url_id"])
                    .to_dict("records")
                )
                save_data = [r for r in save_data if r.get("ad_url_id", "").strip()]
                save_ad_url_mappings(save_data)
                st.success(
                    f"{len(save_data)} 件の広告URL IDマッピングを保存しました。"
                )
                st.rerun()
