"""マスタ管理ページ - 商品サイクル・アップセルマッピングの閲覧・編集."""

from __future__ import annotations

from difflib import SequenceMatcher

import pandas as pd
import streamlit as st

from src.bigquery_client import fetch_filter_options, get_bigquery_client
from src.config_loader import (
    load_product_cycles,
    load_upsell_mappings,
    save_product_cycles,
    save_upsell_mappings,
)
from src.constants import Col
from src.queries.common import get_table_ref
from src.session import get_selected_company_key

st.header("マスタ管理")

tab_cycles, tab_upsell = st.tabs(["商品サイクル", "アップセルマッピング"])


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


# =====================================================================
# ヘルパー: YAMLの行リスト → 編集用グループ形式に変換
# =====================================================================
def _mappings_to_groups(mappings: list[dict]) -> list[dict]:
    """YAMLの1行1マッピング形式を、from_names単位のグループにまとめる.

    YAML形式: [{"from_names": ["A"], "upsell_name": "B", "upsell_upsell_name": "C"}, ...]
    グループ: [{"from_names": ["A"], "upsell_names": ["B"], "upsell_upsell_names": ["C"]}, ...]

    同じfrom_namesの行は1グループにまとめ、upsell_name/upsell_upsell_nameをリストに集約。
    """
    groups: dict[tuple, dict] = {}
    for m in mappings:
        fns = tuple(m.get("from_names", []))
        if not fns:
            continue
        if fns not in groups:
            groups[fns] = {"from_names": list(fns), "upsell_names": [], "upsell_upsell_names": []}
        un = m.get("upsell_name", "")
        uun = m.get("upsell_upsell_name") or ""
        if un and un not in groups[fns]["upsell_names"]:
            groups[fns]["upsell_names"].append(un)
        if uun and uun not in groups[fns]["upsell_upsell_names"]:
            groups[fns]["upsell_upsell_names"].append(uun)
    return list(groups.values())


def _groups_to_mappings(groups: list[dict]) -> list[dict]:
    """グループ形式をYAMLの1行1マッピング形式に展開.

    upsell_namesの各要素ごとに1行。
    upsell_upsell_namesは先頭のupsell_nameに紐づける（複数ある場合は順番に割当）。
    """
    result = []
    for g in groups:
        fns = g.get("from_names", [])
        if not fns:
            continue
        upsell_names = g.get("upsell_names", [])
        upsell_upsell_names = g.get("upsell_upsell_names", [])

        if not upsell_names:
            continue

        for i, un in enumerate(upsell_names):
            uun = upsell_upsell_names[i] if i < len(upsell_upsell_names) else None
            result.append({
                "from_names": fns,
                "upsell_name": un,
                "upsell_upsell_name": uun or None,
            })
    return result


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
    st.caption("アップセル率 = アップセル商品 / (アップセル商品 + 通常商品)")

    # --- 商品名一覧を取得 ---
    company_key = get_selected_company_key()
    if not company_key:
        st.warning("サイドバーから会社を選択してください。")
    else:
        all_product_names: list[str] = _fetch_all_product_names(company_key)
        mappings = load_upsell_mappings()

        # ========== カード形式の編集UI ==========

        # session_state でグループ化したマッピングを管理
        if "upsell_groups_edit" not in st.session_state:
            st.session_state["upsell_groups_edit"] = _mappings_to_groups(mappings)

        edit_groups: list[dict] = st.session_state["upsell_groups_edit"]

        for idx, group in enumerate(edit_groups):
            with st.container(border=True):
                header_col, del_col = st.columns([10, 1])
                with header_col:
                    st.markdown(f"**マッピング {idx + 1}**")
                with del_col:
                    if st.button("🗑️", key=f"del_{idx}", help="この行を削除"):
                        edit_groups.pop(idx)
                        st.session_state["upsell_groups_edit"] = edit_groups
                        st.rerun()

                # 類似度ソートの基準
                ref_name = (group.get("from_names") or [""])[0]
                sorted_candidates = _sort_by_similarity(all_product_names, ref_name)

                # --- アップセル商品 (multiselect) ---
                current_upsells = group.get("upsell_names", [])
                upsell_options = list(sorted_candidates)
                for cv in current_upsells:
                    if cv and cv not in upsell_options:
                        upsell_options.insert(0, cv)

                sel_upsells = st.multiselect(
                    "アップセル商品（複数選択可）",
                    upsell_options,
                    default=current_upsells,
                    key=f"upsell_{idx}",
                )
                group["upsell_names"] = sel_upsells

                # --- 通常商品 (multiselect) ---
                current_froms = group.get("from_names", [])
                from_options = list(sorted_candidates)
                for cv in current_froms:
                    if cv and cv not in from_options:
                        from_options.insert(0, cv)

                sel_froms = st.multiselect(
                    "通常商品（複数選択可）",
                    from_options,
                    default=current_froms,
                    key=f"from_{idx}",
                )
                group["from_names"] = sel_froms

                # --- アップセルアップセル先 (multiselect) ---
                current_upups = group.get("upsell_upsell_names", [])
                upup_ref = (group.get("upsell_names") or [""])[0] or ref_name
                sorted_upup = _sort_by_similarity(all_product_names, upup_ref)
                upup_options = list(sorted_upup)
                for cv in current_upups:
                    if cv and cv not in upup_options:
                        upup_options.insert(0, cv)

                sel_upups = st.multiselect(
                    "アップセルアップセル先（任意）",
                    upup_options,
                    default=current_upups,
                    key=f"upup_{idx}",
                )
                group["upsell_upsell_names"] = sel_upups

        # --- 行追加ボタン ---
        if st.button("＋ マッピングを追加", key="add_mapping"):
            edit_groups.append({"from_names": [], "upsell_names": [], "upsell_upsell_names": []})
            st.session_state["upsell_groups_edit"] = edit_groups
            st.rerun()

        # --- 保存ボタン ---
        st.markdown("")
        if st.button("保存", type="primary", key="save_upsell"):
            valid_groups = [g for g in edit_groups if g.get("from_names") and g.get("upsell_names")]
            flat_mappings = _groups_to_mappings(valid_groups)
            save_upsell_mappings(flat_mappings)
            st.session_state["upsell_groups_edit"] = valid_groups
            st.success(f"{len(flat_mappings)} 件のマッピングを保存しました。")
            st.rerun()
