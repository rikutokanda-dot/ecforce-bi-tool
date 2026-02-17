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
    """YAMLの1行1マッピング形式を、from_name単位のグループにまとめる.

    YAML形式: [{"from_name": "A", "upsell_name": "B", "upsell_upsell_name": "C"}, ...]
    グループ: [{"from_name": "A", "upsell_names": ["B"], "upsell_upsell_names": ["C"]}, ...]

    同じfrom_nameの行は1グループにまとめ、upsell_name/upsell_upsell_nameをリストに集約。
    """
    groups: dict[str, dict] = {}
    for m in mappings:
        fn = m.get("from_name", "")
        if not fn:
            continue
        if fn not in groups:
            groups[fn] = {"from_name": fn, "upsell_names": [], "upsell_upsell_names": []}
        un = m.get("upsell_name", "")
        uun = m.get("upsell_upsell_name") or ""
        if un and un not in groups[fn]["upsell_names"]:
            groups[fn]["upsell_names"].append(un)
        if uun and uun not in groups[fn]["upsell_upsell_names"]:
            groups[fn]["upsell_upsell_names"].append(uun)
    return list(groups.values())


def _groups_to_mappings(groups: list[dict]) -> list[dict]:
    """グループ形式をYAMLの1行1マッピング形式に展開.

    upsell_namesの各要素ごとに1行。
    upsell_upsell_namesは先頭のupsell_nameに紐づける（複数ある場合は順番に割当）。
    """
    result = []
    for g in groups:
        fn = g.get("from_name", "")
        if not fn:
            continue
        upsell_names = g.get("upsell_names", [])
        upsell_upsell_names = g.get("upsell_upsell_names", [])

        if not upsell_names:
            continue

        for i, un in enumerate(upsell_names):
            uun = upsell_upsell_names[i] if i < len(upsell_upsell_names) else None
            result.append({
                "from_name": fn,
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
    st.caption("商品名ごとのアップセル先・アップセルアップセル先を管理します。同一商品に複数のアップセル先を設定できます。")

    # --- 商品名一覧を取得 ---
    company_key = get_selected_company_key()
    if not company_key:
        st.warning("サイドバーから会社を選択してください。")
    else:
        all_product_names: list[str] = _fetch_all_product_names(company_key)
        mappings = load_upsell_mappings()

        # --- 検索フィルタ ---
        upsell_search = st.text_input(
            "商品名で検索",
            placeholder="検索キーワード...",
            key="upsell_search",
        )

        # フィルタ適用時は読み取り専用表示
        if upsell_search.strip():
            keyword = upsell_search.strip()
            upsell_df = pd.DataFrame(mappings) if mappings else pd.DataFrame(
                columns=["from_name", "upsell_name", "upsell_upsell_name"]
            )
            mask = (
                upsell_df["from_name"].str.contains(keyword, case=False, na=False)
                | upsell_df["upsell_name"].str.contains(keyword, case=False, na=False)
                | upsell_df["upsell_upsell_name"].astype(str).str.contains(keyword, case=False, na=False)
            )
            filtered_upsell = upsell_df[mask]
            st.info(f"🔍 {len(filtered_upsell)} / {len(upsell_df)} 件がヒット  —  フィルタを解除すると編集可能になります")
            st.dataframe(
                filtered_upsell,
                column_config={
                    "from_name": st.column_config.TextColumn("元商品名", width="large"),
                    "upsell_name": st.column_config.TextColumn("アップセル先", width="large"),
                    "upsell_upsell_name": st.column_config.TextColumn("アップセルアップセル先", width="large"),
                },
                use_container_width=True,
                height=400,
            )
        else:
            # ========== カード形式の編集UI (グループ単位) ==========

            # session_state でグループ化したマッピングを管理
            if "upsell_groups_edit" not in st.session_state:
                st.session_state["upsell_groups_edit"] = _mappings_to_groups(mappings)

            edit_groups: list[dict] = st.session_state["upsell_groups_edit"]

            _MANUAL_OPTION = "✏️ 手動入力..."

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

                    # --- 元商品名 (ドロップダウン) ---
                    current_from = group.get("from_name", "")
                    from_options = list(all_product_names)
                    if current_from and current_from not in from_options:
                        from_options.insert(0, current_from)

                    from_index = from_options.index(current_from) if current_from in from_options else 0
                    selected_from = st.selectbox(
                        "元商品名",
                        from_options,
                        index=from_index if current_from else None,
                        placeholder="商品名を選択...",
                        key=f"from_{idx}",
                    )
                    group["from_name"] = selected_from or ""

                    # 類似度ソートの基準
                    ref_name = group["from_name"]
                    sorted_candidates = _sort_by_similarity(all_product_names, ref_name)

                    col_up, col_upup = st.columns(2)

                    # --- アップセル先 (multiselect) ---
                    with col_up:
                        current_upsells = group.get("upsell_names", [])
                        # 候補リスト: 類似度順 (現在値がリストになくても選択済みとして表示される)
                        upsell_options = sorted_candidates
                        # 現在値がBQ一覧にない場合は先頭に追加
                        for cv in current_upsells:
                            if cv and cv not in upsell_options:
                                upsell_options.insert(0, cv)

                        use_manual_upsell = st.session_state.get(f"manual_upsell_{idx}", False)

                        if not use_manual_upsell:
                            sel_upsells = st.multiselect(
                                "アップセル先 (複数選択可)",
                                upsell_options,
                                default=current_upsells,
                                key=f"upsell_{idx}",
                            )
                            group["upsell_names"] = sel_upsells
                            if st.button("✏️ 手動入力", key=f"to_manual_up_{idx}", help="一覧にない商品名を入力"):
                                st.session_state[f"manual_upsell_{idx}"] = True
                                st.rerun()
                        else:
                            manual_val = st.text_input(
                                "アップセル先を追加 (手動入力)",
                                value="",
                                key=f"upsell_manual_{idx}",
                                placeholder="商品名を入力してEnter...",
                            )
                            if manual_val.strip():
                                if manual_val.strip() not in group.get("upsell_names", []):
                                    group["upsell_names"].append(manual_val.strip())
                            st.caption(f"現在の選択: {', '.join(group.get('upsell_names', [])) or 'なし'}")
                            if st.button("一覧から選ぶ", key=f"back_upsell_{idx}"):
                                st.session_state[f"manual_upsell_{idx}"] = False
                                st.rerun()

                    # --- アップセルアップセル先 (multiselect) ---
                    with col_upup:
                        current_upups = group.get("upsell_upsell_names", [])
                        upup_ref = group.get("upsell_names", [""])[0] if group.get("upsell_names") else ref_name
                        sorted_upup = _sort_by_similarity(all_product_names, upup_ref)
                        upup_options = sorted_upup
                        for cv in current_upups:
                            if cv and cv not in upup_options:
                                upup_options.insert(0, cv)

                        use_manual_upup = st.session_state.get(f"manual_upup_{idx}", False)

                        if not use_manual_upup:
                            sel_upups = st.multiselect(
                                "アップセルアップセル先 (複数選択可)",
                                upup_options,
                                default=current_upups,
                                key=f"upup_{idx}",
                            )
                            group["upsell_upsell_names"] = sel_upups
                            if st.button("✏️ 手動入力", key=f"to_manual_upup_{idx}", help="一覧にない商品名を入力"):
                                st.session_state[f"manual_upup_{idx}"] = True
                                st.rerun()
                        else:
                            manual_upup = st.text_input(
                                "アップセルアップセル先を追加 (手動入力)",
                                value="",
                                key=f"upup_manual_{idx}",
                                placeholder="商品名を入力してEnter...",
                            )
                            if manual_upup.strip():
                                if manual_upup.strip() not in group.get("upsell_upsell_names", []):
                                    group["upsell_upsell_names"].append(manual_upup.strip())
                            st.caption(f"現在の選択: {', '.join(group.get('upsell_upsell_names', [])) or 'なし'}")
                            if st.button("一覧から選ぶ", key=f"back_upup_{idx}"):
                                st.session_state[f"manual_upup_{idx}"] = False
                                st.rerun()

            # --- 行追加ボタン ---
            if st.button("＋ マッピングを追加", key="add_mapping"):
                edit_groups.append({"from_name": "", "upsell_names": [], "upsell_upsell_names": []})
                st.session_state["upsell_groups_edit"] = edit_groups
                st.rerun()

            # --- 保存ボタン ---
            st.markdown("")
            if st.button("保存", type="primary", key="save_upsell"):
                valid_groups = [g for g in edit_groups if g.get("from_name") and g.get("upsell_names")]
                flat_mappings = _groups_to_mappings(valid_groups)
                save_upsell_mappings(flat_mappings)
                st.session_state["upsell_groups_edit"] = valid_groups
                st.success(f"{len(flat_mappings)} 件のマッピングを保存しました。")
                st.rerun()
