"""マスタ管理ページ - 商品サイクル・アップセルマッピングの閲覧・編集."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.config_loader import (
    load_product_cycles,
    load_upsell_mappings,
    save_product_cycles,
    save_upsell_mappings,
)

st.header("マスタ管理")

tab_cycles, tab_upsell = st.tabs(["商品サイクル", "アップセルマッピング"])

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
    st.caption("商品名ごとのアップセル先・アップセルアップセル先を管理します。")

    mappings = load_upsell_mappings()
    upsell_df = pd.DataFrame(mappings) if mappings else pd.DataFrame(
        columns=["from_name", "upsell_name", "upsell_upsell_name"]
    )

    edited_upsell = st.data_editor(
        upsell_df,
        num_rows="dynamic",
        column_config={
            "from_name": st.column_config.TextColumn("元商品名", required=True, width="large"),
            "upsell_name": st.column_config.TextColumn("アップセル先", required=True, width="large"),
            "upsell_upsell_name": st.column_config.TextColumn("アップセルアップセル先", width="large"),
        },
        use_container_width=True,
        height=400,
        key="upsell_editor",
    )

    if st.button("保存", type="primary", key="save_upsell"):
        records = edited_upsell.dropna(subset=["from_name"]).to_dict("records")
        save_upsell_mappings(records)
        st.success(f"{len(records)} 件のマッピングを保存しました。")
        st.rerun()
