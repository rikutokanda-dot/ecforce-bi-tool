"""設定ファイルの読み込み・書き込み."""

from __future__ import annotations

from pathlib import Path

import streamlit as st
import yaml

CONFIG_DIR = Path(__file__).parent.parent / "config"
PRODUCT_CYCLES_FILE = CONFIG_DIR / "product_cycles.yaml"
UPSELL_MAPPING_FILE = CONFIG_DIR / "upsell_mapping.yaml"


# =====================================================================
# 会社マスタ
# =====================================================================


@st.cache_data
def load_companies() -> list[dict]:
    """会社一覧を読み込む."""
    with open(CONFIG_DIR / "companies.yaml", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("companies", [])


def get_company_keys() -> set[str]:
    """許可された会社キーのセットを返す."""
    return {c["key"] for c in load_companies()}


# =====================================================================
# 商品サイクルマスタ
# =====================================================================


def load_product_cycles() -> dict:
    """商品名別の発送サイクル設定を読み込む.

    Returns:
        {"products": [{"name": ..., "cycle1": ..., "cycle2": ...}, ...],
         "defaults": {"cycle1": 30, "cycle2": 30}}
    """
    if not PRODUCT_CYCLES_FILE.exists():
        return {"products": [], "defaults": {"cycle1": 30, "cycle2": 30}}
    with open(PRODUCT_CYCLES_FILE, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return {
        "products": data.get("products", []),
        "defaults": data.get("defaults", {"cycle1": 30, "cycle2": 30}),
    }


def get_product_cycle(product_name: str) -> tuple[int, int]:
    """商品名に対応する(cycle1, cycle2)を返す. 見つからなければデフォルト値."""
    data = load_product_cycles()
    for product in data.get("products", []):
        if product["name"] == product_name:
            return product.get("cycle1", 30), product.get("cycle2", 30)
    defaults = data.get("defaults", {})
    return defaults.get("cycle1", 30), defaults.get("cycle2", 30)


def save_product_cycles(data: dict) -> None:
    """商品サイクル設定をYAMLに保存."""
    with open(PRODUCT_CYCLES_FILE, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)


# =====================================================================
# アップセルマッピング
# =====================================================================


def load_upsell_mappings() -> list[dict]:
    """アップセルマッピングを読み込む.

    後方互換: from_name (文字列) → from_names (リスト) に自動変換。
    """
    if not UPSELL_MAPPING_FILE.exists():
        return []
    with open(UPSELL_MAPPING_FILE, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    raw = data.get("mappings", [])
    # 後方互換: from_name → from_names
    for m in raw:
        if "from_names" not in m and "from_name" in m:
            m["from_names"] = [m.pop("from_name")]
    return raw


def save_upsell_mappings(mappings: list[dict]) -> None:
    """アップセルマッピングをYAMLに保存."""
    with open(UPSELL_MAPPING_FILE, "w", encoding="utf-8") as f:
        yaml.dump(
            {"mappings": mappings},
            f,
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
        )


def get_upsell_target(product_name: str) -> dict | None:
    """商品名のアップセル先を取得. なければNone."""
    for m in load_upsell_mappings():
        if product_name in m.get("from_names", []):
            return m
    return None


def get_upsell_targets(product_name: str) -> list[dict]:
    """商品名のアップセル先を全て取得. なければ空リスト.

    1つの通常商品に複数のアップセル先がある場合に対応。
    """
    return [m for m in load_upsell_mappings() if product_name in m.get("from_names", [])]
