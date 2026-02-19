"""設定ファイルの読み込み・書き込み."""

from __future__ import annotations

from pathlib import Path

import streamlit as st
import yaml

CONFIG_DIR = Path(__file__).parent.parent / "config"
PRODUCT_CYCLES_FILE = CONFIG_DIR / "product_cycles.yaml"
UPSELL_MAPPING_FILE = CONFIG_DIR / "upsell_mapping.yaml"
AD_URL_MAPPING_FILE = CONFIG_DIR / "ad_url_mapping.yaml"


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

    新形式: label / numerator_names / denominator_names / period_ref_names
    後方互換: 旧形式(from_names / upsell_name / upsell_upsell_name)も自動変換。
    """
    if not UPSELL_MAPPING_FILE.exists():
        return []
    with open(UPSELL_MAPPING_FILE, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    raw = data.get("mappings", [])
    result = []
    for m in raw:
        # 新形式ならそのまま
        if "numerator_names" in m:
            result.append(m)
            continue
        # 旧形式 → 新形式に変換
        fns = m.get("from_names") or ([m["from_name"]] if "from_name" in m else [])
        un = m.get("upsell_name", "")
        if not fns or not un:
            continue
        result.append({
            "label": un,
            "numerator_names": [un],
            "denominator_names": list(fns),
            "period_ref_names": [un],
        })
    return result


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
    """商品名に関連するマッピングを取得. なければNone."""
    for m in load_upsell_mappings():
        if product_name in m.get("denominator_names", []):
            return m
    return None


def get_upsell_targets(product_name: str) -> list[dict]:
    """商品名に関連するマッピングを全て取得. なければ空リスト."""
    return [
        m for m in load_upsell_mappings()
        if product_name in m.get("denominator_names", [])
    ]


# =====================================================================
# 広告URL IDマッピング
# =====================================================================


def load_ad_url_mappings() -> list[dict]:
    """広告URL IDマッピングを読み込む.

    Returns:
        [{"ad_url_id": "xxx", "ad_url_name": "表示名"}, ...]
    """
    if not AD_URL_MAPPING_FILE.exists():
        return []
    with open(AD_URL_MAPPING_FILE, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("mappings", [])


def save_ad_url_mappings(mappings: list[dict]) -> None:
    """広告URL IDマッピングをYAMLに保存."""
    with open(AD_URL_MAPPING_FILE, "w", encoding="utf-8") as f:
        yaml.dump(
            {"mappings": mappings},
            f,
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
        )


def get_ad_url_display_map() -> dict[str, str]:
    """広告URL ID → 表示名 の辞書を返す.

    名前が空または未定義の場合はキーを含めない（呼び出し側でID表示にフォールバック）。
    """
    return {
        m["ad_url_id"]: m["ad_url_name"]
        for m in load_ad_url_mappings()
        if m.get("ad_url_id") and m.get("ad_url_name")
    }
