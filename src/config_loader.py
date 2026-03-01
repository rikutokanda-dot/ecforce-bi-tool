"""設定ファイルの読み込み・書き込み (GCS永続化対応).

Cloud Run上ではGCSバケットに保存・読み込みし、
ローカル開発時はconfig/ディレクトリにフォールバック。
"""

from __future__ import annotations

import os
from pathlib import Path

import streamlit as st
import yaml

CONFIG_DIR = Path(__file__).parent.parent / "config"
PRODUCT_CYCLES_FILE = CONFIG_DIR / "product_cycles.yaml"
UPSELL_MAPPING_FILE = CONFIG_DIR / "upsell_mapping.yaml"
TIER_BOUNDARIES_FILE = CONFIG_DIR / "tier_boundaries.yaml"

# GCS設定
GCS_BUCKET = os.environ.get("CONFIG_GCS_BUCKET", "ecforce-bi-config")
GCS_PREFIX = "config/"


# =====================================================================
# GCS読み書きヘルパー
# =====================================================================

@st.cache_resource
def _get_gcs_client():
    """GCSクライアントを取得。失敗時はNone。"""
    try:
        from google.cloud import storage
        return storage.Client()
    except Exception:
        return None


def _read_from_gcs(filename: str) -> dict | None:
    """GCSからYAMLを読み込む。失敗時はNone。"""
    client = _get_gcs_client()
    if not client:
        return None
    try:
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(f"{GCS_PREFIX}{filename}")
        if not blob.exists():
            return None
        content = blob.download_as_text(encoding="utf-8")
        return yaml.safe_load(content) or {}
    except Exception:
        return None


def _write_to_gcs(filename: str, data: dict) -> bool:
    """GCSにYAMLを書き込む。成功時True。"""
    client = _get_gcs_client()
    if not client:
        return False
    try:
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(f"{GCS_PREFIX}{filename}")
        content = yaml.dump(
            data, allow_unicode=True,
            default_flow_style=False, sort_keys=False,
        )
        blob.upload_from_string(content, content_type="text/yaml")
        return True
    except Exception:
        return False


def _read_yaml(filename: str, local_path: Path) -> dict:
    """GCS優先、ローカルフォールバックでYAMLを読み込む。"""
    # GCSから読み込み
    data = _read_from_gcs(filename)
    if data is not None:
        return data
    # ローカルフォールバック
    if local_path.exists():
        with open(local_path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


def _write_yaml(filename: str, local_path: Path, data: dict) -> None:
    """GCSとローカル両方に書き込む。"""
    # GCSに書き込み
    _write_to_gcs(filename, data)
    # ローカルにも書き込み（開発時用）
    with open(local_path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)


# =====================================================================
# 会社マスタ
# =====================================================================


@st.cache_data
def load_companies() -> list[dict]:
    """会社一覧を読み込む."""
    data = _read_yaml("companies.yaml", CONFIG_DIR / "companies.yaml")
    return data.get("companies", [])


def get_company_keys() -> set[str]:
    """許可された会社キーのセットを返す."""
    return {c["key"] for c in load_companies()}


# =====================================================================
# 商品サイクルマスタ
# =====================================================================


@st.cache_data(ttl=300)
def load_product_cycles() -> dict:
    """商品名別の発送サイクル設定を読み込む.

    Returns:
        {"products": [{"name": ..., "cycle1": ..., "cycle2": ...}, ...],
         "defaults": {"cycle1": 30, "cycle2": 30}}
    """
    data = _read_yaml("product_cycles.yaml", PRODUCT_CYCLES_FILE)
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
    _write_yaml("product_cycles.yaml", PRODUCT_CYCLES_FILE, data)


# =====================================================================
# アップセルマッピング
# =====================================================================


@st.cache_data(ttl=300)
def load_upsell_mappings() -> list[dict]:
    """アップセルマッピングを読み込む.

    新形式: label / numerator_names / denominator_names / period_ref_names
    後方互換: 旧形式(from_names / upsell_name / upsell_upsell_name)も自動変換。
    """
    data = _read_yaml("upsell_mapping.yaml", UPSELL_MAPPING_FILE)
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
    _write_yaml("upsell_mapping.yaml", UPSELL_MAPPING_FILE, {"mappings": mappings})


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
# Tier境界値マスタ
# =====================================================================

_DEFAULT_TIER_BOUNDARIES = [
    5000, 10000, 20000, 30000, 40000, 50000,
    60000, 70000, 80000, 90000, 100000,
]


@st.cache_data(ttl=300)
def load_tier_boundaries() -> list[int]:
    """Tier境界値を読み込む。未設定ならデフォルト値を返す."""
    data = _read_yaml("tier_boundaries.yaml", TIER_BOUNDARIES_FILE)
    boundaries = data.get("boundaries", _DEFAULT_TIER_BOUNDARIES)
    return sorted(int(b) for b in boundaries)


def save_tier_boundaries(boundaries: list[int]) -> None:
    """Tier境界値をYAMLに保存."""
    _write_yaml(
        "tier_boundaries.yaml",
        TIER_BOUNDARIES_FILE,
        {"boundaries": sorted(boundaries)},
    )
