"""設定ファイルの読み込み."""

from pathlib import Path

import streamlit as st
import yaml

CONFIG_DIR = Path(__file__).parent.parent / "config"


@st.cache_data
def load_companies() -> list[dict]:
    """会社一覧を読み込む."""
    with open(CONFIG_DIR / "companies.yaml", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("companies", [])


def get_company_keys() -> set[str]:
    """許可された会社キーのセットを返す."""
    return {c["key"] for c in load_companies()}


@st.cache_data
def load_product_cycles(company_key: str) -> dict:
    """会社ごとの商品発送サイクル設定を読み込む."""
    with open(CONFIG_DIR / "product_cycles.yaml", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    default_config = {"default": {"cycle1": 30, "cycle2": 30}, "products": []}
    return data.get(company_key, default_config)


def get_product_cycle(company_key: str, product_name: str) -> tuple[int, int]:
    """商品名に対応する(cycle1, cycle2)を返す. 見つからなければデフォルト値."""
    config = load_product_cycles(company_key)
    for product in config.get("products", []):
        if product["name"] == product_name:
            return product.get("cycle1", 30), product.get("cycle2", 30)
    default = config.get("default", {})
    return default.get("cycle1", 30), default.get("cycle2", 30)
