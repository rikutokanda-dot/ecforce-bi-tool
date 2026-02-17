"""BigQueryクライアント管理とクエリ実行."""

from __future__ import annotations

import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

from src.constants import BQ_LOCATION, PROJECT_ID


@st.cache_resource
def get_bigquery_client() -> bigquery.Client:
    """BigQueryクライアントのシングルトン生成."""
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(
        credentials=credentials,
        project=PROJECT_ID,
        location=BQ_LOCATION,
    )


@st.cache_data(ttl=3600, show_spinner="BigQueryからデータを取得中...")
def execute_query(_client: bigquery.Client, query: str) -> pd.DataFrame:
    """キャッシュ付きクエリ実行. TTL=1時間."""
    return _client.query(query).to_dataframe()


def fetch_filter_options(
    _client: bigquery.Client, table_ref: str, column: str
) -> list[str]:
    """フィルタ選択肢用に指定カラムのユニーク値を取得."""
    query = f"""
        SELECT DISTINCT `{column}` AS val
        FROM {table_ref}
        WHERE `{column}` IS NOT NULL AND `{column}` != ''
        ORDER BY val
    """
    df = execute_filter_query(_client, query)
    return df["val"].tolist()


def fetch_filtered_options(
    _client: bigquery.Client,
    table_ref: str,
    column: str,
    parent_filters: dict[str, list[str]] | None = None,
) -> list[str]:
    """親フィルタの選択に基づいて絞り込んだユニーク値を取得."""
    where_parts = [f"`{column}` IS NOT NULL", f"`{column}` != ''"]

    if parent_filters:
        for parent_col, parent_values in parent_filters.items():
            if parent_values:
                conditions = " OR ".join(
                    f"`{parent_col}` = '{v}'" for v in parent_values
                )
                where_parts.append(f"({conditions})")

    where_str = " AND ".join(where_parts)
    query = f"""
        SELECT DISTINCT `{column}` AS val
        FROM {table_ref}
        WHERE {where_str}
        ORDER BY val
    """
    df = execute_filter_query(_client, query)
    return df["val"].tolist()


@st.cache_data(ttl=86400, show_spinner=False)
def execute_filter_query(_client: bigquery.Client, query: str) -> pd.DataFrame:
    """フィルタ選択肢用のクエリ実行. TTL=24時間."""
    return _client.query(query).to_dataframe()
