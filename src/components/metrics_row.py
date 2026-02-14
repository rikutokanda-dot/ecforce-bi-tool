"""KPIメトリクスカード表示コンポーネント."""

import streamlit as st


def render_metrics(metrics: list[dict]):
    """KPIメトリクスを横並びで表示.

    Args:
        metrics: [{"label": "新規顧客数", "value": "1,234", "delta": "+5%"}, ...]
    """
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        with col:
            col.metric(
                label=m["label"],
                value=m["value"],
                delta=m.get("delta"),
            )
