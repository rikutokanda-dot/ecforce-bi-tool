"""コホートヒートマップ描画コンポーネント."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def render_cohort_heatmap(matrix: pd.DataFrame, title: str = "継続率ヒートマップ"):
    """Plotlyでコホート継続率ヒートマップを描画.

    Args:
        matrix: 行=コホート月, 列="1回目"〜"12回目", 値=継続率(%)
        title: グラフタイトル
    """
    if matrix.empty:
        st.info("表示するデータがありません。")
        return

    # テキストラベル (値%)
    text = matrix.map(lambda v: f"{v:.1f}%" if pd.notna(v) and v > 0 else "")

    fig = go.Figure(
        data=go.Heatmap(
            z=matrix.values,
            x=matrix.columns.tolist(),
            y=matrix.index.tolist(),
            text=text.values,
            texttemplate="%{text}",
            textfont={"size": 11},
            colorscale=[
                [0.0, "#FEE2E2"],   # 低い: 薄い赤
                [0.3, "#FDE68A"],   # 中低: 黄色
                [0.6, "#A7F3D0"],   # 中高: 薄い緑
                [1.0, "#34D399"],   # 高い: 緑
            ],
            colorbar=dict(title="継続率(%)"),
            zmin=0,
            zmax=100,
            hoverongaps=False,
            hovertemplate="コホート月: %{y}<br>%{x}: %{z:.1f}%<extra></extra>",
        )
    )

    fig.update_layout(
        title=title,
        xaxis_title="定期回数",
        yaxis_title="コホート月",
        yaxis=dict(autorange="reversed"),
        height=max(300, len(matrix) * 30 + 100),
        margin=dict(l=100, r=50, t=60, b=50),
    )

    st.plotly_chart(fig, use_container_width=True)


def render_retention_line_chart(matrix: pd.DataFrame, title: str = "継続率推移"):
    """コホート月ごとの継続率を折れ線グラフで表示.

    Args:
        matrix: 行=コホート月, 列="1回目"〜"12回目", 値=継続率(%)
    """
    if matrix.empty:
        st.info("表示するデータがありません。")
        return

    fig = go.Figure()
    for month in matrix.index:
        values = matrix.loc[month].dropna()
        if len(values) > 0:
            fig.add_trace(
                go.Scatter(
                    x=values.index.tolist(),
                    y=values.values,
                    mode="lines+markers",
                    name=str(month),
                    hovertemplate="%{x}: %{y:.1f}%<extra>" + str(month) + "</extra>",
                )
            )

    fig.update_layout(
        title=title,
        xaxis_title="定期回数",
        yaxis_title="継続率 (%)",
        yaxis=dict(range=[0, 105]),
        height=500,
        margin=dict(l=50, r=50, t=60, b=50),
        legend=dict(title="コホート月"),
    )

    st.plotly_chart(fig, use_container_width=True)
