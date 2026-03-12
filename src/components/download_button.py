"""CSV/Excelダウンロードコンポーネント."""

from io import BytesIO

import pandas as pd
import streamlit as st


def df_to_csv_bytes(df: pd.DataFrame, **kwargs) -> bytes:
    """DataFrameをWindows互換のCSVバイト列に変換.

    - UTF-8 BOM付き (Windows Excelで文字化けしない)
    - CRLF改行 (Windowsテキストエディタ互換)
    """
    csv_str = df.to_csv(index=False, lineterminator="\r\n", **kwargs)
    return csv_str.encode("utf-8-sig")


def render_download_buttons(df: pd.DataFrame, filename_prefix: str = "data"):
    """CSVとExcelのダウンロードボタンを表示."""
    col1, col2 = st.columns(2)

    with col1:
        csv_data = df_to_csv_bytes(df)
        st.download_button(
            label="CSV ダウンロード",
            data=csv_data,
            file_name=f"{filename_prefix}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with col2:
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Data")
        st.download_button(
            label="Excel ダウンロード",
            data=buffer.getvalue(),
            file_name=f"{filename_prefix}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
