"""CSV/Excelダウンロードコンポーネント."""

from io import BytesIO

import pandas as pd
import streamlit as st


def render_download_buttons(df: pd.DataFrame, filename_prefix: str = "data"):
    """CSVとExcelのダウンロードボタンを表示."""
    col1, col2 = st.columns(2)

    with col1:
        csv_data = df.to_csv(index=False).encode("utf-8-sig")
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
