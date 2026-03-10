"""切替回数ごとの継続率・残存率HTMLテーブル描画.

チラシ分析・メール分析で共有する描画関数。
"""

from __future__ import annotations

import math

import pandas as pd


def build_grouped_retention_html(
    group_df: pd.DataFrame, max_n: int, default_cycle2: int = 30,
    max_days: int = 365, product_cycles: dict = None,
    show_product_names: bool = True,
) -> str:
    """切替回数ごとにグループ化した継続率・残存率テーブルをHTML描画."""

    # ヘッダー
    th_style = (
        'padding:6px 8px;text-align:center;border-bottom:2px solid #ddd;'
        'font-size:12px;white-space:nowrap;'
    )
    header = f'<th style="{th_style}">指標</th>'
    for n in range(1, max_n + 1):
        header += f'<th style="{th_style}">{n}回目</th>'

    rows_html = ""
    sorted_df = group_df.sort_values("switch_order_count")

    for i, (_, row) in enumerate(sorted_df.iterrows()):
        switch_n = int(row["switch_order_count"])
        total = int(row["total_switched"])

        # 日数計算用: 商品名からproduct_cyclesマスタを直接参照
        def _lookup_cycles(product_name: str) -> tuple[int, int]:
            """商品名からcycle1, cycle2を取得."""
            if not product_name or not product_cycles:
                return default_cycle2, default_cycle2
            for p in product_cycles.get("products", []):
                if p.get("name") == product_name:
                    c1 = p.get("cycle1")
                    c2 = p.get("cycle2")
                    if c1 is not None and not (isinstance(c1, float) and math.isnan(c1)):
                        c1 = int(c1)
                    else:
                        c1 = default_cycle2
                    if c2 is not None and not (isinstance(c2, float) and math.isnan(c2)):
                        c2 = int(c2)
                    else:
                        c2 = default_cycle2
                    return c1, c2
            return default_cycle2, default_cycle2

        orig_name = row.get("original_product_name", "") if "original_product_name" in row.index else ""
        switched_name = row.get("switched_product_name", "") if "switched_product_name" in row.index else ""
        if pd.isna(orig_name):
            orig_name = ""
        if pd.isna(switched_name):
            switched_name = ""

        orig_c1, orig_c2 = _lookup_cycles(orig_name)
        up_c1, up_c2 = _lookup_cycles(switched_name)

        # retained / eligible / cont_denom を事前に取得
        retained = {}
        eligible = {}
        cont_denom = {}
        for n in range(1, max_n + 1):
            r_col = f"retained_{n}"
            e_col = f"eligible_{n}"
            cd_col = f"cont_denom_{n}"
            retained[n] = int(row[r_col]) if r_col in row.index and pd.notna(row[r_col]) else 0
            eligible[n] = int(row[e_col]) if e_col in row.index and pd.notna(row[e_col]) else 0
            cont_denom[n] = int(row[cd_col]) if cd_col in row.index and pd.notna(row[cd_col]) else 0

        # グループ見出し行
        top_border = 'border-top:2px solid #ccc;' if i > 0 else ''
        chip_style = (
            'display:inline-block;padding:2px 8px;margin:2px 4px;'
            'border-radius:12px;font-size:11px;font-weight:400;'
            'max-width:100%;word-break:break-all;'
        )
        product_info = ""
        if show_product_names and (orig_name or switched_name):
            lines = []
            if orig_name:
                lines.append(
                    f'<div style="margin-top:4px;">'
                    f'<span style="font-size:11px;color:#888;">切替前：</span>'
                    f'<span style="{chip_style}background:#f0f0f0;color:#555;">{orig_name}</span>'
                    f'</div>'
                )
            if switched_name:
                lines.append(
                    f'<div style="margin-top:2px;">'
                    f'<span style="font-size:11px;color:#888;">切替後：</span>'
                    f'<span style="{chip_style}background:#e3f2fd;color:#1565c0;">{switched_name}</span>'
                    f'</div>'
                )
            product_info = "".join(lines)
        rows_html += (
            f'<tr><td colspan="{max_n + 1}" style="padding:8px 8px 2px;'
            f'font-weight:700;font-size:13px;{top_border}">'
            f'{switch_n}回目切替（{total:,}人）{product_info}</td></tr>'
        )

        # 累計日数を事前計算（期間超えの判定に全行で使う）
        pre_total = orig_c1 + max(switch_n - 2, 0) * orig_c2
        cum_days = {}
        for n in range(1, max_n + 1):
            if n == 1:
                cum_days[n] = 0
            elif n <= switch_n:
                cum_days[n] = orig_c1 + (n - 2) * orig_c2
            elif n == switch_n + 1:
                cum_days[n] = pre_total + up_c1
            else:
                cum_days[n] = pre_total + up_c1 + (n - switch_n - 1) * up_c2

        _dash = '<td style="text-align:center;padding:2px 6px;font-size:13px;color:#ccc;">-</td>'

        # 累計日数行
        day_cells = (
            '<td style="padding:2px 8px;font-size:11px;color:#999;'
            'white-space:nowrap;">累計日数</td>'
        )
        for n in range(1, max_n + 1):
            if cum_days[n] > max_days:
                day_cells += _dash
            else:
                day_cells += (
                    f'<td style="text-align:center;padding:2px 6px;'
                    f'font-size:11px;color:#999;white-space:nowrap;">~{cum_days[n]}日</td>'
                )
        rows_html += f"<tr>{day_cells}</tr>"

        # 継続率行
        cells = (
            '<td style="padding:2px 8px;font-size:12px;color:#555;'
            'white-space:nowrap;">継続率</td>'
        )
        for n in range(1, max_n + 1):
            if cum_days[n] > max_days or cont_denom[n] == 0:
                cells += _dash
            elif n <= switch_n:
                cells += (
                    '<td style="text-align:center;padding:2px 6px;white-space:nowrap;">'
                    '<span style="font-size:14px;font-weight:600;">100.0%</span><br>'
                    f'<span style="font-size:10px;color:#888;">'
                    f'({retained[n]:,}/{cont_denom[n]:,})</span></td>'
                )
            else:
                denom = cont_denom[n]
                rate = retained[n] / denom * 100 if denom > 0 else 0
                cells += (
                    f'<td style="text-align:center;padding:2px 6px;white-space:nowrap;">'
                    f'<span style="font-size:14px;font-weight:600;">{rate:.1f}%</span><br>'
                    f'<span style="font-size:10px;color:#888;">'
                    f'({retained[n]:,}/{denom:,})</span></td>'
                )
        rows_html += f"<tr>{cells}</tr>"

        # 残存率行
        cells = (
            '<td style="padding:2px 8px 6px;font-size:12px;color:#555;'
            'white-space:nowrap;">残存率</td>'
        )
        for n in range(1, max_n + 1):
            if cum_days[n] > max_days or eligible[n] == 0:
                cells += _dash
            else:
                rate = retained[n] / eligible[n] * 100
                cells += (
                    f'<td style="text-align:center;padding:2px 6px 6px;white-space:nowrap;">'
                    f'<span style="font-size:14px;font-weight:600;">{rate:.1f}%</span><br>'
                    f'<span style="font-size:10px;color:#888;">'
                    f'({retained[n]:,}/{eligible[n]:,})</span></td>'
                )
        rows_html += f"<tr>{cells}</tr>"

    return f"""
    <div style="overflow-x:auto;">
    <table style="width:100%;border-collapse:collapse;">
    <thead><tr>{header}</tr></thead>
    <tbody>{rows_html}</tbody>
    </table>
    </div>
    """
