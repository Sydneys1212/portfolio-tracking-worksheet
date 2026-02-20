"""
analyze.py  –  WCCP Portfolio Tracker
======================================
Reads WCCP_Master_Trade_Blotter.xlsx from ./data/
Produces:
  • /output/daily_pricing_YYYY-MM-DD.xlsx  – Pricing template with Bloomberg placeholders
"""

import os
import sys
import warnings
from collections import defaultdict
from datetime import date
from pathlib import Path

import pandas as pd
from openpyxl import Workbook
from openpyxl.comments import Comment
from openpyxl.styles import (
    Alignment, Border, Font, PatternFill, Side, numbers
)
from openpyxl.utils import get_column_letter

warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
DATA_DIR    = BASE_DIR / "data"
OUTPUT_DIR  = BASE_DIR / "output"

TODAY        = date.today().isoformat()          # "YYYY-MM-DD"
BLOTTER_FILE = DATA_DIR / "WCCP_Master_Trade_Blotter.xlsx"
EXCEL_PATH   = OUTPUT_DIR / f"daily_pricing_{TODAY}.xlsx"

# Create output dir if missing
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ══════════════════════════════════════════════════════════
# 1. DATA LOADING
# ══════════════════════════════════════════════════════════

def load_blotter(path: Path) -> pd.DataFrame:
    """Load and normalise the trade blotter."""
    if not path.exists():
        sys.exit(
            f"\n[ERROR] Trade blotter not found: {path}\n"
            f"        Place WCCP_Master_Trade_Blotter.xlsx in the ./data/ folder.\n"
        )

    df = pd.read_excel(path, sheet_name="Trade Blotter")

    # Normalise column names
    df.columns = [c.strip() for c in df.columns]

    required = {
        "Date", "Security", "Ticker", "Type",
        "Transaction Type", "Quantity", "Price",
        "Fund Consideration", "Currency", "Entity", "AccountShortName",
    }
    missing = required - set(df.columns)
    if missing:
        sys.exit(f"\n[ERROR] Missing columns in blotter: {missing}\n")

    # Type coercion
    df["Date"]     = pd.to_datetime(df["Date"])
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    df["Price"]    = pd.to_numeric(df["Price"],    errors="coerce").fillna(0)
    df["Fund Consideration"] = pd.to_numeric(
        df["Fund Consideration"], errors="coerce"
    ).fillna(0)
    df["Transaction Type"] = df["Transaction Type"].str.strip().str.title()  # Buy / Sell
    df = df.sort_values("Date").reset_index(drop=True)

    return df


# ══════════════════════════════════════════════════════════
# 2. FIFO TAX-LOT MATCHING
# ══════════════════════════════════════════════════════════

def fifo_match(df: pd.DataFrame):
    """
    FIFO lot matching per Ticker + Entity.

    Returns
    -------
    open_lots   : list of dicts  (one per surviving buy lot)
    realized_pl : list of dicts  (one per matched sell)
    """
    # Group trades by (Ticker, Entity)
    groups = defaultdict(list)
    for _, row in df.iterrows():
        key = (row["Ticker"], row["Entity"])
        groups[key].append(row)

    open_lots    = []   # remaining open tax lots
    realized_pl  = []   # matched sell records

    for (ticker, entity), trades in groups.items():
        buy_queue = []   # list of [date, security, qty_remaining, price, entity]

        for trade in trades:
            qty   = abs(float(trade["Quantity"]))
            price = float(trade["Price"])
            txn   = trade["Transaction Type"]

            if txn == "Buy":
                buy_queue.append({
                    "date":     trade["Date"],
                    "security": trade["Security"],
                    "ticker":   ticker,
                    "entity":   entity,
                    "account":  trade["AccountShortName"],
                    "currency": trade["Currency"],
                    "qty_rem":  qty,
                    "cost_px":  price,
                })

            elif txn == "Sell":
                qty_to_sell = qty
                while qty_to_sell > 0 and buy_queue:
                    lot = buy_queue[0]
                    matched = min(lot["qty_rem"], qty_to_sell)

                    realized_pl.append({
                        "ticker":      ticker,
                        "entity":      entity,
                        "security":    lot["security"],
                        "buy_date":    lot["date"],
                        "sell_date":   trade["Date"],
                        "qty":         matched,
                        "cost_px":     lot["cost_px"],
                        "sell_px":     price,
                        "realized_pl": matched * (price - lot["cost_px"]),
                    })

                    lot["qty_rem"] -= matched
                    qty_to_sell    -= matched

                    if lot["qty_rem"] <= 1e-9:
                        buy_queue.pop(0)

                # If sells exceed buys (short / data issue), ignore remainder
                if qty_to_sell > 1e-4:
                    print(
                        f"  [WARN] Excess sell qty {qty_to_sell:.4f} for "
                        f"{ticker} / {entity} on {trade['Date'].date()} — skipped."
                    )

        # Remaining open lots
        for lot in buy_queue:
            if lot["qty_rem"] > 1e-9:
                open_lots.append(lot)

    return open_lots, realized_pl


# ══════════════════════════════════════════════════════════
# 3. POSITION SUMMARY
# ══════════════════════════════════════════════════════════

def build_positions(open_lots: list) -> pd.DataFrame:
    """
    Aggregate open lots into a position-level summary per (Ticker, Entity).
    """
    rows = []
    # group by ticker + entity
    grouped = defaultdict(list)
    for lot in open_lots:
        grouped[(lot["ticker"], lot["entity"])].append(lot)

    for (ticker, entity), lots in grouped.items():
        total_qty  = sum(l["qty_rem"] for l in lots)
        total_cost = sum(l["qty_rem"] * l["cost_px"] for l in lots)
        avg_cost   = total_cost / total_qty if total_qty else 0

        rows.append({
            "Security":        lots[0]["security"],
            "Ticker":          ticker,
            "Entity":          entity,
            "Account":         lots[0]["account"],
            "Currency":        lots[0]["currency"],
            "Total Shares":    total_qty,
            "Avg Cost":        avg_cost,
            "Total Cost Basis": total_cost,
        })

    df = pd.DataFrame(rows).sort_values(
        ["Entity", "Total Cost Basis"], ascending=[True, False]
    ).reset_index(drop=True)
    return df


# ══════════════════════════════════════════════════════════
# 4. DAILY PRICING EXCEL
# ══════════════════════════════════════════════════════════

def generate_excel(positions: pd.DataFrame):
    print(f"  Generating Excel -> {EXCEL_PATH}")

    wb = Workbook()
    ws = wb.active
    ws.title = "Daily Pricing"

    # ── Styles ──────────────────────────────────────────────
    navy_fill   = PatternFill("solid", fgColor="1B3A5C")
    gold_fill   = PatternFill("solid", fgColor="B8960C")
    yellow_fill = PatternFill("solid", fgColor="FEF9C3")
    light_fill  = PatternFill("solid", fgColor="EBF0F5")
    white_fill  = PatternFill("solid", fgColor="FFFFFF")
    green_fill  = PatternFill("solid", fgColor="DCFCE7")
    red_fill    = PatternFill("solid", fgColor="FEE2E2")

    header_font  = Font(name="Calibri", bold=True, color="FFFFFF", size=10)
    subhdr_font  = Font(name="Calibri", bold=True, color="1B3A5C", size=9)
    data_font    = Font(name="Calibri", size=9)
    formula_font = Font(name="Calibri", size=9, color="1D4ED8")  # blue for formulas

    thin = Side(border_style="thin", color="D1D5DB")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    center_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    right_align  = Alignment(horizontal="right",  vertical="center")
    left_align   = Alignment(horizontal="left",   vertical="center")

    # ── Title row ────────────────────────────────────────────
    ws.merge_cells("A1:L1")
    ws["A1"] = f"WCCP Daily Portfolio Pricing Sheet  –  {TODAY}"
    ws["A1"].font   = Font(name="Calibri", bold=True, color="FFFFFF", size=13)
    ws["A1"].fill   = navy_fill
    ws["A1"].alignment = center_align
    ws.row_dimensions[1].height = 28

    ws.merge_cells("A2:L2")
    ws["A2"] = (
        'Instructions: Enter current prices in Column G, or use Bloomberg formula  '
        '=BDP("TICKER","PX_LAST")  — Market Value, P&L, and % Return auto-calculate.'
    )
    ws["A2"].font      = Font(name="Calibri", italic=True, size=8, color="78350F")
    ws["A2"].fill      = PatternFill("solid", fgColor="FEF3C7")
    ws["A2"].alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
    ws.row_dimensions[2].height = 22

    # ── Column headers (row 3) ───────────────────────────────
    headers = [
        "Security", "Ticker", "Entity", "Account",
        "Total Shares", "Avg Cost", "Current Price",
        "Market Value", "Total Cost Basis", "Unrealized P&L",
        "% Return", "Currency"
    ]
    col_widths = [28, 20, 18, 18, 13, 13, 14, 16, 16, 16, 10, 10]

    for col_idx, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
        cell = ws.cell(row=3, column=col_idx, value=hdr)
        cell.font      = header_font
        cell.fill      = navy_fill
        cell.alignment = center_align
        cell.border    = border
        ws.column_dimensions[get_column_letter(col_idx)].width = width

    ws.row_dimensions[3].height = 20

    # ── Data rows ────────────────────────────────────────────
    # Column letter map
    # A=Security B=Ticker C=Entity D=Account
    # E=Total Shares F=Avg Cost G=Current Price
    # H=Market Value I=Total Cost Basis J=Unrealized P&L K=% Return L=Currency

    data_start_row = 4
    for r_offset, (_, pos) in enumerate(positions.iterrows()):
        row = data_start_row + r_offset
        alt_fill = light_fill if r_offset % 2 == 0 else white_fill

        def cell_set(col, value, num_fmt=None, font=None, fill=None, align=None):
            c = ws.cell(row=row, column=col, value=value)
            c.font      = font  or data_font
            c.fill      = fill  or alt_fill
            c.alignment = align or left_align
            c.border    = border
            if num_fmt:
                c.number_format = num_fmt
            return c

        ticker_raw  = pos["Ticker"]                    # e.g. "LEU US Equity"
        ticker_bbg  = ticker_raw                        # keep Bloomberg format

        cell_set(1,  pos["Security"])
        cell_set(2,  ticker_raw)
        cell_set(3,  pos["Entity"])
        cell_set(4,  pos.get("Account", ""))
        cell_set(5,  pos["Total Shares"],    num_fmt="#,##0.0000", align=right_align)
        cell_set(6,  pos["Avg Cost"],        num_fmt='$#,##0.0000', align=right_align)

        # Current Price – input cell with comment
        price_cell = ws.cell(row=row, column=7, value=None)
        price_cell.font      = formula_font
        price_cell.fill      = yellow_fill
        price_cell.alignment = right_align
        price_cell.border    = border
        price_cell.number_format = '$#,##0.0000'
        comment_text = (
            f'Enter current price manually, or use Bloomberg formula:\n'
            f'=BDP("{ticker_bbg}","PX_LAST")'
        )
        price_cell.comment = Comment(comment_text, "WCCP System")

        e_col  = get_column_letter(5)   # Total Shares
        f_col  = get_column_letter(6)   # Avg Cost
        g_col  = get_column_letter(7)   # Current Price
        h_col  = get_column_letter(8)   # Market Value
        i_col  = get_column_letter(9)   # Total Cost Basis

        # Market Value = Shares * Current Price (formula, shown in blue)
        mv_cell = ws.cell(row=row, column=8,
                           value=f"=IF({g_col}{row}=\"\",\"\",{e_col}{row}*{g_col}{row})")
        mv_cell.font         = formula_font
        mv_cell.fill         = alt_fill
        mv_cell.alignment    = right_align
        mv_cell.border       = border
        mv_cell.number_format = '$#,##0.00'

        # Total Cost Basis (static value)
        cell_set(9, pos["Total Cost Basis"], num_fmt='$#,##0.00', align=right_align)

        # Unrealized P&L = Market Value - Total Cost Basis
        upl_cell = ws.cell(row=row, column=10,
                            value=f"=IF({g_col}{row}=\"\",\"\",{h_col}{row}-{i_col}{row})")
        upl_cell.font         = formula_font
        upl_cell.fill         = alt_fill
        upl_cell.alignment    = right_align
        upl_cell.border       = border
        upl_cell.number_format = '$#,##0.00'

        # % Return = Unrealized P&L / Total Cost Basis
        j_col = get_column_letter(10)
        ret_cell = ws.cell(row=row, column=11,
                           value=f"=IF({g_col}{row}=\"\",\"\",{j_col}{row}/{i_col}{row})")
        ret_cell.font         = formula_font
        ret_cell.fill         = alt_fill
        ret_cell.alignment    = right_align
        ret_cell.border       = border
        ret_cell.number_format = '0.00%'

        cell_set(12, pos.get("Currency", "USD"), align=center_align)

        ws.row_dimensions[row].height = 16

    # ── Totals row ───────────────────────────────────────────
    last_data_row = data_start_row + len(positions) - 1
    totals_row    = last_data_row + 1

    ws.row_dimensions[totals_row].height = 18

    total_lbl = ws.cell(row=totals_row, column=1, value="TOTAL")
    total_lbl.font      = Font(name="Calibri", bold=True, color="FFFFFF", size=10)
    total_lbl.fill      = navy_fill
    total_lbl.alignment = center_align
    total_lbl.border    = border

    # Merge label across A–G (H and I are used for totals)
    ws.merge_cells(f"A{totals_row}:G{totals_row}")

    # Total Cost Basis sum
    tcb_total = ws.cell(
        row=totals_row, column=9,
        value=f"=SUM(I{data_start_row}:I{last_data_row})"
    )
    tcb_total.font         = Font(name="Calibri", bold=True, size=10)
    tcb_total.fill         = navy_fill
    tcb_total.alignment    = right_align
    tcb_total.border       = border
    tcb_total.number_format = '$#,##0.00'
    tcb_total.font         = Font(name="Calibri", bold=True, color="FFFFFF", size=10)

    # Total Market Value sum
    mv_total = ws.cell(
        row=totals_row, column=8,
        value=f"=IF(COUNTA(G{data_start_row}:G{last_data_row})=0,\"\","
              f"SUM(H{data_start_row}:H{last_data_row}))"
    )
    mv_total.font         = Font(name="Calibri", bold=True, color="FFFFFF", size=10)
    mv_total.fill         = navy_fill
    mv_total.alignment    = right_align
    mv_total.border       = border
    mv_total.number_format = '$#,##0.00'

    for col in range(1, 13):
        c = ws.cell(row=totals_row, column=col)
        c.fill   = navy_fill
        c.border = border
        if c.font.color.rgb != "FFFFFF":
            c.font = Font(name="Calibri", bold=True, color="FFFFFF", size=10)

    # ── Freeze panes & filters ───────────────────────────────
    ws.freeze_panes = "A4"
    ws.auto_filter.ref = f"A3:L{last_data_row}"

    # ── Entity summary sheet ─────────────────────────────────
    ws2 = wb.create_sheet("Entity Summary")
    ws2["A1"] = "Entity Summary"
    ws2["A1"].font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
    ws2["A1"].fill = navy_fill
    ws2["A1"].alignment = center_align
    ws2.merge_cells("A1:E1")
    ws2.row_dimensions[1].height = 24

    ent_hdrs = ["Entity", "# Positions", "Total Shares", "Total Cost Basis", "% of Portfolio"]
    for ci, h in enumerate(ent_hdrs, 1):
        c = ws2.cell(row=2, column=ci, value=h)
        c.font = subhdr_font
        c.fill = PatternFill("solid", fgColor="EBF0F5")
        c.alignment = center_align
        c.border = border
    ws2.column_dimensions["A"].width = 22
    ws2.column_dimensions["B"].width = 14
    ws2.column_dimensions["C"].width = 16
    ws2.column_dimensions["D"].width = 18
    ws2.column_dimensions["E"].width = 16

    entity_groups = positions.groupby("Entity").agg(
        positions_count=("Ticker", "count"),
        total_shares=("Total Shares", "sum"),
        total_cost=("Total Cost Basis", "sum"),
    ).reset_index()
    grand_total = entity_groups["total_cost"].sum()

    for ri, (_, eg) in enumerate(entity_groups.iterrows(), start=3):
        ws2.cell(row=ri, column=1, value=eg["Entity"]).border = border
        ws2.cell(row=ri, column=2, value=eg["positions_count"]).border = border
        c3 = ws2.cell(row=ri, column=3, value=eg["total_shares"])
        c3.number_format = "#,##0.0000"; c3.border = border
        c4 = ws2.cell(row=ri, column=4, value=eg["total_cost"])
        c4.number_format = "$#,##0.00";  c4.border = border
        c5 = ws2.cell(row=ri, column=5, value=eg["total_cost"] / grand_total if grand_total else 0)
        c5.number_format = "0.00%"; c5.border = border
        for ci in range(1, 6):
            ws2.cell(row=ri, column=ci).font = data_font

    wb.save(str(EXCEL_PATH))
    print(f"  Excel saved -> {EXCEL_PATH}")


# ══════════════════════════════════════════════════════════
# 5. MAIN
# ══════════════════════════════════════════════════════════

def main():
    print("\n" + "=" * 60)
    print("  WCCP Portfolio Analyzer")
    print("=" * 60)

    print("\n[1/4] Loading trade blotter …")
    blotter = load_blotter(BLOTTER_FILE)
    print(f"      {len(blotter)} trades loaded  "
          f"({blotter['Transaction Type'].value_counts().to_dict()})")

    print("\n[2/4] Running FIFO lot matching …")
    open_lots, realized_pl = fifo_match(blotter)
    print(f"      {len(open_lots)} open lots  |  {len(realized_pl)} matched sells")

    print("\n[3/4] Building position summary …")
    positions = build_positions(open_lots)
    print(f"      {len(positions)} open positions")
    print(f"      Total cost basis: ${positions['Total Cost Basis'].sum():,.2f}")

    if positions.empty:
        print("\n[WARN] No open positions found. Check your blotter data.")
        return

    print("\n[4/4] Generating Excel pricing file …")
    generate_excel(positions)

    print("\n" + "=" * 60)
    print("  Done!")
    print(f"  Excel  -> {EXCEL_PATH}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
