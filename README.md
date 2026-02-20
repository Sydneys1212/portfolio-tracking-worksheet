# WCCP Portfolio Tracker

Reads a trade blotter and produces:
- **PDF report** – summary, allocation charts, tax lot detail, entity breakdown
- **Excel pricing file** – open positions with Bloomberg price placeholders and auto-calculated P&L

---

## Folder structure

```
portfolio-tracking-worksheet/
├── analyze.py               ← main script
├── requirements.txt
├── data/
│   └── WCCP_Master_Trade_Blotter.xlsx   ← place your file here
├── reports/
│   └── portfolio_report_YYYY-MM-DD.pdf  ← generated output
└── output/
    └── daily_pricing_YYYY-MM-DD.xlsx    ← generated output
```

---

## Setup

### 1. Install Python (3.9+)
Download from https://python.org if not already installed.

### 2. Create and activate a virtual environment (recommended)

```bash
# Windows (Command Prompt or PowerShell)
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

## Place your trade blotter

Copy your file to:

```
data/WCCP_Master_Trade_Blotter.xlsx
```

The file must have a sheet named **"Trade Blotter"** with these columns:

| Column | Notes |
|---|---|
| Date | Trade date |
| Security | Full security name |
| Ticker | Bloomberg format, e.g. `LEU US Equity` |
| Type | Security type |
| Transaction Type | `Buy` or `Sell` |
| Quantity | Number of shares (positive for both buys and sells) |
| Price | Price per share |
| Fund Consideration | Total consideration |
| Currency | e.g. `USD` |
| Entity | e.g. `WOCAP II`, `TMP Personal` |
| AccountShortName | Short account label |

---

## Run

```bash
python analyze.py
```

On success you will see:

```
============================================================
  WCCP Portfolio Analyzer
============================================================

[1/5] Loading trade blotter …
[2/5] Running FIFO lot matching …
[3/5] Building position summary …
[4/5] Generating PDF report …
[5/5] Generating Excel pricing file …

============================================================
  Done!
  PDF    → reports/portfolio_report_2025-01-15.pdf
  Excel  → output/daily_pricing_2025-01-15.xlsx
============================================================
```

---

## PDF report contents

| Page | Content |
|---|---|
| 1 | Portfolio summary: total positions, cost basis, realized P&L |
| 2 | Allocation charts: pie by position, pie by entity, bar chart |
| 3 | Realized vs Unrealized P&L analysis |
| 4+ | Tax lot detail per ticker (one page per position) |
| Last | Entity breakdown pages |

---

## Excel pricing file

Open `output/daily_pricing_YYYY-MM-DD.xlsx`:

1. **Column G (Current Price)** — enter prices manually or paste a Bloomberg formula.
   Each cell has a comment with the exact formula:
   ```
   =BDP("LEU US Equity","PX_LAST")
   ```

2. **Columns H, J, K auto-calculate** once prices are entered:
   - **Market Value** = Shares × Current Price
   - **Unrealized P&L** = Market Value − Cost Basis
   - **% Return** = Unrealized P&L ÷ Cost Basis

3. The **Entity Summary** tab aggregates by entity automatically.

---

## FIFO lot matching

- Buys create new tax lots.
- Sells consume the oldest lots first (First In, First Out).
- Matching is done per **(Ticker, Entity)** pair — lots are not mixed across entities.
- Excess sells (sells without matching buys) are logged as warnings and skipped.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `[ERROR] Trade blotter not found` | Place your `.xlsx` in `data/` with the exact filename |
| `[ERROR] Missing columns` | Check column names in the blotter match the table above |
| `ModuleNotFoundError` | Run `pip install -r requirements.txt` |
| Pie chart shows nothing | All positions may have zero cost basis — check Price column |
