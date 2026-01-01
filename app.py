```python
# app.py
# Portfolio Management App (Streamlit)
#
# Included improvements:
# 1) Initial cash (USD) persists across app restarts (saved in SQLite settings table)
# 2) Portfolio Analysis charts:
#    - Portfolio value (USD) over time + optional end-point data labels
#    - Indexed comparison vs SPY + optional end-point data labels
# 3) Portfolio Analysis performer table (Top 3 / Worst 3) within selected window:
#    - Total PnL = Realized PnL within window + (Unrealized_end - Unrealized_start)
# 4) PnL method switch: FIFO / LIFO / AVG (Average Cost) for performer table
#
# Run:
#   python -m pip install streamlit yfinance pandas numpy plotly openpyxl
#   streamlit run app.py

import os
import io
import sqlite3
from io import BytesIO
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf


# =============================
# Config + Theme
# =============================
st.set_page_config(page_title="Portfolio Manager", layout="wide")

PRIMARY = "#911618"
PALETTE = {
    "primary": PRIMARY,
    "primary_dark": "#6E1012",
    "primary_light": "#B33B3D",
    "bg": "#FFFFFF",
    "panel": "#F7F7F9",
    "muted": "#5E6472",
    "border": "#E3E6EB",
    "good": "#1F9D55",
    "bad": "#D64545",
    "text": "#111318",
}

st.markdown(
    f"""
    <style>
      .stApp {{
        background: {PALETTE["bg"]};
        color: {PALETTE["text"]};
      }}
      .block-container {{
        padding-top: 1rem;
      }}
      [data-testid="stSidebar"] {{
        background: {PALETTE["panel"]};
        border-right: 1px solid {PALETTE["border"]};
      }}

      .pm-header {{
        display:flex; align-items:center; gap:12px;
        padding: 10px 14px;
        background:{PALETTE["bg"]};
        border:1px solid {PALETTE["border"]};
        border-radius: 14px;
      }}
      .pm-badge {{
        padding: 4px 10px;
        border-radius: 999px;
        background: {PALETTE["primary_dark"]};
        border: 1px solid {PALETTE["primary"]};
        color: #fff;
        font-size: 12px;
        letter-spacing: 0.5px;
      }}

      .kpi {{
        background:{PALETTE["bg"]};
        border:1px solid {PALETTE["border"]};
        border-radius: 14px;
        padding: 12px 14px;
      }}
      .kpi .label {{
        color:{PALETTE["muted"]};
        font-size: 12px;
        margin-bottom: 6px;
      }}
      .kpi .value {{
        font-size: 20px;
        font-weight: 700;
        color:{PALETTE["text"]};
      }}

      .stButton>button {{
        background: {PALETTE["primary"]};
        border: 1px solid {PALETTE["primary_dark"]};
        color: #fff;
        border-radius: 10px;
        padding: 0.55rem 0.9rem;
      }}
      .stButton>button:hover {{
        background: {PALETTE["primary_dark"]};
        border-color: {PALETTE["primary"]};
      }}

      .stTextInput>div>div>input, .stNumberInput>div>div>input, .stSelectbox>div>div,
      .stDateInput>div>div>input, textarea {{
        background: #FFFFFF;
        border: 1px solid {PALETTE["border"]};
        border-radius: 10px;
        color: {PALETTE["text"]};
      }}

      [data-testid="stDataFrame"] {{
        border: 1px solid {PALETTE["border"]};
        border-radius: 14px;
        overflow: hidden;
      }}

      .small-note {{
        color: {PALETTE["muted"]};
        font-size: 12px;
      }}
    </style>
    """,
    unsafe_allow_html=True,
)


# =============================
# Persistence (SQLite)
# =============================
DB_PATH = os.path.abspath(os.environ.get("PM_DB_PATH", "portfolio.db"))

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dt TEXT NOT NULL,
            ticker TEXT NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            amount INTEGER NOT NULL
        )
        """
    )
    conn.commit()
    return conn

CONN = get_conn()

def init_settings_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.commit()

def get_setting(conn, key: str, default=None):
    cur = conn.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = cur.fetchone()
    if row is None:
        return default
    return row[0]

def set_setting(conn, key: str, value):
    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )
    conn.commit()

init_settings_table(CONN)

def read_trades() -> pd.DataFrame:
    df = pd.read_sql_query("SELECT * FROM trades ORDER BY dt ASC, id ASC", CONN)
    if df.empty:
        return df

    df["dt"] = pd.to_datetime(df["dt"], format="mixed", errors="coerce")
    if df["dt"].isna().any():
        df.loc[df["dt"].isna(), "dt"] = pd.to_datetime(
            df.loc[df["dt"].isna(), "dt"], format="ISO8601", errors="coerce"
        )
    if df["dt"].isna().any():
        bad = df[df["dt"].isna()][["id", "dt"]].head(10)
        raise ValueError(f"Found unparseable dt values (showing up to 10):\n{bad}")

    df["date"] = df["dt"].dt.date
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").astype("Int64")
    return df

def insert_trade(exec_date: date, ticker: str, side: str, price: float, amount: int):
    dt_ = datetime.combine(exec_date, datetime.min.time())
    CONN.execute(
        "INSERT INTO trades (dt, ticker, side, price, amount) VALUES (?, ?, ?, ?, ?)",
        (dt_.isoformat(), ticker.upper().strip(), side.upper().strip(), float(price), int(amount)),
    )
    CONN.commit()

def update_trades(df: pd.DataFrame) -> int:
    """
    df columns: id, date, ticker, side, price, amount
    Returns number of rows updated.
    """
    cur = CONN.cursor()
    updated = 0
    for _, r in df.iterrows():
        d = pd.to_datetime(r["date"], format="mixed", errors="coerce")
        if pd.isna(d):
            raise ValueError(f"Bad date in edited rows: {r['date']}")
        d = d.date()
        dt_ = datetime.combine(d, datetime.min.time())

        cur.execute(
            """
            UPDATE trades
            SET dt=?, ticker=?, side=?, price=?, amount=?
            WHERE id=?
            """,
            (
                dt_.isoformat(),
                str(r["ticker"]).upper().strip(),
                str(r["side"]).upper().strip(),
                float(r["price"]),
                int(r["amount"]),
                int(r["id"]),
            ),
        )
        updated += cur.rowcount
    CONN.commit()
    return updated

def delete_trade_ids(ids):
    if not ids:
        return
    q = "DELETE FROM trades WHERE id IN ({})".format(",".join(["?"] * len(ids)))
    CONN.execute(q, [int(x) for x in ids])
    CONN.commit()

def clear_all_trades():
    CONN.execute("DELETE FROM trades")
    CONN.commit()


# =============================
# Excel workflow helpers
# =============================
def trades_to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="TradeLog")
        rules = pd.DataFrame(
            {
                "Field": ["ID", "Date", "Ticker", "Buy / Sell", "Price", "Amount"],
                "Rule": [
                    "ID optional for Replace/Append; required only for Update-by-ID",
                    "Date only (YYYY-MM-DD or Excel date)",
                    "Ticker (e.g., AAPL)",
                    "BUY or SELL",
                    "Numeric (e.g., 189.50)",
                    "Integer shares (e.g., 10)",
                ],
            }
        )
        rules.to_excel(writer, index=False, sheet_name="Rules")
    return output.getvalue()

def _normalize_excel_trade_df(uploaded_df: pd.DataFrame) -> pd.DataFrame:
    """
    Required columns:
      Date, Ticker, Buy / Sell, Price, Amount
    Optional:
      ID
    Returns clean df with: date,ticker,side,price,amount
    """
    df = uploaded_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    required = ["Date", "Ticker", "Buy / Sell", "Price", "Amount"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Uploaded Excel missing columns: {missing}. Found: {list(df.columns)}")

    df = df.rename(columns={
        "Date": "date",
        "Ticker": "ticker",
        "Buy / Sell": "side",
        "Price": "price",
        "Amount": "amount",
    })

    df["date"] = pd.to_datetime(df["date"], format="mixed", errors="coerce").dt.date
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").round()

    df = df.dropna(subset=["date", "ticker", "side", "price", "amount"])
    df["amount"] = df["amount"].astype(int)

    bad_side = ~df["side"].isin(["BUY", "SELL"])
    if bad_side.any():
        bad = df.loc[bad_side, ["side"]].drop_duplicates().head(10)
        raise ValueError(f"Invalid side values found (expect BUY/SELL). Examples:\n{bad}")

    if (df["amount"] <= 0).any():
        raise ValueError("Amount must be > 0 for all rows.")
    if (df["price"] < 0).any():
        raise ValueError("Price must be >= 0 for all rows.")

    return df[["date", "ticker", "side", "price", "amount"]]

def replace_db_with_excel(uploaded_df: pd.DataFrame) -> int:
    """
    Deletes all existing trades and inserts all Excel rows.
    """
    df = _normalize_excel_trade_df(uploaded_df)
    clear_all_trades()
    n = 0
    for _, r in df.iterrows():
        insert_trade(r["date"], r["ticker"], r["side"], float(r["price"]), int(r["amount"]))
        n += 1
    return n

def append_excel_to_db(uploaded_df: pd.DataFrame, skip_duplicates: bool = True) -> int:
    """
    Inserts Excel rows as NEW trades (ignores IDs). Optionally dedupes by (date,ticker,side,price,amount).
    """
    df = _normalize_excel_trade_df(uploaded_df)

    if skip_duplicates:
        existing = read_trades()
        if not existing.empty:
            ex = existing.copy()
            ex["date"] = ex["dt"].dt.date
            ex = ex[["date", "ticker", "side", "price", "amount"]]
            ex_set = set((r["date"], r["ticker"], r["side"], float(r["price"]), int(r["amount"])) for _, r in ex.iterrows())
            df = df[~df.apply(lambda r: (r["date"], r["ticker"], r["side"], float(r["price"]), int(r["amount"])) in ex_set, axis=1)]

    n = 0
    for _, r in df.iterrows():
        insert_trade(r["date"], r["ticker"], r["side"], float(r["price"]), int(r["amount"]))
        n += 1
    return n

def update_existing_by_id(uploaded_df: pd.DataFrame) -> int:
    """
    Optional: updates only rows whose ID exists in DB.
    Requires ID column.
    """
    if "ID" not in uploaded_df.columns:
        raise ValueError("To update-by-ID, Excel must include an 'ID' column.")

    df = uploaded_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    required = ["ID", "Date", "Ticker", "Buy / Sell", "Price", "Amount"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Uploaded Excel missing columns: {missing}. Found: {list(df.columns)}")

    df = df.rename(columns={
        "ID": "id",
        "Date": "date",
        "Ticker": "ticker",
        "Buy / Sell": "side",
        "Price": "price",
        "Amount": "amount",
    })

    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["date"] = pd.to_datetime(df["date"], format="mixed", errors="coerce").dt.date
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").round()

    df = df.dropna(subset=["id", "date", "ticker", "side", "price", "amount"])
    df["amount"] = df["amount"].astype(int)

    bad_side = ~df["side"].isin(["BUY", "SELL"])
    if bad_side.any():
        raise ValueError("Side must be BUY/SELL for all rows.")

    existing = read_trades()
    if existing.empty:
        raise ValueError("No trades in DB to update.")
    existing_ids = set(existing["id"].astype(int).tolist())

    df = df[df["id"].astype(int).isin(existing_ids)]
    if df.empty:
        raise ValueError("No matching IDs found to update.")

    return update_trades(df[["id", "date", "ticker", "side", "price", "amount"]])


# =============================
# yfinance helpers
# =============================
@st.cache_data(ttl=60)
def fetch_snapshot(ticker: str) -> dict:
    t = yf.Ticker(ticker)
    try:
        fi = getattr(t, "fast_info", None)
        last = None
        prev = None
        if fi is not None:
            last = fi.get("last_price")
            prev = fi.get("previous_close")

        if last is None or prev is None:
            h = t.history(period="5d", interval="1d", auto_adjust=True)
            if not h.empty:
                last = float(h["Close"].iloc[-1])
                prev = float(h["Close"].iloc[-2]) if len(h) >= 2 else float(h["Close"].iloc[-1])

        try:
            name = t.get_info().get("shortName", ticker)
        except Exception:
            name = ticker

        if last is None:
            last = np.nan
        if prev is None or prev == 0:
            chg = np.nan
        else:
            chg = (float(last) / float(prev) - 1.0) * 100.0

        return {
            "name": name,
            "last": float(last),
            "prev_close": float(prev) if prev is not None else np.nan,
            "change_pct": float(chg),
        }
    except Exception:
        return {"name": ticker, "last": np.nan, "prev_close": np.nan, "change_pct": np.nan}

@st.cache_data(ttl=3600)
def fetch_history(tickers, start, end):
    if not tickers:
        return pd.DataFrame()
    data = yf.download(
        tickers=tickers,
        start=start,
        end=end,
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="column",
        threads=True,
    )
    if data.empty:
        return pd.DataFrame()

    if isinstance(data.columns, pd.MultiIndex):
        close = data["Close"].copy() if "Close" in data.columns.get_level_values(0) else pd.DataFrame()
    else:
        close = data[["Close"]].rename(columns={"Close": tickers[0]}) if len(tickers) == 1 else pd.DataFrame()

    close = close.dropna(how="all")
    return close


# =============================
# Cash + holdings over time
# =============================
def cash_series(trades: pd.DataFrame, initial_cash: float) -> pd.Series:
    """
    Daily cash balance = initial_cash + cumulative cashflows
    cashflow:
      BUY  -> -amount*price
      SELL -> +amount*price
    """
    if trades.empty:
        return pd.Series(dtype=float)

    df = trades.copy()
    df["date"] = pd.to_datetime(df["dt"]).dt.date
    df["amount"] = df["amount"].astype(float)
    df["price"] = df["price"].astype(float)

    df["cashflow"] = np.where(df["side"] == "BUY", -df["amount"] * df["price"], df["amount"] * df["price"])
    daily = df.groupby("date")["cashflow"].sum().sort_index()

    start = min(daily.index)
    end = date.today()
    idx = pd.date_range(start=start, end=end, freq="D").date

    daily = daily.reindex(idx, fill_value=0.0)
    cash = initial_cash + daily.cumsum()
    cash.index = pd.to_datetime(cash.index)
    cash.name = "Cash"
    return cash

def holdings_value_series(trades: pd.DataFrame) -> pd.Series:
    """
    Market value of holdings over time (no cash).
    Uses yfinance adjusted close and cumulative shares.
    """
    if trades.empty:
        return pd.Series(dtype=float)

    df = trades.copy()
    df["date"] = pd.to_datetime(df["dt"]).dt.date
    df["ticker"] = df["ticker"].str.upper()
    df["signed_qty"] = np.where(df["side"] == "BUY", df["amount"].astype(float), -df["amount"].astype(float))

    start = min(df["date"])
    end = date.today() + timedelta(days=1)
    tickers = sorted(df["ticker"].unique().tolist())

    px = fetch_history(tickers, start=start, end=end)
    if px.empty:
        return pd.Series(dtype=float)

    for tk in tickers:
        if tk not in px.columns:
            px[tk] = np.nan
    px = px[tickers].ffill().dropna(how="all")
    idx_dates = px.index.date

    agg = df.groupby(["date", "ticker"])["signed_qty"].sum().unstack(fill_value=0.0)
    for tk in tickers:
        if tk not in agg.columns:
            agg[tk] = 0.0
    agg = agg[tickers]

    shares = agg.reindex(idx_dates, fill_value=0.0).cumsum()

    px_d = px.copy()
    px_d.index = px_d.index.date
    px_d = px_d.reindex(idx_dates).ffill()

    value = (shares * px_d).sum(axis=1)
    value.index = pd.to_datetime(value.index)
    value.name = "Holdings"
    return value

def portfolio_value_series(trades: pd.DataFrame, initial_cash: float) -> pd.Series:
    """
    Total portfolio value = cash + holdings value
    """
    if trades.empty:
        return pd.Series([initial_cash], index=[pd.to_datetime(date.today())], name="Portfolio")

    cash = cash_series(trades, initial_cash)
    hv = holdings_value_series(trades)

    if hv.empty:
        out = cash.copy()
        out.name = "Portfolio"
        return out

    idx = cash.index.union(hv.index)
    cash2 = cash.reindex(idx).ffill()
    hv2 = hv.reindex(idx).ffill().fillna(0.0)

    out = cash2 + hv2
    out.name = "Portfolio"
    return out


# =============================
# PnL methods + period performer table
# =============================
def _price_on_or_before(px: pd.Series, d: date) -> float:
    if px is None or px.empty:
        return np.nan
    s = px.copy()
    s.index = pd.to_datetime(s.index)
    s = s.sort_index()
    ts = pd.Timestamp(d)
    s = s[s.index <= ts]
    return float(s.iloc[-1]) if not s.empty else np.nan

@st.cache_data(ttl=1800)
def fetch_prices_window(tickers: list, start_d: date, end_d: date) -> pd.DataFrame:
    """
    Fetch prices with a small lookback so start price can be "on or before" start_d.
    yfinance end is exclusive, so use end_d+1.
    """
    if not tickers:
        return pd.DataFrame()
    lookback = start_d - timedelta(days=10)
    df = fetch_history(sorted(tickers), start=lookback, end=end_d + timedelta(days=1))
    return df

def compute_performance_by_ticker(
    trades: pd.DataFrame,
    start_d: date,
    end_d: date,
    method: str = "FIFO",
    prices: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Returns per-ticker performance within [start_d, end_d]:
      Realized PnL within window
      Unrealized change within window (Unreal_end - Unreal_start)
      Total PnL = Realized + Unrealized change

    Cost basis methods: FIFO / LIFO / AVG (Average cost)
    """
    if trades.empty:
        return pd.DataFrame(columns=[
            "Ticker", "Start Shares", "End Shares",
            "Start Price", "End Price",
            "Realized PnL", "Unrealized Δ", "Total PnL",
            "End MV"
        ])

    df = trades.copy()
    df["date"] = pd.to_datetime(df["dt"]).dt.date
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").astype(float)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").astype(float)
    df = df.sort_values(["dt", "id"])

    tickers = sorted(df["ticker"].unique().tolist())
    if prices is None:
        prices = fetch_prices_window(tickers, start_d, end_d)

    px_map = {}
    for tk in tickers:
        if isinstance(prices, pd.DataFrame) and tk in prices.columns:
            px_map[tk] = prices[tk].dropna()
        else:
            px_map[tk] = pd.Series(dtype=float)

    pre = df[df["date"] < start_d]
    win = df[(df["date"] >= start_d) & (df["date"] <= end_d)]

    state = {}
    for tk in tickers:
        state[tk] = {
            "realized": 0.0,
            "start_shares": 0.0,
            "end_shares": 0.0,
            "unreal_start": 0.0,
            "unreal_end": 0.0,
        }
        if method in ("FIFO", "LIFO"):
            state[tk]["lots"] = []  # list of [qty, cost]
        else:
            state[tk]["avg_shares"] = 0.0
            state[tk]["avg_cost"] = 0.0

    def apply_buy(tk: str, qty: float, px: float):
        if method in ("FIFO", "LIFO"):
            state[tk]["lots"].append([qty, px])
        else:
            sh = state[tk]["avg_shares"]
            ac = state[tk]["avg_cost"]
            new_sh = sh + qty
            new_ac = (sh * ac + qty * px) / new_sh if new_sh > 1e-12 else 0.0
            state[tk]["avg_shares"] = new_sh
            state[tk]["avg_cost"] = new_ac

    def apply_sell(tk: str, qty: float, px: float):
        if qty <= 0:
            return
        if method in ("FIFO", "LIFO"):
            lots = state[tk]["lots"]
            q = qty
            idx_fn = (lambda: 0) if method == "FIFO" else (lambda: -1)

            while q > 1e-12 and lots:
                i = idx_fn()
                lot_qty, lot_cost = lots[i]
                take = min(lot_qty, q)
                state[tk]["realized"] += (px - lot_cost) * take
                lot_qty -= take
                q -= take
                if lot_qty <= 1e-12:
                    lots.pop(i)
                else:
                    lots[i][0] = lot_qty
            # If selling more than held, extra is ignored (no shorts in this model)
        else:
            sh = state[tk]["avg_shares"]
            ac = state[tk]["avg_cost"]
            take = min(sh, qty)  # prevent going short
            state[tk]["realized"] += (px - ac) * take
            sh -= take
            if sh <= 1e-12:
                sh = 0.0
                ac = 0.0
            state[tk]["avg_shares"] = sh
            state[tk]["avg_cost"] = ac

    def current_shares_and_unreal(tk: str, price_for_unreal: float) -> tuple[float, float]:
        if method in ("FIFO", "LIFO"):
            lots = state[tk]["lots"]
            sh = float(sum(q for q, _ in lots))
            if not np.isfinite(price_for_unreal):
                return sh, np.nan
            unreal = float(sum(q * (price_for_unreal - c) for q, c in lots))
            return sh, unreal
        else:
            sh = float(state[tk]["avg_shares"])
            ac = float(state[tk]["avg_cost"])
            if not np.isfinite(price_for_unreal) or sh <= 1e-12:
                return sh, 0.0 if sh <= 1e-12 else np.nan
            unreal = sh * (price_for_unreal - ac)
            return sh, float(unreal)

    # 1) Build inventory as-of start_d (apply pre-window trades)
    for _, r in pre.iterrows():
        tk = r["ticker"]
        qty = float(r["amount"])
        px = float(r["price"])
        if r["side"] == "BUY":
            apply_buy(tk, qty, px)
        else:
            apply_sell(tk, qty, px)

    # 2) Start snapshot
    for tk in tickers:
        p0 = _price_on_or_before(px_map[tk], start_d)
        sh0, u0 = current_shares_and_unreal(tk, p0)
        state[tk]["start_shares"] = sh0
        state[tk]["unreal_start"] = u0

    # 3) Apply in-window trades (realized happens here)
    for _, r in win.iterrows():
        tk = r["ticker"]
        qty = float(r["amount"])
        px = float(r["price"])
        if r["side"] == "BUY":
            apply_buy(tk, qty, px)
        else:
            apply_sell(tk, qty, px)

    # 4) End snapshot + assemble
    rows = []
    for tk in tickers:
        p0 = _price_on_or_before(px_map[tk], start_d)
        p1 = _price_on_or_before(px_map[tk], end_d)
        sh1, u1 = current_shares_and_unreal(tk, p1)
        state[tk]["end_shares"] = sh1
        state[tk]["unreal_end"] = u1

        realized = float(state[tk]["realized"])
        u_start = float(state[tk]["unreal_start"]) if np.isfinite(state[tk]["unreal_start"]) else np.nan
        u_end = float(state[tk]["unreal_end"]) if np.isfinite(state[tk]["unreal_end"]) else np.nan

        unreal_delta = (u_end - u_start) if (np.isfinite(u_end) and np.isfinite(u_start)) else np.nan
        total = realized + unreal_delta if np.isfinite(unreal_delta) else np.nan
        end_mv = (sh1 * p1) if (np.isfinite(p1)) else np.nan

        has_any = (
            (state[tk]["start_shares"] > 1e-12) or
            (state[tk]["end_shares"] > 1e-12) or
            (not win[win["ticker"] == tk].empty)
        )
        if not has_any:
            continue

        rows.append({
            "Ticker": tk,
            "Start Shares": float(state[tk]["start_shares"]),
            "End Shares": float(state[tk]["end_shares"]),
            "Start Price": float(p0) if np.isfinite(p0) else np.nan,
            "End Price": float(p1) if np.isfinite(p1) else np.nan,
            "Realized PnL": realized,
            "Unrealized Δ": float(unreal_delta) if np.isfinite(unreal_delta) else np.nan,
            "Total PnL": float(total) if np.isfinite(total) else np.nan,
            "End MV": float(end_mv) if np.isfinite(end_mv) else np.nan,
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out = out.sort_values("Total PnL", ascending=False).reset_index(drop=True)
    return out


# =============================
# Positions computation (includes CASH row)
# =============================
def compute_positions(trades: pd.DataFrame, initial_cash: float) -> pd.DataFrame:
    cols = [
        "Ticker", "Stock name", "Today's change (%)", "Latest price", "Amount held",
        "Total unrealized P&L", "Total unrealized P&L (%)", "Average cost", "% in portfolio"
    ]
    if trades.empty:
        return pd.DataFrame([{
            "Ticker": "CASH",
            "Stock name": "Cash (USD)",
            "Today's change (%)": 0.0,
            "Latest price": 1.0,
            "Amount held": float(initial_cash),
            "Total unrealized P&L": 0.0,
            "Total unrealized P&L (%)": 0.0,
            "Average cost": np.nan,
            "% in portfolio": 100.0,
        }], columns=cols)

    df = trades.copy()
    df["ticker"] = df["ticker"].str.upper()
    df["signed_qty"] = np.where(df["side"] == "BUY", df["amount"].astype(float), -df["amount"].astype(float))
    df = df.sort_values(["ticker", "dt", "id"])

    # Positions tab still uses simple moving average for display (separate from performer table method)
    state = {}
    for _, r in df.iterrows():
        tk = r["ticker"]
        qty = float(r["signed_qty"])
        px = float(r["price"])
        if tk not in state:
            state[tk] = {"shares": 0.0, "avg_cost": 0.0}

        s = state[tk]["shares"]
        ac = state[tk]["avg_cost"]

        if qty > 0:
            new_shares = s + qty
            new_avg = (s * ac + qty * px) / new_shares if new_shares else 0.0
            state[tk]["shares"] = new_shares
            state[tk]["avg_cost"] = new_avg
        else:
            state[tk]["shares"] = s + qty
            if abs(state[tk]["shares"]) < 1e-9:
                state[tk]["shares"] = 0.0
                state[tk]["avg_cost"] = 0.0

    rows = []
    for tk, v in state.items():
        if v["shares"] == 0:
            continue
        snap = fetch_snapshot(tk)
        last = snap["last"]
        ac = v["avg_cost"] if v["avg_cost"] != 0 else np.nan
        sh = v["shares"]

        unreal = (last - ac) * sh if np.isfinite(last) and np.isfinite(ac) else np.nan
        unreal_pct = ((last / ac) - 1.0) * 100.0 if np.isfinite(last) and np.isfinite(ac) and ac != 0 else np.nan
        mv = last * sh if np.isfinite(last) else np.nan

        rows.append({
            "Ticker": tk,
            "Stock name": snap["name"],
            "Today's change (%)": snap["change_pct"],
            "Latest price": last,
            "Amount held": sh,
            "Total unrealized P&L": unreal,
            "Total unrealized P&L (%)": unreal_pct,
            "Average cost": ac,
            "_mv": mv,
        })

    pos = pd.DataFrame(rows)

    cash_now = cash_series(trades, initial_cash).iloc[-1]
    cash_row = {
        "Ticker": "CASH",
        "Stock name": "Cash (USD)",
        "Today's change (%)": 0.0,
        "Latest price": 1.0,
        "Amount held": float(cash_now),
        "Total unrealized P&L": 0.0,
        "Total unrealized P&L (%)": 0.0,
        "Average cost": np.nan,
        "_mv": float(cash_now),
    }

    if pos.empty:
        pos = pd.DataFrame([cash_row])
    else:
        pos = pd.concat([pos, pd.DataFrame([cash_row])], ignore_index=True)

    total_mv = pos["_mv"].sum(skipna=True)
    pos["% in portfolio"] = (pos["_mv"] / total_mv * 100.0) if total_mv and np.isfinite(total_mv) else np.nan
    pos = pos.drop(columns=["_mv"]).sort_values("% in portfolio", ascending=False).reset_index(drop=True)

    return pos[cols]


# =============================
# Paste import helpers (BUY/SELL tables)
# =============================
def _clean_price(x) -> float:
    s = str(x).strip().replace(",", "").replace("$", "")
    return float(s)

def _clean_amount_to_int(x) -> int:
    s = str(x).strip().replace(",", "")
    return int(round(float(s)))

def _parse_date_with_default_year(x, default_year: int) -> date:
    s = str(x).strip()
    if not s:
        return None
    if "/" in s and len(s.split("/")) == 2:
        s = f"{s}/{default_year}"
    dt = pd.to_datetime(s, format="mixed", errors="coerce")
    return None if pd.isna(dt) else dt.date()

def import_pasted_orders(pasted_text: str, side: str, skip_duplicates: bool = True, default_year: int = 2025) -> int:
    if not pasted_text.strip():
        return 0

    df_in = pd.read_csv(io.StringIO(pasted_text.strip()), sep="\t")
    df_in.columns = [str(c).strip() for c in df_in.columns]

    date_col = next((c for c in df_in.columns if c.lower() == "date"), None)
    ticker_col = next((c for c in df_in.columns if c.lower() in ["ticker", "symbol"]), None)
    price_col = next((c for c in df_in.columns if c.lower() == "price"), None)
    amount_col = next((c for c in df_in.columns if c.lower() in ["# of shares", "# of share", "shares", "amount", "qty", "quantity"]), None)

    if not all([date_col, ticker_col, price_col, amount_col]):
        raise ValueError(f"Cannot detect required columns. Found columns: {list(df_in.columns)}")

    df = df_in[[date_col, ticker_col, price_col, amount_col]].copy()
    df.columns = ["date", "ticker", "price", "amount"]

    df["date"] = df["date"].apply(lambda x: _parse_date_with_default_year(x, default_year))
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["price"] = df["price"].apply(_clean_price)
    df["amount"] = df["amount"].apply(_clean_amount_to_int)

    df = df.dropna(subset=["date", "ticker", "price", "amount"])
    df = df[df["ticker"] != ""]
    df = df[df["amount"] > 0]
    df = df[df["price"] >= 0]

    if skip_duplicates:
        existing = read_trades()
        if not existing.empty:
            ex = existing.copy()
            ex["date"] = ex["dt"].dt.date
            ex = ex[["date", "ticker", "side", "price", "amount"]]
            ex_set = set((r["date"], r["ticker"], r["side"], float(r["price"]), int(r["amount"])) for _, r in ex.iterrows())
            df = df[~df.apply(lambda r: (r["date"], r["ticker"], side, float(r["price"]), int(r["amount"])) in ex_set, axis=1)]

    n = 0
    for _, r in df.iterrows():
        insert_trade(r["date"], r["ticker"], side, float(r["price"]), int(r["amount"]))
        n += 1
    return n


# =============================
# Header + logo
# =============================
logo_path = r"C:\Users\asus\Downloads\logo.png"

c1, c2 = st.columns([2, 8], vertical_alignment="center")
with c1:
    if os.path.exists(logo_path):
        st.image(logo_path, use_container_width=True)
    else:
        st.markdown(f"<div class='small-note'>logo not found:<br>{logo_path}</div>", unsafe_allow_html=True)

with c2:
    st.markdown(
        f"""
        <div class="pm-header">
          <div class="pm-badge">PM TERMINAL</div>
          <div style="font-size:18px; font-weight:800; color:{PALETTE["text"]};">Portfolio Management</div>
          <div style="color:{PALETTE["muted"]}; margin-left:auto; font-size:12px;">DB: {DB_PATH}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.write("")


# =============================
# Sidebar — Settings (persisted)
# =============================
st.sidebar.markdown("### Settings")

def _load_initial_cash_default() -> float:
    saved = get_setting(CONN, "initial_cash", "30000.0")
    try:
        return float(saved)
    except Exception:
        return 30000.0

if "initial_cash_value" not in st.session_state:
    st.session_state["initial_cash_value"] = _load_initial_cash_default()

def _on_initial_cash_change():
    val = float(st.session_state["initial_cash_value"])
    set_setting(CONN, "initial_cash", val)

initial_cash = st.sidebar.number_input(
    "Initial cash (USD)",
    min_value=0.0,
    value=float(st.session_state["initial_cash_value"]),
    step=1000.0,
    format="%.2f",
    key="initial_cash_value",
    on_change=_on_initial_cash_change,
)

st.sidebar.markdown("<div class='small-note'>This value is saved in your portfolio.db and will persist after restart.</div>", unsafe_allow_html=True)

st.sidebar.divider()

# =============================
# Sidebar — Manual Trade Input
# =============================
st.sidebar.markdown("### Trade Input (Manual)")
ticker = st.sidebar.text_input("Ticker", value="AAPL")
side = st.sidebar.selectbox("Buy / Sell", options=["BUY", "SELL"], index=0)
amount = st.sidebar.number_input("Amount (shares)", min_value=0, step=1, value=10)
price = st.sidebar.number_input("Price (per share)", min_value=0.0, step=0.01, value=0.0, format="%.2f")
exec_date = st.sidebar.date_input("Date executed", value=date.today())

if st.sidebar.button("Add Trade", use_container_width=True):
    if not ticker.strip():
        st.error("Ticker cannot be empty.")
    elif amount <= 0:
        st.error("Amount must be a positive integer.")
    else:
        insert_trade(exec_date, ticker, side, float(price), int(amount))
        st.success(f"Saved: {ticker.upper()} {side} {amount} @ {price:.2f} on {exec_date.strftime('%Y-%m-%d')}")
        st.cache_data.clear()
        st.rerun()

st.sidebar.divider()

# =============================
# Sidebar — Import (Paste BUY & SELL tables)
# =============================
st.sidebar.markdown("### Import Trades (Paste)")
default_year = st.sidebar.number_input("Default year for dates like '11/20'", min_value=2000, max_value=2100, value=2025, step=1)
dedupe = st.sidebar.checkbox("Skip duplicates", value=True)

buy_text = st.sidebar.text_area("Paste BUY orders (tab-separated)", height=140)
sell_text = st.sidebar.text_area("Paste SELL orders (tab-separated)", height=140)

colA, colB = st.sidebar.columns(2)
with colA:
    if st.sidebar.button("Import BUY", use_container_width=True):
        n = import_pasted_orders(buy_text, side="BUY", skip_duplicates=dedupe, default_year=int(default_year))
        st.sidebar.success(f"Imported {n} BUY trade(s).")
        st.cache_data.clear()
        st.rerun()

with colB:
    if st.sidebar.button("Import SELL", use_container_width=True):
        n = import_pasted_orders(sell_text, side="SELL", skip_duplicates=dedupe, default_year=int(default_year))
        st.sidebar.success(f"Imported {n} SELL trade(s).")
        st.cache_data.clear()
        st.rerun()

if st.sidebar.button("Import BOTH (BUY + SELL)", use_container_width=True):
    n1 = import_pasted_orders(buy_text, side="BUY", skip_duplicates=dedupe, default_year=int(default_year))
    n2 = import_pasted_orders(sell_text, side="SELL", skip_duplicates=dedupe, default_year=int(default_year))
    st.sidebar.success(f"Imported {n1} BUY + {n2} SELL trade(s).")
    st.cache_data.clear()
    st.rerun()


# =============================
# Tabs
# =============================
tab_pos, tab_log, tab_ana = st.tabs(["Positions", "Trade Log", "Portfolio Analysis"])
trades_df = read_trades()


# =============================
# Positions Tab
# =============================
with tab_pos:
    pos = compute_positions(trades_df, initial_cash)

    if not pos.empty:
        cash_now = float(pos.loc[pos["Ticker"] == "CASH", "Amount held"].iloc[0]) if (pos["Ticker"] == "CASH").any() else 0.0
        holdings_mv = float((pos[pos["Ticker"] != "CASH"]["Latest price"] * pos[pos["Ticker"] != "CASH"]["Amount held"]).sum(skipna=True)) if (pos["Ticker"] != "CASH").any() else 0.0
        total_mv = cash_now + holdings_mv
        total_unreal = float(pos[pos["Ticker"] != "CASH"]["Total unrealized P&L"].sum(skipna=True)) if (pos["Ticker"] != "CASH").any() else 0.0

        k1, k2, k3, k4 = st.columns(4)
        k1.markdown(f"<div class='kpi'><div class='label'>Portfolio Value</div><div class='value'>{total_mv:,.2f}</div></div>", unsafe_allow_html=True)
        k2.markdown(f"<div class='kpi'><div class='label'>Cash</div><div class='value'>{cash_now:,.2f}</div></div>", unsafe_allow_html=True)
        k3.markdown(f"<div class='kpi'><div class='label'>Holdings MV</div><div class='value'>{holdings_mv:,.2f}</div></div>", unsafe_allow_html=True)
        k4.markdown(f"<div class='kpi'><div class='label'>Unrealized P&L (Holdings)</div><div class='value'>{total_unreal:,.2f}</div></div>", unsafe_allow_html=True)
        st.write("")

    def pnl_color(val):
        if pd.isna(val):
            return ""
        return f"color: {PALETTE['good']}; font-weight: 600;" if val >= 0 else f"color: {PALETTE['bad']}; font-weight: 600;"

    if pos.empty:
        st.info("No positions yet. Add/import trades on the left.")
    else:
        styled = pos.style.format({
            "Today's change (%)": "{:.2f}",
            "Latest price": "{:.2f}",
            "Amount held": "{:,.0f}",
            "Total unrealized P&L": "{:,.2f}",
            "Total unrealized P&L (%)": "{:.2f}",
            "Average cost": "{:.2f}",
            "% in portfolio": "{:.2f}",
        }).applymap(pnl_color, subset=["Today's change (%)", "Total unrealized P&L", "Total unrealized P&L (%)"])
        st.dataframe(styled, use_container_width=True, height=540)


# =============================
# Trade Log Tab — Excel + In-app editor
# =============================
with tab_log:
    st.markdown("#### Trade Log")
    trades_df = read_trades()

    st.markdown("#### Excel: Replace / Append / Update")

    export_view = trades_df.copy()
    if not export_view.empty:
        export_view = export_view.rename(columns={
            "id": "ID",
            "date": "Date",
            "ticker": "Ticker",
            "side": "Buy / Sell",
            "price": "Price",
            "amount": "Amount",
        })[["ID", "Date", "Ticker", "Buy / Sell", "Price", "Amount"]]
    else:
        export_view = pd.DataFrame(columns=["ID", "Date", "Ticker", "Buy / Sell", "Price", "Amount"])

    try:
        excel_bytes = trades_to_excel_bytes(export_view)
        st.download_button(
            label="Download Trade Log (Excel)",
            data=excel_bytes,
            file_name="trade_log.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    except Exception as e:
        st.error(f"Excel export failed (install openpyxl): {e}")

    uploaded_xlsx = st.file_uploader("Upload edited Trade Log (Excel)", type=["xlsx"])

    if uploaded_xlsx is not None:
        try:
            xl = pd.ExcelFile(uploaded_xlsx)
            sheet = "TradeLog" if "TradeLog" in xl.sheet_names else xl.sheet_names[0]
            uploaded_df = pd.read_excel(uploaded_xlsx, sheet_name=sheet)

            st.caption(f"Loaded sheet: {sheet} | DB: {DB_PATH}")
            st.write("Preview (uploaded):")
            st.dataframe(uploaded_df.head(30), use_container_width=True)

            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("Replace DB with Excel", use_container_width=True, key="btn_replace_excel"):
                    n = replace_db_with_excel(uploaded_df)
                    st.success(f"Replaced DB. Inserted {n} trade(s).")
                    st.cache_data.clear()
                    st.rerun()
            with c2:
                if st.button("Append Excel rows (insert)", use_container_width=True, key="btn_append_excel"):
                    n = append_excel_to_db(uploaded_df, skip_duplicates=True)
                    st.success(f"Inserted {n} new trade(s).")
                    st.cache_data.clear()
                    st.rerun()
            with c3:
                if st.button("Update existing by ID", use_container_width=True, key="btn_update_excel"):
                    n = update_existing_by_id(uploaded_df)
                    st.success(f"Updated {n} row(s) by ID.")
                    st.cache_data.clear()
                    st.rerun()

        except Exception as e:
            st.error(f"Excel action failed: {e}")

    st.divider()

    trades_df = read_trades()
    if trades_df.empty:
        st.info("No trades in database yet.")
    else:
        view = trades_df.rename(columns={
            "id": "ID",
            "date": "Date",
            "ticker": "Ticker",
            "side": "Buy / Sell",
            "price": "Price",
            "amount": "Amount",
        })[["ID", "Date", "Ticker", "Buy / Sell", "Price", "Amount"]]

        st.markdown("<div class='small-note'>Edit fields in-table and click “Save edits”.</div>", unsafe_allow_html=True)

        edited = st.data_editor(
            view,
            use_container_width=True,
            num_rows="fixed",
            key="trade_editor",
            column_config={
                "ID": st.column_config.NumberColumn(disabled=True),
                "Date": st.column_config.DateColumn(),
                "Ticker": st.column_config.TextColumn(),
                "Buy / Sell": st.column_config.SelectboxColumn(options=["BUY", "SELL"]),
                "Price": st.column_config.NumberColumn(step=0.01),
                "Amount": st.column_config.NumberColumn(step=1),
            },
        )

        csave, cdel = st.columns([1, 2])
        with csave:
            if st.button("Save edits", key="btn_save_edits"):
                to_save = edited.rename(columns={
                    "ID": "id",
                    "Date": "date",
                    "Ticker": "ticker",
                    "Buy / Sell": "side",
                    "Price": "price",
                    "Amount": "amount",
                }).copy()

                if (to_save["ticker"].astype(str).str.strip() == "").any():
                    st.error("Ticker cannot be empty.")
                elif (pd.to_numeric(to_save["amount"], errors="coerce") <= 0).any():
                    st.error("Amount must be > 0 for all rows.")
                else:
                    to_save["amount"] = pd.to_numeric(to_save["amount"], errors="coerce").round().astype(int)
                    n = update_trades(to_save)
                    st.success(f"Edits saved. Rows updated: {n}")
                    st.cache_data.clear()
                    st.rerun()

        with cdel:
            ids = edited["ID"].tolist()
            sel = st.multiselect("Select trades to delete (by ID)", options=ids)
            if st.button("Delete selected", type="secondary", key="btn_delete_selected"):
                delete_trade_ids(sel)
                st.success(f"Deleted {len(sel)} trade(s).")
                st.cache_data.clear()
                st.rerun()


# =============================
# Portfolio Analysis Tab
# =============================
with tab_ana:
    st.markdown("#### Portfolio Analysis")

    trades_df = read_trades()

    # Controls
    cctl1, cctl2, _ = st.columns([2, 2, 6], vertical_alignment="center")
    with cctl1:
        pnl_method = st.selectbox(
            "PnL method (for Top/Worst table)",
            ["FIFO", "LIFO", "AVG"],
            index=0,
            help="Used to compute realized PnL from sells + unrealized change within the selected window."
        )
    with cctl2:
        show_labels = st.checkbox("Show data labels (end points)", value=True)

    if trades_df.empty:
        pv_const = pd.Series([initial_cash], index=[pd.to_datetime(date.today())], name="Portfolio")
        st.info("No trades yet. Showing initial cash only.")
        figv = go.Figure()
        figv.add_trace(go.Scatter(
            x=pv_const.index, y=pv_const.values, mode="lines",
            name="Portfolio Value", line=dict(width=3, color=PRIMARY)
        ))
        if show_labels:
            figv.add_trace(go.Scatter(
                x=[pv_const.index[-1]], y=[pv_const.values[-1]],
                mode="markers+text", text=[f"{pv_const.values[-1]:,.0f}"],
                textposition="top right",
                showlegend=False
            ))
        figv.update_layout(template="plotly_white", height=420, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(figv, use_container_width=True)
    else:
        pv_all = portfolio_value_series(trades_df, initial_cash)
        cash_all = cash_series(trades_df, initial_cash)
        hv_all = holdings_value_series(trades_df)

        if pv_all.empty or pv_all.dropna().empty:
            st.warning("Could not compute portfolio series (missing price history from yfinance).")
        else:
            min_d = pv_all.index.min().date()
            max_d = pv_all.index.max().date()

            cL, _ = st.columns([3, 7], vertical_alignment="center")
            with cL:
                start_end = st.date_input(
                    "View window (start → end)",
                    value=(min_d, max_d),
                    min_value=min_d,
                    max_value=max_d,
                )

            if isinstance(start_end, tuple) and len(start_end) == 2:
                start_d, end_d = start_end
            else:
                start_d, end_d = min_d, max_d

            if start_d > end_d:
                st.error("Start date cannot be after end date.")
                st.stop()

            pv = pv_all[(pv_all.index.date >= start_d) & (pv_all.index.date <= end_d)].dropna()
            if pv.empty or pv.size < 2:
                st.warning("Not enough data in the selected window to compute performance.")
                st.stop()

            # KPIs
            win_return = (pv.iloc[-1] / pv.iloc[0] - 1.0) * 100.0

            def compute_period_returns(value: pd.Series) -> dict:
                value = value.dropna()
                end = value.index.max()

                def ret_since(dt_from):
                    s = value[value.index >= dt_from]
                    if len(s) < 2:
                        return np.nan
                    return (s.iloc[-1] / s.iloc[0] - 1.0) * 100.0

                out = {
                    "1M": ret_since(end - pd.Timedelta(days=30)),
                    "2M": ret_since(end - pd.Timedelta(days=60)),
                    "3M": ret_since(end - pd.Timedelta(days=90)),
                    "6M": ret_since(end - pd.Timedelta(days=180)),
                }
                ytd_start = pd.Timestamp(year=end.year, month=1, day=1)
                out["YTD"] = ret_since(ytd_start)
                return out

            rets = compute_period_returns(pv_all)

            k1, k2, k3, k4, k5, k6 = st.columns(6)
            def kpi(col, label, val):
                txt = "—" if pd.isna(val) else f"{val:.2f}%"
                col.markdown(f"<div class='kpi'><div class='label'>{label}</div><div class='value'>{txt}</div></div>", unsafe_allow_html=True)

            kpi(k1, "Selected Window", win_return)
            kpi(k2, "Return (1M)", rets["1M"])
            kpi(k3, "Return (2M)", rets["2M"])
            kpi(k4, "Return (3M)", rets["3M"])
            kpi(k5, "Return (6M)", rets["6M"])
            kpi(k6, "Return (YTD)", rets["YTD"])

            st.write("")

            # Chart 1: Portfolio value (USD)
            fig_val = go.Figure()
            fig_val.add_trace(go.Scatter(
                x=pv.index, y=pv.values, mode="lines",
                name="Portfolio Value (USD)", line=dict(width=3, color=PRIMARY)
            ))

            cwin = cash_all[(cash_all.index.date >= start_d) & (cash_all.index.date <= end_d)].reindex(pv.index).ffill()
            hwin = hv_all[(hv_all.index.date >= start_d) & (hv_all.index.date <= end_d)].reindex(pv.index).ffill() if not hv_all.empty else None

            fig_val.add_trace(go.Scatter(
                x=cwin.index, y=cwin.values, mode="lines",
                name="Cash (USD)", line=dict(width=2, dash="dot", color="#6B7280")
            ))
            if hwin is not None and not hwin.empty:
                fig_val.add_trace(go.Scatter(
                    x=hwin.index, y=hwin.values, mode="lines",
                    name="Holdings MV (USD)", line=dict(width=2, dash="dash", color="#9CA3AF")
                ))

            if show_labels:
                fig_val.add_trace(go.Scatter(
                    x=[pv.index[-1]], y=[pv.values[-1]],
                    mode="markers+text",
                    text=[f"{pv.values[-1]:,.0f}"],
                    textposition="top right",
                    showlegend=False
                ))
                fig_val.add_trace(go.Scatter(
                    x=[cwin.index[-1]], y=[cwin.values[-1]],
                    mode="markers+text",
                    text=[f"{cwin.values[-1]:,.0f}"],
                    textposition="bottom right",
                    showlegend=False
                ))
                if hwin is not None and not hwin.empty:
                    fig_val.add_trace(go.Scatter(
                        x=[hwin.index[-1]], y=[hwin.values[-1]],
                        mode="markers+text",
                        text=[f"{hwin.values[-1]:,.0f}"],
                        textposition="middle right",
                        showlegend=False
                    ))

            fig_val.update_layout(
                template="plotly_white",
                height=420,
                margin=dict(l=10, r=10, t=10, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                xaxis=dict(showgrid=False),
                yaxis=dict(title="USD", gridcolor="rgba(0,0,0,0.08)"),
            )
            st.plotly_chart(fig_val, use_container_width=True)

            # Chart 2: Indexed comparison vs SPY
            spy_px = fetch_history(["SPY"], start=start_d, end=end_d + timedelta(days=1))
            spy = spy_px["SPY"].copy() if isinstance(spy_px, pd.DataFrame) and "SPY" in spy_px.columns else pd.Series(dtype=float)
            if not spy.empty:
                spy.index = pd.to_datetime(spy.index)

            pv_norm = (pv / pv.iloc[0]) * 100.0
            spy_norm = None
            if not spy.empty and spy.dropna().size > 2:
                spy = spy.reindex(pv_norm.index).ffill().dropna()
                if not spy.empty:
                    spy_norm = (spy / spy.iloc[0]) * 100.0

            fig_idx = go.Figure()
            fig_idx.add_trace(go.Scatter(
                x=pv_norm.index, y=pv_norm.values,
                mode="lines", name="Portfolio (Indexed)",
                line=dict(width=3, color=PRIMARY)
            ))
            if spy_norm is not None and not spy_norm.empty:
                fig_idx.add_trace(go.Scatter(
                    x=spy_norm.index, y=spy_norm.values,
                    mode="lines", name="S&P 500 (SPY, Indexed)",
                    line=dict(width=2, dash="dot", color="#6B7280")
                ))

            if show_labels:
                fig_idx.add_trace(go.Scatter(
                    x=[pv_norm.index[-1]], y=[pv_norm.values[-1]],
                    mode="markers+text",
                    text=[f"{pv_norm.values[-1]:.1f}"],
                    textposition="top right",
                    showlegend=False
                ))
                if spy_norm is not None and not spy_norm.empty:
                    fig_idx.add_trace(go.Scatter(
                        x=[spy_norm.index[-1]], y=[spy_norm.values[-1]],
                        mode="markers+text",
                        text=[f"{spy_norm.values[-1]:.1f}"],
                        textposition="bottom right",
                        showlegend=False
                    ))

            fig_idx.update_layout(
                template="plotly_white",
                height=520,
                margin=dict(l=10, r=10, t=10, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                xaxis=dict(showgrid=False),
                yaxis=dict(title="Indexed (Start = 100)", gridcolor="rgba(0,0,0,0.08)"),
            )
            st.plotly_chart(fig_idx, use_container_width=True)

            # Top / Worst performers
            st.markdown("#### Top / Worst Performers (within selected window)")

            tickers = sorted(trades_df["ticker"].astype(str).str.upper().unique().tolist())
            prices_window = fetch_prices_window(tickers, start_d, end_d)

            perf = compute_performance_by_ticker(
                trades=trades_df,
                start_d=start_d,
                end_d=end_d,
                method=pnl_method,
                prices=prices_window,
            )

            if perf is None or perf.empty:
                st.info("No ticker-level performance available for the selected window.")
            else:
                top3 = perf.sort_values("Total PnL", ascending=False).head(3).copy()
                bot3 = perf.sort_values("Total PnL", ascending=True).head(3).copy()

                def fmt_perf(df_):
                    df_ = df_.copy()
                    return df_[["Ticker", "Realized PnL", "Unrealized Δ", "Total PnL", "Start Shares", "End Shares", "End MV"]]

                cA, cB = st.columns(2)
                with cA:
                    st.markdown("**Top 3**")
                    st.dataframe(
                        fmt_perf(top3).style.format({
                            "Realized PnL": "{:,.2f}",
                            "Unrealized Δ": "{:,.2f}",
                            "Total PnL": "{:,.2f}",
                            "Start Shares": "{:,.0f}",
                            "End Shares": "{:,.0f}",
                            "End MV": "{:,.2f}",
                        }),
                        use_container_width=True
                    )
                with cB:
                    st.markdown("**Worst 3**")
                    st.dataframe(
                        fmt_perf(bot3).style.format({
                            "Realized PnL": "{:,.2f}",
                            "Unrealized Δ": "{:,.2f}",
                            "Total PnL": "{:,.2f}",
                            "Start Shares": "{:,.0f}",
                            "End Shares": "{:,.0f}",
                            "End MV": "{:,.2f}",
                        }),
                        use_container_width=True
                    )

                with st.expander("Show full ranking"):
                    st.dataframe(
                        fmt_perf(perf).style.format({
                            "Realized PnL": "{:,.2f}",
                            "Unrealized Δ": "{:,.2f}",
                            "Total PnL": "{:,.2f}",
                            "Start Shares": "{:,.0f}",
                            "End Shares": "{:,.0f}",
                            "End MV": "{:,.2f}",
                        }),
                        use_container_width=True
                    )

            st.markdown(
                "<div class='small-note'>Notes: Initial cash is persisted to SQLite (settings table). Performer table uses FIFO/LIFO/AVG cost basis for realized PnL from sells inside the window, and unrealized change computed from inventory value at start vs end prices.</div>",
                unsafe_allow_html=True
            )
```

