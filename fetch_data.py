"""
fetch_data.py
=============
Fetches financial data for a given NSE ticker and saves it to data/<TICKER>.json

Usage:
    python fetch_data.py LAURUSLABS

Each value in the JSON is stored as:
    { "value": ..., "color": "green"/"yellow"/"orange"/"red", "source": "...", "note": "..." }

Color meaning:
    green  — directly pulled from a source website
    yellow — calculated / derived from other pulled values
    orange — estimated (year-end price × reported metric)
    red    — unavailable / could not be fetched
"""

import sys
import json
import time
import re
import math
from datetime import datetime, date
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import yfinance as yf

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def make_cell(value, color, source, note=""):
    """Create a standardised data cell dictionary."""
    return {"value": value, "color": color, "source": source, "note": note}


def red_cell(note="Unavailable"):
    """Shortcut for a red (unavailable) cell."""
    return make_cell(None, "red", "", note)


def fetch_url(url, retries=3, delay=2):
    """Fetch a URL, retry on failure, return (response | None)."""
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=20)
            if r.status_code == 200:
                return r
            print(f"  [fetch] HTTP {r.status_code} for {url}")
        except Exception as e:
            print(f"  [fetch] Error on attempt {attempt + 1}: {e}")
        if attempt < retries - 1:
            time.sleep(delay)
    return None


def parse_number(text):
    """
    Convert screener.in number strings like '1,778.43' or '-45.20' to float.
    Returns None if parsing fails.
    """
    if not text:
        return None
    cleaned = text.strip().replace(",", "").replace("%", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def safe_divide(numerator, denominator):
    """Return numerator / denominator, or None if denominator is 0 or None."""
    if numerator is None or denominator is None:
        return None
    if denominator == 0:
        return None
    return numerator / denominator


def cagr(start, end, years):
    """Compute CAGR given start value, end value, and number of years."""
    if start is None or end is None or years <= 0:
        return None
    if start <= 0:
        return None
    return (end / start) ** (1 / years) - 1


def extract_key_metric(soup, keyword):
    """
    Extract a single current value from screener.in's top key metrics panel.
    The panel renders as <li class='flex flex-space-between'> items.
    Returns a float or None.
    """
    for li in soup.find_all("li"):
        classes = li.get("class") or []
        if "flex-space-between" in classes:
            text = li.get_text(" ", strip=True)
            if keyword.lower() in text.lower():
                numbers = re.findall(r'[\d]+\.?\d*', text)
                if numbers:
                    return float(numbers[-1])
    return None


# ─────────────────────────────────────────────
# Login helper — screener.in authenticated session
# ─────────────────────────────────────────────

def login_screener(config_path="config.json"):
    """
    Log in to screener.in using credentials from config.json.
    On success, SESSION holds the auth cookie for all subsequent requests.
    If config is missing or login fails, continues with unauthenticated session.
    """
    try:
        config_file = Path(__file__).parent / config_path
        with open(config_file) as f:
            cfg = json.load(f)
        email    = cfg["screener_email"]
        password = cfg["screener_password"]
    except Exception:
        print("  [login] config.json not found or invalid — skipping login")
        return False

    login_url = "https://www.screener.in/login/"
    r = fetch_url(login_url)
    if r is None:
        print("  [login] Could not reach screener.in login page")
        return False

    soup = BeautifulSoup(r.text, "lxml")
    csrf_input = soup.find("input", {"name": "csrfmiddlewaretoken"})
    if not csrf_input:
        print("  [login] Could not find CSRF token on login page")
        return False

    payload = {
        "username": email,
        "password": password,
        "csrfmiddlewaretoken": csrf_input["value"],
    }
    r2 = SESSION.post(login_url, data=payload,
                      headers={"Referer": login_url}, timeout=20)
    success = r2.url != login_url or "logout" in r2.text.lower()
    print(f"  [login] screener.in login {'succeeded' if success else 'FAILED — check credentials'}")
    return success


# ─────────────────────────────────────────────
# Shared screener.in table extractor (module-level)
# ─────────────────────────────────────────────

def extract_screener_table(soup, table_id):
    """
    Given a BeautifulSoup object and a screener.in section id,
    return (years_list, rows_dict).
    Mirrors the inner extract_table() used in scrape_screener_annual().
    """
    tbl = soup.find("section", {"id": table_id})
    if tbl is None:
        return [], {}
    table = tbl.find("table")
    if table is None:
        return [], {}

    rows_raw = table.find_all("tr")
    if not rows_raw:
        return [], {}

    header_cells = rows_raw[0].find_all(["th", "td"])
    years = [c.get_text(strip=True) for c in header_cells[1:]]

    rows_dict = {}
    for tr in rows_raw[1:]:
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue
        label = cells[0].get_text(strip=True)
        values = [parse_number(c.get_text(strip=True)) for c in cells[1:]]
        rows_dict[label] = values

    return years, rows_dict


# ─────────────────────────────────────────────
# Section 1 — Annual data from screener.in
# ─────────────────────────────────────────────

SCREENER_BASE = "https://www.screener.in/company/{ticker}/consolidated/"


def scrape_screener_annual(ticker):
    """
    Fetch the consolidated screener.in page for <ticker>.
    Returns a dict keyed by metric name, each value being a list of
    { fy_label: str, raw_value: float } dicts — one per fiscal year column.

    Metrics pulled (all green — directly sourced):
        Sales, Net Profit, EPS, Equity Capital, Face Value,
        Dividend Per Share, OPM%, ROE%, ROCE%, Book Value,
        Total Borrowings, Cash Equivalents, Total Assets
    """
    url = SCREENER_BASE.format(ticker=ticker)
    print(f"\n[Section 1] Fetching screener.in annual data: {url}")

    r = fetch_url(url)
    if r is None:
        print("  Could not fetch screener.in page.")
        return {}, url, []

    soup = BeautifulSoup(r.text, "lxml")

    # ── Find the fiscal-year header row ──────────────────────────────────
    # screener.in renders a <table id="profit-loss"> with the P&L data.
    # The first row contains the year labels (e.g., "Mar 2016", "Mar 2017" …).

    def extract_table(table_id):
        """
        Given a screener.in table id, return:
          years  : list of FY label strings (e.g. ['Mar 2016', …])
          rows   : dict { row_label: [float_or_None, …] }
        """
        tbl = soup.find("section", {"id": table_id})
        if tbl is None:
            return [], {}
        table = tbl.find("table")
        if table is None:
            return [], {}

        rows_raw = table.find_all("tr")
        if not rows_raw:
            return [], {}

        # Year labels are in the first <tr>
        header_cells = rows_raw[0].find_all(["th", "td"])
        years = [c.get_text(strip=True) for c in header_cells[1:]]  # skip first blank

        rows_dict = {}
        for tr in rows_raw[1:]:
            cells = tr.find_all(["th", "td"])
            if not cells:
                continue
            label = cells[0].get_text(strip=True)
            values = [parse_number(c.get_text(strip=True)) for c in cells[1:]]
            rows_dict[label] = values

        return years, rows_dict

    # ── Pull P&L table ───────────────────────────────────────────────────
    pl_years, pl_rows = extract_table("profit-loss")

    # ── Pull Balance Sheet table ─────────────────────────────────────────
    bs_years, bs_rows = extract_table("balance-sheet")

    # ── Pull Ratios table ────────────────────────────────────────────────
    rat_years, rat_rows = extract_table("ratios")

    print(f"  P&L years found   : {pl_years}")
    print(f"  Balance sheet years: {bs_years}")

    # ── Map screener row labels to our internal metric names ─────────────
    # Screener labels can vary slightly; we search for the closest match.

    def find_row(rows_dict, keywords):
        """Return the first row whose label contains all keywords (case-insensitive)."""
        for label, values in rows_dict.items():
            label_lower = label.lower()
            if all(kw.lower() in label_lower for kw in keywords):
                return values
        return None

    # P&L metrics (green)
    sales_row        = find_row(pl_rows, ["sales"])
    net_profit_row   = find_row(pl_rows, ["net profit"])
    eps_row          = find_row(pl_rows, ["eps"])
    dividend_row     = find_row(pl_rows, ["dividend"])

    # Balance sheet metrics (green)
    equity_cap_row   = find_row(bs_rows, ["equity capital"])
    reserves_row     = find_row(bs_rows, ["reserves"])
    borrowings_row   = find_row(bs_rows, ["borrowings"])
    cash_row         = find_row(bs_rows, ["cash"])
    total_assets_row = find_row(bs_rows, ["total assets"])

    # Ratios (green) — try multiple label variants for resilience across company types
    roe_row = (find_row(rat_rows, ["roe"]) or
               find_row(rat_rows, ["return on equity"]) or
               find_row(rat_rows, ["return", "equity"]))

    roce_row = (find_row(rat_rows, ["roce"]) or
                find_row(rat_rows, ["return on capital"]) or
                find_row(rat_rows, ["return", "capital"]))

    bv_row = (find_row(rat_rows, ["book value"]) or
              find_row(rat_rows, ["book val"]) or
              find_row(bs_rows,  ["book value"]))

    # Face value: try tables first, then fall back to key metrics panel (current value)
    fv_row = find_row(rat_rows, ["face value"]) or find_row(pl_rows, ["face value"])
    if not fv_row:
        fv_current = extract_key_metric(soup, "face value")
        if fv_current is not None:
            # FV is almost always constant across years; fill for all years
            fv_row = [fv_current] * len(pl_years)

    # Enterprise Value and Market Cap from screener.in ratios (direct, green)
    ev_row = (find_row(rat_rows, ["enterprise value"]) or
              find_row(rat_rows, ["enterprise"]))
    mc_row = (find_row(rat_rows, ["market cap"]) or
              find_row(rat_rows, ["market capitalisation"]) or
              find_row(rat_rows, ["market capitalization"]))

    return {
        "years"        : pl_years,
        "bs_years"     : bs_years,
        "rat_years"    : rat_years if rat_years else pl_years,
        "sales"        : sales_row,
        "net_profit"   : net_profit_row,
        "eps"          : eps_row,
        "dividend"     : dividend_row,
        "equity_cap"   : equity_cap_row,
        "reserves"     : reserves_row,
        "borrowings"   : borrowings_row,
        "cash"         : cash_row,
        "total_assets" : total_assets_row,
        "roe"          : roe_row,
        "roce"         : roce_row,
        "book_value"   : bv_row,
        "face_value"   : fv_row,
        "ev_screener"  : ev_row,
        "mc_screener"  : mc_row,
    }, url, pl_years, soup


# ─────────────────────────────────────────────
# Section 2 — Calculate derived annual metrics
# ─────────────────────────────────────────────

def compute_annual_derived(raw, screener_url, years):
    """
    Given raw data lists from screener.in (parallel to `years`),
    compute derived / calculated metrics (all yellow).

    Returns a dict { metric_name: [ cell_dict, … ] } keyed by metric,
    one cell per year.
    """
    print("\n[Section 2] Computing derived annual metrics...")

    n = len(years)

    def get_list(key):
        lst = raw.get(key) or []
        # Pad or trim to length n
        if len(lst) < n:
            lst = lst + [None] * (n - len(lst))
        return lst[:n]

    sales       = get_list("sales")
    net_profit  = get_list("net_profit")
    borrowings  = get_list("borrowings")
    equity_cap  = get_list("equity_cap")
    reserves    = get_list("reserves")
    total_assets = get_list("total_assets")
    dividend    = get_list("dividend")
    face_value  = get_list("face_value")

    derived = {}

    # ── NPM% = NP / Sales × 100 ─────────────────────────────────────────
    npm_cells = []
    for i in range(n):
        val = safe_divide(net_profit[i], sales[i])
        if val is not None:
            val = round(val * 100, 2)
        npm_cells.append(make_cell(
            val, "yellow", screener_url,
            "NPM% = Net Profit / Sales × 100"
        ))
    derived["npm_pct"] = npm_cells

    # ── 3-yr CAGR Sales ──────────────────────────────────────────────────
    cagr_sales_cells = []
    for i in range(n):
        if i < 3:
            cagr_sales_cells.append(red_cell("Need 3 prior years"))
            continue
        val = cagr(sales[i - 3], sales[i], 3)
        if val is not None:
            val = round(val * 100, 2)
        cagr_sales_cells.append(make_cell(
            val, "yellow", screener_url,
            "3yr CAGR Sales = (Sales_t / Sales_{t-3})^(1/3) - 1"
        ))
    derived["cagr_sales_3yr"] = cagr_sales_cells

    # ── 3-yr CAGR Net Profit ─────────────────────────────────────────────
    cagr_np_cells = []
    for i in range(n):
        if i < 3:
            cagr_np_cells.append(red_cell("Need 3 prior years"))
            continue
        # CAGR is undefined when start value is negative
        start_np = net_profit[i - 3]
        end_np   = net_profit[i]
        if start_np is None or start_np <= 0 or end_np is None:
            cagr_np_cells.append(red_cell("N/A (negative base)"))
            continue
        val = cagr(start_np, end_np, 3)
        if val is not None:
            val = round(val * 100, 2)
        cagr_np_cells.append(make_cell(
            val, "yellow", screener_url,
            "3yr CAGR NP = (NP_t / NP_{t-3})^(1/3) - 1"
        ))
    derived["cagr_np_3yr"] = cagr_np_cells

    # ── RoA% = NP / Total Assets × 100 ───────────────────────────────────
    roa_cells = []
    for i in range(n):
        val = safe_divide(net_profit[i], total_assets[i])
        if val is not None:
            val = round(val * 100, 2)
        roa_cells.append(make_cell(
            val, "yellow", screener_url,
            "RoA% = Net Profit / Total Assets × 100"
        ))
    derived["roa_pct"] = roa_cells

    # ── D/E Ratio = Total Borrowings / (Equity Capital + Reserves) ───────
    de_cells = []
    for i in range(n):
        eq = equity_cap[i]
        res = reserves[i]
        net_worth = None
        if eq is not None and res is not None:
            net_worth = eq + res
        elif eq is not None:
            net_worth = eq
        val = safe_divide(borrowings[i], net_worth)
        if val is not None:
            val = round(val, 2)
        de_cells.append(make_cell(
            val, "yellow", screener_url,
            "D/E = Total Borrowings / (Equity Capital + Reserves)"
        ))
    derived["de_ratio"] = de_cells

    # ── Div% = (Div per share / Face Value) × 100 ────────────────────────
    div_pct_cells = []
    for i in range(n):
        fv = face_value[i]
        dv = dividend[i]
        # Face value may be constant; use last known value if current is None
        if fv is None and face_value:
            fv_known = [x for x in face_value if x is not None]
            fv = fv_known[-1] if fv_known else None
        val = safe_divide(dv, fv)
        if val is not None:
            val = round(val * 100, 2)
        div_pct_cells.append(make_cell(
            val, "yellow", screener_url,
            "Div% = (Dividend per share / Face Value) × 100"
        ))
    derived["div_pct"] = div_pct_cells

    # ── ROE% = Net Profit / (Equity Capital + Reserves) × 100 ───────────
    roe_calc_cells = []
    for i in range(n):
        ec  = equity_cap[i]
        res = reserves[i]
        net_worth = None
        if ec is not None and res is not None:
            net_worth = ec + res
        elif ec is not None:
            net_worth = ec
        val = safe_divide(net_profit[i], net_worth)
        if val is not None:
            val = round(val * 100, 2)
        roe_calc_cells.append(make_cell(
            val, "yellow", screener_url,
            "ROE% = Net Profit / (Equity Capital + Reserves) × 100"
        ))
    derived["roe_calc"] = roe_calc_cells

    # ── Book Value per share = (EC + Reserves) × FV / EC ─────────────────
    bv_calc_cells = []
    for i in range(n):
        ec  = equity_cap[i]
        res = reserves[i]
        fv  = face_value[i]
        if fv is None:
            fv_known = [x for x in face_value if x is not None]
            fv = fv_known[-1] if fv_known else None
        val = None
        if ec and ec > 0 and res is not None and fv:
            val = round((ec + res) * fv / ec, 2)
        bv_calc_cells.append(make_cell(
            val, "yellow", screener_url,
            "BV = (Equity Capital + Reserves) × Face Value / Equity Capital"
        ))
    derived["bv_calc"] = bv_calc_cells

    return derived


# ─────────────────────────────────────────────
# Section 3 — Historical shareholding (screener.in)
# ─────────────────────────────────────────────

def extract_shareholding(soup, url):
    """
    Extract shareholding pattern from the already-fetched screener.in page.
    The #shareholding section has quarters as columns and categories as rows
    (Promoters, FIIs, DIIs, Public & Other).

    Returns:
        annual_sh    : dict { 'Mar YYYY': { fii, dii, promoter, public } }
        quarterly_sh : dict { 'Dec YYYY': { fii, dii, promoter, public } }
    Both green (directly from screener.in).
    """
    print("\n[Section 3] Extracting shareholding from screener.in page...")

    quarters, rows = extract_screener_table(soup, "shareholding")

    if not quarters:
        print("  No shareholding section found (may require login).")
        return {}, {}

    print(f"  Shareholding quarters found: {quarters[:5]} ...")

    def find_row(keywords):
        for label, values in rows.items():
            label_lower = label.lower()
            if all(kw.lower() in label_lower for kw in keywords):
                return values
        return None

    promoter_row = find_row(["promoter"])
    fii_row      = find_row(["fii"]) or find_row(["foreign"])
    dii_row      = find_row(["dii"]) or find_row(["domestic inst"])
    public_row   = find_row(["public"])

    n = len(quarters)

    def get_val(row, i):
        if row is None or i >= len(row):
            return None
        return row[i]

    quarterly_sh = {}
    for i, q in enumerate(quarters):
        quarterly_sh[q] = {
            "fii"      : get_val(fii_row, i),
            "dii"      : get_val(dii_row, i),
            "promoter" : get_val(promoter_row, i),
            "public"   : get_val(public_row, i),
        }

    # Annual = March-end quarters only
    annual_sh = {
        label: vals for label, vals in quarterly_sh.items()
        if "mar" in label.lower()
    }

    return annual_sh, quarterly_sh


# ─────────────────────────────────────────────
# Section 4 — Historical year-end stock prices + market metrics
# ─────────────────────────────────────────────

def fetch_prices_yfinance(ticker):
    """
    Fetch historical March-end (fiscal year-end) closing prices from Yahoo Finance.
    Uses the NSE ticker format: TICKER.NS (e.g. LAURUSLABS.NS).
    Returns dict { 'Mar YYYY': price_float } — orange (estimated from year-end price).
    """
    symbol = f"{ticker}.NS"
    source_url = f"https://finance.yahoo.com/quote/{symbol}"
    print(f"\n[Section 4] Fetching price history from Yahoo Finance: {symbol}")

    try:
        df = yf.download(symbol, period="15y", interval="1mo",
                         auto_adjust=True, progress=False)
        if df.empty:
            print(f"  No data returned for {symbol}")
            return {}, source_url

        prices = {}
        for dt, row in df.iterrows():
            if dt.month == 3:   # March = fiscal year end
                close = row["Close"]
                # yfinance may return a Series for close; extract scalar
                if hasattr(close, "iloc"):
                    close = close.iloc[0]
                prices[f"Mar {dt.year}"] = round(float(close), 2)

        print(f"  Price years found: {list(prices.keys())[:5]} ...")
        return prices, source_url

    except Exception as e:
        print(f"  [yfinance] Error fetching {symbol}: {e}")
        return {}, source_url


def compute_market_metrics(years, raw, prices, price_url, screener_url):
    """
    Compute PE, Market Cap, EV, MC/EV for each fiscal year — all orange.
    `years` is the list of FY labels from screener.
    `prices` is dict from trendlyne { label: price }.
    """
    print("\n[Section 4b] Computing market metrics...")

    n = len(years)

    def get_list(key):
        lst = raw.get(key) or []
        if len(lst) < n:
            lst = lst + [None] * (n - len(lst))
        return lst[:n]

    eps        = get_list("eps")
    equity_cap = get_list("equity_cap")
    borrowings = get_list("borrowings")
    cash       = get_list("cash")
    face_value = get_list("face_value")

    def match_price(year_label):
        """
        Match a screener year label like 'Mar 2022' to a trendlyne price.
        Trendlyne labels might be 'Mar 2022' or 'Q4 FY21-22'.
        """
        for k, v in prices.items():
            if year_label.lower() in k.lower() or k.lower() in year_label.lower():
                return v
        # Try matching the year number
        m = re.search(r"(\d{4})", year_label)
        if m:
            yr = m.group(1)
            for k, v in prices.items():
                if yr in k:
                    return v
        return None

    pe_cells, mc_cells, ev_cells, mc_ev_cells = [], [], [], []

    for i, yr in enumerate(years):
        price = match_price(yr)
        note_base = f"Year-end price (Mar closing) from trendlyne; year={yr}"

        # PE = price / EPS
        pe_val = safe_divide(price, eps[i])
        pe_val = round(pe_val, 1) if pe_val else None
        pe_cells.append(make_cell(pe_val, "orange", price_url,
            f"PE = Year-end price / EPS; {note_base}"))

        # Shares outstanding = Equity Capital / Face Value × 1 Cr (since EC is in ₹Cr, FV in ₹)
        fv = face_value[i]
        if fv is None:
            fv_known = [x for x in face_value if x is not None]
            fv = fv_known[-1] if fv_known else 10.0

        shares_cr = None
        if equity_cap[i] and fv:
            # equity_cap is in ₹Cr; face value in ₹; shares = (EC_in_Rs) / FV
            shares_cr = (equity_cap[i] * 1e7) / fv / 1e7  # result in Cr shares

        mc_val = None
        if price and shares_cr:
            mc_val = round(price * shares_cr / 1e2, 2)  # in ₹Cr (price in ₹, shares in Cr)
        mc_cells.append(make_cell(mc_val, "orange", price_url,
            f"MC = Year-end price × Shares outstanding (in ₹Cr); {note_base}"))

        ev_val = None
        if mc_val is not None:
            borrow = borrowings[i] or 0
            cash_v = cash[i] or 0
            ev_val = round(mc_val + borrow - cash_v, 2)
        ev_cells.append(make_cell(ev_val, "orange", price_url,
            f"EV = MC + Total Borrowings - Cash; {note_base}"))

        mc_ev_val = safe_divide(mc_val, ev_val)
        mc_ev_val = round(mc_ev_val, 2) if mc_ev_val else None
        mc_ev_cells.append(make_cell(mc_ev_val, "orange", price_url,
            "MC/EV = Market Cap / Enterprise Value"))

    return {
        "pe"    : pe_cells,
        "mc"    : mc_cells,
        "ev"    : ev_cells,
        "mc_ev" : mc_ev_cells,
    }


# ─────────────────────────────────────────────
# Section 5 — Quarterly P&L data
# ─────────────────────────────────────────────

SCREENER_QUARTERLY_URL = "https://www.screener.in/company/{ticker}/consolidated/"


def scrape_quarterly_pl(ticker):
    """
    Fetch quarterly P&L from screener.in (Q3 FY23 onward).
    Returns:
        quarters  : list of quarter labels
        q_sales   : list of sales values
        q_np      : list of NP values
        q_npm     : list of NPM% cells (yellow, calculated)
    All sourced values are green; NPM is yellow.
    """
    url = SCREENER_QUARTERLY_URL.format(ticker=ticker)
    print(f"\n[Section 5] Fetching quarterly P&L from screener.in: {url}")

    # We already have the page from Section 1; re-fetch to keep sections independent.
    r = fetch_url(url)
    if r is None:
        print("  Could not re-fetch screener.in.")
        return [], [], [], []

    soup = BeautifulSoup(r.text, "lxml")

    # The quarterly results section has id="quarters"
    q_section = soup.find("section", {"id": "quarters"})
    if q_section is None:
        print("  No quarterly section found on screener.in.")
        return [], [], [], []

    table = q_section.find("table")
    if table is None:
        return [], [], [], []

    rows_raw = table.find_all("tr")
    if not rows_raw:
        return [], [], [], []

    # Header row = quarter labels
    header_cells = rows_raw[0].find_all(["th", "td"])
    quarters = [c.get_text(strip=True) for c in header_cells[1:]]

    # Find Sales and Net Profit rows
    def find_row(keyword):
        for tr in rows_raw[1:]:
            cells = tr.find_all(["th", "td"])
            if not cells:
                continue
            if keyword.lower() in cells[0].get_text(strip=True).lower():
                return [parse_number(c.get_text(strip=True)) for c in cells[1:]]
        return None

    q_sales_raw = find_row("sales") or [None] * len(quarters)
    q_np_raw    = find_row("net profit") or [None] * len(quarters)

    # NPM% per quarter (yellow)
    q_npm_cells = []
    for s, np in zip(q_sales_raw, q_np_raw):
        val = safe_divide(np, s)
        if val is not None:
            val = round(val * 100, 2)
        q_npm_cells.append(make_cell(val, "yellow", url,
            "Quarterly NPM% = Net Profit / Sales × 100"))

    print(f"  Quarterly periods: {quarters[:5]} ...")
    return quarters, q_sales_raw, q_np_raw, q_npm_cells, url


# ─────────────────────────────────────────────
# Section 6 — Assemble and save JSON
# ─────────────────────────────────────────────

def build_json(ticker, raw, screener_url, years,
               derived, annual_sh, quarterly_sh, sh_url,
               market_metrics, price_url,
               q_quarters, q_sales, q_np, q_npm, q_url):
    """
    Assemble all data into a single JSON structure and save to data/<ticker>.json
    """
    print(f"\n[Section 6] Assembling JSON for {ticker}...")

    n = len(years)

    def get_raw_cells(key, source_url, note=""):
        """Wrap a raw list from screener into green cell dicts."""
        lst = raw.get(key) or []
        if len(lst) < n:
            lst = lst + [None] * (n - len(lst))
        return [make_cell(v, "green" if v is not None else "red", source_url, note)
                for v in lst[:n]]

    # ── Annual section ──────────────────────────────────────────────────

    annual = {
        "years"          : years,
        "sales"          : get_raw_cells("sales",       screener_url, "Sales in ₹Cr"),
        "net_profit"     : get_raw_cells("net_profit",  screener_url, "Net Profit in ₹Cr"),
        "npm_pct"        : derived["npm_pct"],
        "cagr_sales_3yr" : derived["cagr_sales_3yr"],
        "cagr_np_3yr"    : derived["cagr_np_3yr"],
        "eps"            : get_raw_cells("eps",         screener_url, "EPS in ₹"),
        "pe"             : market_metrics["pe"],
        "roe_pct"        : (get_raw_cells("roe", screener_url, "ROE%")
                            if any(v is not None for v in (raw.get("roe") or []))
                            else derived["roe_calc"]),
        "roce_pct"       : get_raw_cells("roce",        screener_url, "ROCE%"),
        "roa_pct"        : derived["roa_pct"],
        "de_ratio"       : derived["de_ratio"],
        "mc"             : (get_raw_cells("mc_screener", screener_url, "Market Cap in ₹Cr")
                            if any(v is not None for v in (raw.get("mc_screener") or []))
                            else market_metrics["mc"]),
        "ev"             : (get_raw_cells("ev_screener", screener_url, "Enterprise Value in ₹Cr")
                            if any(v is not None for v in (raw.get("ev_screener") or []))
                            else market_metrics["ev"]),
        "mc_ev"          : market_metrics["mc_ev"],
        "book_value"     : derived["bv_calc"],
        "equity_capital" : get_raw_cells("equity_cap",  screener_url, "Equity Capital in ₹Cr"),
        "face_value"     : get_raw_cells("face_value",  screener_url, "Face Value in ₹"),
        "dividend"       : get_raw_cells("dividend",    screener_url, "Dividend per share in ₹"),
        "div_pct"        : derived["div_pct"],
        "borrowings"     : get_raw_cells("borrowings",  screener_url, "Total Borrowings in ₹Cr"),
        "cash"           : get_raw_cells("cash",        screener_url, "Cash & Equivalents in ₹Cr"),
        "total_assets"   : get_raw_cells("total_assets",screener_url, "Total Assets in ₹Cr"),
    }

    # ── Shareholding — annual ────────────────────────────────────────────
    def sh_cell(val, key):
        return make_cell(val, "green" if val is not None else "red", sh_url,
                         f"Historical {key}% from trendlyne.com")

    annual["shareholding"] = {}
    for label, vals in annual_sh.items():
        annual["shareholding"][label] = {
            k: sh_cell(vals.get(k), k) for k in ["fii", "dii", "promoter", "public"]
        }

    # ── Quarterly section ────────────────────────────────────────────────
    q_len = len(q_quarters)

    def wrap_q(lst, color, source, note=""):
        if len(lst) < q_len:
            lst = lst + [None] * (q_len - len(lst))
        return [make_cell(v, color if v is not None else "red", source, note) for v in lst[:q_len]]

    quarterly = {
        "quarters"   : q_quarters,
        "sales"      : wrap_q(q_sales, "green",  q_url, "Quarterly Sales in ₹Cr"),
        "net_profit" : wrap_q(q_np,    "green",  q_url, "Quarterly Net Profit in ₹Cr"),
        "npm_pct"    : q_npm,
        "shareholding": {}
    }

    for label, vals in quarterly_sh.items():
        quarterly["shareholding"][label] = {
            k: sh_cell(vals.get(k), k) for k in ["fii", "dii", "promoter", "public"]
        }

    # ── Top-level structure ──────────────────────────────────────────────
    output = {
        "ticker"      : ticker,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "sources"     : {
            "screener" : screener_url,
            "trendlyne": sh_url,
            "prices"   : price_url,
        },
        "annual"      : annual,
        "quarterly"   : quarterly,
    }

    # ── Save ─────────────────────────────────────────────────────────────
    out_dir = Path(__file__).parent / "data"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"{ticker}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n  Saved to: {out_path}")
    return output


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage: python fetch_data.py <TICKER>")
        print("Example: python fetch_data.py LAURUSLABS")
        sys.exit(1)

    ticker = sys.argv[1].upper().strip()
    print(f"\n{'='*55}")
    print(f"  Fetching data for: {ticker}")
    print(f"{'='*55}")

    # Login to screener.in (uses config.json if present)
    login_screener()

    # Section 1 — Annual raw data (also returns soup for shareholding)
    raw, screener_url, years, soup = scrape_screener_annual(ticker)

    if not years:
        print("\n[ERROR] No annual data found. Check the ticker symbol and internet connection.")
        sys.exit(1)

    # Section 2 — Derived annual metrics
    derived = compute_annual_derived(raw, screener_url, years)

    # Section 3 — Shareholding history (from screener.in, reuses fetched soup)
    annual_sh, quarterly_sh = extract_shareholding(soup, screener_url)
    sh_url = screener_url

    # Section 4 — Historical prices + market metrics (via Yahoo Finance)
    prices, price_url = fetch_prices_yfinance(ticker)
    market_metrics = compute_market_metrics(years, raw, prices, price_url, screener_url)

    # Section 5 — Quarterly P&L
    q_quarters, q_sales, q_np, q_npm, q_url = scrape_quarterly_pl(ticker)

    # Section 6 — Assemble and save
    build_json(
        ticker, raw, screener_url, years,
        derived, annual_sh, quarterly_sh, sh_url,
        market_metrics, price_url,
        q_quarters, q_sales, q_np, q_npm, q_url
    )

    print(f"\n{'='*55}")
    print(f"  Done! JSON saved to data/{ticker}.json")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
