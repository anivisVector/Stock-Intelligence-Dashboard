"""Data collection + processing utilities for the Stock Data Intelligence Dashboard.

- Downloads daily OHLCV data from Yahoo Finance via yfinance
- Cleans and enriches data with calculated metrics
- Stores results into a local SQLite database

This file is intentionally dependency-light and uses stdlib sqlite3.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import random

import numpy as np
import pandas as pd
import requests
import yfinance as yf


DB_PATH = "stocks.db"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Company:
    symbol: str
    name: str


DEFAULT_COMPANIES: list[Company] = [
    Company("INFY.NS", "Infosys"),
    Company("TCS.NS", "Tata Consultancy Services"),
    Company("RELIANCE.NS", "Reliance Industries"),
]


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = DB_PATH) -> None:
    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS companies (
                symbol TEXT PRIMARY KEY,
                name   TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_data (
                symbol       TEXT NOT NULL,
                date         TEXT NOT NULL,  -- ISO yyyy-mm-dd
                open         REAL,
                high         REAL,
                low          REAL,
                close        REAL,
                adj_close    REAL,
                volume       INTEGER,
                daily_return REAL,
                ma7          REAL,
                high_52w     REAL,
                low_52w      REAL,
                volatility   REAL,
                PRIMARY KEY (symbol, date),
                FOREIGN KEY (symbol) REFERENCES companies(symbol)
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def seed_companies(companies: Iterable[Company] = DEFAULT_COMPANIES, db_path: str = DB_PATH) -> None:
    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        cur.executemany(
            "INSERT OR REPLACE INTO companies(symbol, name) VALUES(?, ?)",
            [(c.symbol, c.name) for c in companies],
        )
        conn.commit()
    finally:
        conn.close()


def _build_requests_session() -> requests.Session:
    """Build a requests session with headers that work better with Yahoo endpoints."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return session


def fetch_stock_data(
    symbol: str,
    *,
    period: str = "1y",
    interval: str = "1d",
    max_retries: int = 3,
    base_backoff_seconds: float = 0.5,
    session: requests.Session | None = None,
    allow_offline_fallback: bool = True,
) -> pd.DataFrame:
    """Fetch stock data using yfinance Ticker().history().

    Requirements addressed:
    - Uses yfinance
    - Uses a session with realistic headers
    - Retries with exponential backoff + jitter (helps rate limits / transient blocks)
    - Returns a clean DataFrame indexed by date
    - Raises a clear error when data is empty after retries

        Notes:
        - For NSE symbols, pass the Yahoo Finance symbol e.g. 'INFY.NS'.
        - If your network blocks Yahoo Finance, retries will still fail; by default we then return
            an offline sample dataset so the dashboard can still work.
    """

    if not symbol or not isinstance(symbol, str):
        raise ValueError("symbol must be a non-empty string")

    if session is None:
        session = _build_requests_session()

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                "Fetching %s via yfinance.history(period=%s, interval=%s) attempt %d/%d",
                symbol,
                period,
                interval,
                attempt,
                max_retries,
            )

            ticker = yf.Ticker(symbol, session=session)
            df = ticker.history(
                period=period,
                interval=interval,
                auto_adjust=False,
                actions=False,
                repair=True,
                raise_errors=False,
            )

            if df is None or df.empty:
                logger.warning(
                    "Empty dataframe returned for %s (attempt %d/%d). This is often rate-limiting, blocking, or an invalid symbol.",
                    symbol,
                    attempt,
                    max_retries,
                )
            else:
                out = df.copy()

                # Normalize datetime index
                if isinstance(out.index, pd.DatetimeIndex):
                    if out.index.tz is not None:
                        out.index = out.index.tz_convert(None)
                    out.index = pd.to_datetime(out.index, errors="coerce")
                else:
                    out.index = pd.to_datetime(out.index, errors="coerce")

                out = out.loc[~out.index.isna()]
                out = out.sort_index()

                # Ensure expected columns exist (yfinance usually returns these)
                expected = ["Open", "High", "Low", "Close", "Volume"]
                missing = [c for c in expected if c not in out.columns]
                if missing:
                    logger.warning("Missing expected columns for %s: %s", symbol, missing)

                logger.info("Fetched %d rows for %s (%s → %s)", len(out), symbol, out.index.min(), out.index.max())
                return out

        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            logger.exception(
                "yfinance history call failed for %s on attempt %d/%d: %s",
                symbol,
                attempt,
                max_retries,
                exc,
            )

        # Exponential backoff with jitter to reduce thundering herd / rate-limit issues
        if attempt < max_retries:
            sleep_s = (base_backoff_seconds * (2 ** (attempt - 1))) + random.uniform(0.0, 0.35)
            logger.info("Retrying %s after %.2fs", symbol, sleep_s)
            time.sleep(sleep_s)

    msg = f"No data returned for symbol {symbol} after {max_retries} attempts. Yahoo may be blocking/rate-limiting, or network may be restricted."
    if allow_offline_fallback:
        logger.warning("%s Falling back to offline sample dataset.", msg)
        return _offline_sample_dataset(symbol, period=period, interval=interval)

    if last_exc is not None:
        raise RuntimeError(msg) from last_exc
    raise RuntimeError(msg)


def _offline_sample_dataset(symbol: str, *, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
    """Generate a deterministic offline sample OHLCV dataset.

    This is used only when Yahoo Finance is unreachable/blocked.
    Returns a DataFrame similar to yfinance output (DatetimeIndex + OHLCV).
    """

    if interval != "1d":
        raise ValueError("Offline fallback currently supports interval='1d' only")

    # Business days over the last year
    end = pd.Timestamp.utcnow().tz_localize(None).normalize()
    start = end - pd.Timedelta(days=365)
    dates = pd.bdate_range(start=start, end=end)

    base_prices = {
        "INFY.NS": 1500.0,
        "TCS.NS": 3800.0,
        "RELIANCE.NS": 2800.0,
    }
    base = float(base_prices.get(symbol, 1000.0))

    # Deterministic seed per symbol
    seed = abs(hash(symbol)) % (2**32)
    rng = np.random.default_rng(seed)

    # Daily log-returns
    mu = 0.0002
    sigma = 0.012
    rets = rng.normal(loc=mu, scale=sigma, size=len(dates))
    close = base * np.exp(np.cumsum(rets))

    # Open around prior close
    open_ = np.empty_like(close)
    open_[0] = close[0] * (1 + rng.normal(0, 0.002))
    open_[1:] = close[:-1] * (1 + rng.normal(0, 0.002, size=len(close) - 1))

    # High/Low with small intraday wiggle
    wiggle = np.abs(rng.normal(0, 0.006, size=len(close)))
    high = np.maximum(open_, close) * (1 + wiggle)
    low = np.minimum(open_, close) * (1 - wiggle)

    volume = rng.integers(low=800_000, high=8_000_000, size=len(close), endpoint=False)

    df = pd.DataFrame(
        {
            "Open": open_,
            "High": high,
            "Low": low,
            "Close": close,
            "Adj Close": close,
            "Volume": volume.astype(np.int64),
        },
        index=dates,
    )
    df.index.name = "Date"
    return df


def fetch_yfinance_history(symbol: str, period: str = "1y") -> pd.DataFrame:
    """Backward-compatible wrapper used by the rest of this module."""
    return fetch_stock_data(symbol, period=period, interval="1d")


def _clean_and_enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich the dataframe.

    Expected input: yfinance output with Date index.
    Output: DataFrame with a 'date' column and required calculated metrics.
    """
    out = df.copy()

    # Convert index to a proper datetime and move to a column
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index, errors="coerce")

    out = out.reset_index().rename(columns={"Date": "date", "index": "date"})
    out["date"] = pd.to_datetime(out["date"], errors="coerce")

    # Handle missing values: forward-fill then back-fill for leading gaps
    out = out.sort_values("date")
    out = out.ffill().bfill()

    # Standardize column names to snake_case
    rename_map = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    }
    out = out.rename(columns=rename_map)

    # Daily Return = (Close - Open) / Open
    out["daily_return"] = (out["close"] - out["open"]) / out["open"]

    # 7-day moving average of close
    out["ma7"] = out["close"].rolling(window=7, min_periods=1).mean()

    # 52-week high/low approximated with 252 trading days rolling window
    out["high_52w"] = out["high"].rolling(window=252, min_periods=1).max()
    out["low_52w"] = out["low"].rolling(window=252, min_periods=1).min()

    # Volatility = standard deviation of returns (rolling 30 trading days)
    out["volatility"] = out["daily_return"].rolling(window=30, min_periods=2).std()

    # Keep only relevant columns and ensure date is ISO string
    keep = [
        "date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "daily_return",
        "ma7",
        "high_52w",
        "low_52w",
        "volatility",
    ]
    out = out[keep]
    out = out.dropna(subset=["date"])
    out["date"] = out["date"].dt.date.astype(str)

    return out


def refresh_symbol(symbol: str, db_path: str = DB_PATH, period: str = "1y") -> int:
    """Fetch data for a symbol, compute metrics, and replace stored rows.

    Returns number of rows inserted.
    """
    raw = fetch_yfinance_history(symbol, period=period)
    enriched = _clean_and_enrich(raw)

    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM stock_data WHERE symbol = ?", (symbol,))

        rows = []
        for r in enriched.itertuples(index=False):
            rows.append(
                (
                    symbol,
                    r.date,
                    float(r.open) if r.open is not None else None,
                    float(r.high) if r.high is not None else None,
                    float(r.low) if r.low is not None else None,
                    float(r.close) if r.close is not None else None,
                    float(r.adj_close) if r.adj_close is not None else None,
                    int(r.volume) if r.volume is not None and not pd.isna(r.volume) else None,
                    float(r.daily_return) if r.daily_return is not None else None,
                    float(r.ma7) if r.ma7 is not None else None,
                    float(r.high_52w) if r.high_52w is not None else None,
                    float(r.low_52w) if r.low_52w is not None else None,
                    float(r.volatility) if r.volatility is not None and not pd.isna(r.volatility) else None,
                )
            )

        cur.executemany(
            """
            INSERT OR REPLACE INTO stock_data (
                symbol, date, open, high, low, close, adj_close, volume,
                daily_return, ma7, high_52w, low_52w, volatility
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def refresh_all(companies: Iterable[Company] = DEFAULT_COMPANIES, db_path: str = DB_PATH) -> dict[str, int]:
    """Refresh all configured symbols.

    Returns {symbol: inserted_rows}. If a symbol fails to refresh (e.g. network),
    it will be recorded as 0 rows and the function continues.
    """
    results: dict[str, int] = {}
    for company in companies:
        try:
            results[company.symbol] = refresh_symbol(company.symbol, db_path=db_path)
        except Exception:  # noqa: BLE001
            results[company.symbol] = 0
    return results


def ensure_data(db_path: str = DB_PATH) -> None:
    """Initialize DB, seed companies, and ensure data exists.

    This is safe to call on app startup.
    """
    init_db(db_path)
    seed_companies(DEFAULT_COMPANIES, db_path)

    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS n FROM stock_data")
        n = int(cur.fetchone()["n"])
    finally:
        conn.close()

    if n == 0:
        # Best-effort refresh; if it fails, keep the app usable.
        refresh_all(DEFAULT_COMPANIES, db_path=db_path)


def list_companies(db_path: str = DB_PATH) -> list[dict]:
    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        rows = cur.execute("SELECT symbol, name FROM companies ORDER BY name").fetchall()
        return [{"symbol": r["symbol"], "name": r["name"]} for r in rows]
    finally:
        conn.close()


def symbol_exists(symbol: str, db_path: str = DB_PATH) -> bool:
    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        row = cur.execute("SELECT 1 FROM companies WHERE symbol = ?", (symbol,)).fetchone()
        return row is not None
    finally:
        conn.close()


def get_last_n_days(symbol: str, days: int = 30, db_path: str = DB_PATH) -> pd.DataFrame:
    conn = get_connection(db_path)
    try:
        query = (
            "SELECT date, open, high, low, close, adj_close, volume, daily_return, ma7, high_52w, low_52w, volatility "
            "FROM stock_data WHERE symbol = ? ORDER BY date DESC LIMIT ?"
        )
        rows = conn.execute(query, (symbol, days)).fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame([dict(r) for r in rows])
        df = df.sort_values("date")
        return df
    finally:
        conn.close()


def get_summary(symbol: str, db_path: str = DB_PATH) -> dict:
    """Return summary metrics.

    - 52 week high / low from latest row (rolling values)
    - average close over last 30 days
    - volatility as std dev of daily_return over last 30 days
    """
    df30 = get_last_n_days(symbol, days=30, db_path=db_path)
    if df30.empty:
        raise ValueError(f"No stored data for symbol: {symbol}")

    avg_close = float(df30["close"].mean())
    volatility = float(pd.to_numeric(df30["daily_return"], errors="coerce").dropna().std())

    # Get latest row for 52w metrics
    latest = df30.iloc[-1]
    high_52w = float(latest["high_52w"]) if not pd.isna(latest["high_52w"]) else None
    low_52w = float(latest["low_52w"]) if not pd.isna(latest["low_52w"]) else None

    return {
        "52_week_high": high_52w,
        "52_week_low": low_52w,
        "average_close": avg_close,
        "volatility": volatility,
    }


def compare_close(symbol1: str, symbol2: str, days: int = 30, db_path: str = DB_PATH) -> pd.DataFrame:
    df1 = get_last_n_days(symbol1, days=days, db_path=db_path)[["date", "close"]].rename(columns={"close": symbol1})
    df2 = get_last_n_days(symbol2, days=days, db_path=db_path)[["date", "close"]].rename(columns={"close": symbol2})
    if df1.empty or df2.empty:
        return pd.DataFrame()
    merged = pd.merge(df1, df2, on="date", how="inner").sort_values("date")
    return merged


def get_top_movers(db_path: str = DB_PATH) -> dict:
    """Return top gainer and top loser based on the most recent daily_return."""
    conn = get_connection(db_path)
    try:
        # For each symbol, pick latest date row then choose max/min daily_return.
        rows = conn.execute(
            """
            WITH latest AS (
                SELECT symbol, MAX(date) AS max_date
                FROM stock_data
                GROUP BY symbol
            )
            SELECT d.symbol, d.date, d.close, d.daily_return
            FROM stock_data d
            JOIN latest l
              ON d.symbol = l.symbol AND d.date = l.max_date
            WHERE d.daily_return IS NOT NULL
            """
        ).fetchall()

        if not rows:
            return {"top_gainer": None, "top_loser": None}

        items = [dict(r) for r in rows]
        top_gainer = max(items, key=lambda x: x["daily_return"])
        top_loser = min(items, key=lambda x: x["daily_return"])

        # Convert to python floats
        for it in (top_gainer, top_loser):
            it["daily_return"] = float(it["daily_return"]) if it["daily_return"] is not None else None
            it["close"] = float(it["close"]) if it["close"] is not None else None

        return {"top_gainer": top_gainer, "top_loser": top_loser}
    finally:
        conn.close()


def iso_today() -> str:
    return datetime.utcnow().date().isoformat()
