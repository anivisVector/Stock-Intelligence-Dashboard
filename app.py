from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from flask import Flask, jsonify, render_template, request


# Reverted to the original 3-company demo setup.
# These are auto-seeded into data/{symbol}.csv so the app works immediately.
COMPANIES: dict[str, str] = {
    "INFY": "Infosys",
    "TCS": "Tata Consultancy Services",
    "RELIANCE": "Reliance Industries",
    "HDFCBANK": "HDFC Bank",
    "ICICIBANK": "ICICI Bank",
    "SBIN": "State Bank of India",
    "WIPRO": "Wipro Ltd",
    "HCLTECH": "HCL Technologies",
}

DATA_DIR = Path(__file__).resolve().parent / "data"


class DataError(Exception):
    pass


_csv_cache: dict[str, tuple[float, pd.DataFrame]] = {}


def _norm_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def _csv_path(symbol: str) -> Path:
    return DATA_DIR / f"{symbol}.csv"


def _seed_sample_csvs() -> None:
    """Create demo CSVs for the built-in companies when missing.

    This keeps the dashboard functional even when the user hasn't provided CSV files.
    """

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    base_prices = {
        "INFY": 1500.0,
        "TCS": 3800.0,
        "RELIANCE": 2800.0,
        "HDFCBANK": 1500.0,
        "ICICIBANK": 1100.0,
        "SBIN": 800.0,
        "WIPRO": 500.0,
        "HCLTECH": 1500.0,
    }

    end = pd.Timestamp.utcnow().tz_localize(None).normalize()
    start = end - pd.Timedelta(days=365)
    dates = pd.bdate_range(start=start, end=end)

    for sym in COMPANIES.keys():
        path = _csv_path(sym)
        if path.exists():
            continue

        seed = abs(hash(sym)) % (2**32)
        np_rng = np.random.default_rng(seed)
        mu = 0.0002
        sigma = 0.012
        rets = np_rng.normal(loc=mu, scale=sigma, size=len(dates))

        base = float(base_prices.get(sym, 1000.0))
        close = base * np.exp(np.cumsum(rets))
        close = pd.Series(close, index=dates)

        # Build OHLC around close
        open_ = close.shift(1).fillna(close) * (1 + pd.Series(np_rng.normal(0, 0.002, len(dates)), index=dates))
        high = pd.concat([open_, close], axis=1).max(axis=1) * (1 + pd.Series(np_rng.uniform(0.0005, 0.01, len(dates)), index=dates))
        low = pd.concat([open_, close], axis=1).min(axis=1) * (1 - pd.Series(np_rng.uniform(0.0005, 0.01, len(dates)), index=dates))
        volume = pd.Series(np_rng.integers(2_000_000, 12_000_000, size=len(dates)), index=dates)

        df = pd.DataFrame(
            {
                "Date": dates.date,
                "Open": open_.values,
                "High": high.values,
                "Low": low.values,
                "Close": close.values,
                "Adj Close": close.values,
                "Volume": volume.values,
            }
        )

        df.to_csv(path, index=False)


def _read_csv(symbol: str) -> pd.DataFrame:
    symbol = _norm_symbol(symbol)
    if symbol not in COMPANIES:
        raise DataError(f"Unknown symbol: {symbol}")

    path = _csv_path(symbol)
    if not path.exists():
        raise DataError(f"Missing CSV file: data/{symbol}.csv")

    # Cache by file mtime for scalability
    mtime = path.stat().st_mtime
    cached = _csv_cache.get(symbol)
    if cached and cached[0] == mtime:
        return cached[1].copy()

    df = pd.read_csv(path)
    if df.empty:
        raise DataError(f"Empty CSV file: data/{symbol}.csv")

    # Normalize columns: accept common Yahoo-style names or lowercase names
    df.columns = [str(c).strip() for c in df.columns]
    lower_map = {c.lower().replace(" ", "_"): c for c in df.columns}

    date_col = None
    for key in ("date", "datetime", "timestamp"):
        if key in lower_map:
            date_col = lower_map[key]
            break
    if date_col is None:
        raise DataError("CSV must contain a 'Date' column")

    def pick(*candidates: str) -> str | None:
        for cand in candidates:
            if cand in lower_map:
                return lower_map[cand]
        return None

    col_open = pick("open")
    col_high = pick("high")
    col_low = pick("low")
    col_close = pick("close")
    col_adj_close = pick("adj_close", "adjclose", "adj_close_")
    if col_adj_close is None and "Adj Close" in df.columns:
        col_adj_close = "Adj Close"
    col_volume = pick("volume")

    # Build canonical frame
    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date.astype("string")
    out = out.loc[out["date"].notna()]

    for name, src in (
        ("open", col_open),
        ("high", col_high),
        ("low", col_low),
        ("close", col_close),
        ("adj_close", col_adj_close),
        ("volume", col_volume),
    ):
        if src is None:
            out[name] = pd.NA
        else:
            out[name] = pd.to_numeric(df[src], errors="coerce")

    # Require at least close to function
    if out["close"].isna().all():
        raise DataError("CSV must contain a valid 'Close' column")

    out = out.dropna(subset=["date"]).sort_values("date")
    out = out.drop_duplicates(subset=["date"], keep="last")

    # Enrich metrics
    out["daily_return"] = out["close"].pct_change()
    out["ma7"] = out["close"].rolling(window=7, min_periods=1).mean()

    # 52-week rolling values (approx 252 trading days)
    if out["high"].isna().all():
        out["high_52w"] = out["close"].rolling(window=252, min_periods=1).max()
    else:
        out["high_52w"] = out["high"].rolling(window=252, min_periods=1).max()

    if out["low"].isna().all():
        out["low_52w"] = out["close"].rolling(window=252, min_periods=1).min()
    else:
        out["low_52w"] = out["low"].rolling(window=252, min_periods=1).min()

    out["volatility"] = out["daily_return"].rolling(window=30, min_periods=2).std()

    # Cache canonical enriched DF
    _csv_cache[symbol] = (mtime, out.copy())
    return out


def _read_csv_safe(symbol: str) -> pd.DataFrame:
    """Best-effort CSV loader.

    Never raises; returns an empty DataFrame when data is missing/invalid.
    This is used by API endpoints that should respond with empty JSON instead
    of throwing errors.
    """

    try:
        return _read_csv(symbol)
    except DataError:
        return pd.DataFrame()


def _safe_json(obj: Any):
    """Force strict JSON output (no NaN/Infinity) by round-tripping with json."""
    return json.loads(json.dumps(obj, allow_nan=False, default=str))


def _df_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    safe_df = df.astype(object).where(pd.notnull(df), None)
    return _safe_json(safe_df.to_dict(orient="records"))


def _summary_for(symbol: str) -> dict[str, Any]:
    df = _read_csv_safe(symbol)
    if df.empty:
        raise DataError(f"No data for symbol: {symbol}")

    df30 = df.tail(30)
    avg_series = pd.to_numeric(df30["close"], errors="coerce").dropna()
    vol_series = pd.to_numeric(df30["daily_return"], errors="coerce").dropna()

    avg_val = float(avg_series.mean()) if not avg_series.empty else float("nan")
    vol_val = float(vol_series.std()) if len(vol_series) >= 2 else float("nan")

    avg_close = None if pd.isna(avg_val) else avg_val
    vol = None if pd.isna(vol_val) else vol_val

    latest = df.iloc[-1]
    high_52w = float(latest["high_52w"]) if pd.notnull(latest["high_52w"]) else None
    low_52w = float(latest["low_52w"]) if pd.notnull(latest["low_52w"]) else None

    return {
        "52_week_high": high_52w,
        "52_week_low": low_52w,
        "average_close": avg_close,
        "volatility": vol,
    }


def _compare(symbol1: str, symbol2: str, days: int) -> pd.DataFrame:
    df1_all = _read_csv_safe(symbol1)
    df2_all = _read_csv_safe(symbol2)
    if df1_all.empty or df2_all.empty:
        return pd.DataFrame(columns=["date", _norm_symbol(symbol1), _norm_symbol(symbol2)])

    df1 = df1_all.tail(days)[["date", "close"]].rename(columns={"close": _norm_symbol(symbol1)})
    df2 = df2_all.tail(days)[["date", "close"]].rename(columns={"close": _norm_symbol(symbol2)})
    merged = pd.merge(df1, df2, on="date", how="inner").sort_values("date")
    return merged


def _top_movers() -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for sym in COMPANIES.keys():
        df = _read_csv_safe(sym)
        if df.empty:
            continue
        latest = df.iloc[-1]
        dr = latest.get("daily_return")
        if pd.isna(dr):
            continue
        items.append(
            {
                "symbol": sym,
                "date": latest.get("date"),
                "close": float(latest.get("close")) if pd.notnull(latest.get("close")) else None,
                "daily_return": float(dr) if pd.notnull(dr) else None,
            }
        )

    if not items:
        return {"top_gainer": None, "top_loser": None}
    top_gainer = max(items, key=lambda x: x["daily_return"])
    top_loser = min(items, key=lambda x: x["daily_return"])
    return {"top_gainer": top_gainer, "top_loser": top_loser}


def create_app() -> Flask:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    app = Flask(__name__)

    # Seed demo CSVs so the app works without manual data setup.
    try:
        _seed_sample_csvs()
    except Exception as exc:  # noqa: BLE001
        app.logger.exception("CSV seeding failed: %s", exc)

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/companies")
    def companies():
        # Spec: "Return list of companies"
        items = [{"symbol": s, "name": n} for s, n in COMPANIES.items()]
        items.sort(key=lambda x: x["name"])
        return jsonify(items)

    @app.get("/data/<path:symbol>")
    def data(symbol: str):
        days = request.args.get("days", default="30")
        try:
            days_int = int(days)
        except ValueError:
            return jsonify({"error": "Invalid 'days' query parameter"}), 400

        sym = _norm_symbol(symbol)
        if sym not in COMPANIES:
            return jsonify({"error": f"Unknown symbol: {sym}"}), 404

        # Return empty JSON ([]) for missing/invalid data instead of erroring.
        df = _read_csv_safe(sym)
        df = df.tail(days_int) if not df.empty else df
        return jsonify(_df_records(df)) if not df.empty else jsonify([])

    @app.get("/summary/<path:symbol>")
    def summary(symbol: str):
        try:
            sym = _norm_symbol(symbol)
            if sym not in COMPANIES:
                return jsonify({"error": f"Unknown symbol: {sym}"}), 404
            return jsonify(_safe_json(_summary_for(sym)))
        except DataError as exc:
            # Return empty JSON ({}) when summary can't be computed.
            app.logger.info("Summary unavailable for %s: %s", symbol, exc)
            return jsonify({})

    @app.get("/compare")
    def compare():
        symbol1 = request.args.get("symbol1")
        symbol2 = request.args.get("symbol2")
        days = request.args.get("days", default="30")

        if not symbol1 or not symbol2:
            return jsonify({"error": "Query params 'symbol1' and 'symbol2' are required"}), 400

        try:
            days_int = int(days)
        except ValueError:
            return jsonify({"error": "Invalid 'days' query parameter"}), 400

        s1 = _norm_symbol(symbol1)
        s2 = _norm_symbol(symbol2)
        if s1 not in COMPANIES or s2 not in COMPANIES:
            return jsonify({"error": "One or both symbols are unknown"}), 404

        # Return empty JSON ([]) if data is missing or there is no overlap.
        merged = _compare(s1, s2, days=days_int)
        return jsonify(_df_records(merged)) if not merged.empty else jsonify([])

    @app.get("/top-gainers")
    def top_gainers():
        movers = _top_movers()
        return jsonify(_safe_json(movers.get("top_gainer")))

    @app.get("/top-losers")
    def top_losers():
        movers = _top_movers()
        return jsonify(_safe_json(movers.get("top_loser")))

    @app.errorhandler(404)
    def not_found(_):
        return jsonify({"error": "Not found"}), 404

    @app.errorhandler(500)
    def server_error(_):
        return jsonify({"error": "Internal server error"}), 500

    return app


if __name__ == "__main__":
    app = create_app()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
