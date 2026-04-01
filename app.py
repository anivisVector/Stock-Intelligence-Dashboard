from __future__ import annotations

import csv
import json
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
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


_csv_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}


def _norm_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def _csv_path(symbol: str) -> Path:
    return DATA_DIR / f"{symbol}.csv"


def _parse_date_to_iso(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    # Fast path: already ISO-ish (yyyy-mm-dd)
    # Keep exactly date portion if timestamp is included.
    if len(s) >= 10 and s[4:5] == "-" and s[7:8] == "-":
        return s[:10]

    # Try a few common formats.
    for fmt in ("%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _to_float(value: Any) -> float:
    if value is None:
        return float("nan")
    s = str(value).strip()
    if not s:
        return float("nan")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def _to_int_or_nan(value: Any) -> float:
    if value is None:
        return float("nan")
    s = str(value).strip()
    if not s:
        return float("nan")
    try:
        return float(int(float(s)))
    except ValueError:
        return float("nan")


def _none_if_nan(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, float) and (np.isnan(x) or np.isinf(x)):
        return None
    return x


def _rolling_mean(values: np.ndarray, window: int) -> np.ndarray:
    out = np.full_like(values, np.nan, dtype=float)
    for i in range(len(values)):
        start = max(0, i - window + 1)
        chunk = values[start : i + 1]
        out[i] = float(np.nanmean(chunk)) if np.isfinite(np.nanmean(chunk)) else np.nan
    return out


def _rolling_max(values: np.ndarray, window: int) -> np.ndarray:
    out = np.full_like(values, np.nan, dtype=float)
    for i in range(len(values)):
        start = max(0, i - window + 1)
        out[i] = float(np.nanmax(values[start : i + 1]))
    return out


def _rolling_min(values: np.ndarray, window: int) -> np.ndarray:
    out = np.full_like(values, np.nan, dtype=float)
    for i in range(len(values)):
        start = max(0, i - window + 1)
        out[i] = float(np.nanmin(values[start : i + 1]))
    return out


def _rolling_std(values: np.ndarray, window: int, *, min_periods: int = 2) -> np.ndarray:
    out = np.full_like(values, np.nan, dtype=float)
    for i in range(len(values)):
        start = max(0, i - window + 1)
        chunk = values[start : i + 1]
        chunk = chunk[np.isfinite(chunk)]
        if len(chunk) < min_periods:
            out[i] = np.nan
        else:
            # Match pandas default ddof=1 behavior.
            out[i] = float(np.std(chunk, ddof=1))
    return out


def _enrich_records(dates: list[str], open_: np.ndarray, high: np.ndarray, low: np.ndarray, close: np.ndarray, adj_close: np.ndarray, volume: np.ndarray) -> list[dict[str, Any]]:
    n = len(dates)
    if n == 0:
        return []

    daily_return = np.full(n, np.nan, dtype=float)
    for i in range(1, n):
        prev = close[i - 1]
        cur = close[i]
        if np.isfinite(prev) and prev != 0 and np.isfinite(cur):
            daily_return[i] = (cur / prev) - 1.0

    ma7 = _rolling_mean(close, window=7)

    # 52-week rolling values (approx 252 trading days)
    use_high = high if np.isfinite(high).any() else close
    use_low = low if np.isfinite(low).any() else close
    high_52w = _rolling_max(use_high, window=252)
    low_52w = _rolling_min(use_low, window=252)

    volatility = _rolling_std(daily_return, window=30, min_periods=2)

    records: list[dict[str, Any]] = []
    for i in range(n):
        rec: dict[str, Any] = {
            "date": dates[i],
            "open": _none_if_nan(float(open_[i])),
            "high": _none_if_nan(float(high[i])),
            "low": _none_if_nan(float(low[i])),
            "close": _none_if_nan(float(close[i])),
            "adj_close": _none_if_nan(float(adj_close[i])),
            "volume": _none_if_nan(float(volume[i])),
            "daily_return": _none_if_nan(float(daily_return[i])),
            "ma7": _none_if_nan(float(ma7[i])),
            "high_52w": _none_if_nan(float(high_52w[i])),
            "low_52w": _none_if_nan(float(low_52w[i])),
            "volatility": _none_if_nan(float(volatility[i])),
        }

        # Keep JSON strict: normalize NaN/Inf to None.
        for k, v in list(rec.items()):
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                rec[k] = None
        records.append(rec)
    return records


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise DataError(f"Empty CSV file: {path.as_posix()}")

        cols = [str(c).strip() for c in reader.fieldnames]
        lower_map = {c.lower().replace(" ", "_"): c for c in cols}

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
        if col_adj_close is None and "Adj Close" in cols:
            col_adj_close = "Adj Close"
        col_volume = pick("volume")

        dates: list[str] = []
        open_vals: list[float] = []
        high_vals: list[float] = []
        low_vals: list[float] = []
        close_vals: list[float] = []
        adj_close_vals: list[float] = []
        volume_vals: list[float] = []

        for row in reader:
            d = _parse_date_to_iso(row.get(date_col))
            if not d:
                continue

            dates.append(d)
            open_vals.append(_to_float(row.get(col_open)) if col_open else float("nan"))
            high_vals.append(_to_float(row.get(col_high)) if col_high else float("nan"))
            low_vals.append(_to_float(row.get(col_low)) if col_low else float("nan"))
            close_vals.append(_to_float(row.get(col_close)) if col_close else float("nan"))
            adj_close_vals.append(_to_float(row.get(col_adj_close)) if col_adj_close else float("nan"))
            volume_vals.append(_to_int_or_nan(row.get(col_volume)) if col_volume else float("nan"))

    if not dates:
        raise DataError(f"Empty CSV file: {path.as_posix()}")

    # Sort by date and drop duplicates (keep last)
    idx = np.argsort(np.array(dates))
    dates_sorted = [dates[i] for i in idx]
    open_arr = np.array([open_vals[i] for i in idx], dtype=float)
    high_arr = np.array([high_vals[i] for i in idx], dtype=float)
    low_arr = np.array([low_vals[i] for i in idx], dtype=float)
    close_arr = np.array([close_vals[i] for i in idx], dtype=float)
    adj_close_arr = np.array([adj_close_vals[i] for i in idx], dtype=float)
    volume_arr = np.array([volume_vals[i] for i in idx], dtype=float)

    dedup_dates: list[str] = []
    dedup_open: list[float] = []
    dedup_high: list[float] = []
    dedup_low: list[float] = []
    dedup_close: list[float] = []
    dedup_adj: list[float] = []
    dedup_vol: list[float] = []

    last_seen = None
    for i, d in enumerate(dates_sorted):
        if d != last_seen:
            dedup_dates.append(d)
            dedup_open.append(float(open_arr[i]))
            dedup_high.append(float(high_arr[i]))
            dedup_low.append(float(low_arr[i]))
            dedup_close.append(float(close_arr[i]))
            dedup_adj.append(float(adj_close_arr[i]))
            dedup_vol.append(float(volume_arr[i]))
            last_seen = d
        else:
            # overwrite previous (keep last)
            dedup_open[-1] = float(open_arr[i])
            dedup_high[-1] = float(high_arr[i])
            dedup_low[-1] = float(low_arr[i])
            dedup_close[-1] = float(close_arr[i])
            dedup_adj[-1] = float(adj_close_arr[i])
            dedup_vol[-1] = float(volume_arr[i])

    open_arr2 = np.array(dedup_open, dtype=float)
    high_arr2 = np.array(dedup_high, dtype=float)
    low_arr2 = np.array(dedup_low, dtype=float)
    close_arr2 = np.array(dedup_close, dtype=float)
    adj_arr2 = np.array(dedup_adj, dtype=float)
    vol_arr2 = np.array(dedup_vol, dtype=float)

    if not np.isfinite(close_arr2).any():
        raise DataError("CSV must contain a valid 'Close' column")

    return _enrich_records(dedup_dates, open_arr2, high_arr2, low_arr2, close_arr2, adj_arr2, vol_arr2)


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

    end = date.today()
    start = end - timedelta(days=365)
    dates: list[date] = []
    d = start
    while d <= end:
        # Business day (Mon-Fri)
        if d.weekday() < 5:
            dates.append(d)
        d += timedelta(days=1)

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

        # Build OHLC around close
        open_noise = np_rng.normal(0, 0.002, len(dates))
        open_ = np.empty_like(close)
        open_[0] = close[0]
        open_[1:] = close[:-1]
        open_ = open_ * (1 + open_noise)

        high = np.maximum(open_, close) * (1 + np_rng.uniform(0.0005, 0.01, len(dates)))
        low = np.minimum(open_, close) * (1 - np_rng.uniform(0.0005, 0.01, len(dates)))
        volume = np_rng.integers(2_000_000, 12_000_000, size=len(dates))

        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"])
            for i, dt in enumerate(dates):
                w.writerow(
                    [
                        dt.isoformat(),
                        float(open_[i]),
                        float(high[i]),
                        float(low[i]),
                        float(close[i]),
                        float(close[i]),
                        int(volume[i]),
                    ]
                )


def _read_csv(symbol: str) -> list[dict[str, Any]]:
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
        return list(cached[1])

    records = _read_csv_rows(path)
    _csv_cache[symbol] = (mtime, list(records))
    return records


def _read_csv_safe(symbol: str) -> list[dict[str, Any]]:
    """Best-effort CSV loader.

    Never raises; returns an empty DataFrame when data is missing/invalid.
    This is used by API endpoints that should respond with empty JSON instead
    of throwing errors.
    """

    try:
        return _read_csv(symbol)
    except DataError:
        return []


def _safe_json(obj: Any):
    """Force strict JSON output (no NaN/Infinity) by round-tripping with json."""
    return json.loads(json.dumps(obj, allow_nan=False, default=str))


def _df_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _safe_json(records)


def _summary_for(symbol: str, *, days: int = 30) -> dict[str, Any]:
    records = _read_csv_safe(symbol)
    if not records:
        raise DataError(f"No data for symbol: {symbol}")

    window = max(1, int(days))
    window_records = records[-window:]

    closes = np.array([r.get("close") for r in window_records], dtype=float)
    rets = np.array([r.get("daily_return") for r in window_records], dtype=float)
    closes = closes[np.isfinite(closes)]
    rets = rets[np.isfinite(rets)]

    avg_val = float(np.mean(closes)) if len(closes) else float("nan")
    vol_val = float(np.std(rets, ddof=1)) if len(rets) >= 2 else float("nan")

    avg_close = None if (isinstance(avg_val, float) and np.isnan(avg_val)) else avg_val
    vol = None if (isinstance(vol_val, float) and np.isnan(vol_val)) else vol_val

    # Compute range high/low from the filtered dataset so the UI updates with 30d/90d/1y.
    highs = np.array([r.get("high") for r in window_records], dtype=float)
    lows = np.array([r.get("low") for r in window_records], dtype=float)

    # Fall back to close if OHLC high/low are missing.
    if not np.isfinite(highs).any():
        highs = np.array([r.get("close") for r in window_records], dtype=float)
    if not np.isfinite(lows).any():
        lows = np.array([r.get("close") for r in window_records], dtype=float)

    highs = highs[np.isfinite(highs)]
    lows = lows[np.isfinite(lows)]
    range_high = float(np.max(highs)) if len(highs) else None
    range_low = float(np.min(lows)) if len(lows) else None

    return {
        "52_week_high": range_high,
        "52_week_low": range_low,
        "average_close": avg_close,
        "volatility": vol,
    }


def _compare(symbol1: str, symbol2: str, days: int) -> list[dict[str, Any]]:
    r1_all = _read_csv_safe(symbol1)
    r2_all = _read_csv_safe(symbol2)
    if not r1_all or not r2_all:
        return []

    r1 = r1_all[-days:]
    r2 = r2_all[-days:]
    k1 = _norm_symbol(symbol1)
    k2 = _norm_symbol(symbol2)

    m1 = {r["date"]: r.get("close") for r in r1 if r.get("date")}
    m2 = {r["date"]: r.get("close") for r in r2 if r.get("date")}

    dates = sorted(set(m1.keys()) & set(m2.keys()))
    out: list[dict[str, Any]] = []
    for d in dates:
        out.append({"date": d, k1: m1.get(d), k2: m2.get(d)})
    return out


def _top_movers() -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for sym in COMPANIES.keys():
        records = _read_csv_safe(sym)
        if not records:
            continue
        latest = records[-1]
        dr = latest.get("daily_return")
        if dr is None:
            continue
        items.append(
            {
                "symbol": sym,
                "date": latest.get("date"),
                "close": latest.get("close"),
                "daily_return": dr,
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

    cache_bust = (
        os.getenv("RENDER_GIT_COMMIT")
        or os.getenv("GITHUB_SHA")
        or os.getenv("COMMIT_SHA")
        or str(int(datetime.utcnow().timestamp()))
    )

    # Seed demo CSVs so the app works without manual data setup.
    try:
        _seed_sample_csvs()
    except Exception as exc:  # noqa: BLE001
        app.logger.exception("CSV seeding failed: %s", exc)

    @app.after_request
    def _no_cache(response):
        """Avoid stale responses (especially for local/dev and redeploys)."""
        p = request.path or ""
        if (
            p == "/companies"
            or p == "/compare"
            or p == "/top-gainers"
            or p == "/top-losers"
            or p.startswith("/data/")
            or p.startswith("/summary/")
        ):
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/")
    def index():
        return render_template("index.html", cache_bust=cache_bust)

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
        records = _read_csv_safe(sym)
        records = records[-days_int:] if records else records
        return jsonify(_df_records(records)) if records else jsonify([])

    @app.get("/summary/<path:symbol>")
    def summary(symbol: str):
        try:
            sym = _norm_symbol(symbol)
            if sym not in COMPANIES:
                return jsonify({"error": f"Unknown symbol: {sym}"}), 404
            days = request.args.get("days", default="30")
            try:
                days_int = int(days)
            except ValueError:
                return jsonify({"error": "Invalid 'days' query parameter"}), 400
            return jsonify(_safe_json(_summary_for(sym, days=days_int)))
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
        return jsonify(_df_records(merged)) if merged else jsonify([])

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


# Expose a module-level WSGI application for production servers (e.g. Render/Gunicorn).
# This keeps the entry point stable: `gunicorn app:app`.
app = create_app()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.getenv("FLASK_DEBUG") == "1" or os.getenv("DEBUG") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug, use_reloader=debug)
