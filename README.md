# Stock Data Intelligence Dashboard

Mini financial data platform built with **Python + Flask + pandas + yfinance + SQLite + Chart.js**.

## Features

- Fetches and stores daily stock OHLCV data for:
  - `INFY.NS`
  - `TCS.NS`
  - `RELIANCE.NS`
- Cleans data (missing values, date conversion, sorted by date)
- Calculates metrics:
  - Daily Return = (Close - Open) / Open
  - 7-day moving average of Close
  - 52-week high (rolling 252 trading days)
  - 52-week low (rolling 252 trading days)
  - Volatility = standard deviation of returns (rolling 30 trading days per-row; summary uses std dev over last 30 days)
- REST APIs for data, summary, comparisons
- Chart.js dashboard:
  - Company list (left)
  - Company chart + MA(7)
  - Filter buttons (30/90 days)
  - Compare two stocks chart
  - Summary metrics
  - Top gainer / top loser

## Setup

### 1) Create a virtual environment (recommended)

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2) Install dependencies

```powershell
pip install -r requirements.txt
```

### 3) Run the app

```powershell
python app.py
```

Open: http://127.0.0.1:5000

On first run, the app creates `stocks.db` and downloads data from Yahoo Finance.

## Project Structure

```
project/
  app.py
  data_fetcher.py
  requirements.txt
  README.md
  templates/
    index.html
  static/
    script.js
```

## API Documentation

### GET `/companies`
Returns the list of available companies.

Example response:

```json
[
  {"symbol": "INFY.NS", "name": "Infosys"},
  {"symbol": "TCS.NS", "name": "Tata Consultancy Services"},
  {"symbol": "RELIANCE.NS", "name": "Reliance Industries"}
]
```

### GET `/data/<symbol>`
Returns last 30 days by default.

Query params:
- `days` (optional, int): number of latest rows to return (e.g. 30 or 90)

Example:
- `/data/INFY.NS?days=30`

Example response (truncated):

```json
[
  {
    "date": "2026-03-03",
    "open": 1600.2,
    "high": 1622.5,
    "low": 1592.8,
    "close": 1618.1,
    "adj_close": 1618.1,
    "volume": 1234567,
    "daily_return": 0.0112,
    "ma7": 1609.4,
    "high_52w": 1820.3,
    "low_52w": 1350.0,
    "volatility": 0.0123
  }
]
```

### GET `/summary/<symbol>`
Returns 52-week high/low + average close + volatility.

Example response:

```json
{
  "52_week_high": 1820.3,
  "52_week_low": 1350.0,
  "average_close": 1609.4,
  "volatility": 0.012345
}
```

### GET `/compare?symbol1=&symbol2=&days=`
Compares closing prices for two symbols on matching dates.

Example:
- `/compare?symbol1=INFY.NS&symbol2=TCS.NS&days=90`

Example response (truncated):

```json
[
  {"date": "2026-01-02", "INFY.NS": 1588.2, "TCS.NS": 3980.4}
]
```

### GET `/top-gainers`
Returns the stock with the highest latest-day return.

Example response:

```json
{
  "symbol": "TCS.NS",
  "date": "2026-03-31",
  "close": 4010.5,
  "daily_return": 0.021
}
```

### GET `/top-losers`
Returns the stock with the lowest latest-day return.

Example response:

```json
{
  "symbol": "INFY.NS",
  "date": "2026-03-31",
  "close": 1592.2,
  "daily_return": -0.013
}
```

## Notes / Error Handling

- If Yahoo Finance is temporarily unavailable, the server still starts; APIs may return helpful errors until data is refreshed.
- If Yahoo Finance is blocked/rate-limited in your network, `fetch_stock_data()` will retry and then fall back to an **offline sample dataset** (last ~1 year of business days) so the dashboard remains usable for demos.
