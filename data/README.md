# CSV data files

Place one CSV per company symbol in this folder:

- `data/INFY.csv`
- `data/TCS.csv`
- `data/RELIANCE.csv`
- `data/HDFCBANK.csv`
- `data/ICICIBANK.csv`
- `data/SBIN.csv`
- `data/WIPRO.csv`
- `data/HCLTECH.csv`
- `data/LT.csv`
- `data/ITC.csv`

## Required columns

At minimum:

- `Date` (or `date`) — parseable by pandas
- `Close` (or `close`) — numeric

## Recommended columns

If present, these will be used (case-insensitive):

- `Open`, `High`, `Low`, `Adj Close`, `Volume`

## Metrics computed automatically

The app computes these from the CSV (no need to include them):

- `daily_return` (close % change)
- `ma7` (7-day moving average)
- `high_52w`, `low_52w` (rolling ~252 trading-day high/low)
- `volatility` (rolling 30-day std dev of `daily_return`)
