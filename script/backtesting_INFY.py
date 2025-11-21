import pandas as pd
import psycopg2

#  CONNECT TO POSTGRES
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Vishw#2004"
)

#  FETCH HISTORICAL OHLC DATA
query = """
SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM historical_data
WHERE tradingsymbol = 'INFY'
ORDER BY timestamp ASC;
"""

df = pd.read_sql(query, conn)

# Convert timestamp column
df["timestamp"] = pd.to_datetime(df["timestamp"])


#  Detect Doji Pattern
def is_doji(row, threshold=0.1):
    body = abs(row["close"] - row["open"])
    range_candle = row["high"] - row["low"]

    if range_candle == 0:
        return False

    return body <= (threshold * range_candle)


#  Detect Engulfing Patterns
def is_bullish_engulfing(prev, curr):
    return (
        prev["close"] < prev["open"] and
        curr["close"] > curr["open"] and
        curr["open"] <= prev["close"] and
        curr["close"] >= prev["open"]
    )

def is_bearish_engulfing(prev, curr):
    return (
        prev["close"] > prev["open"] and
        curr["close"] < curr["open"] and
        curr["open"] >= prev["close"] and
        curr["close"] <= prev["open"]
    )


#  Generate Signals
signals = []

for i in range(1, len(df)):
    prev = df.iloc[i-1]
    curr = df.iloc[i]

    # DOJI
    if is_doji(curr):
        signals.append({
            "timestamp": curr["timestamp"],
            "pattern": "Doji",
            "signal": "Neutral",
            "price": curr["close"]
        })

    # BULLISH ENGULFING → BUY
    if is_bullish_engulfing(prev, curr):
        signals.append({
            "timestamp": curr["timestamp"],
            "pattern": "Bullish Engulfing",
            "signal": "BUY",
            "price": curr["close"]
        })

    # BEARISH ENGULFING → SELL
    if is_bearish_engulfing(prev, curr):
        signals.append({
            "timestamp": curr["timestamp"],
            "pattern": "Bearish Engulfing",
            "signal": "SELL",
            "price": curr["close"]
        })


#  Save Backtest Report
report = pd.DataFrame(signals)
report.to_csv("backtest_report.csv", index=False)

print("Backtest report generated: backtest_report.csv")
