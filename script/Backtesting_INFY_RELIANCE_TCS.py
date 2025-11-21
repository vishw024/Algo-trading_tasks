import psycopg2
import pandas as pd


# 1. DATABASE CONNECTION
def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="Vishw#2004",
        port=5432
    )


# 2. FETCH 5-MIN OHLC DATA FOR ONE INSTRUMENT
def fetch_ohlc(symbol, conn):
    query = f"""
        SELECT timestamp, open, high, low, close, volume
        FROM historical_data
        WHERE tradingsymbol = '{symbol}'
        ORDER BY timestamp;
    """

    df = pd.read_sql(query, conn)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


# 3. PATTERN DETECTION FUNCTIONS
def is_doji(row, body_ratio=0.1):
    body = abs(row["close"] - row["open"])
    range_candle = row["high"] - row["low"]

    if range_candle == 0:
        return False

    return body <= (body_ratio * range_candle)



def is_bullish_engulfing(prev, curr):
    return (
        prev["close"] < prev["open"]
        and curr["close"] > curr["open"]
        and curr["open"] <= prev["close"]
        and curr["close"] >= prev["open"]
    )


def is_bearish_engulfing(prev, curr):
    return (
        prev["close"] > prev["open"]
        and curr["close"] < curr["open"]
        and curr["open"] >= prev["close"]
        and curr["close"] <= prev["open"]
    )


# GENERATE SIGNALS FOR ONE INSTRUMENT
def generate_signals(df, symbol):
    signals = []

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]

        timestamp = curr["timestamp"]
        price = curr["close"]

        # DOJI
        if is_doji(curr):
            signals.append({
                "timestamp": timestamp,
                "symbol": symbol,
                "pattern": "Doji",
                "signal": "NEUTRAL",
                "price": price
            })

        # BULLISH ENGULFING
        if is_bullish_engulfing(prev, curr):
            signals.append({
                "timestamp": timestamp,
                "symbol": symbol,
                "pattern": "Bullish Engulfing",
                "signal": "BUY",
                "price": price
            })

        # BEARISH ENGULFING
        if is_bearish_engulfing(prev, curr):
            signals.append({
                "timestamp": timestamp,
                "symbol": symbol,
                "pattern": "Bearish Engulfing",
                "signal": "SELL",
                "price": price
            })

    return pd.DataFrame(signals)


# RUN BACKTEST FOR MULTIPLE INSTRUMENTS
def run_backtest(symbols):
    conn = get_connection()
    all_reports = []

    for symbol in symbols:
        print(f"Processing {symbol}...")

        df = fetch_ohlc(symbol, conn)
        signals_df = generate_signals(df, symbol)

        all_reports.append(signals_df)

    conn.close()

    final_report = pd.concat(all_reports, ignore_index=True)
    return final_report


# MAIN
if __name__ == "__main__":
    instruments = ["INFY", "RELIANCE", "TCS"]

    report = run_backtest(instruments)

    print("\nFINAL REPORT")
    print(report)

    report.to_csv("backtest_signals_report.csv", index=False)
    print("\nSaved: backtest_signals_report.csv")
