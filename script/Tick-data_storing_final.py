from kiteconnect import KiteConnect, KiteTicker
import pandas as pd
import pandas_ta as ta
import psycopg2
import datetime as dt
import time
import os

# CONFIG
cwd = os.chdir("C:\\Users\\VISHW\\Desktop\\Algorithemic_trading")
key_secret = open("api_key.txt",'r').read().split()
api_key = key_secret[0]
access_token = key_secret[1]

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "Vishw#2004"

tickers = ["INFY", "ACC", "ICICIBANK"]
interval_minutes = 1

# INIT KITE
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

instruments = pd.DataFrame(kite.instruments("NSE"))
symbol_token_map = dict(zip(instruments["tradingsymbol"], instruments["instrument_token"]))

for sym in tickers:
    if sym not in symbol_token_map:
        print(f"ERROR: Instrument not found for {sym}")
        exit()


tokens = [symbol_token_map[sym] for sym in tickers]

token_symbol_map = {symbol_token_map[sym]: sym for sym in tickers}

print("Tokens:", tokens)
print("Token-Symbol Map:", token_symbol_map)


conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS live_stream_1min_new_new (
    instrument TEXT,
    datetime TIMESTAMP,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    atr DOUBLE PRECISION,
    st1 DOUBLE PRECISION,
    st2 DOUBLE PRECISION,
    st3 DOUBLE PRECISION,
    adx DOUBLE PRECISION,
    boll_bnd_high DOUBLE PRECISION,
    boll_bnd_mid DOUBLE PRECISION,
    boll_bnd_low DOUBLE PRECISION
)
""")
conn.commit()

print("Table 'live_stream_1min_new_new' is ready.")


# INDICATOR CALCULATION (same as historical script)
def calculate_indicators(df):
    if len(df) < 20:  # Minimum for Bollinger + ATR + SuperTrend
        # Fill indicators with NaN
        df["atr"] = None
        df["st1"] = None
        df["st2"] = None
        df["st3"] = None
        df["adx"] = None
        df["boll_bnd_high"] = None
        df["boll_bnd_mid"] = None
        df["boll_bnd_low"] = None
        return df

    df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)

    st1 = ta.supertrend(df["high"], df["low"], df["close"], length=10, multiplier=3)
    df["st1"] = st1["SUPERT_10_3.0"] if st1 is not None else float('nan')

    st2 = ta.supertrend(df["high"], df["low"], df["close"], length=7, multiplier=3)
    df["st2"] = st2["SUPERT_7_3.0"] if st2 is not None else float('nan')

    st3 = ta.supertrend(df["high"], df["low"], df["close"], length=5, multiplier=2)
    df["st3"] = st3["SUPERT_5_2.0"] if st3 is not None else float('nan')

    adx = ta.adx(df["high"], df["low"], df["close"], length=14)
    df["adx"] = adx["ADX_14"] if adx is not None else float('nan')

    bb = ta.bbands(df["close"], length=20, std=2)
    if bb is not None:
        df["boll_bnd_high"] = bb["BBU_20_2.0"]
        df["boll_bnd_mid"] = bb["BBM_20_2.0"]
        df["boll_bnd_low"] = bb["BBL_20_2.0"]
    else:
        df["boll_bnd_high"] = float('nan')
        df["boll_bnd_mid"] = float('nan')
        df["boll_bnd_low"] = float('nan')

    return df



# GLOBAL DATA FOR CANDLE BUILDING
live_candles = {}     # building 1min_new_new candles
closed_candles = []   # completed candles


# STREAM CALLBACK: on_ticks
def on_ticks(ws, ticks):
    print("Ticks:", ticks)

    global live_candles, closed_candles

    for tick in ticks:
        token = tick["instrument_token"]
        symbol = token_symbol_map[token]
        last_price = tick["last_price"]
        volume = tick.get("last_traded_quantity", 0)

        # Determine 1-min candle time
        now = dt.datetime.now()
        minute = (now.minute // interval_minutes) * interval_minutes
        candle_time = now.replace(minute=minute, second=0, microsecond=0)

        # If new candle window starts
        if symbol not in live_candles or live_candles[symbol]["time"] != candle_time:

            # Close previous candle
            if symbol in live_candles:
                closed_candles.append(live_candles[symbol])

            # New candle
            live_candles[symbol] = {
                "time": candle_time,
                "symbol": symbol,
                "open": last_price,
                "high": last_price,
                "low": last_price,
                "close": last_price,
                "volume": volume
            }

        else:
            c = live_candles[symbol]
            c["high"] = max(c["high"], last_price)
            c["low"] = min(c["low"], last_price)
            c["close"] = last_price
            c["volume"] += volume


def on_connect(ws, response):
    print("WebSocket Connected. Subscribing...")
    ws.set_mode(ws.MODE_FULL, tokens)
    ws.subscribe(tokens)


# PROCESS CLOSED CANDLES → APPLY INDICATORS → INSERT DB
def process_closed_candles(force=False):
    global closed_candles, live_candles

    if force:
        for symbol, c in live_candles.items():
            closed_candles.append(c)
        live_candles.clear()

    if not closed_candles:
        return

    historical_df = pd.DataFrame()
    for symbol in tickers:
        cur.execute("""
            SELECT datetime, open, high, low, close, volume
            FROM live_stream_1min_new_new
            WHERE instrument = %s
            ORDER BY datetime DESC
            LIMIT 50
        """, (symbol,))
        rows = cur.fetchall()
        if rows:
            df = pd.DataFrame(rows, columns=["date","open","high","low","close","volume"])
            df["symbol"] = symbol
            historical_df = pd.concat([historical_df, df], ignore_index=True)

    new_df = pd.DataFrame(closed_candles)
    new_df.rename(columns={"time": "date"}, inplace=True)

    combined_df = pd.concat([historical_df, new_df], ignore_index=True)

    # Sort by date ONLY
    combined_df.sort_values(by=["symbol","date"], inplace=True)

    # --- FIXED INDICATOR CALCULATION PER SYMBOL ---
    final_list = []
    for symbol in tickers:
        df_symbol = combined_df[combined_df["symbol"] == symbol].copy()
        df_symbol = calculate_indicators(df_symbol)
        final_list.append(df_symbol)
    
    combined_df = pd.concat(final_list, ignore_index=True)

    # Filter only new candles
    final_rows = combined_df[
        combined_df["date"].isin(new_df["date"])
    ]

    # Insert each row
    for _, row in final_rows.iterrows():
        cur.execute("""
            INSERT INTO live_stream_1min_new_new (
                instrument, datetime, open, high, low, close, volume,
                atr, st1, st2, st3, adx, boll_bnd_high, boll_bnd_mid, boll_bnd_low
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row["symbol"], row["date"], row["open"], row["high"], row["low"],
            row["close"], row["volume"],
            row.get("atr"), row.get("st1"), row.get("st2"), row.get("st3"),
            row.get("adx"),
            row.get("boll_bnd_high"), row.get("boll_bnd_mid"), row.get("boll_bnd_low")
        ))

    conn.commit()
    closed_candles.clear()

    print(f"Inserted {len(final_rows)} candles")





# START STREAMING
ws = KiteTicker(api_key, access_token)
ws.on_ticks = on_ticks
ws.on_connect = on_connect

ws.connect(threaded=True)
print("Streaming started...")

# Loop every few seconds to check for completed candles
while True:
    process_closed_candles()
    time.sleep(5)
