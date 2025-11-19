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

ATR_LENGTH = 14
ST1_LENGTH = 10
ST1_MULT = 3
ST2_LENGTH = 7
ST2_MULT = 3
ST3_LENGTH = 5
ST3_MULT = 2
ADX_LENGTH = 14
BB_LENGTH = 20
BB_STD = 2

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

conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT, database=DB_NAME,
    user=DB_USER, password=DB_PASS
)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS Tickdata_prime2 (
    id SERIAL PRIMARY KEY,
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
    boll_bnd_low DOUBLE PRECISION,
    UNIQUE (instrument, datetime)
)
""")
conn.commit()

print("Table 'Tickdata_prime2' is ready.")


# INDICATOR CALCULATION
def calculate_indicators(df):

    if len(df) < 20:
        df["atr"] = None
        df["st1"] = None
        df["st2"] = None
        df["st3"] = None
        df["adx"] = None
        df["boll_bnd_high"] = None
        df["boll_bnd_mid"] = None
        df["boll_bnd_low"] = None
        return df

    df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=ATR_LENGTH)

    st1 = ta.supertrend(df["high"], df["low"], df["close"], length=ST1_LENGTH, multiplier=ST1_MULT)
    df["st1"] = st1[f"SUPERT_{ST1_LENGTH}_{float(ST1_MULT)}"] if st1 is not None else None

    st2 = ta.supertrend(df["high"], df["low"], df["close"], length=ST2_LENGTH, multiplier=ST2_MULT)
    df["st2"] = st2[f"SUPERT_{ST2_LENGTH}_{float(ST2_MULT)}"] if st2 is not None else None

    st3 = ta.supertrend(df["high"], df["low"], df["close"], length=ST3_LENGTH, multiplier=ST3_MULT)
    df["st3"] = st3[f"SUPERT_{ST3_LENGTH}_{float(ST3_MULT)}"] if st3 is not None else None

    adx = ta.adx(df["high"], df["low"], df["close"], length=ADX_LENGTH)
    df["adx"] = adx[f"ADX_{ADX_LENGTH}"] if adx is not None else None

    bb = ta.bbands(df["close"], length=BB_LENGTH, std=BB_STD)
    if bb is not None:
        df["boll_bnd_high"] = bb[f"BBU_{BB_LENGTH}_{float(BB_STD)}"]
        df["boll_bnd_mid"] = bb[f"BBM_{BB_LENGTH}_{float(BB_STD)}"]
        df["boll_bnd_low"] = bb[f"BBL_{BB_LENGTH}_{float(BB_STD)}"]
    else:
        df["boll_bnd_high"] = None
        df["boll_bnd_mid"] = None
        df["boll_bnd_low"] = None

    return df


live_candles = {}
closed_candles = []


# NEW IMPORTANT UPDATE HERE — MINIMAL EDIT
def preload_last_50_candles():
    print("Preloading last 50 candles from DB or API...")

    for symbol in tickers:
        token = symbol_token_map[symbol]

        cur.execute("""
            SELECT datetime, open, high, low, close, volume
            FROM Tickdata_prime2
            WHERE instrument = %s
            ORDER BY datetime DESC
            LIMIT 50
        """, (symbol,))
        rows = cur.fetchall()

        if len(rows) < 50:
            print(f"{symbol}: Only {len(rows)} found → Fetching from API")

            today = dt.date.today()
            yesterday = today - dt.timedelta(days=1)

            hist = kite.historical_data(
                token, from_date=yesterday,
                to_date=today, interval="minute"
            )

            hist = hist[-50:]

            for c in hist:
                cur.execute("""
                    INSERT INTO Tickdata_prime2 (
                        instrument, datetime, open, high, low, close, volume
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (instrument, datetime) DO NOTHING
                """, (
                    symbol, c["date"], c["open"], c["high"],
                    c["low"], c["close"], c["volume"]
                ))
            conn.commit()
            print(f"{symbol}: Inserted missing candles.")

        # fetch again and calculate indicators
        cur.execute("""
            SELECT datetime, open, high, low, close, volume
            FROM Tickdata_prime2
            WHERE instrument = %s
            ORDER BY datetime ASC
        """, (symbol,))
        rows = cur.fetchall()

        df = pd.DataFrame(rows, columns=["date","open","high","low","close","volume"])
        df = calculate_indicators(df)

        # UPDATE DB with indicators
        for _, row in df.iterrows():
            cur.execute("""
                UPDATE Tickdata_prime2
                SET
                    atr=%s, st1=%s, st2=%s, st3=%s,
                    adx=%s, boll_bnd_high=%s, boll_bnd_mid=%s, boll_bnd_low=%s
                WHERE instrument=%s AND datetime=%s
            """, (
                row["atr"], row["st1"], row["st2"], row["st3"],
                row["adx"], row["boll_bnd_high"], row["boll_bnd_mid"], row["boll_bnd_low"],
                symbol, row["date"]
            ))

        conn.commit()
        print(f"{symbol}: Indicators updated for last 50 candles.")



def on_ticks(ws, ticks):
    print("Ticks:", ticks)
    
    global live_candles, closed_candles

    for tick in ticks:
        token = tick["instrument_token"]
        symbol = token_symbol_map[token]
        last_price = tick["last_price"]
        volume = tick.get("last_traded_quantity", 0)

        now = dt.datetime.now()
        minute = (now.minute // interval_minutes) * interval_minutes
        candle_time = now.replace(minute=minute, second=0, microsecond=0)

        if symbol not in live_candles or live_candles[symbol]["time"] != candle_time:

            if symbol in live_candles:
                closed_candles.append(live_candles[symbol])

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
    ws.set_mode(ws.MODE_FULL, tokens)
    ws.subscribe(tokens)


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
            FROM Tickdata_prime2
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

    combined_df.sort_values(by=["symbol","date"], inplace=True)

    final_list = []
    for symbol in tickers:
        df_symbol = combined_df[combined_df["symbol"] == symbol].copy()
        df_symbol = calculate_indicators(df_symbol)
        final_list.append(df_symbol)

    combined_df = pd.concat(final_list, ignore_index=True)

    final_rows = combined_df.merge(
        new_df[["date", "symbol"]],
        on=["date", "symbol"],
        how="inner"
    )

    for _, row in final_rows.iterrows():
        cur.execute("""
            INSERT INTO Tickdata_prime2 (
                instrument, datetime, open, high, low, close, volume,
                atr, st1, st2, st3, adx, boll_bnd_high, boll_bnd_mid, boll_bnd_low
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (instrument, datetime) DO NOTHING
        """, (
            row["symbol"], row["date"], row["open"], row["high"], row["low"],
            row["close"], row["volume"],
            row.get("atr"), row.get("st1"), row.get("st2"), row.get("st3"),
            row.get("adx"), row.get("boll_bnd_high"), row.get("boll_bnd_mid"), row.get("boll_bnd_low")
        ))

    conn.commit()
    closed_candles.clear()

    print(f"Inserted {len(final_rows)} candles")


preload_last_50_candles()

ws = KiteTicker(api_key, access_token)
ws.on_ticks = on_ticks
ws.on_connect = on_connect

ws.connect(threaded=True)
print("Streaming started...")

while True:
    process_closed_candles()
    time.sleep(5)
