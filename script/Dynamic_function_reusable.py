import psycopg2
from kiteconnect import KiteConnect
import pandas as pd
import datetime as dt
import os

cwd = os.chdir("C:\\Users\\VISHW\\Desktop\\Algorithemic_trading")
key_secret = open("api_key.txt", 'r').read().split()
api_key = key_secret[0]
access_token = key_secret[1]

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="Vishw#2004"
    )

def create_ohlc_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historical_ohlc (
            id SERIAL PRIMARY KEY,
            instrument BIGINT,
            datetime TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT
        );
    """)
    conn.commit()
    cursor.close()

def insert_data(conn, table_name, column_names, data):
    cursor = conn.cursor()
    if isinstance(data, dict):
        data = [data]
    columns = ", ".join(column_names)
    placeholders = ", ".join(["%s"] * len(column_names))
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    values_list = [tuple(row[col] for col in column_names) for row in data]
    cursor.executemany(sql, values_list)
    conn.commit()
    cursor.close()

def fetch_historical(kite, instrument_token, from_date, to_date, interval):
    df = pd.DataFrame(
        kite.historical_data(
            instrument_token,
            from_date,
            to_date,
            interval
        )
    )
    return df

def main():
    conn = get_connection()
    create_ohlc_table(conn)

    instruments = pd.DataFrame(kite.instruments("NSE"))
    symbol_token_map = dict(zip(instruments["tradingsymbol"], instruments["instrument_token"]))

    symbols = ["NIFTY", "RELIANCE", "BANKNIFTY"]

    from_date = dt.date.today() - dt.timedelta(days=20)
    to_date = dt.date.today()
    interval = "5minute"

    columns = ["instrument", "datetime", "open", "high", "low", "close", "volume"]

    for symbol in symbols:
        token = symbol_token_map[symbol]
        df = fetch_historical(kite, token, from_date, to_date, interval)
        if df.empty:
            continue

        records = []
        for _, row in df.iterrows():
            records.append({
                "instrument": symbol,   # <-- changed from token to symbol
                "datetime": row["date"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"]
            })

        insert_data(conn, "historical_ohlc", columns, records)
        print(f"{len(records)} candles inserted for {symbol}")

    conn.close()
    print("DONE")

if __name__ == "__main__":
    main()
