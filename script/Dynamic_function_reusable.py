from kiteconnect import KiteConnect
import psycopg2
import datetime as dt
import pandas as pd
import os

# Config
cwd = os.chdir("C:\\Users\\VISHW\\Desktop\\Algorithemic_trading")
key_secret = open("api_key.txt",'r').read().split()
api_key = key_secret[0]
access_token = key_secret[1]

DB_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'Vishw#2004'
}

SYMBOLS = ["RELIANCE", "TCS", "INFY"]
INTERVAL = "5minute"  # can be 'minute', '5minute', 'day', etc.
DAYS_TO_FETCH = 5      # past N days for intraday

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

def insert_data(conn, table_name, column_names, data):
    cursor = conn.cursor()

    # Convert single dict → list of dicts
    if isinstance(data, dict):
        data = [data]

    # Build column list manually
    columns = ""
    for i, col in enumerate(column_names):
        columns += col
        if i != len(column_names) - 1:
            columns += ", "

    # Build placeholder list manually
    placeholders = ""
    for i in range(len(column_names)):
        placeholders += "%s"
        if i != len(column_names) - 1:
            placeholders += ", "

    # Create final SQL query
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    # Create values list manually
    values_list = []
    for row in data:
        row_values = []
        for col in column_names:
            row_values.append(row[col])
        values_list.append(tuple(row_values))

    cursor.executemany(sql, values_list)
    conn.commit()
    cursor.close()

def create_table_if_not_exists(conn):
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS historical_data (
        tradingsymbol TEXT,
        exchange TEXT,
        timestamp TIMESTAMP,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume BIGINT,
        PRIMARY KEY (tradingsymbol, exchange, timestamp)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()

def fetch_and_store_historical_data(conn, symbols, interval, days):
    instruments = pd.DataFrame(kite.instruments("NSE"))
    symbol_token_map = dict(zip(instruments["tradingsymbol"], instruments["instrument_token"]))

    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=days)

    for symbol in symbols:
        if symbol not in symbol_token_map:
            print(f"Symbol {symbol} not found. Skipping.")
            continue

        instrument_token = symbol_token_map[symbol]
        try:
            historical_data = kite.historical_data(
                instrument_token,
                start_date,
                end_date,
                interval
            )
            data_to_insert = [{
                'tradingsymbol': symbol,
                'exchange': 'NSE',
                'timestamp': row['date'],
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': row['volume']
            } for row in historical_data]

            insert_data(conn, 'historical_data',
                        ['tradingsymbol', 'exchange', 'timestamp', 'open', 'high', 'low', 'close', 'volume'],
                        data_to_insert)
            print(f"Data for {symbol} inserted.")
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    create_table_if_not_exists(conn)
    fetch_and_store_historical_data(conn, SYMBOLS, INTERVAL, DAYS_TO_FETCH)
    conn.close()
    print("All data fetched and stored successfully.")
