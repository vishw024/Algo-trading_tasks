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
INTERVAL = "5minute"
DAYS_TO_FETCH = 5

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# Insert data specifically for historical_data table
def insert_data(conn, data):
    cursor = conn.cursor()
    sql = """
    INSERT INTO historical_data
    (tradingsymbol, exchange, timestamp, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    values_list = [(row['tradingsymbol'], row['exchange'], row['timestamp'],
                    row['open'], row['high'], row['low'], row['close'], row['volume'])
                   for row in data]
    cursor.executemany(sql, values_list)
    conn.commit()
    cursor.close()

# Create historical_data table if not exists
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

# Fetch and store historical/intraday data
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

            insert_data(conn, data_to_insert)
            print(f"Data for {symbol} inserted.")
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    create_table_if_not_exists(conn)
    fetch_and_store_historical_data(conn, SYMBOLS, INTERVAL, DAYS_TO_FETCH)
    conn.close()
    print("All data fetched and stored successfully.")
