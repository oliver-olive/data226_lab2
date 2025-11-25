import requests
import yfinance as yf
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.decorators import task
from snowflake.connector.pandas_tools import write_pandas
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def get_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn()

@task
def extract(ticker):
    stock_df = yf.download(ticker, period="180d", multi_level_index=False)
    stock_df.reset_index(inplace=True)
    return stock_df

@task
def transform(ticker, data):
    data['symbol'] = ticker
    data.rename(columns={
      'Date': 'date',
      'Open': 'open',
      'High': 'high',
      'Low': 'low',
      'Close': 'close',
      'Volume': 'volume'
    }, inplace=True)
    data['date'] = data['date'].dt.date
    stock_df = data[['symbol', 'date', 'open', 'close', 'low', 'high', 'volume']]
    return stock_df

@task
def load(records):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
      cur.execute("BEGIN;")
      cur.execute("USE DATABASE USER_DB_BOA")
      cur.execute("USE SCHEMA RAW")
      cur.execute("""
          CREATE TABLE IF NOT EXISTS stock_prices (
              symbol VARCHAR(10) NOT NULL,
              date DATE NOT NULL,
              open FLOAT,
              high FLOAT,
              low FLOAT,
              close FLOAT,
              volume BIGINT,
              PRIMARY KEY(symbol, date)
          );
      """)
      cur.execute("DELETE FROM stock_prices;")
      success, nchunks, nrows, _ = write_pandas(
          conn=conn,
          df=records,
          table_name="stock_prices",
          quote_identifiers=False
      )
      if success:
          print(f"Loaded {nrows} rows into STOCK_PRICES using {nchunks} chunk(s).")
      else:
          print("write_pandas failed to load data.")
      cur.execute("COMMIT;")
    except Exception as e:
      cur.execute("ROLLBACK;")
      print("Failed to load data:", e)
      raise
    finally:
      cur.close()
      conn.close()

with DAG(
    dag_id="stock_prices_dag_etl_lab1",
    start_date=datetime(2025, 11, 20),
    schedule_interval="0 9 * * *",
    catchup=False,
    tags=["ETL", "stocks"]
) as dag:
    symbols = Variable.get("symbols", deserialize_json=True)
    for symbol in symbols:
        raw_data = extract(symbol)
        transformed_data = transform(symbol, raw_data)
        load(transformed_data)

