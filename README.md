# data226_lab2
# Data226 — Stock Transformations (Airflow + dbt + Snowflake + Preset)

End-to-end pipeline that:
- **Extracts** stock prices (ETL DAG)
- **Transforms** them with **dbt** (staging + indicators)
- **Stores** in **Snowflake**
- **Visualizes** in **Preset/Superset**

![System Diagram]
<img width="726" height="912" alt="image" src="https://github.com/user-attachments/assets/5cb1bece-0ebe-40d4-b788-fec971f77869" />


---

## Architecture

**Airflow**
- `stock_price_dag_etl_lab1`: yfinance → Pandas → load to `RAW.STOCK_PRICES`
- `BuildELT_dbt`: `dbt run` → build `ANALYTICS.STG_STOCK_PRICES` (view) & `ANALYTICS.FCT_STOCK_ABSTRACT` (table); `dbt test`; optional `dbt snapshot`

**Snowflake**
- `RAW.STOCK_PRICES`
- `ANALYTICS.STG_STOCK_PRICES` (view)
- `ANALYTICS.FCT_STOCK_ABSTRACT` (table with SMA7/14/30, RSI14, momentum_7d)
- `SNAPSHOT.FCT_STOCK_ABSTRACT_SNAPSHOT` (optional SCD2 via dbt snapshots)

**BI**
- Preset/Superset dataset: `ANALYTICS.FCT_STOCK_ABSTRACT`
- Charts: Price + SMA overlays, RSI(14), 7-day momentum

---

## Repo Layout

