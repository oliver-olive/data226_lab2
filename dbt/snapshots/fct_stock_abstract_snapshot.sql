-- snapshots/fct_stock_abstract_snapshot.sql
{% snapshot fct_stock_abstract_snapshot %}
{{
  config(
    target_schema='SNAPSHOT',
    unique_key='snap_id',
    strategy='check',
    check_cols=['open','high','low','close','volume','sma_7','sma_14','sma_30','rsi_14','momentum_7d'],
    invalidate_hard_deletes=True
  )
}}
select
  hash(symbol, dt) as snap_id,
  symbol,
  dt,
  open, high, low, close, volume,
  sma_7, sma_14, sma_30,
  rsi_14,
  momentum_7d
from {{ ref('fct_stock_abstract') }}
{% endsnapshot %}

