
  
    

        create or replace transient table USER_DB_BOA.analytics.FCT_STOCK_ABSTRACT
         as
        (

with base as (
  select
    symbol,
    dt,
    open, high, low, close, volume,
    row_number() over (partition by symbol order by dt) as rn,
    lag(close) over (partition by symbol order by dt) as prev_close
  from USER_DB_BOA.analytics.stg_stock_prices
),
deltas as (
  select
    *,
    iff(prev_close is null, null, greatest(close - prev_close, 0)) as gain,
    iff(prev_close is null, null, greatest(prev_close - close, 0)) as loss
  from base
),
ma as (
  select
    *,
    avg(close) over (partition by symbol order by dt rows between 6 preceding and current row)  as sma_7,
    avg(close) over (partition by symbol order by dt rows between 13 preceding and current row) as sma_14,
    avg(close) over (partition by symbol order by dt rows between 29 preceding and current row) as sma_30,
    lag(close, 7) over (partition by symbol order by dt) as close_7
  from deltas
),
rsi as (
  select
    *,
    avg(gain) over (partition by symbol order by dt rows between 13 preceding and current row) as avg_gain_14,
    avg(loss) over (partition by symbol order by dt rows between 13 preceding and current row) as avg_loss_14
  from ma
)
select
  symbol,
  dt,
  open, high, low, close, volume,
  sma_7, sma_14, sma_30,
  case
    when avg_loss_14 = 0 then 100
    when avg_gain_14 = 0 then 0
    when avg_gain_14 is null or avg_loss_14 is null then null
    else 100 - 100 / (1 + (avg_gain_14 / avg_loss_14))
  end as rsi_14,
  iff(close_7 is null, null, (close / nullif(close_7, 0)) - 1) as momentum_7d
from rsi
qualify rn > 30
order by symbol, dt
        );
      
  