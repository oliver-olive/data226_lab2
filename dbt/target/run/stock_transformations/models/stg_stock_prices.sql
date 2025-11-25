
  create or replace   view USER_DB_BOA.analytics.stg_stock_prices
  
   as (
    

with src as (
  select
    upper(symbol)            as symbol,
    cast(date as date)       as dt,
    cast(open as float)      as open,
    cast(high as float)      as high,
    cast(low  as float)      as low,
    cast(close as float)     as close,
    cast(volume as number)   as volume
  from USER_DB_BOA.RAW.stock_prices
  where symbol is not null
    and date   is not null
    and close  is not null
),
dedup as (
  select
    symbol, dt, open, high, low, close, volume,
    row_number() over (partition by symbol, dt order by dt desc) as rn
  from src
)
select
  symbol, dt, open, high, low, close, volume
from dedup
where rn = 1
  );

