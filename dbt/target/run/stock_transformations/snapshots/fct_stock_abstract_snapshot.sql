
      begin;
    merge into "USER_DB_BOA"."SNAPSHOT"."FCT_STOCK_ABSTRACT_SNAPSHOT" as DBT_INTERNAL_DEST
    using "USER_DB_BOA"."SNAPSHOT"."FCT_STOCK_ABSTRACT_SNAPSHOT__dbt_tmp" as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ("SNAP_ID", "SYMBOL", "DT", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "SMA_7", "SMA_14", "SMA_30", "RSI_14", "MOMENTUM_7D", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
        values ("SNAP_ID", "SYMBOL", "DT", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "SMA_7", "SMA_14", "SMA_30", "RSI_14", "MOMENTUM_7D", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")

;
    commit;
  