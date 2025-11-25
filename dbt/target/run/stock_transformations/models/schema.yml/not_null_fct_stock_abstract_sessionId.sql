
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sessionId
from USER_DB_BOA.analytics.FCT_STOCK_ABSTRACT
where sessionId is null



  
  
      
    ) dbt_internal_test