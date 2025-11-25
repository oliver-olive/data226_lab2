
    
    

select
    sessionId as unique_field,
    count(*) as n_records

from USER_DB_BOA.analytics.FCT_STOCK_ABSTRACT
where sessionId is not null
group by sessionId
having count(*) > 1


