
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cultura
from "datalake"."bronze"."culturas"
where cultura is null



  
  
      
    ) dbt_internal_test