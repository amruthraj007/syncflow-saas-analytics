
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select email_username
from "syncflow"."main"."stg_users"
where email_username is null



  
  
      
    ) dbt_internal_test