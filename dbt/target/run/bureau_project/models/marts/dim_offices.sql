
  
    

    create or replace table `dtc-de-340821`.`marts`.`dim_offices`
      
    
    

    OPTIONS()
    as (
      select 
    office_id,
    state,
    municipality,
    office_name
from `dtc-de-340821`.`seeds`.`offices`
    );
  