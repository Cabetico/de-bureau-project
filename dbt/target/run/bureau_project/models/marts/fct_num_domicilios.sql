
  
    

    create or replace table `dtc-de-340821`.`marts`.`fct_num_domicilios`
      
    
    

    OPTIONS()
    as (
      with domicilios as (
    select * from `dtc-de-340821`.`intermediate`.`inter_domicilios`
),

aggregated as ( 
    select
    application_uuid,
    count(*) as num_domicilios,
from domicilios
group by application_uuid
)

select * from aggregated
    );
  