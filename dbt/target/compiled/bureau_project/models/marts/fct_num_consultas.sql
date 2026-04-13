with consultas as (
    select * from `dtc-de-340821`.`intermediate`.`inter_consultas`
),

aggregated as ( 
    select
    application_uuid,
    count(*) as num_consultas,
from consultas
group by application_uuid
)


select * from aggregated