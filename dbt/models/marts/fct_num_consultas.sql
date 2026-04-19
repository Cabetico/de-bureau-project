with consultas as (
    select * from {{ ref('inter_consultas') }}
),

aggregated as ( 
    select
    application_uuid,
    count(*) as num_consultas,
from consultas
group by application_uuid
)


select * from aggregated