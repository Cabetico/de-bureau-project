with domicilios as (
    select * from {{ ref('inter_domicilios') }}
),

aggregated as ( 
    select
    application_uuid,
    count(*) as num_domicilios,
from domicilios
group by application_uuid
)

select * from aggregated