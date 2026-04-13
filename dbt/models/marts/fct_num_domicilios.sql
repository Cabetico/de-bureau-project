with domicilios as (
    select * from {{ source('intermediate', 'inter_domicilios') }}
),

aggregated as ( 
    select
    application_uuid,
    count(*) as num_domicilios,
from domicilios
group by application_uuid
)

select * from aggregated