with cuentas as (
    select * from {{ ref('inter_cuentas') }}
),

aggregated as ( 
    select
    application_uuid,
    count(*) as num_cuentas_abiertas,
from cuentas
where estado_cuenta = 'Open'
group by application_uuid
)

select * from aggregated