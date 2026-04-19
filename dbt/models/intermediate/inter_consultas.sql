with consultas as (
    select * from {{ ref('stg_consultas') }}
),
applications as (
    select * from {{ ref('stg_applications') }}
),
enriched as (
    select
        c.*,
        a.application_uuid,
        a.fecha_solicitud,
        a.monto_dispersion,
        a.archivo_xml,
        a.office_id
    from consultas c
    left join applications a on c.curp = a.curp
)

select * from enriched