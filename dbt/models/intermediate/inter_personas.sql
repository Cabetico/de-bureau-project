with personas as (
    select * from {{ ref('stg_personas') }}
),

applications as (
    select * from {{ ref('stg_applications') }}
),

enriched as (
    select
        p.*,
        a.application_uuid,
        a.fecha_solicitud,
        a.monto_dispersion,
        a.archivo_xml,
        a.office_id
    from personas p
    left join applications a on  p.curp = a.curp
)

select * from enriched