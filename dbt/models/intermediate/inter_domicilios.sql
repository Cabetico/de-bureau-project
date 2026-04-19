with domicilios as (
    select * from {{ ref('stg_domicilios') }}   
),

applications as (
    select * from {{ ref('stg_applications') }}
),

enriched as (
    select
        d.*,
        a.application_uuid,
        a.fecha_solicitud,
        a.monto_dispersion,
        a.archivo_xml,
        a.office_id
    from domicilios d
    left join applications a on d.curp = a.curp
)

select * from enriched