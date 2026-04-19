with offices as (
    select * from {{ ref('offices') }}
),
applications as (
    select * from {{ ref('stg_applications') }}
),

enriched as (
    select 
    a.*,
    {{ year_month('a.fecha_solicitud') }} as year_month,
    o.state,
    o.municipality,
    o.office_name,
    o.office_key
    from applications a
    left join offices o on a.office_id = o.office_id
)

select * from enriched