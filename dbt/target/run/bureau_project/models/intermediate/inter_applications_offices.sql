

  create or replace view `dtc-de-340821`.`intermediate`.`inter_applications_offices`
  OPTIONS()
  as with offices as (
    select * from `dtc-de-340821`.`seeds`.`offices`
),
applications as (
    select * from `dtc-de-340821`.`staging`.`stg_applications`
),

enriched as (
    select 
    a.*,
    
    format_date('%Y%m', a.fecha_solicitud)
 as year_month,
    o.state,
    o.municipality,
    o.office_name,
    o.office_key
    from applications a
    left join offices o on a.office_id = o.office_id
)

select * from enriched;

