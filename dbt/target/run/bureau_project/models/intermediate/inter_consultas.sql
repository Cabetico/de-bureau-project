

  create or replace view `dtc-de-340821`.`intermediate`.`inter_consultas`
  OPTIONS()
  as with consultas as (
    select * from `dtc-de-340821`.`staging`.`stg_consultas`
),
applications as (
    select * from `dtc-de-340821`.`staging`.`stg_applications`
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

select * from enriched;

