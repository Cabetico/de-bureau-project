with source as (
    select * from `dtc-de-340821`.`raw`.`applications`
),

renamed as (
select
    cast(application_uuid as string) as application_uuid,
    cast(fecha_solicitud as date) as fecha_solicitud,
    cast(monto_dispersion as float64) as monto_dispersion,
    cast(curp as string) as curp,
    cast(archivo_xml as string) as archivo_xml,
    cast(office_id as string) as office_id,
    --cast(_dlt_load_id as string) as _dlt_load_id,
    --cast(_dlt_id as string) as _dlt_id
    from source
)

select * from renamed