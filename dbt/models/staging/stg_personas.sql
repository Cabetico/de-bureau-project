with source as (
    select * from {{ source('raw', 'personas') }}
),


renamed as (

    SELECT
        folio_consulta,
        curp,
        rfc,
        nombres,
        apellido_paterno,
        apellido_materno,
        fecha_nacimiento,
        estado_civil,
        sexo,
        nacionalidad,
        cast(score_valor as int64)  as score_fico,
        score_razon_1,
        score_razon_2,
        score_razon_3,
        score_razon_4,
        archivo_origen,
        --_dlt_load_id,
        --_dlt_id
    from source
)

select * from renamed