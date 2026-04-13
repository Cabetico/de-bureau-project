-- dbt/models/staging/stg_domicilios.sql
-- One row per address per person.
-- Casts types, normalises nulls, and trims whitespace from free-text fields.
 

with source as (
    select * from {{ source('raw', 'domicilios') }}
),

renamed as (
    select
        folio_consulta,
        curp,
        rfc,
        domicilio_idx,
    
        -- address fields
        nullif(trim(direccion),            '') as direccion,
        nullif(trim(colonia_poblacion),    '') as colonia_poblacion,
        nullif(trim(delegacion_municipio), '') as delegacion_municipio,
        nullif(trim(ciudad),               '') as ciudad,
        nullif(trim(estado),               '') as estado,
        nullif(trim(cp),                   '') as codigo_postal,
    
        -- contact
        nullif(trim(numero_telefono), '') as numero_telefono,
    
        -- catalogues
        case tipo_domicilio
            when 'C' then 'Conocido'
            when 'P' then 'Previo'
            when 'N' then 'Nuevo'
            else tipo_domicilio
        end as tipo_domicilio,
    
        nullif(trim(tipo_asentamiento), '') as tipo_asentamiento,
    
        -- dates
        parse_date('%Y-%m-%d', nullif(fecha_residencia, '')) as fecha_residencia,
        parse_date('%Y-%m-%d', nullif(fecha_registro,   '')) as fecha_registro,
    
        -- dlt metadata
        -- _dlt_load_id,
        -- _dlt_id
    from source)

select * from renamed