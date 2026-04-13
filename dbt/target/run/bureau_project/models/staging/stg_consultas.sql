

  create or replace view `dtc-de-340821`.`staging`.`stg_consultas`
  OPTIONS()
  as -- dbt/models/staging/stg_consultas.sql
-- One row per credit inquiry per person.
-- Casts types, decodes catalogues, and normalises nulls.


with source as (
    select * from `dtc-de-340821`.`raw`.`consultas`
),


renamed as (
 
select
    folio_consulta,
    curp,
    rfc,
    consulta_idx,
 
    -- inquiry date
    parse_date('%Y-%m-%d', nullif(fecha_consulta, '')) as fecha_consulta,
 
    -- lender
    nullif(trim(nombre_otorgante),   '') as nombre_otorgante,
    nullif(trim(telefono_otorgante), '') as telefono_otorgante,
 
    -- credit type catalogue (same codes as cuentas)
    case tipo_credito
        when 'PP' then 'Préstamo Personal'
        when 'CF' then 'Crédito de Fomento / Gobierno'
        when 'TC' then 'Tarjeta de Crédito'
        when 'CP' then 'Crédito de Proveedores / Comercial'
        when 'HI' then 'Hipotecario'
        when 'AU' then 'Automotriz'
        when 'NM' then 'Nómina'
        when 'AB' then 'Arrendamiento / Bienes'
        when 'PE' then 'Préstamo Empresarial'
        when 'F'  then 'Fijo'
        when 'R'  then 'Revolvente'
        when 'H'  then 'Hipotecario'
        else nullif(trim(tipo_credito), '')
    end as tipo_credito,
 
    -- responsibility
    case tipo_responsabilidad
        when 'I' then 'Individual'
        when 'M' then 'Mancomunado'
        when 'O' then 'Obligado Solidario / Aval'
        else nullif(trim(tipo_responsabilidad), '')
    end as tipo_responsabilidad,
 
    -- amount
    cast(nullif(trim(importe_credito), '') as float64) as importe_credito,
 
    -- dlt metadata
    --_dlt_load_id,
    --_dlt_id
 
from source)

select * from renamed;

