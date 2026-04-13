with cuentas as (
    select * from `dtc-de-340821`.`staging`.`stg_cuentas`
),

applications as (
    select * from `dtc-de-340821`.`staging`.`stg_applications`
),

enriched as (
    select
        a.*,
        case when 
            a.fecha_cierre_cuenta is not null and a.fecha_cierre_cuenta < current_date() then 'Closed'
            else 'Open'
        end as estado_cuenta,
        case when
            a.saldo_vencido > 0 then 'Delinquent'
            else 'Current'
        end as estado_pago,
        case when
            a.saldo_actual > a.credito_maximo then 'Overlimit'
            else 'Within Limit'
        end as estado_limite,
        b.application_uuid,
        b.fecha_solicitud,
        b.monto_dispersion,
        b.archivo_xml,
        b.office_id

from cuentas a
left join applications b on a.curp = b.curp

)

select * from enriched