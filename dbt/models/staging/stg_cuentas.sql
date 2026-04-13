with source as (
    select * from {{ source('raw', 'cuentas') }}
),

renamed as (
    select
        folio_consulta,
        curp,
        rfc,
        cuenta_idx,
        nombre_otorgante,
        tipo_credito,
        tipo_cuenta,
        tipo_responsabilidad,
        cast(credito_maximo       as float64) as credito_maximo,
        cast(saldo_actual         as float64) as saldo_actual,
        cast(saldo_vencido        as float64) as saldo_vencido,
        cast(peor_atraso          as int64)   as peor_atraso,
        cast(numero_pagos         as int64)   as numero_pagos,
        cast(numero_pagos_vencidos as int64)  as numero_pagos_vencidos,
        historico_pagos,
        pago_actual,
        clave_prevencion,
        parse_date('%Y-%m-%d', fecha_apertura_cuenta) as fecha_apertura_cuenta,
        parse_date('%Y-%m-%d', fecha_ultimo_pago)      as fecha_ultimo_pago,
        parse_date('%Y-%m-%d', fecha_cierre_cuenta)    as fecha_cierre_cuenta
        --_dlt_load_id,
        --_dlt_id
    from source)


select * from renamed
