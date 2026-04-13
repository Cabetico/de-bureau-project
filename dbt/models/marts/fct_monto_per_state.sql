with source as (
    select * from {{source('intermediate', 'inter_applications_offices')}}
),

grouped as (
    select 
    state,
    avg(monto_dispersion) as avg_monto_disperssion
    from source
    group by state
)

select * from grouped