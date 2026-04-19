with source as (
    select * from {{ref('inter_applications_offices')}}
),

grouped as (
    select 
    state,
    year_month,
    count(application_uuid) as N
    from source
    group by state, year_month
    order by state, year_month
)

select * from grouped