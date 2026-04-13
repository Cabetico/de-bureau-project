with source as (
    select * from {{source('intermediate', 'inter_applications_offices')}}
),

grouped as (
    select
    state,
    count(application_uuid) as N 
    from source
    group by state
)

select * from grouped

