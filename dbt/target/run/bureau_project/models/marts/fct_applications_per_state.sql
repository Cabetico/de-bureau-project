
  
    

    create or replace table `dtc-de-340821`.`marts`.`fct_applications_per_state`
      
    
    

    OPTIONS()
    as (
      with source as (
    select * from `dtc-de-340821`.`intermediate`.`inter_applications_offices`
),

grouped as (
    select
    state,
    count(application_uuid) as N 
    from source
    group by state
)

select * from grouped
    );
  