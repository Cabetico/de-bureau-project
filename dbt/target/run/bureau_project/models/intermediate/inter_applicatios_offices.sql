

  create or replace view `dtc-de-340821`.`intermediate`.`inter_applicatios_offices`
  OPTIONS()
  as with offices as (
    select * from `dtc-de-340821`.`seeds`.`offices`
),

applications as (
    select * from `dtc-de-340821`.`staging`.`stg_applications`
),

enriched as (
    a.*,
    o.*
    from applications a
    left join offices o on a.office_id = o.office_id
)

select * from enriched;

