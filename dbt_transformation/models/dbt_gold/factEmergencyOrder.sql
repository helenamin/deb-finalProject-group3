{{
    config(
        materialized='table',
        database='hive_metastore',
        schema='dbt_gold'
    )
}}

select
    FACILITYNAME as DISTRIBUTION_CENTER,
    ORDERTYPE,
    sum(PENDINGITEMQUANTITY) as COUNT_OF_ITEMS
from
    {{ ref('parts') }}
where FACILITYNAME is not null
group by FACILITYNAME, ORDERTYPE
having ORDERTYPE = 'EMERGENCY'
order by COUNT_OF_ITEMS desc
limit 10
