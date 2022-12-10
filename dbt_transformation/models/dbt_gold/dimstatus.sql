{{
    config(
        materialized='table',
        database='hive_metastore',
        schema='dbt_gold'
    )
}}
select *
from
    {{ ref('status') }}
