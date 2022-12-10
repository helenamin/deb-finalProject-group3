{{
    config(
        materialized='table',
        database='hive_metastore',
        schema='dbt_gold'
    )
}}
select distinct
    CARRIERCODE,
    CARRIERNAME
from
    {{ ref('parts') }}
