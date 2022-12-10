{{
    config(
        materialized='table',
        database='hive_metastore',
        schema='dbt_silver'
    )
}}
select
    cast(sequence as int) as sequence,
    trim(status) as status
from
    {{ source('dbt_bronze','statuses') }}
