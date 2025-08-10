{{ config(materialized='table') }}

-- Airbyte raw -> normalized columns that the upstream staging (stg_klaviyo__metric.sql)
-- expects to find in stg_klaviyo__metric_tmp

with raw as (
  select
    -- canonical id & names
    cast(id as {{ dbt.type_string() }})                                     as id,
    lower(get_json_object(attributes, '$.name'))                             as name,

    -- timestamps (Airbyte keeps 'updated' top-level; 'created' lives in attributes)
    try_cast(get_json_object(attributes, '$.created') as timestamp)          as created,
    try_cast(updated as timestamp)                                           as updated,

    -- Fivetran destination usually has integration* fields; Airbyte doesn't → null out
    cast(null as {{ dbt.type_string() }})                                    as integration_id,
    cast(null as {{ dbt.type_string() }})                                    as integration_name,
    cast(null as {{ dbt.type_string() }})                                    as integration_category,

    -- Fivetran soft-delete flag doesn't exist in Airbyte → false
    false                                                                    as _fivetran_deleted
  from {{ source('klaviyo_source', var('klaviyo_metric_identifier', 'metrics')) }}
)

select * from raw
