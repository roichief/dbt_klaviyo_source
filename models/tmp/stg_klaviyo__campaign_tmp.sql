{{ config(materialized='table') }}

with raw as (
  select
    cast(id as {{ dbt.type_string() }})                                       as id,
    get_json_object(attributes, '$.name')                                     as name,

    -- created can be nested or top-level depending on connector/version
    coalesce(
      try_cast(get_json_object(attributes, '$.created') as timestamp),
      try_cast(get_json_object(attributes, '$.created_at') as timestamp)
    )                                                                         as created,

    -- Airbyte shows updated_at in your table; fall back to 'updated' if ever present
    coalesce(
      try_cast(updated_at as timestamp),
      try_cast(updated    as timestamp)
    )                                                                         as updated,

    cast(null as {{ dbt.type_string() }})                                     as integration_id,
    cast(null as {{ dbt.type_string() }})                                     as integration_name,
    cast(null as {{ dbt.type_string() }})                                     as integration_category,

    false                                                                     as _fivetran_deleted
  from {{ source('klaviyo_source', var('klaviyo_campaign_identifier', 'campaigns')) }}
)

select * from raw
