{{ config(materialized='table') }}

with raw as (
  select
    cast(id as {{ dbt.type_string() }})                                      as id,
    get_json_object(attributes, '$.name')                                    as name,
    try_cast(get_json_object(attributes, '$.created') as timestamp)          as created,
    try_cast(updated as timestamp)                                           as updated,
    cast(null as {{ dbt.type_string() }})                                    as integration_id,
    cast(null as {{ dbt.type_string() }})                                    as integration_name,
    cast(null as {{ dbt.type_string() }})                                    as integration_category,
    false                                                                    as _fivetran_deleted
  from {{ source('klaviyo_source', var('klaviyo_campaign_identifier', 'campaigns')) }}
)

select * from raw
