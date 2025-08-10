{{ config(materialized='table') }}

with raw as (
  select
    cast(id as {{ dbt.type_string() }})                                      as id,
    try_cast(datetime as timestamp)                                          as occurred_at,
    -- relationships JSON may vary (profile vs profiles[]); coalesce both patterns
    coalesce(
      get_json_object(relationships, '$.profile.data.id'),
      get_json_object(relationships, '$.profiles.data[0].id')
    )                                                                        as person_id,    -- aka profile_id
    get_json_object(relationships, '$.metric.data.id')                       as metric_id,
    get_json_object(relationships, '$.campaign.data.id')                     as campaign_id,
    get_json_object(relationships, '$.flow.data.id')                         as flow_id,
    get_json_object(relationships, '$.variation.data.id')                    as variation_id,

    attributes                                                               as event_properties_raw,
    try_cast(get_json_object(attributes, '$.value') as double)               as property_value,  -- numeric value if present

    -- These satisfy the generic column set expected upstream
    cast(null as {{ dbt.type_string() }})                                    as integration_id,
    cast(null as {{ dbt.type_string() }})                                    as integration_name,
    cast(null as {{ dbt.type_string() }})                                    as integration_category,
    false                                                                    as _fivetran_deleted
  from {{ source('klaviyo_source', var('klaviyo_event_identifier', 'events')) }}
)

select * from raw
