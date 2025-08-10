-- Airbyte → parse JSON → expose columns expected by Fivetran transform package.

with base as (
  select * from {{ ref('stg_klaviyo__event_tmp') }}
),

parsed as (
  select
    -- ids
    id,

    -- relationships (JSON)
    get_json_object(relationships, '$.metric.data.id')   as metric_id,
    get_json_object(relationships, '$.profile.data.id')  as person_id,

    -- attributes (JSON)
    get_json_object(attributes, '$.uuid')                as uuid,

    -- Klaviyo events often carry both ISO datetime and epoch seconds
    try_to_timestamp(
      from_unixtime(try_cast(get_json_object(attributes, '$.timestamp') as bigint))
    )                                                    as timestamp,
    try_to_timestamp(get_json_object(attributes, '$.datetime')) as datetime,

    -- attribution-ish fields from event_properties (when present)
    get_json_object(attributes, '$.event_properties.$flow')     as flow_id,
    get_json_object(attributes, '$.event_properties.$message')  as flow_message_id,
    coalesce(
      get_json_object(attributes, '$.event_properties.$campaign_id'),
      get_json_object(attributes, '$.event_properties.$campaign')
    )                                                    as campaign_id,

    -- variation (optional)
    coalesce(
      get_json_object(attributes, '$.event_properties.$variation'),
      get_json_object(attributes, '$.event_properties.variation'),
      get_json_object(attributes, '$.event_properties._variation')
    )                                                    as _variation,

    -- numeric-ish property (leave as string for numeric cleaning later)
    coalesce(
      get_json_object(attributes, '$.event_properties.$value'),
      get_json_object(attributes, '$.event_properties.value'),
      get_json_object(attributes, '$.event_properties.total'),
      get_json_object(attributes, '$.event_properties.price'),
      get_json_object(attributes, '$.event_properties.amount')
    )                                                    as property_value,

    -- system / compat
    cast(_airbyte_extracted_at as {{ dbt.type_timestamp() }}) as _fivetran_synced,
    false as _fivetran_deleted,

    {{ fivetran_utils.source_relation(
         union_schema_variable   = 'klaviyo_union_schemas',
         union_database_variable = 'klaviyo_union_databases'
    ) }} as source_relation
  from base
),

rename as (
  select 
    _variation                                           as variation_id,
    cast(campaign_id as {{ dbt.type_string() }} )        as campaign_id,
    cast(timestamp as {{ dbt.type_timestamp() }} )       as occurred_at,
    cast(flow_id as {{ dbt.type_string() }} )            as flow_id,
    flow_message_id,
    cast(id as {{ dbt.type_string() }} )                 as event_id,
    cast(metric_id as {{ dbt.type_string() }} )          as metric_id,
    cast(person_id as {{ dbt.type_string() }} )          as person_id,
    /* 'type' (metric name) not present in your Airbyte sample; keep null */
    cast(null as {{ dbt.type_string() }})                as type,
    uuid,
    {{ klaviyo_source.remove_string_from_numeric('property_value') }} as numeric_value,
    cast(_fivetran_synced as {{ dbt.type_timestamp() }} ) as _fivetran_synced,
    source_relation
    {{ fivetran_utils.fill_pass_through_columns('klaviyo__event_pass_through_columns') }}
  from parsed
  where not coalesce(_fivetran_deleted, false)
),

final as (
  select 
    *,
    cast( {{ dbt.date_trunc('day', 'occurred_at') }} as date) as occurred_on,
    {{ dbt_utils.generate_surrogate_key(['event_id', 'source_relation']) }} as unique_event_id
  from rename
)

select * from final;
