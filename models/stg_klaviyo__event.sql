-- Parse Airbyte JSON for Events into columns expected by Fivetran's source spec
-- before applying fill_staging_columns().

with base as (
  select * 
  from {{ ref('stg_klaviyo__event_tmp') }}
),

/* Pull relationships + attributes into concrete columns */
parsed as (
  select
    -- ids
    id,

    -- relationships
    get_json_object(relationships, '$.metric.data.id')  as metric_id,
    get_json_object(relationships, '$.profile.data.id') as person_id,

    -- event "type" (Fivetran uses metric name; we may backfill it later in transforms)
    null                                                as type,

    -- from attributes JSON (string)
    get_json_object(attributes, '$.uuid')               as uuid,

    -- timestamps:
    -- raw has both an ISO 'datetime' and an epoch-second 'timestamp' in attributes.
    -- Prefer epoch-second when present; keep iso as 'datetime' for completeness.
    try_to_timestamp(
      from_unixtime(try_cast(get_json_object(attributes, '$.timestamp') as bigint))
    )                                                   as timestamp,
    try_to_timestamp(
      get_json_object(attributes, '$.datetime')
    )                                                   as datetime,

    -- attribution-ish fields commonly present in event_properties (not guaranteed):
    -- $flow, $message, $campaign or $campaign_id, numeric value under various keys
    get_json_object(attributes, '$.event_properties.$flow')      as flow_id,
    get_json_object(attributes, '$.event_properties.$message')   as flow_message_id,
    coalesce(
      get_json_object(attributes, '$.event_properties.$campaign_id'),
      get_json_object(attributes, '$.event_properties.$campaign')
    )                                                   as campaign_id,

    -- variation sometimes appears with different keys; capture if present
    coalesce(
      get_json_object(attributes, '$.event_properties.$variation'),
      get_json_object(attributes, '$.event_properties.variation'),
      get_json_object(attributes, '$.event_properties._variation')
    )                                                   as _variation,

    -- property_value: try common locations/keys, leave as string for Fivetran macro to clean
    coalesce(
      get_json_object(attributes, '$.event_properties.$value'),
      get_json_object(attributes, '$.event_properties.value'),
      get_json_object(attributes, '$.event_properties.total'),
      get_json_object(attributes, '$.event_properties.price'),
      get_json_object(attributes, '$.event_properties.amount')
    )                                                   as property_value,

    -- passthrough/system
    cast(_airbyte_extracted_at as {{ dbt.type_timestamp() }}) as _fivetran_synced,
    _airbyte_raw_id,
    _airbyte_meta,
    _airbyte_generation_id

  from base
),

fields as (
  select
    {{
      fivetran_utils.fill_staging_columns(
        source_columns = adapter.get_columns_in_relation(ref('parsed')),
        staging_columns = get_event_columns()
      )
    }}
    {{ fivetran_utils.source_relation(
         union_schema_variable   = 'klaviyo_union_schemas',
         union_database_variable = 'klaviyo_union_databases'
    ) }}
  from parsed
),

rename as (
  select 
    _variation                                         as variation_id,
    cast(campaign_id as {{ dbt.type_string() }} )      as campaign_id,
    cast(timestamp as {{ dbt.type_timestamp() }} )     as occurred_at,
    cast(flow_id as {{ dbt.type_string() }} )          as flow_id,
    flow_message_id,
    cast(id as {{ dbt.type_string() }} )               as event_id,
    cast(metric_id as {{ dbt.type_string() }} )        as metric_id,
    cast(person_id as {{ dbt.type_string() }} )        as person_id,
    type,
    uuid,
    {{ klaviyo_source.remove_string_from_numeric('property_value') }} as numeric_value,
    cast(_fivetran_synced as {{ dbt.type_timestamp() }} ) as _fivetran_synced,
    source_relation
    {{ fivetran_utils.fill_pass_through_columns('klaviyo__event_pass_through_columns') }}
  from fields
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
