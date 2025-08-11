{{ config(materialized='table') }}

-- Airbyte → parse JSON → expose columns expected by the Fivetran transforms.
with base as (
  select * from {{ ref('stg_klaviyo__event_tmp') }}
),

parsed as (
  select
    -- ids
    cast(id as string) as id,

    -- relationships (prefer normalized cols if present, else parse JSON)
    coalesce(cast(metric_id as string), get_json_object(relationships, '$.metric.data.id'))  as metric_id,
    coalesce(cast(person_id as string), get_json_object(relationships, '$.profile.data.id')) as person_id,

    -- attributes (JSON)
    get_json_object(attributes, '$.uuid') as uuid,

    -- occurred_at from epoch seconds OR ISO string OR the normalized column
    coalesce(
      try_to_timestamp(from_unixtime(try_cast(get_json_object(attributes, '$.timestamp') as bigint))),
      try_to_timestamp(get_json_object(attributes, '$.datetime')),
      cast(datetime as timestamp)
    ) as occurred_at,

    -- attribution-ish fields from event_properties (when present)
    get_json_object(attributes, '$.event_properties.$flow')     as flow_id,
    get_json_object(attributes, '$.event_properties.$message')  as flow_message_id,
    coalesce(
      get_json_object(attributes, '$.event_properties.$campaign_id'),
      get_json_object(attributes, '$.event_properties.$campaign')
    ) as campaign_id,

    -- variation (optional)
    coalesce(
      get_json_object(attributes, '$.event_properties.$variation'),
      get_json_object(attributes, '$.event_properties.variation'),
      get_json_object(attributes, '$.event_properties._variation')
    ) as variation_id,

    -- numeric-ish property (leave as string for numeric cleaning later)
    coalesce(
      get_json_object(attributes, '$.event_properties.$value'),
      get_json_object(attributes, '$.event_properties.value'),
      get_json_object(attributes, '$.event_properties.total'),
      get_json_object(attributes, '$.event_properties.price'),
      get_json_object(attributes, '$.event_properties.amount')
    ) as property_value_raw,

    -- system / compat
    cast(_fivetran_synced as timestamp) as _fivetran_synced,
    false as _fivetran_deleted,

    -- union helper
    cast(source_relation as string) as source_relation
  from base
),

rename as (
  select
    p.variation_id,
    cast(p.campaign_id as string)  as campaign_id,
    cast(p.occurred_at as timestamp) as occurred_at,
    cast(p.flow_id as string)      as flow_id,
    p.flow_message_id,
    cast(p.id as string)           as event_id,
    cast(p.metric_id as string)    as metric_id,
    cast(p.person_id as string)    as person_id,

    -- fill event type from the metric dimension
    m.metric_name                  as type,

    p.uuid,

    -- robust numeric parsing
    cast(regexp_replace(cast(p.property_value_raw as string), '[^0-9.]*', '') as decimal(28,6)) as numeric_value,

    p._fivetran_synced,
    p.source_relation
  from parsed p
  left join {{ ref('stg_klaviyo__metric') }} m
    on cast(p.metric_id as string) = m.metric_id
   and coalesce(p.source_relation, '') = coalesce(m.source_relation, '')
  where not coalesce(p._fivetran_deleted, false)
),

final as (
  select
    *,
    cast(date_trunc('day', occurred_at) as date) as occurred_on,
    md5(
      cast(
        concat(
          coalesce(cast(event_id as string), '_dbt_utils_surrogate_key_null_'),
          '-',
          coalesce(cast(source_relation as string), '_dbt_utils_surrogate_key_null_')
        ) as string
      )
    ) as unique_event_id
  from rename
)

select * from final;

