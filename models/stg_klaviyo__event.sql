-- models/stg_klaviyo__event.sql

with base as (
  select * from {{ ref('stg_klaviyo__event_tmp') }}
),

parsed as (
  select
    id,

    -- metric/profile ids (prefer Airbyte's extracted columns; fall back to JSON)
    trim(
      coalesce(
        cast(metric_id as string),
        get_json_object(relationships, '$.metric.data.id')
      )
    ) as metric_id,
    trim(
      coalesce(
        cast(person_id as string),
        get_json_object(relationships, '$.profile.data.id')
      )
    ) as person_id,

    -- attributes
    get_json_object(attributes, '$.uuid') as uuid,

    try_to_timestamp(
      from_unixtime(try_cast(get_json_object(attributes, '$.timestamp') as bigint))
    ) as timestamp,
    try_to_timestamp(get_json_object(attributes, '$.datetime')) as datetime,

    -- attribution-ish
    get_json_object(attributes, '$.event_properties.$flow')    as flow_id,
    get_json_object(attributes, '$.event_properties.$message') as flow_message_id,
    coalesce(
      get_json_object(attributes, '$.event_properties.$campaign_id'),
      get_json_object(attributes, '$.event_properties.$campaign')
    ) as campaign_id,

    -- variation
    coalesce(
      get_json_object(attributes, '$.event_properties.$variation'),
      get_json_object(attributes, '$.event_properties.variation'),
      get_json_object(attributes, '$.event_properties._variation')
    ) as _variation,

    -- numeric-ish value (string for now)
    coalesce(
      get_json_object(attributes, '$.event_properties.$value'),
      get_json_object(attributes, '$.event_properties.value'),
      get_json_object(attributes, '$.event_properties.total'),
      get_json_object(attributes, '$.event_properties.price'),
      get_json_object(attributes, '$.event_properties.amount')
    ) as property_value,

    -- system / compat
    cast(_fivetran_synced as {{ dbt.type_timestamp() }}) as _fivetran_synced,
    false as _fivetran_deleted,

    cast('' as {{ dbt.type_string() }}) as source_relation
  from base
),

metrics as (
  select
    trim(metric_id) as metric_id,
    metric_name,
    source_relation
  from {{ ref('stg_klaviyo__metric') }}
),

joined as (
  select
    p._variation                                         as variation_id,
    cast(p.campaign_id as {{ dbt.type_string() }})       as campaign_id,
    cast(p.timestamp as {{ dbt.type_timestamp() }})       as occurred_at,
    cast(p.flow_id as {{ dbt.type_string() }})            as flow_id,
    p.flow_message_id,
    cast(p.id as {{ dbt.type_string() }})                 as event_id,
    cast(p.metric_id as {{ dbt.type_string() }})          as metric_id,
    cast(p.person_id as {{ dbt.type_string() }})          as person_id,

    -- ← Fill type from the metric table
    m.metric_name                                        as type,

    p.uuid,

    -- clean numeric value
    {{ klaviyo_source.remove_string_from_numeric('p.property_value') }} as numeric_value,

    cast(p._fivetran_synced as {{ dbt.type_timestamp() }}) as _fivetran_synced,
    p.source_relation
  from parsed p
  left join metrics m
    on trim(p.metric_id) = m.metric_id
   and coalesce(p.source_relation, '') = coalesce(m.source_relation, '')
  where not coalesce(p._fivetran_deleted, false)
),

final as (
  select
    *,
    cast({{ dbt.date_trunc('day', 'occurred_at') }} as date) as occurred_on,
    {{ dbt_utils.generate_surrogate_key(['event_id', 'source_relation']) }} as unique_event_id
  from joined
)

select * from final

