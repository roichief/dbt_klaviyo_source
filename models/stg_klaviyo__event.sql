{{ config(materialized='table') }}

with base as (
  select * from {{ ref('stg_klaviyo__event_tmp') }}
),

/* Map campaign message -> campaign */
campaign_message_map as (
  select
    cast(c.id as string)                 as campaign_id,
    cast(c.source_relation as string)    as source_relation,
    m.id                                 as message_id
  from {{ ref('stg_klaviyo__campaign_tmp') }} c
  -- explode array<struct<type,id>> at relationships.messages.data
  lateral view outer inline(
    from_json(
      get_json_object(c.relationships, '$.messages.data'),
      'array<struct<type:string,id:string>>'
    )
  ) m
),

parsed as (
  select
    -- event id
    cast(id as string) as id,

    -- relationship ids if present
    coalesce(cast(metric_id as string), get_json_object(relationships, '$.metric.data.id'))  as metric_id,
    coalesce(cast(person_id as string), get_json_object(relationships, '$.profile.data.id')) as person_id,

    -- timestamps (epoch seconds or ISO)
    coalesce(
      try_to_timestamp(from_unixtime(try_cast(get_json_object(attributes, '$.timestamp') as bigint))),
      try_to_timestamp(get_json_object(attributes, '$.datetime')),
      cast(datetime as timestamp)
    ) as occurred_at,

    get_json_object(attributes, '$.uuid') as uuid,

    /* ---------- Attribution fields from attributes/properties ---------- */

    -- flow id variants
    coalesce(
      get_json_object(attributes, '$.event_properties.$flow'),
      get_json_object(attributes, '$.event_properties.$flow_id'),
      get_json_object(attributes, '$.event_properties.flow_id'),
      get_json_object(attributes, '$.properties.$flow'),
      get_json_object(attributes, '$.properties.$flow_id'),
      get_json_object(attributes, '$.flow.id'),
      get_json_object(attributes, '$.flow')
    ) as flow_id,

    -- message id seen on both flows and campaigns
    coalesce(
      get_json_object(attributes, '$.event_properties.$message'),
      get_json_object(attributes, '$.event_properties.$message_interaction'),
      get_json_object(attributes, '$.event_properties.$message_id'),
      get_json_object(attributes, '$.properties.$message'),
      get_json_object(attributes, '$.properties.$message_interaction'),
      get_json_object(attributes, '$.properties.$message_id'),
      get_json_object(attributes, '$.message.id'),
      get_json_object(attributes, '$.message')
    ) as message_id,

    -- direct campaign id if ever present (rare)
    coalesce(
      get_json_object(attributes, '$.event_properties.$campaign_id'),
      get_json_object(attributes, '$.event_properties.$campaign'),
      get_json_object(attributes, '$.event_properties.campaign_id'),
      get_json_object(attributes, '$.properties.$campaign_id'),
      get_json_object(attributes, '$.properties.$campaign'),
      get_json_object(attributes, '$.properties.campaign_id'),
      get_json_object(attributes, '$.email.campaign_id'),
      get_json_object(attributes, '$.campaign.id'),
      get_json_object(attributes, '$.campaign')
    ) as campaign_id_direct,

    -- variation id variants
    coalesce(
      get_json_object(attributes, '$.event_properties.$variation'),
      get_json_object(attributes, '$.event_properties.variation'),
      get_json_object(attributes, '$.event_properties._variation'),
      get_json_object(attributes, '$.properties.$variation'),
      get_json_object(attributes, '$.variation.id'),
      get_json_object(attributes, '$.variation')
    ) as variation_id,

    -- generic numeric value
    coalesce(
      get_json_object(attributes, '$.event_properties.$value'),
      get_json_object(attributes, '$.event_properties.value'),
      get_json_object(attributes, '$.event_properties.total'),
      get_json_object(attributes, '$.event_properties.price'),
      get_json_object(attributes, '$.event_properties.amount'),
      get_json_object(attributes, '$.properties.$value'),
      get_json_object(attributes, '$.properties.value'),
      get_json_object(attributes, '$.properties.total'),
      get_json_object(attributes, '$.properties.price'),
      get_json_object(attributes, '$.properties.amount')
    ) as property_value_raw,

    cast(_fivetran_synced as timestamp) as _fivetran_synced,
    cast(source_relation as string)     as source_relation
  from base
),

-- Attach metric name and map message -> campaign
enriched as (
  select
    p.variation_id,
    -- prefer direct campaign id, else map from message id
    coalesce(p.campaign_id_direct, cmm.campaign_id) as campaign_id,
    p.occurred_at,
    p.flow_id,
    p.message_id                                    as flow_message_id,
    p.id                                            as event_id,
    p.metric_id,
    p.person_id,
    m.metric_name                                   as type,
    p.uuid,
    cast(regexp_replace(cast(p.property_value_raw as string), '[^0-9.]*', '') as decimal(28,6)) as numeric_value,
    p._fivetran_synced,
    p.source_relation
  from parsed p
  left join {{ ref('stg_klaviyo__metric') }} m
    on p.metric_id = m.metric_id
   and coalesce(p.source_relation,'') = coalesce(m.source_relation,'')
  left join campaign_message_map cmm
    on p.message_id = cmm.message_id
   and coalesce(p.source_relation,'') = coalesce(cmm.source_relation,'')
),

final as (
  select
    variation_id,
    cast(campaign_id as string)      as campaign_id,
    cast(occurred_at as timestamp)   as occurred_at,
    cast(flow_id as string)          as flow_id,
    cast(flow_message_id as string)  as flow_message_id,
    cast(event_id as string)         as event_id,
    cast(metric_id as string)        as metric_id,
    cast(person_id as string)        as person_id,
    type,
    uuid,
    numeric_value,
    _fivetran_synced,
    source_relation,
    cast(date_trunc('day', occurred_at) as date) as occurred_on,
    md5(concat_ws('-', coalesce(event_id,'_null_'), coalesce(source_relation,'_null_'))) as unique_event_id
  from enriched
)

select * from final;
