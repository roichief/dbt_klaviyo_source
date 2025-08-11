{{ config(materialized='table') }}

-- stg_klaviyo__event.sql
-- Inline campaign attribution (no separate helper model).
-- Safely appends campaign_name, campaign_subject, campaign_type.

with
/* ------------------------------------------------------------------ */
/* 0) RAW EVENTS                                                       */
/* ------------------------------------------------------------------ */
base as (
  select * from {{ ref('stg_klaviyo__event_tmp') }}
),

/* ------------------------------------------------------------------ */
/* 1) INLINE message_id -> campaign_id map from campaign_tmp           */
/*    (pulled from both relationships.campaign-messages and the        */
/*     expanded campaign_messages array that includes channel/subject) */
/* ------------------------------------------------------------------ */
cmp_src as (
  select
    cast(id as string)                as campaign_id,
    cast(source_relation as string)   as source_relation,
    cast(relationships as string)     as relationships_json,
    cast(campaign_messages as string) as campaign_messages_json
  from {{ ref('stg_klaviyo__campaign_tmp') }}
),

-- A) message ids from relationships."campaign-messages".data[]
cmp_rel_msgs as (
  select
    s.campaign_id,
    s.source_relation,
    cast(rm.id as string) as message_id,
    null                  as message_channel,
    null                  as message_subject
  from cmp_src s
  lateral view outer explode(
    from_json(
      get_json_object(s.relationships_json, '$."campaign-messages".data'),
      'array<struct<type:string,id:string>>'
    )
  ) e as rm
),

-- B) message ids (with channel/subject) from campaign_messages[]
cmp_cm_msgs as (
  select
    s.campaign_id,
    s.source_relation,
    cast(cm.id as string)                                                           as message_id,
    coalesce(cast(cm.attributes.channel as string), cast(cm.attributes.CHANNEL as string)) as message_channel,
    coalesce(
      cast(cm.attributes.content.subject as string),
      cast(cm.attributes.CONTENT.SUBJECT as string)
    )                                                                              as message_subject
  from cmp_src s
  lateral view outer explode(
    from_json(
      s.campaign_messages_json,
      'array<struct<
         type:string,
         id:string,
         attributes:struct<
           label:string,
           channel:string,
           content:struct<subject:string,preview_text:string,from_email:string,from_label:string,reply_to_email:string,cc_email:string,bcc_email:string>,
           send_times:array<struct<datetime:string,is_local:boolean>>,
           render_options:map<string,string>
         >,
         relationships:struct<
           campaign:struct<data:struct<type:string,id:string>>,
           template:struct<data:struct<type:string,id:string>>
         >
       >>'
    )
  ) e as cm
),

cmp_union as (
  select * from cmp_rel_msgs
  union all
  select * from cmp_cm_msgs
),

cmp_msg_map as (
  select
    campaign_id,
    source_relation,
    message_id,
    max(message_channel) as message_channel,
    max(message_subject) as message_subject
  from cmp_union
  where message_id is not null
  group by 1,2,3
),

/* ------------------------------------------------------------------ */
/* 2) CAMPAIGNS + CHANNEL PER CAMPAIGN                                */
/* ------------------------------------------------------------------ */
camp as (
  select
    cast(campaign_id as string)             as campaign_id,
    cast(source_relation as string)         as source_relation,
    cast(campaign_name as string)           as campaign_name,
    cast(subject as string)                 as campaign_subject,
    cast(created_at as timestamp)           as created_at,
    cast(scheduled_to_send_at as timestamp) as scheduled_to_send_at,
    cast(sent_at as timestamp)              as sent_at
  from {{ ref('stg_klaviyo__campaign') }}
),

camp_channel as (
  select
    campaign_id,
    source_relation,
    min(lower(message_channel)) as campaign_channel
  from cmp_msg_map
  where message_channel is not null
  group by 1,2
),

/* ------------------------------------------------------------------ */
/* 3) METRIC NAMES (unchanged)                                        */
/* ------------------------------------------------------------------ */
metric as (
  select
    cast(metric_id as string)  as metric_id,
    cast(metric_name as string) as metric_name,
    cast(source_relation as string) as source_relation
  from {{ ref('stg_klaviyo__metric') }}
),

/* ------------------------------------------------------------------ */
/* 4) PARSE EVENTS (robust token extraction, no breaking changes)      */
/* ------------------------------------------------------------------ */
parsed as (
  select
    cast(id as string)                                   as event_id,
    cast(metric_id as string)                            as metric_id,
    cast(person_id as string)                            as person_id,

    coalesce(
      try_to_timestamp(get_json_object(attributes, '$.datetime')),
      try_to_timestamp(from_unixtime(try_cast(get_json_object(attributes, '$.timestamp') as bigint))),
      cast(datetime as timestamp)
    )                                                    as occurred_at,

    get_json_object(attributes, '$.uuid')                as uuid,

    -- flow identifier (keep campaign null if flow present)
    coalesce(
      get_json_object(attributes, '$.event_properties.$flow'),
      get_json_object(attributes, '$.event_properties.$flow_id'),
      get_json_object(attributes, '$.event_properties.flow_id'),
      get_json_object(attributes, '$.properties.$flow'),
      get_json_object(attributes, '$.properties.$flow_id'),
      get_json_object(attributes, '$.flow.id'),
      get_json_object(attributes, '$.flow')
    )                                                    as flow_id,

    -- message token: sometimes a message_id, sometimes actually the campaign_id
    coalesce(
      get_json_object(attributes, '$.event_properties.$message'),
      get_json_object(attributes, '$.event_properties.$message_interaction'),
      get_json_object(attributes, '$.event_properties.$message_id'),
      get_json_object(attributes, '$.properties.$message'),
      get_json_object(attributes, '$.properties.$message_interaction'),
      get_json_object(attributes, '$.properties.$message_id'),
      get_json_object(attributes, '$.message.id'),
      get_json_object(attributes, '$.message')
    )                                                    as message_token,

    -- direct campaign id if present
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
    )                                                    as campaign_id_direct,

    -- fallbacks used for matching
    get_json_object(attributes, '$.event_properties.Subject')                 as subject_raw,
    get_json_object(attributes, '$.event_properties["Campaign Name"]')       as campaign_name_raw,

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
    )                                                    as property_value_raw,

    cast(_fivetran_synced as timestamp)                  as _fivetran_synced,
    cast(source_relation as string)                      as source_relation
  from base
),

/* ------------------------------------------------------------------ */
/* 5) ENRICH + DERIVE campaign_id for NON-FLOW events                  */
/* ------------------------------------------------------------------ */
enriched as (
  select
    p.*,
    m.metric_name as type,

    case
      when p.flow_id is not null then null
      else coalesce(
        -- 0) explicit campaign id
        p.campaign_id_direct,

        -- 1) $message -> campaign_message -> campaign
        (select mm.campaign_id
           from cmp_msg_map mm
          where mm.message_id = p.message_token
            and coalesce(mm.source_relation,'') = coalesce(p.source_relation,'')
          limit 1
        ),

        -- 2) $message is actually a campaign_id (observed in your samples)
        (select c.campaign_id
           from camp c
          where c.campaign_id = p.message_token
            and coalesce(c.source_relation,'') = coalesce(p.source_relation,'')
          limit 1
        ),

        -- 3) fallback by campaign name
        (select c.campaign_id
           from camp c
          where coalesce(c.campaign_name,'') = coalesce(p.campaign_name_raw,'')
            and coalesce(c.source_relation,'') = coalesce(p.source_relation,'')
          limit 1
        ),

        -- 4) fallback by subject within ±7 days of send/scheduled
        (select c.campaign_id
           from camp c
          where coalesce(c.campaign_subject,'') = coalesce(p.subject_raw,'')
            and coalesce(c.source_relation,'') = coalesce(p.source_relation,'')
            and p.occurred_at between coalesce(c.sent_at, c.scheduled_to_send_at, c.created_at) - interval 7 days
                                 and coalesce(c.sent_at, c.scheduled_to_send_at, c.created_at) + interval 7 days
          limit 1
        )
      )
    end as derived_campaign_id
  from parsed p
  left join metric m
    on p.metric_id = m.metric_id
   and coalesce(p.source_relation,'') = coalesce(m.source_relation,'')
),

/* ------------------------------------------------------------------ */
/* 6) FINAL SHAPE (existing cols unchanged + appended campaign attrs)  */
/* ------------------------------------------------------------------ */
final as (
  select
    -- Existing columns (preserved)
    cast(null as string)                                     as variation_id,
    cast(derived_campaign_id as string)                      as campaign_id,
    cast(occurred_at as timestamp)                           as occurred_at,
    cast(flow_id as string)                                  as flow_id,
    cast(message_token as string)                            as flow_message_id,
    cast(event_id as string)                                 as event_id,
    cast(metric_id as string)                                as metric_id,
    cast(person_id as string)                                as person_id,
    type,
    uuid,
    cast(regexp_replace(cast(property_value_raw as string), '[^0-9.]*', '') as decimal(28,6)) as numeric_value,
    _fivetran_synced,
    source_relation,
    cast(date_trunc('day', occurred_at) as date)             as occurred_on,
    md5(concat_ws('-', coalesce(event_id,'_null_'), coalesce(source_relation,'_null_'))) as unique_event_id,

    -- New appended columns (safe to add)
    c.campaign_name                                          as campaign_name,
    c.campaign_subject                                       as campaign_subject,
    coalesce(
      case
        when ch.campaign_channel in ('email','sms') then ch.campaign_channel
        else null
      end,
      case
        when lower(type) like '%sms%' then 'sms'
        else 'email'
      end
    )                                                        as campaign_type

  from enriched e
  left join camp c
    on e.derived_campaign_id = c.campaign_id
   and coalesce(e.source_relation,'') = coalesce(c.source_relation,'')
  left join camp_channel ch
    on c.campaign_id = ch.campaign_id
   and coalesce(c.source_relation,'') = coalesce(ch.source_relation,'')
)

select * from final;
