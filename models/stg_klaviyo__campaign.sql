-- models/stg_klaviyo__campaign.sql
-- Airbyte → parse JSON → expose columns expected by downstream transforms.

with base as (
  select * from {{ ref('stg_klaviyo__campaign_tmp') }}
),

attrs as (
  select
    id,

    -- attributes (JSON string)
    get_json_object(attributes, '$.name')         as name,
    lower(get_json_object(attributes, '$.status')) as status,
    get_json_object(attributes, '$.send_time')    as send_time,
    get_json_object(attributes, '$.archived')     as archived,
    get_json_object(attributes, '$.scheduled_at') as scheduled,

    -- normalized timestamps carried from _tmp
    created,
    updated,

    -- extra
    estimated_recipient_count,
    campaign_messages,

    -- system / compat
    cast(_fivetran_synced as {{ dbt.type_timestamp() }}) as _fivetran_synced,
    _airbyte_raw_id,
    _airbyte_meta,
    _airbyte_generation_id,
    false as _fivetran_deleted,

    -- keep for unioning
    cast('' as {{ dbt.type_string() }}) as source_relation
  from base
),

-- Pull message fields straight from JSON array's [0] element.
-- Using JSON paths avoids explode schema quirks across Databricks versions.
msg as (
  select
    a.id as campaign_id,

    -- subject / from fields (try both places these sometimes live)
    get_json_object(a.campaign_messages, '$[0].attributes.content.subject')      as subject,
    coalesce(
      get_json_object(a.campaign_messages, '$[0].attributes.content.from_email'),
      get_json_object(a.campaign_messages, '$[0].attributes.from_email')
    ) as from_email,
    coalesce(
      get_json_object(a.campaign_messages, '$[0].attributes.content.from_label'),
      get_json_object(a.campaign_messages, '$[0].attributes.from_label'),
      get_json_object(a.campaign_messages, '$[0].attributes.content.from_name'),
      get_json_object(a.campaign_messages, '$[0].attributes.from_name')
    ) as from_name,

    -- template id (when present)
    get_json_object(a.campaign_messages, '$[0].relationships.template.data.id')  as email_template_id,

    -- sent_at from first send_times entry (if present)
    try_to_timestamp(
      get_json_object(a.campaign_messages, '$[0].attributes.send_times[0].datetime')
    ) as sent_at_msg
  from attrs a
),

-- Fallback sent_at from events: earliest time we see activity tied to this campaign
sent_from_events as (
  select
    e.campaign_id,
    min(e.occurred_at) as sent_at_events
  from {{ ref('stg_klaviyo__event') }} e
  where e.campaign_id is not null
  group by 1
),

final as (
  select
    -- optional fields sometimes present in other loaders
    cast(null as {{ dbt.type_string() }})                     as campaign_type,

    cast(a.created as {{ dbt.type_timestamp() }})             as created_at,
    m.email_template_id,
    m.from_email,
    m.from_name,

    cast(a.id as {{ dbt.type_string() }})                     as campaign_id,
    a.name                                                    as campaign_name,

    -- this is the scheduled time (string in API) → timestamp
    try_to_timestamp(a.send_time)                             as scheduled_to_send_at,

    -- prefer message-provided send_time, fall back to earliest event time
    coalesce(m.sent_at_msg, sfe.sent_at_events)               as sent_at,

    a.status                                                  as status,

    -- derive a stable status_id (string) for downstream joins/filters
    case a.status
      when 'draft'     then '0'
      when 'scheduled' then '1'
      when 'sent'      then '2'
      when 'cancelled' then '3'
      else null
    end                                                      as status_id,

    m.subject,
    cast(a.updated as {{ dbt.type_timestamp() }})             as updated_at,
    try_cast(a.archived as boolean)                           as is_archived,
    try_to_timestamp(a.scheduled)                             as scheduled_at,
    a.source_relation
  from attrs a
  left join msg m
    on a.id = m.campaign_id
  left join sent_from_events sfe
    on cast(a.id as {{ dbt.type_string() }}) = sfe.campaign_id
  where not coalesce(a._fivetran_deleted, false)
)

select * from final
