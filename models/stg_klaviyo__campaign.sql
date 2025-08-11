-- models/stg_klaviyo__campaign.sql
-- Airbyte → parse JSON → expose columns expected by Fivetran transform package.

with base as (
  select * from {{ ref('stg_klaviyo__campaign_tmp') }}
),

attrs as (
  select
    id,

    -- JSON attributes on the Airbyte row
    get_json_object(attributes, '$.name')           as name,
    lower(get_json_object(attributes, '$.status'))  as status,
    get_json_object(attributes, '$.send_time')      as send_time,     -- ISO string
    get_json_object(attributes, '$.archived')       as archived,
    get_json_object(attributes, '$.scheduled_at')   as scheduled,

    -- normalized timestamps / extras carried from _tmp
    created,
    updated,
    estimated_recipient_count,
    campaign_messages,

    -- lineage / system
    cast(_fivetran_synced as {{ dbt.type_timestamp() }}) as _fivetran_synced,
    _airbyte_raw_id,
    _airbyte_meta,
    _airbyte_generation_id,

    -- compat soft-delete flag (Airbyte doesn't soft-delete)
    false as _fivetran_deleted,

    -- compat with fivetran_utils.source_relation()
    cast('' as {{ dbt.type_string() }}) as source_relation
  from base
),

/* Parse and explode campaign_messages (first message only, if any) */
messages as (
  select
    a.id as campaign_id,

    -- NOTE: we alias the exploded column as `msg`, so we can reference msg.attributes...
    msg.attributes.content.subject        as subject,
    msg.attributes.content.from_email     as from_email,
    msg.attributes.content.from_label     as from_name,
    msg.relationships.template.data.id    as email_template_id,

    -- tolerate empty/missing arrays using `get(...)`
    try_to_timestamp(
      get(transform(msg.attributes.send_times, x -> x.datetime), 1)
    ) as sent_at

  from attrs a
  lateral view outer explode(
    from_json(
      a.campaign_messages,
      'array<struct<
        type:string,
        id:string,
        attributes:struct<
          label:string,
          channel:string,
          content:struct<
            subject:string,
            preview_text:string,
            from_email:string,
            from_label:string,
            reply_to_email:string,
            cc_email:string,
            bcc_email:string
          >,
          send_times:array<struct<datetime:string,is_local:boolean>>,
          render_options:string,
          created_at:string,
          updated_at:string
        >,
        relationships:struct<
          campaign:struct<data:struct<type:string,id:string>>,
          template:struct<data:struct<type:string,id:string>>
        >
      >>'
    )
  ) lv as msg
),

-- If multiple messages exist, keep a single deterministic row
messages_dedup as (
  select *
  from (
    select
      *,
      row_number() over (
        partition by campaign_id
        order by email_template_id nulls last
      ) as rn
    from messages
  ) t
  where rn = 1
)

select
  -- optional Fivetran fields
  cast(null as {{ dbt.type_string() }})               as campaign_type,

  -- mapped / normalized fields
  cast(a.created as {{ dbt.type_timestamp() }})       as created_at,
  md.email_template_id,
  md.from_email,
  md.from_name,
  cast(a.id as {{ dbt.type_string() }})               as campaign_id,
  a.name                                              as campaign_name,
  a.send_time                                         as scheduled_to_send_at, -- keep as string if present
  md.sent_at,
  a.status                                            as status,
  cast(null as {{ dbt.type_string() }})               as status_id,
  md.subject,
  cast(a.updated as {{ dbt.type_timestamp() }})       as updated_at,
  try_cast(a.archived as boolean)                     as is_archived,
  a.scheduled                                         as scheduled_at,

  -- passthrough for unioning
  a.source_relation

from attrs a
left join messages_dedup md
  on a.id = md.campaign_id
where not coalesce(a._fivetran_deleted, false)
