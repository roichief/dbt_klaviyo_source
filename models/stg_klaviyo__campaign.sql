
-- Parses Airbyte JSON into real columns FIRST, then lets the Fivetran macros
-- align to the expected staging contract for the transform package.

with base as (
  select * 
  from {{ ref('stg_klaviyo__campaign_tmp') }}
),

/* NEW: parse JSON (attributes) and the array (campaign_messages) */
parsed as (
  select
    -- ids
    id,

    -- from attributes JSON (string)
    get_json_object(attributes, '$.name')        as name,
    get_json_object(attributes, '$.status')      as status,
    get_json_object(attributes, '$.send_time')   as send_time,
    get_json_object(attributes, '$.archived')    as archived,
    get_json_object(attributes, '$.scheduled_at') as scheduled,

    -- keep your normalized timestamps
    created,
    updated,

    -- extra fields you already surface
    estimated_recipient_count,

    -- explode the first campaign message (if present) to get subject/from/template/sent_at
    m.attributes.content.subject                 as subject,
    m.attributes.content.from_email              as from_email,
    m.attributes.from_label                      as from_name,
    m.relationships.template.data.id             as email_template_id,

    -- best-effort sent_at from first send_times[0].datetime
    try_to_timestamp(
      element_at(transform(m.attributes.send_times, x -> x.datetime), 1)
    )                                            as sent_at,

    -- passthrough/system
    _airbyte_raw_id,
    _fivetran_synced,
    _airbyte_meta,
    _airbyte_generation_id,
    source_relation

  from base
  lateral view outer explode(
    from_json(
      campaign_messages,
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
  ) m
),

/* Let the Fivetran macro align to expected column names/types */
fields as (
  select
    {{
      fivetran_utils.fill_staging_columns(
        source_columns = adapter.get_columns_in_relation(ref('parsed')),
        staging_columns = get_campaign_columns()
      )
    }}
    {{ fivetran_utils.source_relation(
         union_schema_variable   = 'klaviyo_union_schemas',
         union_database_variable = 'klaviyo_union_databases'
    ) }}
  from parsed
),

final as (
  select
    campaign_type,
    created as created_at,
    email_template_id,
    from_email,
    from_name,
    cast(id as {{ dbt.type_string() }} ) as campaign_id,
    name as campaign_name,
    send_time as scheduled_to_send_at,
    sent_at,
    coalesce(status, lower(status_label)) as status,
    status_id,
    subject,
    updated as updated_at,
    archived as is_archived,
    scheduled as scheduled_at,
    source_relation
  from fields
  where not coalesce(_fivetran_deleted, false)
)

select * from final;

