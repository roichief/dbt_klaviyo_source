-- Parse Airbyte JSON for Flows into columns expected by Fivetran's source spec.

with base as (
  select * 
  from {{ ref('stg_klaviyo__flow_tmp') }}
),

parsed as (
  select
    id,

    -- attributes JSON (string)
    get_json_object(attributes, '$.name')         as name,
    get_json_object(attributes, '$.status')       as status,
    try_to_timestamp(get_json_object(attributes, '$.created')) as created,
    try_to_timestamp(get_json_object(attributes, '$.updated')) as updated,
    try_cast(get_json_object(attributes, '$.archived') as boolean) as archived,
    get_json_object(attributes, '$.trigger_type') as trigger_type,

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
        staging_columns = get_flow_columns()
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
    created as created_at,
    cast(id as {{ dbt.type_string() }})  as flow_id,
    name  as flow_name,
    status,
    updated as updated_at,
    archived as is_archived,
    trigger_type,
    source_relation
  from fields
  where not coalesce(_fivetran_deleted, false)
)

select * from final;
