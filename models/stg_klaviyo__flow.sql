-- Airbyte → parse JSON → expose columns expected by Fivetran transform package.

with base as (
  select * from {{ ref('stg_klaviyo__flow_tmp') }}
),

parsed as (
  select
    id,
    try_to_timestamp(get_json_object(attributes, '$.created')) as created,
    get_json_object(attributes, '$.name')         as name,
    get_json_object(attributes, '$.status')       as status,
    try_to_timestamp(get_json_object(attributes, '$.updated')) as updated,
    try_cast(get_json_object(attributes, '$.archived') as boolean) as archived,
    get_json_object(attributes, '$.trigger_type') as trigger_type,

    -- system / compat
    cast(_airbyte_extracted_at as {{ dbt.type_timestamp() }}) as _fivetran_synced,
    false as _fivetran_deleted,

    {{ fivetran_utils.source_relation(
         union_schema_variable   = 'klaviyo_union_schemas',
         union_database_variable = 'klaviyo_union_databases'
    ) }} as source_relation
  from base
)

select 
  created as created_at,
  cast(id as {{ dbt.type_string() }}) as flow_id,
  name as flow_name,
  status,
  updated as updated_at,
  archived as is_archived,
  trigger_type,
  source_relation
from parsed
where not coalesce(_fivetran_deleted, false);
