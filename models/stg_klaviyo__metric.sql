-- Airbyte → parse JSON → expose columns expected by Fivetran transform package.

with base as (
  select * from {{ ref('stg_klaviyo__metric_tmp') }}
),

parsed as (
  select
    id,
    try_to_timestamp(get_json_object(attributes, '$.created')) as created,
    try_to_timestamp(get_json_object(attributes, '$.updated')) as updated,
    get_json_object(attributes, '$.name')                      as name,
    get_json_object(attributes, '$.integration.id')            as integration_id,
    get_json_object(attributes, '$.integration.name')          as integration_name,
    get_json_object(attributes, '$.integration.category')      as integration_category,

    -- system / compat
    cast(_airbyte_extracted_at as {{ dbt.type_timestamp() }}) as _fivetran_synced,
    false as _fivetran_deleted,

    {{ fivetran_utils.source_relation(
         union_schema_variable   = 'klaviyo_union_schemas',
         union_database_variable = 'klaviyo_union_databases'
    ) }}
  from base
)

select 
  created as created_at,
  cast(id as {{ dbt.type_string() }})             as metric_id,
  cast(integration_id as {{ dbt.type_string() }}) as integration_id,
  integration_name,
  integration_category,
  name as metric_name,
  updated as updated_at,
  source_relation
from parsed
where not coalesce(_fivetran_deleted, false);
