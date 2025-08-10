-- Parse Airbyte JSON for Metrics into columns expected by Fivetran's source spec.

with base as (
  select * 
  from {{ ref('stg_klaviyo__metric_tmp') }}
),

parsed as (
  select
    id,

    -- attributes JSON (string)
    get_json_object(attributes, '$.name')                       as name,
    try_to_timestamp(get_json_object(attributes, '$.created'))  as created,
    try_to_timestamp(get_json_object(attributes, '$.updated'))  as updated,

    -- integration nested object
    get_json_object(attributes, '$.integration.id')             as integration_id,
    get_json_object(attributes, '$.integration.name')           as integration_name,
    get_json_object(attributes, '$.integration.category')       as integration_category,

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
        staging_columns = get_metric_columns()
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
    cast(id as {{ dbt.type_string() }})           as metric_id,
    cast(integration_id as {{ dbt.type_string() }}) as integration_id,
    integration_name,
    integration_category,
    name  as metric_name,
    updated as updated_at,
    source_relation
  from fields
  where not coalesce(_fivetran_deleted, false)
)

select * from final;
