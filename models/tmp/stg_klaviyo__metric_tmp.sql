with base as (
  select * from {{ ref('stg_klaviyo__metric_tmp') }}
),
fields as (
  select
    {{
      fivetran_utils.fill_staging_columns(
        source_columns=adapter.get_columns_in_relation(ref('stg_klaviyo__metric_tmp')),
        staging_columns=get_metric_columns()
      )
    }}
    {{ fivetran_utils.source_relation('klaviyo_union_schemas','klaviyo_union_databases') }}
  from base
),
final as (
  select
      cast(id as {{ dbt.type_string() }})          as metric_id,
      cast(type as {{ dbt.type_string() }})        as record_type,
      links,
      attributes,
      created,
      updated,
      cast(integration_id as {{ dbt.type_string() }})       as integration_id,
      cast(integration_name as {{ dbt.type_string() }})     as integration_name,
      cast(integration_category as {{ dbt.type_string() }}) as integration_category,
      cast(get_json_object(attributes, '$.name') as {{ dbt.type_string() }}) as metric_name,
      source_relation
  from fields
  where not coalesce(_fivetran_deleted, false)
)
select * from final
