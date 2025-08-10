with base as (
  select * from {{ ref('stg_klaviyo__event_tmp') }}
),
fields as (
  select
    {{
      fivetran_utils.fill_staging_columns(
        source_columns=adapter.get_columns_in_relation(ref('stg_klaviyo__event_tmp')),
        staging_columns=get_event_columns()
      )
    }}
    {{ fivetran_utils.source_relation('klaviyo_union_schemas','klaviyo_union_databases') }}
  from base
),
final as (
  select
      cast(id as {{ dbt.type_string() }})   as event_id,
      cast(type as {{ dbt.type_string() }}) as record_type,
      links,
      attributes,
      relationships,
      datetime,
      cast(metric_id as {{ dbt.type_string() }}) as metric_id,
      cast(person_id as {{ dbt.type_string() }}) as person_id,
      source_relation
  from fields
  where not coalesce(_fivetran_deleted, false)
)
select * from final
