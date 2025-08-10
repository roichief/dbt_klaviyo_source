-- Events: datetime is top-level; metric/profile ids are in relationships
select
  id,
  type,
  links,
  attributes,
  relationships,
  cast(datetime as timestamp) as datetime,
  get_json_object(relationships, '$.metric.data.id')  as metric_id,
  get_json_object(relationships, '$.profile.data.id') as person_id,
  {{ fivetran_utils.source_relation(
        union_schema_variable='klaviyo_union_schemas',
        union_database_variable='klaviyo_union_databases') }}
from {{ source('klaviyo_source', 'events') }}
