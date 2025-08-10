select
  id,
  type,
  links,
  attributes,
  cast(get_json_object(attributes, '$.created') as timestamp) as created,
  cast(updated as timestamp) as updated,
  get_json_object(attributes, '$.integration.id')       as integration_id,
  get_json_object(attributes, '$.integration.name')     as integration_name,
  get_json_object(attributes, '$.integration.category') as integration_category
  {{ fivetran_utils.source_relation(
        union_schema_variable='klaviyo_union_schemas',
        union_database_variable='klaviyo_union_databases') }}
from {{ source('klaviyo_source', 'metrics') }}
