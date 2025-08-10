-- Airbyte has updated_at; Fivetran models expect updated
select
  id,
  type,
  links,
  attributes,
  cast(updated_at as timestamp) as updated,
  cast(get_json_object(attributes, '$.created_at') as timestamp) as created
  {{ fivetran_utils.source_relation(
        union_schema_variable='klaviyo_union_schemas',
        union_database_variable='klaviyo_union_databases') }}
from {{ source('klaviyo_source', 'campaigns') }}
