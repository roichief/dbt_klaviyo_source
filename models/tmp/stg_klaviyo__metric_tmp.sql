-- Airbyte metrics: top-level updated; integration details inside attributes
select
  id,
  type,
  links,
  attributes,
  relationships,
  cast(get_json_object(attributes, '$.created') as timestamp) as created,
  cast(updated as timestamp) as updated,
  get_json_object(attributes, '$.integration.id')       as integration_id,
  get_json_object(attributes, '$.integration.name')     as integration_name,
  get_json_object(attributes, '$.integration.category') as integration_category,

  _airbyte_raw_id,
  _airbyte_extracted_at as _fivetran_synced,
  _airbyte_meta,
  _airbyte_generation_id,

  cast('' as string) as source_relation
from {{ source('klaviyo_source', 'metrics') }}
