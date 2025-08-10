-- Airbyte flows: top-level updated; created inside attributes
select
  id,
  type,
  links,
  attributes,
  relationships,
  cast(updated as timestamp) as updated,
  cast(get_json_object(attributes, '$.created') as timestamp) as created,

  _airbyte_raw_id,
  _airbyte_extracted_at as _fivetran_synced,
  _airbyte_meta,
  _airbyte_generation_id,

  cast('' as string) as source_relation
from {{ source('klaviyo_source', 'flows') }}
