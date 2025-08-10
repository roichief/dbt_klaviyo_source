-- Airbyte events: top-level datetime; metric/profile ids in relationships
select
  id,
  type,
  links,
  attributes,
  relationships,
  cast(datetime as timestamp) as datetime,
  get_json_object(relationships, '$.metric.data.id')  as metric_id,
  get_json_object(relationships, '$.profile.data.id') as person_id,

  _airbyte_raw_id,
  _airbyte_extracted_at as _fivetran_synced,
  _airbyte_meta,
  _airbyte_generation_id,

  cast('' as string) as source_relation
from {{ source('klaviyo_source', 'events') }}
