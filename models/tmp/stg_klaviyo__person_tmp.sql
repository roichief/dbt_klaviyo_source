-- Airbyte profiles: top-level updated; created/email/phone in attributes
select
  id,
  type,
  links,
  attributes,
  relationships,
  cast(updated as timestamp) as updated,
  cast(get_json_object(attributes, '$.created') as timestamp) as created,
  get_json_object(attributes, '$.email')         as email,
  get_json_object(attributes, '$.phone_number')  as phone_number,

  _airbyte_raw_id,
  _airbyte_extracted_at as _fivetran_synced,
  _airbyte_meta,
  _airbyte_generation_id,

  cast('' as string) as source_relation
from {{ source('klaviyo_source', 'profiles') }}
