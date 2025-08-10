-- Airbyte campaigns: updated_at exists (map to updated), created_at in attributes
select
  id,
  type,
  links,
  attributes,
  relationships,
  cast(updated_at as timestamp) as updated,
  cast(get_json_object(attributes, '$.created_at') as timestamp) as created,
  try_cast(campaign_messages as string) as campaign_messages,
  try_cast(estimated_recipient_count as bigint) as estimated_recipient_count,
  
  -- Airbyte system columns, preserved
  _airbyte_raw_id,
  _airbyte_extracted_at as _fivetran_synced,
  _airbyte_meta,
  _airbyte_generation_id,

  -- expected by downstream (harmless placeholder)
  cast('' as string) as source_relation
from {{ source('klaviyo_source', 'campaigns') }}
