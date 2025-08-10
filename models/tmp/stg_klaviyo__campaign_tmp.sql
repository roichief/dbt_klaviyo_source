-- Airbyte "campaign_details" includes extra top-level fields we want to preserve
select
  -- core
  id,
  type,
  links,
  attributes,
  relationships,

  -- normalize timestamps for downstream fivetran models
  cast(updated_at as timestamp)                                  as updated,
  cast(get_json_object(attributes, '$.created_at') as timestamp) as created,

  -- extra detail available only in the detailed table
  try_cast(campaign_messages as string) as campaign_messages,
  try_cast(estimated_recipient_count as bigint) as estimated_recipient_count,

  -- Airbyte lineage/system fields (we’ll surface _airbyte_extracted_at as _fivetran_synced)
  _airbyte_raw_id,
  _airbyte_extracted_at as _fivetran_synced,
  _airbyte_meta,
  _airbyte_generation_id,

  -- keep for compatibility with fivetran_utils.source_relation()
  cast('' as string) as source_relation
from {{ source('klaviyo_source', 'campaigns_detailed') }}
