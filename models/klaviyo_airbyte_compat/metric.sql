{{
  config(
    materialized='table',
    alias='klaviyo_airbyte_compat__metric',
    schema=var('klaviyo_airbyte_compat_schema', 'klaviyo_airbyte_compat'),
    database=var('klaviyo_airbyte_compat_database', target.database)
  )
}}

select
    cast(id as string) as id,
    {{ json_get('attributes', '$.name') }} as name,
    {{ json_ts('attributes', '$.created') }} as created,
    {{ json_ts('attributes', '$.updated') }} as updated,
    {{ json_get('attributes', '$.integration.id') }} as integration_id,
    {{ json_get('attributes', '$.integration.name') }} as integration_name,
    {{ json_get('attributes', '$.integration.category') }} as integration_category,
    cast(_airbyte_extracted_at as timestamp) as _fivetran_synced,
    false as _fivetran_deleted,
    cast(concat_ws('.',
      '{{ var('"'"'klaviyo_airbyte_raw_database'"'"', '"'"'0006_wsg'"'"') }}',
      '{{ var('"'"'klaviyo_airbyte_raw_schema'"'"', '"'"'klaviyo_raw'"'"') }}'
    ) as string) as source_relation
from {{ source('klaviyo_airbyte_raw', 'metrics') }}
