{{
  config(
    materialized='table',
    alias='klaviyo_airbyte_compat__campaign',
    schema=var('klaviyo_airbyte_compat_schema', 'klaviyo_airbyte_compat'),
    database=var('klaviyo_airbyte_compat_database', target.database)
  )
}}

select
    cast(id as string) as id,
    type as campaign_type,
    {{ json_get('attributes', '$.name') }} as name,
    {{ json_ts('attributes', '$.created_at') }} as created,
    {{ json_ts('attributes', '$.updated_at') }} as updated,
    {{ json_ts('attributes', '$.scheduled_at') }} as scheduled,
    {{ json_ts('attributes', '$.send_time') }} as send_time,
    {{ json_ts('attributes', '$.send_time') }} as sent_at,
    {{ json_get('attributes', '$.subject') }} as subject,
    {{ json_get('attributes', '$.status') }} as status,
    {{ json_get('attributes', '$.status_id') }} as status_id,
    {{ json_get('attributes', '$.status_label') }} as status_label,
    {{ json_get('attributes', '$.from_email') }} as from_email,
    {{ json_get('attributes', '$.from_name') }} as from_name,
    {{ json_get('attributes', '$.email_template_id') }} as email_template_id,
    {{ json_bool('attributes', '$.archived') }} as archived,
    cast(_airbyte_extracted_at as timestamp) as _fivetran_synced,
    false as _fivetran_deleted,
    cast(concat_ws('.',
      '{{ var('"'"'klaviyo_airbyte_raw_database'"'"', '"'"'0006_wsg'"'"') }}',
      '{{ var('"'"'klaviyo_airbyte_raw_schema'"'"', '"'"'klaviyo_raw'"'"') }}'
    ) as string) as source_relation
from {{ source('klaviyo_airbyte_raw', 'campaigns') }}
