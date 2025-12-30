{{
  config(
    materialized='table',
    alias='klaviyo_airbyte_compat__person',
    schema=var('klaviyo_airbyte_compat_schema', 'klaviyo_airbyte_compat'),
    database=var('klaviyo_airbyte_compat_database', target.database)
  )
}}

select
    cast(id as string) as id,
    {{ json_get('attributes', '$.email') }} as email,
    {{ json_get('attributes', '$.phone_number') }} as phone_number,
    {{ json_get('attributes', '$.first_name') }} as first_name,
    {{ json_get('attributes', '$.last_name') }} as last_name,
    {{ json_get('attributes', '$.organization') }} as organization,
    {{ json_get('attributes', '$.title') }} as title,
    {{ json_get('attributes', '$.locale') }} as locale,
    {{ json_get('attributes', '$.location.address1') }} as address_1,
    {{ json_get('attributes', '$.location.address2') }} as address_2,
    {{ json_get('attributes', '$.location.city') }} as city,
    {{ json_get('attributes', '$.location.region') }} as region,
    {{ json_get('attributes', '$.location.country') }} as country,
    {{ json_get('attributes', '$.location.zip') }} as zip,
    cast({{ json_get('attributes', '$.location.latitude') }} as double) as latitude,
    cast({{ json_get('attributes', '$.location.longitude') }} as double) as longitude,
    {{ json_get('attributes', '$.location.timezone') }} as timezone,
    {{ json_ts('attributes', '$.created') }} as created,
    {{ json_ts('attributes', '$.updated') }} as updated,
    {{ json_ts('attributes', '$.last_event_date') }} as last_event_date,
    cast(_airbyte_extracted_at as timestamp) as _fivetran_synced,
    false as _fivetran_deleted,
    cast(concat_ws('.',
      '{{ var('"'"'klaviyo_airbyte_raw_database'"'"', '"'"'0006_wsg'"'"') }}',
      '{{ var('"'"'klaviyo_airbyte_raw_schema'"'"', '"'"'klaviyo_raw'"'"') }}'
    ) as string) as source_relation
from {{ source('klaviyo_airbyte_raw', 'profiles') }}
