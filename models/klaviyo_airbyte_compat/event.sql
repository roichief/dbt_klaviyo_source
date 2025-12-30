{{
  config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
    alias='klaviyo_airbyte_compat__event',
    schema=var('klaviyo_airbyte_compat_schema', 'klaviyo_airbyte_compat'),
    database=var('klaviyo_airbyte_compat_database', target.database)
  )
}}

with src as (
    select
        cast(id as string) as event_id,
        cast(datetime as timestamp) as datetime,
        cast(datetime as timestamp) as timestamp,
        coalesce(
            {{ json_get('attributes', '$.event_name') }},
            {{ json_get('attributes', '$.name') }},
            type
        ) as type,
        {{ json_get('relationships', '$.metric.data.id') }} as metric_id,
        {{ json_get('relationships', '$.profile.data.id') }} as person_id,
        {{ json_get('attributes', '$.event_properties.$campaign_id') }} as campaign_id,
        {{ json_get('attributes', '$.event_properties.$flow_id') }} as flow_id,
        {{ json_get('attributes', '$.event_properties.$flow_message_id') }} as flow_message_id,
        {{ json_get('attributes', '$.event_properties.$message_interaction') }} as _variation,
        {{ json_get('attributes', '$.event_properties.$value') }} as property_value,
        {{ json_get('attributes', '$.uuid') }} as uuid,
        cast(_airbyte_extracted_at as timestamp) as _fivetran_synced,
        false as _fivetran_deleted,
        cast(concat_ws('.',
          '{{ var('"'"'klaviyo_airbyte_raw_database'"'"', '"'"'0006_wsg'"'"') }}',
          '{{ var('"'"'klaviyo_airbyte_raw_schema'"'"', '"'"'klaviyo_raw'"'"') }}'
        ) as string) as source_relation,
        attributes as attributes_json,
        relationships as relationships_json
    from {{ source('klaviyo_airbyte_raw', 'events') }}
    {% if is_incremental() %}
      where _airbyte_extracted_at > (select coalesce(max(_fivetran_synced), timestamp('1900-01-01')) from {{ this }})
    {% endif %}
)

select * from src
