-- Parse Airbyte JSON for Profiles (Persons) into columns expected by Fivetran's source spec.

with base as (
  select * 
  from {{ ref('stg_klaviyo__person_tmp') }}
),

parsed as (
  select
    id,

    -- attributes JSON (string)
    get_json_object(attributes, '$.email')                as email,
    get_json_object(attributes, '$.first_name')           as first_name,
    get_json_object(attributes, '$.last_name')            as last_name,
    get_json_object(attributes, '$.organization')         as organization,
    get_json_object(attributes, '$.phone_number')         as phone_number,
    get_json_object(attributes, '$.title')                as title,
    try_to_timestamp(get_json_object(attributes, '$.created')) as created,
    try_to_timestamp(get_json_object(attributes, '$.updated')) as updated,
    try_to_timestamp(get_json_object(attributes, '$.last_event_date')) as last_event_date,

    -- location nested
    get_json_object(attributes, '$.location.address1')    as address_1,
    get_json_object(attributes, '$.location.address2')    as address_2,
    get_json_object(attributes, '$.location.city')        as city,
    get_json_object(attributes, '$.location.region')      as region,
    get_json_object(attributes, '$.location.zip')         as zip,
    get_json_object(attributes, '$.location.country')     as country,
    try_cast(get_json_object(attributes, '$.location.latitude')  as double)   as latitude,
    try_cast(get_json_object(attributes, '$.location.longitude') as double)   as longitude,
    get_json_object(attributes, '$.location.timezone')    as timezone,

    -- passthrough/system
    cast(_airbyte_extracted_at as {{ dbt.type_timestamp() }}) as _fivetran_synced,
    _airbyte_raw_id,
    _airbyte_meta,
    _airbyte_generation_id
  from base
),

fields as (
  select
    {{
      fivetran_utils.fill_staging_columns(
        source_columns = adapter.get_columns_in_relation(ref('parsed')),
        staging_columns = get_person_columns()
      )
    }}
    {{ fivetran_utils.source_relation(
         union_schema_variable   = 'klaviyo_union_schemas',
         union_database_variable = 'klaviyo_union_databases'
    ) }}
  from parsed
),

final as (
  select 
    cast(id as {{ dbt.type_string() }}) as person_id,
    address_1,
    address_2,
    city,
    country,
    zip,
    created as created_at,
    email,
    first_name || ' ' || last_name as full_name,
    latitude,
    longitude,
    organization,
    phone_number,
    region, -- state in USA
    timezone,
    title,
    updated as updated_at,
    last_event_date,
    source_relation
    {{ fivetran_utils.fill_pass_through_columns('klaviyo__person_pass_through_columns') }}
  from fields
  where not coalesce(_fivetran_deleted, false)
)

select * from final;
