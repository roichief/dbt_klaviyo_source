with base as (
  select * from {{ ref('stg_klaviyo__person_tmp') }}
),
parsed as (
  select
    id,
    get_json_object(attributes, '$.email')                 as email,
    get_json_object(attributes, '$.first_name')            as first_name,
    get_json_object(attributes, '$.last_name')             as last_name,
    get_json_object(attributes, '$.organization')          as organization,
    get_json_object(attributes, '$.phone_number')          as phone_number,
    get_json_object(attributes, '$.title')                 as title,
    try_to_timestamp(get_json_object(attributes, '$.created')) as created,
    try_to_timestamp(get_json_object(attributes, '$.updated')) as updated,
    try_to_timestamp(get_json_object(attributes, '$.last_event_date')) as last_event_date,
    get_json_object(attributes, '$.location.address1')     as address_1,
    get_json_object(attributes, '$.location.address2')     as address_2,
    get_json_object(attributes, '$.location.city')         as city,
    get_json_object(attributes, '$.location.region')       as region,
    get_json_object(attributes, '$.location.zip')          as zip,
    get_json_object(attributes, '$.location.country')      as country,
    try_cast(get_json_object(attributes, '$.location.latitude')  as double) as latitude,
    try_cast(get_json_object(attributes, '$.location.longitude') as double) as longitude,
    get_json_object(attributes, '$.location.timezone')     as timezone,
    cast(_fivetran_synced as {{ dbt.type_timestamp() }})   as _fivetran_synced,
    false as _fivetran_deleted
    {{ fivetran_utils.source_relation(
         union_schema_variable   = 'klaviyo_union_schemas',
         union_database_variable = 'klaviyo_union_databases'
    ) }}
  from base
)
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
  region,
  timezone,
  title,
  updated as updated_at,
  last_event_date,
  source_relation
  {{ fivetran_utils.fill_pass_through_columns('klaviyo__person_pass_through_columns') }}
from parsed
where not coalesce(_fivetran_deleted, false);
