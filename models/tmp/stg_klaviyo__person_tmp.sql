{{ config(materialized='table') }}

with raw as (
  select
    cast(id as {{ dbt.type_string() }})                                      as id,
    lower(get_json_object(attributes, '$.email'))                            as email,
    get_json_object(attributes, '$.phone_number')                            as phone_number,
    try_cast(get_json_object(attributes, '$.created') as timestamp)          as created,
    try_cast(updated as timestamp)                                           as updated,
    attributes                                                               as person_properties,
    cast(null as {{ dbt.type_string() }})                                    as integration_id,
    cast(null as {{ dbt.type_string() }})                                    as integration_name,
    cast(null as {{ dbt.type_string() }})                                    as integration_category,
    false                                                                    as _fivetran_deleted
  from {{ source('klaviyo_source', var('klaviyo_person_identifier', 'profiles')) }}
)

select * from raw
