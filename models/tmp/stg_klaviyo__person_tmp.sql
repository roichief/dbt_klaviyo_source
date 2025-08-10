-- Profiles (persons): updated is top-level; created/email live in attributes
select
  id,
  type,
  links,
  attributes,
  cast(updated as timestamp) as updated,
  cast(get_json_object(attributes, '$.created') as timestamp) as created,
  get_json_object(attributes, '$.email')        as email,
  get_json_object(attributes, '$.phone_number') as phone_number,
  {{ fivetran_utils.source_relation(
        union_schema_variable='klaviyo_union_schemas',
        union_database_variable='klaviyo_union_databases') }}
from {{ source('klaviyo_source', 'profiles') }}
