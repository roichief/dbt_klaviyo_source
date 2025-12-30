{% macro json_get(json_col, json_path) -%}
  get_json_object({{ json_col }}, '{{ json_path }}')
{%- endmacro %}

{% macro json_bool(json_col, json_path) -%}
  cast({{ json_get(json_col, json_path) }} as boolean)
{%- endmacro %}

{% macro json_ts(json_col, json_path) -%}
  cast({{ json_get(json_col, json_path) }} as timestamp)
{%- endmacro %}
