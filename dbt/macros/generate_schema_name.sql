{#
  Use the model's configured +schema verbatim (bronze / silver / gold) instead of
  dbt's default "<target_schema>_<custom_schema>" concatenation. Every paytrail
  model declares its layer schema, so this yields paytrail.silver.* / paytrail.gold.*
  exactly as the PRD's medallion layout requires, not paytrail.silver_silver.*.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
