
{# 
What this does (in plain English)
If a model does not specify a schema → use target.schema (safe default)
If a model does specify a schema → use it as-is
❌ Do not prefix with main_
❌ Do not concatenate schemas
 #}

{% macro generate_schema_name(schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ schema_name | trim }}

    {%- endif -%}

{%- endmacro -%}