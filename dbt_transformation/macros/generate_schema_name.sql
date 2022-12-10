{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {% if target.name == "ci" %}

        {%- if custom_schema_name is none -%}
            pr_{{ env_var("GH_REF") }}_{{ default_schema }}
        {%- else -%}
            pr_{{ env_var("GH_REF") }}_{{ custom_schema_name | trim }}
        {%- endif -%}
    {% else %}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro %}
