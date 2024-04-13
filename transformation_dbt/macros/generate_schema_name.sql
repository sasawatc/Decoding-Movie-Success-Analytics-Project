-- https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas#an-alternative-pattern-for-generating-schema-names

{%- macro generate_schema_name(custom_schema_name, node) -%}

    {{ custom_schema_name | trim }}

{%- endmacro -%}
