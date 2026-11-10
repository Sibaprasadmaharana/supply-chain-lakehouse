{#- Convert a date/timestamp expression to an integer yyyymmdd key used to join dim_date. -#}
{% macro date_key(expression) -%}
    cast(strftime(cast({{ expression }} as date), '%Y%m%d') as integer)
{%- endmacro %}
