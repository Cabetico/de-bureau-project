{% macro year_month(date_column) %}
    format_date('%Y%m', {{ date_column }})
{% endmacro %}