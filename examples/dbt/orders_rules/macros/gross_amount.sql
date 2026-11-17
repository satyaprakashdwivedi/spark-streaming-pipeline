{% macro gross_amount(column, tax_rate=0.18) %}round({{ column }} * (1 + {{ tax_rate }}), 2){% endmacro %}
