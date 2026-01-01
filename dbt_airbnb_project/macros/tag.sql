{% macro tag(col) %}
    CASE
        WHEN {{ col }} < 100 THEN 'low'
        WHEN {{ col }} < 200 THEN 'midium'
        ELSE 'high'
    END
{% endmacro %}