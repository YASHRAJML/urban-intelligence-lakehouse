-- macros/generate_surrogate_key.sql
-- Generate a surrogate key from multiple columns

{% macro generate_surrogate_key(column_list) %}
    MD5(CONCAT_WS('|',
        {% for col in column_list %}
            COALESCE(CAST({{ col }} AS VARCHAR), '__null__')
            {% if not loop.last %},{% endif %}
        {% endfor %}
    ))
{% endmacro %}
