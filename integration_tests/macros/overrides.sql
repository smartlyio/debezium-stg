{% macro ref() %}
   {{ return(dbt_unit_testing.ref(*varargs, **kwargs)) }}
{% endmacro %}

{% macro source() %}
   {{ return(dbt_unit_testing.source(*varargs, **kwargs)) }}
{% endmacro %}

{% macro is_incremental() %}
  {{ return (dbt_unit_testing.is_incremental()) }}
{% endmacro %}
