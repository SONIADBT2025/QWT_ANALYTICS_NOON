{% macro get_order_linenos() %} ----name of macros
 
{% set order_linenos_query %}
 
select distinct
lineno
from {{ ref('fct_orders') }}
order by 1
 
{% endset %}
 
{% set results = run_query(order_linenos_query) %}
 
{% if execute %}
 
{# Return the first column #}
 
{% set results_list = results.columns[0].values() %}
 
{% else %}
 
{% set results_list = [] %}
 
{% endif %}
 
{{ return(results_list) }}
 
{% endmacro %}