{{config(materialized = 'view', schema = 'reporting_dev')}}
 
 --{% set v_linenos = [1,2,3,4] %}
 {% set v_linenos = get_order_linenos() %} ---here now we are using macros name 

select
 
orderid,
{% for linenumber in v_linenos -%}

sum(case when lineno = {{linenumber}}  then linesalesamount else 0 end) 
as lineno{{linenumber}}_sales,

{% endfor %}
sum(linesalesamount) as total_sales
 
from {{ref('fct_orders')}}
 
group by orderid