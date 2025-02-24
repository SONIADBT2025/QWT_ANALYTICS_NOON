{{config(materialized='table', schema= env_var('DBT_TRANSFORMSCHEMA','transforming_dev'))}}

--{% set min_order_date="2007-04-19" %} hardcoded 
--{% set max_order_date="2012-04-01" %} hardcoded
{% set v_min_date = min_date() %}
{% set v_max_date = max_date() %}

{{ dbt_date.get_date_dimension(v_min_date, v_max_date) }} 
----min date and max date from fct_orders tables