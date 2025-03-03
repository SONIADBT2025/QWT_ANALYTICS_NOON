{{ config(
    materialized = 'view' , schema = 'salesmart_dev',
    post_hook = 'grant usage on database qwt_analytics_dev to role sysadmin;
                 grant usage on schema salesmart_dev to role sysadmin; 
                 grant select on view qwt_analytics_dev.salesmart_dev.fct_orders to role sysadmin;' 
    )
}}
-----post_hook means post sql query that will run on after this model sql
select * 
from 
{{ref("trf_orders")}}