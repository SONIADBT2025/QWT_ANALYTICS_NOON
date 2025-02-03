{{ config(materialised = 'incremental',unique_key= ['OrderID'])}}

select * 
from 
{{ source('raw_qwt','raw_orders')}}

{% if is_incremental() %}

where OrderDate > (select max(OrderDate) from {{this}})

{% endif %}