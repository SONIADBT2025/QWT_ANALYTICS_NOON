{{ config(materialized = 'incremental' , unique_key=['OrderID','LineNo'])}}

select od.*,o.OrderDate
from
{{ source('raw_qwt','raw_orders')}} as o inner join
{{ source('raw_qwt','raw_orderdetails')}} as od
on o.OrderID=od.OrderID

{%if is_incremental() %}

where o.OrderDate> (select max(OrderDate) from {{this}})
{% endif %}