{{config(materialized = 'table', schema = 'transforming_dev') }}

select
ss.orderid,
ss.lineno,
sh.companyname,
ss.ShipmentDate,
ss.status as currentstatus
from
{{ref("shipments_snapshot")}} as ss
inner join
{{ref("lkp_shippers")}} as sh
on ss.ShipperID=sh.ShipperID
where ss.dbt_valid_to is null
