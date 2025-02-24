{{config(materialized='view', schema = 'salesmart_dev')}}
select 
orderid,
lineno,
companyname,
ShipmentDate,
currentstatus
from {{ ref('trf_shipments') }}