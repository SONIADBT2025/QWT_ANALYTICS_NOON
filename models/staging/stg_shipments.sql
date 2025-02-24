{{ config(materialized = 'table')}}

select 
OrderID,	
LineNo ,	
ShipperID,	
CustomerID ,	
ProductID ,	
EmployeeID,	
TO_DATE(SPLIT_PART(ShipmentDate,' ',1)) AS ShipmentDate  ,
STATUS
from 
{{ source('raw_qwt','raw_shipments')}}