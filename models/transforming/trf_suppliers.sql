{{config(materialized = 'table', schema = env_var('DBT_TRANSFORMSCHEMA','transforming_dev')) }}

select get(xmlget(SHIPPERINFO,'SupplierID'),'$') as SupplierID ,
get(xmlget(SHIPPERINFO,'CompanyName'),'$')::varchar as CompanyName,
get(xmlget(SHIPPERINFO,'ContactName'),'$')::varchar as ContactName,
get(xmlget(SHIPPERINFO,'Address'),'$')::varchar as Address,
get(xmlget(SHIPPERINFO,'City'),'$')::varchar as City,
get(xmlget(SHIPPERINFO,'PostalCode'),'$')::varchar as PostalCode,
get(xmlget(SHIPPERINFO,'Country'),'$')::varchar as Country,
get(xmlget(SHIPPERINFO,'Phone'),'$')::varchar as Phone,
get(xmlget(SHIPPERINFO,'Fax'),'$')::varchar as Fax,
from {{ref("stg_Suppliers")}}