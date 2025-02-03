{{ config(materialised = 'table')}}

select * 
from 
{{ source('raw_qwt','raw_products')}}