{{config(materialized = 'view', schema = 'reporting_dev')}}
select product,
company,
quantity,
max(quantity) over (partition by product order by company)as max_quantity
from(
select 
c.companyname as company,
p.productname as product,
sum(o.quantity) as quantity
from QWT_ANALYTICS_DEV.SALESMART_DEV.DIM_CUSTOMER as c
inner join {{ref('fct_orders')}} as o
on c.customerid=o.customerid
inner join {{ref('dim_products')}} as p
on p.productid=o.productid
group by c.companyname,p.productname
order by p.productname)