{{config(materialized = 'view', schema = 'reporting_dev')}}

select 
e.country,
c.companyname,
c.contactname,
count(o.orderid) as total_order,
sum(o.quantity) as total_quantity,
sum(o.linesalesamount) as total_sales,
avg(o.margin) as avg_margin
from {{ref("fct_orders")}} as o
inner join {{ref("dim_customer")}} as c on o.customerid=c.customerid
inner join {{ref("dim_employees")}} as e on o.employeeid=e.EMPLOYEE_ID
where e.country='{{var('v_country','Sweden')}}'
group by e.country,c.companyname,c.contactname
order by total_Sales desc

