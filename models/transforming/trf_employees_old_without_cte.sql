{{config(materialized = 'table', schema = 'transforming_dev') }}

select 
emp.EmpID,
emp.LastName,
emp.FirstName,
emp.Title,
emp.HireDate,
emp.Extension,
IFF(mgr.FirstName is null,emp.FirstName,mgr.FirstName) as managername,
IFF(mgr.Title is null,emp.Title,mgr.Title) as managertitle,
emp.YearSalary,
ofc.address,
ofc.city,
ofc.country
from
{{ref('stg_employees')}} as emp
left join
{{ref('stg_employees')}} as mgr
 on emp.ReportsTo=mgr.EmpID
left join
{{ref('stg_offices')}} as ofc
on emp.Office=ofc.Officeid