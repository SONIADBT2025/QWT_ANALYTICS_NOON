{{config(materialized = 'table',transient = false)}}
 --it creates a transient table at snow. to create permanent table,-- use transient=false.
select 
Officeid ,
OfficeAddress AS Address,
OfficePostalCode AS PostalCode,
OfficeCity  AS City,
OfficeStateProvince AS StateProvince,
OfficePhone AS Phone,
OfficeFax AS Fax,
OfficeCountry AS Country

 from {{source('raw_qwt','raw_offices')}}