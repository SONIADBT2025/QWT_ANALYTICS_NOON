import snowflake.snowpark.functions as F
 
 
def model(dbt, session):
 
    dbt.config(
        materialized = "table", schema = "reporting_dev",
    )
 
    dim_customers_df = dbt.ref('dim_customer')
    fct_orders_df = dbt.ref('fct_orders')
 
    final_df = (
       dim_customers_df
       .join(fct_orders_df, dim_customers_df.customerid == fct_orders_df.customerid, 'left')
       .select(dim_customers_df.customerid,
               dim_customers_df.companyname,
               dim_customers_df.contactname,
               fct_orders_df.orderdate,
               fct_orders_df.orderid,
               fct_orders_df.linesalesamount,
               fct_orders_df.quantity,
               fct_orders_df.margin
 
               
       )
   )
 
 
   
    return final_df