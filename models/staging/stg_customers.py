def model(dbt, session):
   
    customer_df = dbt.source('raw_qwt','raw_customers')
 
   
    return customer_df 