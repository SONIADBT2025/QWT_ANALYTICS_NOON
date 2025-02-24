import snowflake.snowpark.functions as F
import pandas as pd
 
def model(dbt, session):
    dbt.config(materialized = 'table', schema = 'reporting_dev',pre_hook='use warehouse python_model_wh;')
    ###pre_hook means this statement will run before running this code(like pre sql) in snowsql backend query
    orders_df = dbt.ref('fct_orders')
    orders_agg_df = (
                    orders_df
                    .group_by('customerid')
                    .agg
                    (
                        F.min(F.col('orderdate')).alias('First_Order_Date'),
                        F.max(F.col('orderdate')).alias('Recent_Order_Date'),
                        F.count(F.col('orderid')).alias('Total_Orders'),
                        F.countDistinct(F.col('productid')).alias('No_Of_Products'),
                        F.sum(F.col('quantity')).alias('Total_Quantity'),
                        F.sum(F.col('linesalesamount')).alias('Total_Sales'),
                        F.avg(F.col('margin')).alias('Avg_Margin')
                    )
                )
 
    customer_df = dbt.ref('dim_customer')
 
    customer_order_df = (
                            customer_df
                            .join(orders_agg_df, orders_agg_df.customerid == customer_df.customerid, 'left')
                            .select(customer_df.companyname,
                                    customer_df.contactname,
                                    orders_agg_df.First_Order_Date,
                                    orders_agg_df.Recent_Order_Date,
                                    orders_agg_df.Total_Orders,
                                    orders_agg_df.No_Of_Products,
                                    orders_agg_df.Total_Quantity,
                                    orders_agg_df.Total_Sales,
                                    orders_agg_df.Avg_Margin
                                    )
    )
 
    return customer_order_df