select ORDERID,
COUNT(LINENO) AS TOTAL_LINE
from {{ref('fct_orders')}}
GROUP BY ORDERID
HAVING TOTAL_LINE<1