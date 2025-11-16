import os
import datetime as dt
import numpy as np
import pandas as pd
import mysql.connector

host="localhost"
user="root"
password=""
schema_path="customerdb.sql"
sample_path="sampledata1.sql"

def run_sql_file(cursor, path):
    with open(path,"r",encoding="utf-8") as f:
        sql=f.read()
    stmts=[s.strip() for s in sql.split(";") if s.strip()]
    for s in stmts:
        cursor.execute(s)

cnx=mysql.connector.connect(host=host,user=user,password=password,autocommit=True)
cur=cnx.cursor()
run_sql_file(cur,schema_path)
run_sql_file(cur,sample_path)

cur.execute("USE customerdb")

q_daily_sales="""
SELECT DATE(o.OrderDate) day,
SUM(oi.Quantity*oi.UnitPriceAtSale*(1-oi.DiscountPct/100)) revenue
FROM `Order` o
JOIN OrderItem oi ON oi.OrderID=o.OrderID
GROUP BY day
ORDER BY day
"""
q_product_perf="""
SELECT p.ProductID,p.Sku,p.ProductName,
SUM(oi.Quantity) qty_sold,
SUM(oi.Quantity*oi.UnitPriceAtSale*(1-oi.DiscountPct/100)) revenue
FROM OrderItem oi
JOIN Product p ON p.ProductID=oi.ProductID
JOIN `Order` o ON o.OrderID=oi.OrderID
GROUP BY p.ProductID,p.Sku,p.ProductName
ORDER BY revenue DESC
"""
q_customer_ltv="""
SELECT c.CustomerID,c.FirstName,c.LastName,c.Email,
COUNT(DISTINCT o.OrderID) orders_count,
SUM(oi.Quantity*oi.UnitPriceAtSale*(1-oi.DiscountPct/100)) revenue
FROM Customer c
JOIN `Order` o ON o.CustomerID=c.CustomerID
JOIN OrderItem oi ON oi.OrderID=o.OrderID
GROUP BY c.CustomerID,c.FirstName,c.LastName,c.Email
ORDER BY revenue DESC
"""
q_employee_sales="""
SELECT e.EmployeeID,e.FirstName,e.LastName,e.Email,
COUNT(DISTINCT o.OrderID) orders_count,
SUM(oi.Quantity*oi.UnitPriceAtSale*(1-oi.DiscountPct/100)) revenue
FROM Employee e
JOIN `Order` o ON o.EmployeeID=e.EmployeeID
JOIN OrderItem oi ON oi.OrderID=o.OrderID
GROUP BY e.EmployeeID,e.FirstName,e.LastName,e.Email
ORDER BY revenue DESC
"""
q_payment_mix="""
SELECT pm.MethodName,
SUM(p.Amount) amount
FROM Payment p
JOIN PaymentMethod pm ON pm.PaymentMethodID=p.PaymentMethodID
GROUP BY pm.MethodName
ORDER BY amount DESC
"""
q_status_funnel="""
SELECT os.StatusName,COUNT(*) orders_count
FROM `Order` o
JOIN OrderStatus os ON os.OrderStatusID=o.OrderStatusID
GROUP BY os.StatusName
ORDER BY orders_count DESC
"""
q_rfm="""
WITH order_totals AS (
  SELECT o.CustomerID,o.OrderID,DATE(o.OrderDate) day,
  SUM(oi.Quantity*oi.UnitPriceAtSale*(1-oi.DiscountPct/100)) revenue
  FROM `Order` o
  JOIN OrderItem oi ON oi.OrderID=o.OrderID
  GROUP BY o.CustomerID,o.OrderID,DATE(o.OrderDate)
)
SELECT c.CustomerID,c.FirstName,c.LastName,c.Email,
DATEDIFF(CURDATE(),MAX(ot.day)) Recency,
COUNT(DISTINCT ot.OrderID) Frequency,
SUM(ot.revenue) Monetary
FROM Customer c
JOIN order_totals ot ON ot.CustomerID=c.CustomerID
GROUP BY c.CustomerID,c.FirstName,c.LastName,c.Email
"""
dfs={}
for name,query in {
    "daily_sales":q_daily_sales,
    "product_performance":q_product_perf,
    "customer_ltv":q_customer_ltv,
    "employee_sales":q_employee_sales,
    "payment_mix":q_payment_mix,
    "status_funnel":q_status_funnel,
    "rfm":q_rfm
}.items():
    cur.execute(query)
    cols=[d[0] for d in cur.description]
    rows=cur.fetchall()
    dfs[name]=pd.DataFrame(rows,columns=cols)

os.makedirs("reports",exist_ok=True)
for k,v in dfs.items():
    v.to_csv(f"reports/{k}.csv",index=False)

if not dfs["daily_sales"].empty:
    s=dfs["daily_sales"].copy()
    s["day"]=pd.to_datetime(s["day"])
    s=s.set_index("day").asfreq("D",fill_value=0)
    s["ma7"]=s["revenue"].rolling(7,min_periods=1).mean()
    x=np.array([d.toordinal() for d in s.index]).astype(float)
    y=s["revenue"].values.astype(float)
    coef=np.polyfit(x,y,1)
    start_date=s.index.max()+pd.Timedelta(days=1)
    future_dates=pd.date_range(start_date,periods=30,freq="D")
    xf=np.array([d.toordinal() for d in future_dates]).astype(float)
    trend=np.polyval(coef,xf)
    base=s["ma7"].iloc[-1] if len(s["ma7"])>0 else 0.0
    forecast=np.maximum(trend,0.0)*0.5+base*0.5
    pred=pd.DataFrame({"day":future_dates,"predicted_revenue":forecast})
    s.reset_index().rename(columns={"day":"date","revenue":"actual_revenue","ma7":"rolling_7d"}).to_csv("reports/daily_sales_timeseries.csv",index=False)
    pred.to_csv("reports/daily_sales_forecast_30d.csv",index=False)

cur.close()
cnx.close()
