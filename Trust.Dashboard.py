import os
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

q_order_totals="""
WITH ot AS (
  SELECT o.OrderID,o.OrderDate,o.RequiredDate,o.ShippedDate,o.EmployeeID,o.CustomerID,o.OrderStatusID,
         SUM(oi.Quantity*oi.UnitPriceAtSale*(1-oi.DiscountPct/100)) AS order_total,
         MAX(CASE WHEN oi.DiscountPct>0 THEN 1 ELSE 0 END) AS has_discount
  FROM `Order` o
  JOIN OrderItem oi ON oi.OrderID=o.OrderID
  GROUP BY o.OrderID,o.OrderDate,o.RequiredDate,o.ShippedDate,o.EmployeeID,o.CustomerID,o.OrderStatusID
),
pp AS (
  SELECT p.OrderID,SUM(p.Amount) AS paid_amount,MIN(p.PaidAt) AS first_paid_at
  FROM Payment p
  GROUP BY p.OrderID
)
SELECT ot.*,COALESCE(pp.paid_amount,0) AS paid_amount,pp.first_paid_at
FROM ot
LEFT JOIN pp ON pp.OrderID=ot.OrderID
"""
q_item_stats="""
SELECT COUNT(*) AS item_count,
SUM(oi.Quantity*oi.UnitPriceAtSale) AS gross_amount,
SUM(oi.Quantity*oi.UnitPriceAtSale*(oi.DiscountPct/100)) AS discounted_amount,
AVG(oi.DiscountPct) AS avg_item_discount,
SUM(CASE WHEN oi.DiscountPct>=20 THEN 1 ELSE 0 END) AS high_discount_items
FROM OrderItem oi
"""
q_price_mismatch="""
SELECT oi.OrderID,oi.ProductID,oi.UnitPriceAtSale,p.UnitPrice,
ABS(oi.UnitPriceAtSale-p.UnitPrice) AS diff
FROM OrderItem oi
JOIN Product p ON p.ProductID=oi.ProductID
WHERE ABS(oi.UnitPriceAtSale-p.UnitPrice)>0.01
"""
q_status_dist="""
SELECT os.StatusName,COUNT(*) orders_count
FROM `Order` o
JOIN OrderStatus os ON os.OrderStatusID=o.OrderStatusID
GROUP BY os.StatusName
ORDER BY orders_count DESC
"""
q_payment_mix="""
SELECT pm.MethodName,SUM(p.Amount) amount
FROM Payment p
JOIN PaymentMethod pm ON pm.PaymentMethodID=p.PaymentMethodID
GROUP BY pm.MethodName
ORDER BY amount DESC
"""
q_employee_assign="""
SELECT COUNT(*) total_orders,
SUM(CASE WHEN EmployeeID IS NULL THEN 0 ELSE 1 END) AS with_employee
FROM `Order`
"""
q_customer_quality="""
SELECT
(SELECT COUNT(*) FROM Customer) AS total_customers,
(SELECT COUNT(*) FROM Customer WHERE Email IS NULL OR Email='' OR Email NOT LIKE '%@%') AS invalid_email
"""
dfs={}
for name,query in {
    "orders":q_order_totals,
    "item_stats":q_item_stats,
    "price_mismatch":q_price_mismatch,
    "status_dist":q_status_dist,
    "payment_mix":q_payment_mix,
    "employee_assign":q_employee_assign,
    "customer_quality":q_customer_quality
}.items():
    cur.execute(query)
    cols=[d[0] for d in cur.description]
    rows=cur.fetchall()
    dfs[name]=pd.DataFrame(rows,columns=cols)

orders=dfs["orders"].copy()
orders["order_total"]=orders["order_total"].astype(float)
orders["paid_amount"]=orders["paid_amount"].astype(float)
orders["is_shipped"]=orders["ShippedDate"].notna()
orders["has_required"]=orders["RequiredDate"].notna()
orders["on_time"]=np.where(orders["is_shipped"] & orders["has_required"] & (orders["ShippedDate"]<=orders["RequiredDate"]),1,0)
orders["late_days"]=np.where(orders["is_shipped"] & orders["has_required"],(orders["ShippedDate"]-orders["RequiredDate"]).dt.days, np.nan)
orders["paid_status"]=np.where(orders["paid_amount"]>=orders["order_total"]*1.001,"overpaid",
                        np.where(orders["paid_amount"]>=orders["order_total"]*0.999,"paid",
                        np.where(orders["paid_amount"]>0,"partial","unpaid")))
total_orders=len(orders)
on_time_base=int((orders["has_required"]).sum())
on_time_rate=float(orders["on_time"].sum()/on_time_base) if on_time_base>0 else np.nan
fulfillment_rate=float(orders["is_shipped"].sum()/total_orders) if total_orders>0 else np.nan
full_payment_rate=float((orders["paid_status"]=="paid").sum()/total_orders) if total_orders>0 else np.nan
partial_payment_rate=float((orders["paid_status"]=="partial").sum()/total_orders) if total_orders>0 else np.nan
unpaid_rate=float((orders["paid_status"]=="unpaid").sum()/total_orders) if total_orders>0 else np.nan
overpaid_count=int((orders["paid_status"]=="overpaid").sum())
discount_order_rate=float((orders["has_discount"]==1).sum()/total_orders) if total_orders>0 else np.nan
employee_assign_rate=float(dfs["employee_assign"]["with_employee"].iloc[0]/dfs["employee_assign"]["total_orders"].iloc[0]) if dfs["employee_assign"]["total_orders"].iloc[0]>0 else np.nan
invalid_email=int(dfs["customer_quality"]["invalid_email"].iloc[0])
total_customers=int(dfs["customer_quality"]["total_customers"].iloc[0])
invalid_email_rate=float(invalid_email/total_customers) if total_customers>0 else np.nan
item_stats=dfs["item_stats"].iloc[0]
gross_amount=float(item_stats["gross_amount"]) if pd.notna(item_stats["gross_amount"]) else 0.0
discounted_amount=float(item_stats["discounted_amount"]) if pd.notna(item_stats["discounted_amount"]) else 0.0
effective_discount_rate=float(discounted_amount/gross_amount) if gross_amount>0 else np.nan
high_discount_items=int(item_stats["high_discount_items"]) if pd.notna(item_stats["high_discount_items"]) else 0

summary=pd.DataFrame([
    ["total_orders",total_orders],
    ["on_time_ship_rate",on_time_rate],
    ["fulfillment_rate",fulfillment_rate],
    ["full_payment_rate",full_payment_rate],
    ["partial_payment_rate",partial_payment_rate],
    ["unpaid_rate",unpaid_rate],
    ["overpaid_orders",overpaid_count],
    ["discount_order_rate",discount_order_rate],
    ["effective_discount_rate",effective_discount_rate],
    ["employee_assignment_rate",employee_assign_rate],
    ["total_customers",total_customers],
    ["invalid_email_count",invalid_email],
    ["invalid_email_rate",invalid_email_rate]
],columns=["metric","value"])

os.makedirs("reports/trust",exist_ok=True)
summary.to_csv("reports/trust/trust_dashboard_summary.csv",index=False)
orders[["OrderID","OrderDate","RequiredDate","ShippedDate","late_days","on_time","paid_status","order_total","paid_amount","has_discount"]].to_csv("reports/trust/payment_and_fulfillment_by_order.csv",index=False)
dfs["status_dist"].to_csv("reports/trust/order_status_distribution.csv",index=False)
dfs["payment_mix"].to_csv("reports/trust/payment_method_mix.csv",index=False)
dfs["price_mismatch"].to_csv("reports/trust/product_price_mismatches.csv",index=False)
