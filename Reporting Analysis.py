# Databricks notebook source
# DBTITLE 1,Sales Analysis
from pyspark.sql.functions import year, month, sum

# Read the Orders and OrderItems tables from Delta Lake
orders_df=spark.read.format('delta').table('delta_orders')
order_items_df = spark.read.format("delta").table('delta_OrderItems')
products_df = spark.read.format("delta").table('delta_products')
customers_df = spark.read.format("delta").table('delta_customers')
inventory_df = spark.read.format("delta").table('delta_inventory')
payments_df = spark.read.format("delta").table('delta_payments')


# Join Orders and OrderItems tables
sales_df = orders_df.join(order_items_df, "OrderID", "inner")

# Calculate total sales amount over time
total_sales_df = sales_df.groupBy(year("OrderDate").alias("Year"), month("OrderDate").alias("Month")) \
                         .agg(sum("TotalPrice").alias("TotalSales")) \
                         .orderBy("Year", "Month")

# Calculate sales trends by year
yearly_sales_df = total_sales_df.groupBy("Year") \
                                 .agg(sum("TotalSales").alias("TotalSales")) \
                                 .orderBy("Year")

# Calculate top-selling products/categories
top_products_df = sales_df.groupBy("ProductID") \
                           .agg(sum("Quantity").alias("TotalQuantity")) \
                           .orderBy("TotalQuantity", ascending=False)

# Perform order analysis
order_analysis_df = orders_df.join(order_items_df, "OrderID", "inner") \
    .groupBy("OrderID") \
    .agg({"TotalPrice": "sum", "Quantity": "sum"}) \
    .withColumnRenamed("sum(TotalPrice)", "TotalOrderAmount") \
    .withColumnRenamed("sum(Quantity)", "TotalOrderQuantity")


sales_analysis_df = order_items_df.join(orders_df, "OrderID", "inner") \
    .join(payments_df, "OrderID", "inner") \
    .withColumn("OrderYear", year("OrderDate")) \
    .withColumn("OrderMonth", month("OrderDate")) \
    .groupBy("ProductID", "OrderYear", "OrderMonth") \
    .agg({"TotalPrice": "sum", "OrderID": "count"}) \
    .withColumnRenamed("ProductID", "ProductID") \
    .withColumnRenamed("OrderYear", "OrderYear") \
    .withColumnRenamed("OrderMonth", "OrderMonth") \
    .withColumnRenamed("sum(TotalPrice)", "TotalSalesAmount") \
    .withColumnRenamed("count(OrderID)", "TotalNumberOfOrders")




# COMMAND ----------

dbpwd=dbutils.secrets.get('secretscopeproject','dbpassword')

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://efnproject.database.windows.net:1433;databaseName=efndb"

connection_properties = {
    "user": "efnadmin",
    "password": dbpwd,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

total_sales_df.write.jdbc(url=jdbc_url, table="YearMonthlySales", mode="overwrite", properties=connection_properties)
top_products_df.write.jdbc(url=jdbc_url, table="TopProducts", mode="overwrite", properties=connection_properties)
order_analysis_df.write.jdbc(url=jdbc_url, table="OrderAnalysis", mode="overwrite", properties=connection_properties)
sales_analysis_df.write.jdbc(url=jdbc_url, table="SalesAnalysis", mode="overwrite", properties=connection_properties)
