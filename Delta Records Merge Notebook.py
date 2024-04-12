# Databricks notebook source
from datetime import datetime

# Get the current date
current_date = datetime.now()

# Format the date as yyyy/mm/dd
formatted_date = current_date.strftime("%Y/%m/%d")
filepath='dbfs:/mnt/deltaProject/'+formatted_date
print(filepath)

# COMMAND ----------

# DBTITLE 1,1. Customers
df_customers=spark.read.csv(filepath+'/Customers.csv',header=True)
df_customers.createOrReplaceTempView('staging_customers')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge records
# MAGIC MERGE INTO delta_customers AS target
# MAGIC USING staging_customers AS source
# MAGIC ON target.CustomerID = source.CustomerID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.FirstName = source.FirstName,
# MAGIC     target.LastName = source.LastName,
# MAGIC     target.Email = source.Email,
# MAGIC     target.Phone = source.Phone,
# MAGIC     target.ModifiedDate = source.ModifiedDate
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (CustomerID, FirstName, LastName, Email, Phone, CreatedDate, ModifiedDate)
# MAGIC   VALUES (source.CustomerID, source.FirstName, source.LastName, source.Email, source.Phone, source.CreatedDate, source.ModifiedDate);
# MAGIC

# COMMAND ----------

# DBTITLE 1,2. Products
df_products=spark.read.csv(filepath+'/Products.csv',header=True)
df_products.createOrReplaceTempView('staging_products')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge records for Products table
# MAGIC MERGE INTO delta_products AS target
# MAGIC USING staging_products AS source
# MAGIC ON target.ProductID = source.ProductID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.ProductName = source.ProductName,
# MAGIC     target.Price = source.Price,
# MAGIC     target.Category = source.Category,
# MAGIC     target.CreatedDate = source.CreatedDate,
# MAGIC     target.ModifiedDate = source.ModifiedDate
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (ProductID, ProductName, Price, Category, CreatedDate, ModifiedDate)
# MAGIC   VALUES (source.ProductID, source.ProductName, source.Price, source.Category, source.CreatedDate, source.ModifiedDate);
# MAGIC

# COMMAND ----------

# DBTITLE 1,3 Orders
df_orders=spark.read.csv(filepath+'/Orders.csv',header=True)
df_orders.createOrReplaceTempView('staging_orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge records
# MAGIC MERGE INTO delta_orders AS target
# MAGIC USING staging_orders AS source
# MAGIC ON target.OrderID = source.OrderID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.CustomerID = source.CustomerID,
# MAGIC     target.OrderDate = source.OrderDate,
# MAGIC     target.OrderStatus = source.OrderStatus,
# MAGIC     target.CreatedDate = source.CreatedDate,
# MAGIC     target.ModifiedDate = source.ModifiedDate
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (OrderID, CustomerID, OrderDate, OrderStatus, CreatedDate, ModifiedDate)
# MAGIC   VALUES (source.OrderID, source.CustomerID, source.OrderDate, source.OrderStatus, source.CreatedDate, source.ModifiedDate);
# MAGIC

# COMMAND ----------

# DBTITLE 1,4 Order Items
df_OrderItems=spark.read.csv(filepath+'/OrderItems.csv',header=True)
df_OrderItems.createOrReplaceTempView('staging_orderItems')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge records
# MAGIC MERGE INTO delta_orderitems AS target
# MAGIC USING staging_orderItems AS source
# MAGIC ON target.OrderItemID = source.OrderItemID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.OrderID = source.OrderID,
# MAGIC     target.ProductID = source.ProductID,
# MAGIC     target.Quantity = source.Quantity,
# MAGIC     target.TotalPrice=source.TotalPrice,
# MAGIC     target.CreatedDate = source.CreatedDate,
# MAGIC     target.ModifiedDate = source.ModifiedDate
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (OrderItemID, OrderID, ProductID, Quantity, TotalPrice, CreatedDate, ModifiedDate)
# MAGIC   VALUES (source.OrderItemID, source.OrderID, source.ProductID, source.Quantity, source.TotalPrice, source.CreatedDate, source.ModifiedDate);
# MAGIC

# COMMAND ----------

# DBTITLE 1,5 inventory
df_Inventory=spark.read.csv(filepath+'/Inventory.csv',header=True)
df_Inventory.createOrReplaceTempView('staging_inventory')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge records for Inventory table
# MAGIC MERGE INTO delta_inventory AS target
# MAGIC USING staging_inventory AS source
# MAGIC ON target.ProductID = source.ProductID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.StockQuantity = source.StockQuantity,
# MAGIC     target.CreatedDate = source.CreatedDate,
# MAGIC     target.ModifiedDate = source.ModifiedDate
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (ProductID, StockQuantity, CreatedDate, ModifiedDate)
# MAGIC   VALUES (source.ProductID, source.StockQuantity, source.CreatedDate, source.ModifiedDate);
# MAGIC

# COMMAND ----------

# DBTITLE 1,6 Payments
df_payments=spark.read.csv(filepath+'/Payments.csv',header=True)
df_payments.createOrReplaceTempView('staging_payments')

# COMMAND ----------

# DBTITLE 1,SCD Type 2
# MAGIC %sql
# MAGIC -- Add a Status column with a default value of 1 to the staging_payments table
# MAGIC --ALTER TABLE staging_payments ADD COLUMN Status INT DEFAULT 1;
# MAGIC
# MAGIC -- Merge operation to handle updates and inserts for SCD Type 2
# MAGIC MERGE INTO delta_Payments AS target
# MAGIC USING staging_payments AS source
# MAGIC ON target.PaymentID = source.PaymentID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.Status = 0;
# MAGIC
# MAGIC -- Insert all records from staging_payments
# MAGIC INSERT INTO delta_payments 
# MAGIC select *,1 from staging_payments
# MAGIC

# COMMAND ----------

# DBTITLE 1,7 Promotions
df_Promotions=spark.read.csv(filepath+'/Promotions.csv',header=True)
df_Promotions.createOrReplaceTempView('staging_promotions')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge records for Promotions table
# MAGIC MERGE INTO delta_promotions AS target
# MAGIC USING staging_promotions AS source
# MAGIC ON target.PromotionID = source.PromotionID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.PromotionName = source.PromotionName,
# MAGIC     target.Discount = source.Discount,
# MAGIC     target.StartDate = source.StartDate,
# MAGIC     target.EndDate = source.EndDate,
# MAGIC     target.CreatedDate = source.CreatedDate,
# MAGIC     target.ModifiedDate = source.ModifiedDate
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (PromotionID, PromotionName, Discount, StartDate, EndDate, CreatedDate, ModifiedDate)
# MAGIC   VALUES (source.PromotionID, source.PromotionName, source.Discount, source.StartDate, source.EndDate, source.CreatedDate, source.ModifiedDate);
# MAGIC

# COMMAND ----------

# DBTITLE 1,8 Reviews
df_Reviews=spark.read.csv(filepath+'/Reviews.csv',header=True)
df_Reviews.createOrReplaceTempView('staging_Reviews')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta_reviews AS target
# MAGIC USING staging_reviews AS source
# MAGIC ON target.ReviewID = source.ReviewID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.ProductID = source.ProductID,
# MAGIC     target.CustomerID = source.CustomerID,
# MAGIC     target.Rating = source.Rating,
# MAGIC     target.Comment = source.Comment,
# MAGIC     target.CreatedDate = source.CreatedDate,
# MAGIC     target.ModifiedDate = source.ModifiedDate
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (ReviewID, ProductID, CustomerID, Rating, Comment, CreatedDate, ModifiedDate)
# MAGIC   VALUES (source.ReviewID, source.ProductID, source.CustomerID, source.Rating, source.Comment, source.CreatedDate, source.ModifiedDate)
# MAGIC

# COMMAND ----------

# DBTITLE 1,9 Promotions
df_Shippingdetails=spark.read.csv(filepath+'/ShippingDetails.csv',header=True)
df_Shippingdetails.createOrReplaceTempView('staging_shippingdetails')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta_shipping_details AS target
# MAGIC USING staging_shippingdetails AS source
# MAGIC ON target.ShippingID = source.ShippingID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.OrderID = source.OrderID,
# MAGIC     target.ShippingMethod = source.ShippingMethod,
# MAGIC     target.TrackingNumber = source.TrackingNumber,
# MAGIC     target.ShipDate = source.ShipDate,
# MAGIC     target.CreatedDate = source.CreatedDate,
# MAGIC     target.ModifiedDate = source.ModifiedDate
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (ShippingID, OrderID, ShippingMethod, TrackingNumber, ShipDate, CreatedDate, ModifiedDate)
# MAGIC   VALUES (source.ShippingID, source.OrderID, source.ShippingMethod, source.TrackingNumber, source.ShipDate, source.CreatedDate, source.ModifiedDate)
# MAGIC

# COMMAND ----------

# DBTITLE 1,10 Returns
df_Returns=spark.read.csv(filepath+'/Returns.csv',header=True)
df_Returns.createOrReplaceTempView('staging_Returns')

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO delta_returns AS target
# MAGIC USING staging_returns AS source
# MAGIC ON target.ReturnID = source.ReturnID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.OrderID = source.OrderID,
# MAGIC     target.ProductID = source.ProductID,
# MAGIC     target.ReturnReason = source.ReturnReason,
# MAGIC     target.ReturnDate = source.ReturnDate,
# MAGIC     target.CreatedDate = source.CreatedDate,
# MAGIC     target.ModifiedDate = source.ModifiedDate
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (ReturnID, OrderID, ProductID, ReturnReason, ReturnDate, CreatedDate, ModifiedDate)
# MAGIC   VALUES (source.ReturnID, source.OrderID, source.ProductID, source.ReturnReason, source.ReturnDate, source.CreatedDate, source.ModifiedDate)
# MAGIC

# COMMAND ----------


