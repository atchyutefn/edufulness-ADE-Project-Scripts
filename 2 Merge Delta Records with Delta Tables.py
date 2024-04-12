# Databricks notebook source
df_customers=spark.read.csv('dbfs:/mnt/deltaProject/2024/03/19/Customers.csv',header=True)
df_customers.createOrReplaceTempView('staging_customers')


# COMMAND ----------

# DBTITLE 1,Merge _ SCD type 1
# MAGIC %sql
# MAGIC MERGE INTO delta_customers  AS target
# MAGIC USING staging_customers  AS source
# MAGIC ON target.CustomerID=source.CustomerID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET 
# MAGIC     target.FirstName=source.FirstName,
# MAGIC     target.LastName=source.LastName,
# MAGIC     target.Email = source.Email,
# MAGIC     target.Phone = source.Phone,
# MAGIC     target.CreatedDate = source.CreatedDate,
# MAGIC     target.ModifiedDate = source.ModifiedDate
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (CustomerID, FirstName, LastName, Email, Phone, CreatedDate, ModifiedDate)
# MAGIC     VALUES (source.CustomerID, source.FirstName, source.LastName, source.Email, source.Phone, source.CreatedDate, source.ModifiedDate);    
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from delta_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from staging_customers

# COMMAND ----------

# DBTITLE 1,SCD Type 2
df_payments=spark.read.csv('dbfs:/mnt/deltaProject/2024/03/19/Payments.csv',header=True)
df_payments.createOrReplaceTempView('staging_payments')


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta_payments as target
# MAGIC using staging_payments as source
# MAGIC on target.paymentid=source.paymentid and target.status=1
# MAGIC when matched then
# MAGIC     update set
# MAGIC         target.status=0;
# MAGIC
# MAGIC --Insert into delta_payemnts from staging
# MAGIC insert into delta_payments
# MAGIC select *,1 from staging_payments;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from staging_payments

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from delta_payments

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from delta_payments where paymentid=2003

# COMMAND ----------


