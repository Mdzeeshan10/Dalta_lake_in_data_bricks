# Databricks notebook source
# DBTITLE 1,Method 1: Pyspark
from delta.tables import *


DeltaTable.create(spark) \
    .tableName("employee_demo") \
    .addColumn("emp_id", "INT") \
    .addColumn("emp_name", "STRING") \
    .addColumn("gender", "STRING") \
    .addColumn("salary", "INT") \
    .addColumn("Dept", "STRING") \
    .property("description", "table created for demo purpose") \
    .location("/FileStore/table/delta/createtable01") \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from employee_demo

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("employee_demo") \
    .addColumn("emp_id", "INT") \
    .addColumn("emp_name", "STRING") \
    .addColumn("gender", "STRING") \
    .addColumn("salary", "INT") \
    .addColumn("Dept", "STRING") \
    .property("description", "table created for demo purpose") \
    .location("/FileStore/table/delta/createtable01") \
    .execute()

# COMMAND ----------

DeltaTable.createOrReplace(spark) \
    .tableName("employee_demo") \
    .addColumn("emp_id", "INT") \
    .addColumn("emp_name", "STRING") \
    .addColumn("gender", "STRING") \
    .addColumn("salary", "INT") \
    .addColumn("Dept", "STRING") \
    .property("description", "table created for demo purpose") \
    .location("/FileStore/table/delta/createtable01") \
    .execute()

# COMMAND ----------

# DBTITLE 1,Method 2: SQL  in this sql method to create delta table and this is created in hive 
# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE empolyee_demosql (
# MAGIC   emp_id INT,
# MAGIC   emp_Name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept STRING
# MAGIC   )USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS empolyee_demosql (
# MAGIC   emp_id INT,
# MAGIC   emp_Name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept STRING
# MAGIC   )USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE empolyee_demosql (
# MAGIC   emp_id INT,
# MAGIC   emp_Name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept STRING
# MAGIC   )USING DELTA

# COMMAND ----------

employee_data = [(900,"Zeeshan","M",9000,"DS"),
               (200,"Zee","M",5000,"IT"),
               (100,"XYZ","F",6000,"DEV")]

employee_schema = ["emp_id", "emp_name", "gender", "salary", "dept"]

df = spark.createDataFrame(data=employee_data, schema=employee_schema)

display(df)

# COMMAND ----------

# Create table in the metastore using DataFrame's schema and write data to it

df.write.format("delta").saveAsTable("employee_demo_df")

# COMMAND ----------

# DBTITLE 1,Method 3: Data Frame to create Delta table
# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM employee_demo_df

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC head dbfs:/user/hive/warehouse/employee_demo_df/_delta_log/00000000000000000000.json

# COMMAND ----------



# COMMAND ----------


