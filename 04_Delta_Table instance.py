# Databricks notebook source
# DBTITLE 1,Delta Table Instance
# Delta table instance is reploca of delta table.
# It is mainly used to perform DML operations on delta table using Pyspark language
# It can be created using 2 ways

# COMMAND ----------

## Approach 1: Using forPath
## Approach 2: Using forName

# COMMAND ----------

# DBTITLE 1,Syntax
# deltaTable = DeltaTable.forPAth(spark, "/path/to/table")
# deltaTable = DeltaTable.forName(spart, "table_name")

# COMMAND ----------

from delta.tables import *


DeltaTable.create(spark) \
    .tableName("employee_demo_in") \
    .addColumn("emp_id", "INT") \
    .addColumn("emp_name", "STRING") \
    .addColumn("gender", "STRING") \
    .addColumn("salary", "INT") \
    .addColumn("Dept", "STRING") \
    .property("description", "table created for demo purpose") \
    .location("/FileStore/table/delta/createtable01_in") \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into employee_demo_in values(100,"shan","M",4000,"IT");
# MAGIC insert into employee_demo_in values(500,"zee","M",8000,"IT");
# MAGIC insert into employee_demo_in values(200,"ABC","F",9000,"cs");

# COMMAND ----------

from delta.tables import *


deltaInstance1 = DeltaTable.forPath(spark, "/FileStore/table/delta/createtable01_in")

# COMMAND ----------

display(deltaInstance1.toDF())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC delete from employee_demo_in where emp_id=200

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from employee_demo_in

# COMMAND ----------

display(deltaInstance1.toDF())

# COMMAND ----------

deltaInstance1.delete("emp_id = 100")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from employee_demo_in

# COMMAND ----------

display(deltaInstance1.toDF())

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Second Approach
deltaInstance2 = DeltaTable.forName(spark, "employee_demo_in")

# COMMAND ----------

display(deltaInstance2.toDF())

# COMMAND ----------

# DBTITLE 1,using sql
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY employee_demo_in

# COMMAND ----------

# DBTITLE 1,using pyspark
display(deltaInstance2.history())

# COMMAND ----------


