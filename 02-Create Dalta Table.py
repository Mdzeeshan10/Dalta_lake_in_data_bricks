# Databricks notebook source
# DBTITLE 1,Create Dalta Tables 
create Dalta table using python Code

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark) \
    .tableName("delta_internal_demo") \
    .addColumn("emp_id", "INT") \
    .addColumn("emp_name", "STRING") \
    .addColumn("gender", "STRING") \
    .addColumn("salary", "INT") \
    .addColumn("dept", "STRING") \
    .property("description", "Table created for demo purpose") \
    .location("/dbfs/FileStore/tables/delta/arach_demo") \
    .execute()

# COMMAND ----------

DeltaTable.create(spark) \
    .tableName("delta_internal_demo1") \
    .addColumn("emp_id", "INT") \
    .addColumn("emp_name", "STRING") \
    .addColumn("gender", "STRING") \
    .addColumn("salary", "INT") \
    .addColumn("dept", "STRING") \
    .property("description", "Table created for demo purpose") \
    .location("/FileStore/tables/delta/arach_demo") \
    .execute()

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /dbfs/FileStore/tables/delta/arach_demo/_delta_log

# COMMAND ----------

# DBTITLE 1,In this operation perform Create table storing metadata in json file formate
# MAGIC %fs
# MAGIC 
# MAGIC head /dbfs/FileStore/tables/delta/arach_demo/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from delta_internal_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into delta_internal_demo values(200,"zeeshan","M",3000,"IT");

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from delta_internal_demo

# COMMAND ----------

# DBTITLE 1,In this operation perform Write storing metadata in json file formate
# MAGIC %fs
# MAGIC 
# MAGIC head /dbfs/FileStore/tables/delta/arach_demo/_delta_log/00000000000000000001.json

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into delta_internal_demo values(100,"shan","M",4000,"IT");
# MAGIC insert into delta_internal_demo values(500,"zee","M",8000,"IT");
# MAGIC insert into delta_internal_demo values(200,"ABC","F",9000,"cs");

# COMMAND ----------

# DBTITLE 1,All the json file shows metadata record you are insert update Delete Merge etc perform on table
# MAGIC %fs
# MAGIC 
# MAGIC ls /dbfs/FileStore/tables/delta/arach_demo/_delta_log

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC head /dbfs/FileStore/tables/delta/arach_demo/_delta_log/00000000000000000003.json

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into delta_internal_demo values(700,"xyz","M",4000,"DE");
# MAGIC insert into delta_internal_demo values(800,"PQR","F",1122,"IT");
# MAGIC insert into delta_internal_demo values(600,"MNO","M",3456,"DS");
# MAGIC insert into delta_internal_demo values(500,"LMN","F",6000,"IT");
# MAGIC insert into delta_internal_demo values(300,"CDE","M",1000,"IT");
# MAGIC insert into delta_internal_demo values(110,"IJK","F",5000,"IT");
# MAGIC insert into delta_internal_demo values(210,"OPQ","M",4000,"IT");
# MAGIC insert into delta_internal_demo values(120,"UVW","F",7000,"IT");

# COMMAND ----------

# DBTITLE 1,You perform operation to add or anything metadata record in json formate every 10 operation later create a checkpoint in parquet format in this store metadata about metadata information 
# MAGIC %fs
# MAGIC 
# MAGIC ls /dbfs/FileStore/tables/delta/arach_demo/_delta_log

# COMMAND ----------

# DBTITLE 1,See metadata about metadata in this parquet file All information 
display(spark.read.format('parquet').load('/dbfs/FileStore/tables/delta/arach_demo/_delta_log/00000000000000000010.checkpoint.parquet'))
