# Databricks notebook source
# MAGIC %md
# MAGIC ## Lab 06: Data Lakehouse with Structured Streaming
# MAGIC This lab will help you learn to use many of the software libraries and programming techniques required to fulfill the requirements of the final end-of-session capstone project for course **DS-2002: Data Systems**. The spirit of the project is to provide a capstone challenge that requires students to demonstrate a practical and functional understanding of each of the data systems and architectural principles covered throughout the session.
# MAGIC
# MAGIC **These include:**
# MAGIC - Relational Database Management Systems (e.g., MySQL, Microsoft SQL Server, Oracle, IBM DB2)
# MAGIC   - Online Transaction Processing Systems (OLTP): *Optimized for High-Volume Write Operations; Normalized to 3rd Normal Form.*
# MAGIC   - Online Analytical Processing Systems (OLAP): *Optimized for Read/Aggregation Operations; Dimensional Model (i.e, Star Schema)*
# MAGIC - NoSQL *(Not Only SQL)* Systems (e.g., MongoDB, CosmosDB, Cassandra, HBase, Redis)
# MAGIC - File System *(Data Lake)* Source Systems (e.g., AWS S3, Microsoft Azure Data Lake Storage)
# MAGIC   - Various Datafile Formats (e.g., JSON, CSV, Parquet, Text, Binary)
# MAGIC - Massively Parallel Processing *(MPP)* Data Integration Systems (e.g., Apache Spark, Databricks)
# MAGIC - Data Integration Patterns (e.g., Extract-Transform-Load, Extract-Load-Transform, Extract-Load-Transform-Load, Lambda & Kappa Architectures)
# MAGIC
# MAGIC ### Section I: Prerequisites
# MAGIC
# MAGIC #### 1.0. Import Required Libraries

# COMMAND ----------

### Calvin's NOTE: you will need to download the following on your cluster to run this (I unfortunately couldn't get the code below to work with any of the more updated versions of those libraries)
# mongo-spark-connector_2.12-2.4.2.jar
# mongo-java-driver-3.8.2.jar
# bson-3.8.2.jar

# COMMAND ----------

# MAGIC %pip install pymongo
# MAGIC %pip install --upgrade pymongo[srv]

# COMMAND ----------

import os
import json
import pymongo
import pyspark.pandas as pd  # This uses Koalas that is included in PySpark version 3.2 or newer.
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BinaryType
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DecimalType

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.0. Instantiate Global Variables

# COMMAND ----------

# Azure MySQL Server Connection Information ###################
jdbc_hostname = "ds2002-mysql-2.mysql.database.azure.com"
jdbc_port = 3306
src_database = "northwind_dw2"

connection_properties = {
  "user" : "calvinpan",
  "password" : "#Pelican1",
  "driver" : "org.mariadb.jdbc.Driver"
}

# MongoDB Atlas Connection Information ########################
atlas_cluster_name = "ds2002.hc2ph"
atlas_database_name = "northwind_dw2"
atlas_user_name = "calvinpan1"
atlas_password = "Pelican1"

# Data Files (JSON) Information ###############################
dst_database = "northwind_dlh"

base_dir = "dbfs:/FileStore/lab_data"
database_dir = f"{base_dir}/{dst_database}"

data_dir = f"{base_dir}/retail"
batch_dir = f"{data_dir}/batch"
stream_dir = f"{data_dir}/stream"

orders_stream_dir = f"{stream_dir}/orders"
purchase_orders_stream_dir = f"{stream_dir}/purchase_orders"
inventory_trans_stream_dir = f"{stream_dir}/inventory_transactions"

orders_output_bronze = f"{database_dir}/fact_orders/bronze"
orders_output_silver = f"{database_dir}/fact_orders/silver"
orders_output_gold   = f"{database_dir}/fact_orders/gold"

purchase_orders_output_bronze = f"{database_dir}/fact_purchase_orders/bronze"
purchase_orders_output_silver = f"{database_dir}/fact_purchase_orders/silver"
purchase_orders_output_gold   = f"{database_dir}/fact_purchase_orders/gold"

inventory_trans_output_bronze = f"{database_dir}/fact_inventory_transactions/bronze"
inventory_trans_output_silver = f"{database_dir}/fact_inventory_transactions/silver"
inventory_trans_output_gold   = f"{database_dir}/fact_inventory_transactions/gold"

# Delete the Streaming Files ################################## 
dbutils.fs.rm(f"{database_dir}/fact_orders", True) 
dbutils.fs.rm(f"{database_dir}/fact_purchase_orders", True) 
dbutils.fs.rm(f"{database_dir}/fact_inventory_transactions", True)

# Delete the Database Files ###################################
dbutils.fs.rm(database_dir, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.0. Define Global Functions

# COMMAND ----------

##################################################################################################################
# Use this Function to Fetch a DataFrame from the MongoDB Atlas database server Using PyMongo.
##################################################################################################################
def get_mongo_dataframe(user_id, pwd, cluster_name, db_name, collection, conditions, projection, sort):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.mongodb.net/{db_name}"
    
    client = pymongo.MongoClient(mongo_uri)

    '''Query MongoDB, and fill a python list with documents to create a DataFrame'''
    db = client[db_name]
    if conditions and projection and sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection).sort(sort)))
    elif conditions and projection and not sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection)))
    else:
        dframe = pd.DataFrame(list(db[collection].find()))

    client.close()
    
    return dframe

##################################################################################################################
# Use this Function to Create New Collections by Uploading JSON file(s) to the MongoDB Atlas server.
##################################################################################################################
def set_mongo_collection(user_id, pwd, cluster_name, db_name, src_file_path, json_files):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.mongodb.net/{db_name}"
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    
    '''Read in a JSON file, and Use It to Create a New Collection'''
    for file in json_files:
        db.drop_collection(file)
        json_file = os.path.join(src_file_path, json_files[file])
        with open(json_file, 'r') as openfile:
            json_object = json.load(openfile)
            file = db[file]
            result = file.insert_many(json_object)

    client.close()
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section II: Populate Dimensions by Ingesting Reference (Cold-path) Data 
# MAGIC #### 1.0. Fetch Reference Data From an Azure MySQL Database
# MAGIC ##### 1.1. Create a New Databricks Metadata Database.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS northwind_dlh CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS northwind_dlh
# MAGIC COMMENT "DS-2002 Lab 06 Database"
# MAGIC LOCATION "dbfs:/FileStore/lab_data/northwind_dlh"
# MAGIC WITH DBPROPERTIES (contains_pii = true, purpose = "DS-2002 Lab 6.0");

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2. Create a New Table that Sources Date Dimension Data from a Table in an Azure MySQL database. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_date
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:mysql://ds2002-mysql-2.mysql.database.azure.com:3306/northwind_dw2", --Replace with your Server Name
# MAGIC   dbtable "dim_date",
# MAGIC   user "calvinpan",    --Replace with your User Name
# MAGIC   password "#Pelican1"  --Replace with you password
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE northwind_dlh;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE northwind_dlh.dim_date
# MAGIC COMMENT "Date Dimension Table"
# MAGIC LOCATION "dbfs:/FileStore/lab_data/northwind_dlh/dim_date"
# MAGIC AS SELECT * FROM view_date

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED northwind_dlh.dim_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM northwind_dlh.dim_date LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.3. Create a New Table that Sources Product Dimension Data from an Azure MySQL database.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a Temporary View named "view_product" that extracts data from your MySQL Northwind database.
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_product
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:mysql://ds2002-mysql-2.mysql.database.azure.com:3306/northwind_dw2", --Replace with your Server Name
# MAGIC   dbtable "dim_products",
# MAGIC   user "calvinpan",    --Replace with your User Name
# MAGIC   password "#Pelican1"  --Replace with you password
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE northwind_dlh;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE northwind_dlh.dim_product
# MAGIC COMMENT "Products Dimension Table"
# MAGIC LOCATION "dbfs:/FileStore/lab_data/northwind_dlh/dim_product"
# MAGIC AS SELECT * FROM view_product;
# MAGIC
# MAGIC -- Create a new table named "northwind_dlh.dim_product" using data from the view named "view_product"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED northwind_dlh.dim_product;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM northwind_dlh.dim_product LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.0. Fetch Reference Data from a MongoDB Atlas Database
# MAGIC ##### 2.1. View the Data Files on the Databricks File System

# COMMAND ----------

display(dbutils.fs.ls("file:/Workspace/Users/nqc8gh@virginia.edu/DS-2002/04-Databricks/lab_data/retail/batch"))

#OLD : display(dbutils.fs.ls(batch_dir))  # '/dbfs/FileStore/lab_data/retail/batch'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.2. Create a New MongoDB Database, and Load JSON Data Into a New MongoDB Collection
# MAGIC **NOTE:** The following cell **can** be run more than once because the **set_mongo_collection()** function **is** idempotent.

# COMMAND ----------

source_dir = '/Workspace/Users/nqc8gh@virginia.edu/DS-2002/04-Databricks/lab_data/retail/batch'
json_files = {"customers" : 'Northwind_DimCustomers.json'
              , "suppliers" : 'Northwind_DimSuppliers.json'
              , "invoices" : 'Northwind_DimInvoices.json'}

set_mongo_collection(atlas_user_name, atlas_password, atlas_cluster_name, atlas_database_name, source_dir, json_files) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.3.1. Fetch Customer Dimension Data from the New MongoDB Collection

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.mongodb.spark._
# MAGIC
# MAGIC val userName = "calvinpan1"
# MAGIC val pwd = "Pelican1"
# MAGIC val clusterName = "ds2002.hc2ph"
# MAGIC val atlas_uri = s"mongodb+srv://$userName:$pwd@$clusterName.mongodb.net/northwind_dw2.customers?retryWrites=true&w=majority"

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val df_customer = spark.read
# MAGIC   .format("com.mongodb.spark.sql.DefaultSource")
# MAGIC   .option("uri", atlas_uri)
# MAGIC   .load()
# MAGIC   .select(
# MAGIC     "customer_key", "company", "last_name", "first_name", "job_title",
# MAGIC     "business_phone", "fax_number", "address", "city", "state_province",
# MAGIC     "zip_postal_code", "country_region"
# MAGIC   )
# MAGIC
# MAGIC display(df_customer)
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC df_customer.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.3.2. Use the Spark DataFrame to Create a New Customer Dimension Table in the Databricks Metadata Database (northwind_dlh)

# COMMAND ----------

# MAGIC %scala
# MAGIC df_customer.write.format("delta").mode("overwrite").saveAsTable("northwind_dlh.dim_customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED northwind_dlh.dim_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM northwind_dlh.dim_customer LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.4.1 Fetch Supplier Dimension Data from the New MongoDB Collection

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val atlas_uri = s"mongodb+srv://$userName:$pwd@$clusterName.mongodb.net/northwind_dw2.suppliers?retryWrites=true&w=majority"
# MAGIC
# MAGIC val df_supplier = spark.read
# MAGIC   .format("com.mongodb.spark.sql.DefaultSource")
# MAGIC   .option("uri", atlas_uri)
# MAGIC   .load()
# MAGIC   .select(
# MAGIC     "supplier_key", "company", "last_name", "first_name", "job_title"
# MAGIC   )
# MAGIC
# MAGIC display(df_supplier)

# COMMAND ----------

# MAGIC %scala
# MAGIC df_supplier.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.4.2. Use the Spark DataFrame to Create a New Suppliers Dimension Table in the Databricks Metadata Database (northwind_dlh)

# COMMAND ----------

# MAGIC %scala
# MAGIC df_supplier.write.format("delta").mode("overwrite").saveAsTable("northwind_dlh.dim_supplier")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED northwind_dlh.dim_supplier

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM northwind_dlh.dim_supplier LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.5.1 Fetch Invoice Dimension Data from teh New MongoDB Collection

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val atlas_uri = s"mongodb+srv://$userName:$pwd@$clusterName.mongodb.net/northwind_dw2.invoices?retryWrites=true&w=majority"
# MAGIC
# MAGIC val df_invoice = spark.read
# MAGIC   .format("com.mongodb.spark.sql.DefaultSource")
# MAGIC   .option("uri", atlas_uri)
# MAGIC   .load()
# MAGIC   .select(
# MAGIC     "invoice_key", "order_key", "invoice_date", "due_date", "tax", "shipping", "amount_due"
# MAGIC   )
# MAGIC
# MAGIC display(df_invoice)

# COMMAND ----------

# MAGIC %scala
# MAGIC df_invoice.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.5.2. Use the Spark DataFrame to Create a New Invoices Dimension Table in the Databricks Metadata Database (northwind_dlh)

# COMMAND ----------

# MAGIC %scala
# MAGIC df_invoice.write.format("delta").mode("overwrite").saveAsTable("northwind_dlh.dim_invoice")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED northwind_dlh.dim_invoice

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM northwind_dlh.dim_invoice LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.0. Fetch Data from a File System
# MAGIC ##### 3.1. Use PySpark to Read From a CSV File

# COMMAND ----------

employee_csv = f"file:/Workspace/Users/nqc8gh@virginia.edu/DS-2002/04-Databricks/lab_data/retail/batch/Northwind_DimEmployees.csv"
df_employee = spark.read.format('csv').options(header='true', inferSchema='true').load(employee_csv)
display(df_employee)

# COMMAND ----------

df_employee.printSchema()

# COMMAND ----------

df_employee.write.format("delta").mode("overwrite").saveAsTable("northwind_dlh.dim_employee")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED northwind_dlh.dim_employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM northwind_dlh.dim_employee LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3.2 Use PySpark to Read Shipper Dimension Data from CSV File

# COMMAND ----------

shipper_csv = f"file:/Workspace/Users/nqc8gh@virginia.edu/DS-2002/04-Databricks/lab_data/retail/batch/Northwind_DimShippers.csv"
df_shipper = spark.read.format('csv').options(header='true', inferSchema='true').load(shipper_csv)
display(df_shipper)

# COMMAND ----------

df_shipper.printSchema()

# COMMAND ----------

df_shipper.write.format("delta").mode("overwrite").saveAsTable("northwind_dlh.dim_shipper")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED northwind_dlh.dim_shipper;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM northwind_dlh.dim_shipper LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Verify Dimension Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE northwind_dlh;
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section III: Integrate Reference Data with Real-Time Data
# MAGIC #### 6.0. Use AutoLoader to Process Streaming (Hot Path) Orders Fact Data 
# MAGIC ##### 6.1. Bronze Table: Process 'Raw' JSON Data

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 #.option("cloudFiles.schemaHints", "fact_order_key BIGINT")
 #.option("cloudFiles.schemaHints", "order_key BIGINT")
 #.option("cloudFiles.schemaHints", "employee_key BIGINT")
 #.option("cloudFiles.schemaHints", "customer_key BIGINT") 
 #.option("cloudFiles.schemaHints", "product_key BIGINT")
 #.option("cloudFiles.schemaHints", "shipper_key DECIMAL")
 #.option("cloudFiles.schemaHints", "order_date_key DECIMAL")
 #.option("cloudFiles.schemaHints", "paid_date_key DECIMAL")
 #.option("cloudFiles.schemaHints", "shipped_date_key DECIMAL") 
 #.option("cloudFiles.schemaHints", "quantity DECIMAL")
 #.option("cloudFiles.schemaHints", "unit_price DECIMAL")
 #.option("cloudFiles.schemaHints", "discount DECIMAL")
 #.option("cloudFiles.schemaHints", "shipping_fee DECIMAL")
 #.option("cloudFiles.schemaHints", "taxes DECIMAL")
 #.option("cloudFiles.schemaHints", "tax_rate DECIMAL")
 #.option("cloudFiles.schemaHints", "payment_type STRING")
 #.option("cloudFiles.schemaHints", "order_status STRING")
 #.option("cloudFiles.schemaHints", "order_details_status STRING")
 .option("cloudFiles.schemaLocation", orders_output_bronze)
 .option("cloudFiles.inferColumnTypes", "true")
 .option("multiLine", "true")
 .load("file:/Workspace/Users/nqc8gh@virginia.edu/DS-2002/04-Databricks/lab_data/retail/stream/orders")
 .createOrReplaceTempView("orders_raw_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Add Metadata for Traceability */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_bronze_tempview AS (
# MAGIC   SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM orders_raw_tempview
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_bronze_tempview

# COMMAND ----------

(spark.table("orders_bronze_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{orders_output_bronze}/_checkpoint")
      .outputMode("append")
      .table("fact_orders_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6.2. Silver Table: Include Reference Data

# COMMAND ----------

(spark.readStream
  .table("fact_orders_bronze")
  .createOrReplaceTempView("orders_silver_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM northwind_dlh.dim_date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fact_orders_silver_tempview AS (
# MAGIC   SELECT o.fact_order_key,
# MAGIC       o.order_key,
# MAGIC       o.employee_key,
# MAGIC       e.last_name AS employee_last_name,
# MAGIC       e.first_name AS employee_first_name,
# MAGIC       e.job_title AS employee_job_title,
# MAGIC       e.company AS employee_company,
# MAGIC       o.customer_key,
# MAGIC       c.last_name AS customer_last_name,
# MAGIC       c.first_name AS customer_first_name,
# MAGIC       o.product_key,
# MAGIC       p.product_code,
# MAGIC       p.product_name,
# MAGIC       p.standard_cost AS product_standard_cost,
# MAGIC       p.list_price AS product_list_price,
# MAGIC       p.category AS product_category,
# MAGIC       o.shipper_key,
# MAGIC       s.company AS shipper_company,
# MAGIC       s.state_province AS shipper_state_province,
# MAGIC       s.country_region AS shipper_country_region,
# MAGIC       o.order_date_key,
# MAGIC       od.day_name_of_week AS order_day_name_of_week,
# MAGIC       od.day_of_month AS order_day_of_month,
# MAGIC       od.weekday_weekend AS order_weekday_weekend,
# MAGIC       od.month_name AS order_month_name,
# MAGIC       od.calendar_quarter AS order_quarter,
# MAGIC       od.calendar_year AS order_year,
# MAGIC       o.paid_date_key,
# MAGIC       pd.day_name_of_week AS paid_day_name_of_week,
# MAGIC       pd.day_of_month AS paid_day_of_month,
# MAGIC       pd.weekday_weekend AS paid_weekday_weekend,
# MAGIC       pd.month_name AS paid_month_name,
# MAGIC       pd.calendar_quarter AS paid_calendar_quarter,
# MAGIC       pd.calendar_year AS paid_calendar_year,
# MAGIC       o.shipped_date_key,
# MAGIC       sd.day_name_of_week AS shipped_day_name_of_week,
# MAGIC       sd.day_of_month AS shipped_day_of_month,
# MAGIC       sd.weekday_weekend AS shipped_weekday_weekend,
# MAGIC       sd.month_name AS shipped_month_name,
# MAGIC       sd.calendar_quarter AS shipped_calendar_quarter,
# MAGIC       sd.calendar_year AS shipped_calendar_year,
# MAGIC       o.quantity,
# MAGIC       o.unit_price,
# MAGIC       o.discount,
# MAGIC       o.shipping_fee,
# MAGIC       o.taxes,
# MAGIC       o.tax_rate,
# MAGIC       o.payment_type,
# MAGIC       o.order_status,
# MAGIC       o.order_details_status
# MAGIC   FROM orders_silver_tempview AS o
# MAGIC   INNER JOIN northwind_dlh.dim_employee AS e
# MAGIC   ON e.employee_key = o.employee_key
# MAGIC   INNER JOIN northwind_dlh.dim_customer AS c
# MAGIC   ON c.customer_key = o.customer_key
# MAGIC   INNER JOIN northwind_dlh.dim_product AS p
# MAGIC   ON p.product_key = o.product_key
# MAGIC   INNER JOIN northwind_dlh.dim_shipper AS s
# MAGIC   ON s.shipper_key = o.shipper_key
# MAGIC   LEFT OUTER JOIN northwind_dlh.dim_date AS od
# MAGIC   ON od.date_key = o.order_date_key
# MAGIC   LEFT OUTER JOIN northwind_dlh.dim_date AS pd
# MAGIC   ON pd.date_key = o.paid_date_key
# MAGIC   LEFT OUTER JOIN northwind_dlh.dim_date AS sd
# MAGIC   ON sd.date_key = o.shipped_date_key
# MAGIC )

# COMMAND ----------

(spark.table("fact_orders_silver_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{orders_output_silver}/_checkpoint")
      .outputMode("append")
      .table("fact_orders_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fact_orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED northwind_dlh.fact_orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6.3. Gold Table: Perform Aggregations
# MAGIC Create a new Gold table using the CTAS approach. The table should include the number of products sold per customer each Month, along with the Customers' ID, First & Last Name, and the Month in which the order was placed.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE northwind_dlh.fact_monthly_orders_by_customer_gold AS (
# MAGIC   SELECT customer_key AS CustomerID
# MAGIC     , customer_last_name AS LastName
# MAGIC     , customer_first_name AS FirstName
# MAGIC     , order_month_name AS OrderMonth
# MAGIC     , COUNT(product_key) AS ProductCount
# MAGIC   FROM northwind_dlh.fact_orders_silver
# MAGIC   GROUP BY CustomerID, LastName, FirstName, OrderMonth
# MAGIC   ORDER BY ProductCount DESC);
# MAGIC
# MAGIC SELECT * FROM northwind_dlh.fact_monthly_orders_by_customer_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE northwind_dlh.fact_product_orders_by_customer_gold AS (
# MAGIC   SELECT pc.CustomerID
# MAGIC     , os.customer_last_name AS CustomerName
# MAGIC     , os.product_key AS ProductNumber
# MAGIC     , pc.ProductCount
# MAGIC   FROM northwind_dlh.fact_orders_silver AS os
# MAGIC   INNER JOIN (
# MAGIC     SELECT customer_key AS CustomerID
# MAGIC     , COUNT(product_key) AS ProductCount
# MAGIC     FROM northwind_dlh.fact_orders_silver
# MAGIC     GROUP BY customer_key
# MAGIC   ) AS pc
# MAGIC   ON pc.CustomerID = os.customer_key
# MAGIC   ORDER BY ProductCount DESC);
# MAGIC
# MAGIC SELECT * FROM northwind_dlh.fact_product_orders_by_customer_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7.0. Use AutoLoader to Process Streaming (Hot Path) Purchase Orders Fact Data 
# MAGIC ##### 7.1. Bronze Table: Process 'Raw' JSON Data

# COMMAND ----------

# Use spark.readStream and the AutoLoader to read in the JSON files in the "purchase_orders_stream_dir"
# directory and then create a TempView named "purchase_orders_raw_tempview".
# Be sure to set the "cloudFiles.schemaLocation" Option using the "purchase_orders_output_bronze" directory

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation", purchase_orders_output_bronze)
 .option("cloudFiles.inferColumnTypes", "true")
 .option("multiLine", "true")
 .load("file:/Workspace/Users/nqc8gh@virginia.edu/DS-2002/04-Databricks/lab_data/retail/stream/purchase_orders")
 .createOrReplaceTempView("purchase_orders_raw_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Add Metadata for Traceability */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW purchase_orders_bronze_tempview AS (
# MAGIC   SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM purchase_orders_raw_tempview
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM purchase_orders_bronze_tempview

# COMMAND ----------

(spark.table("purchase_orders_bronze_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{purchase_orders_output_bronze}/_checkpoint")
      .outputMode("append")
      .table("fact_purchase_orders_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7.2. Silver Table: Include Reference Data

# COMMAND ----------

(spark.readStream
  .table("fact_purchase_orders_bronze")
  .createOrReplaceTempView("purchase_orders_silver_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM purchase_orders_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED purchase_orders_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW COLUMNS IN orders_silver_tempview
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fact_purchase_orders_silver_tempview AS (
# MAGIC   SELECT
# MAGIC       po.fact_purchase_order_key,
# MAGIC       po.purchase_order_key,
# MAGIC       
# MAGIC       -- 1. Employee Info (Role-based: Submitted By, Approved By, Created By)
# MAGIC       po.submitted_by,
# MAGIC       esub.first_name AS submitted_by_first_name,
# MAGIC       esub.last_name AS submitted_by_last_name,
# MAGIC       esub.job_title AS submitted_by_job_title,
# MAGIC       esub.company AS submitted_by_company,
# MAGIC
# MAGIC       po.approved_by,
# MAGIC       eapp.first_name AS approved_by_first_name,
# MAGIC       eapp.last_name AS approved_by_last_name,
# MAGIC       eapp.job_title AS approved_by_job_title,
# MAGIC       eapp.company AS approved_by_company,
# MAGIC
# MAGIC       po.created_by,
# MAGIC       ecrt.first_name AS created_by_first_name,
# MAGIC       ecrt.last_name AS created_by_last_name,
# MAGIC       ecrt.job_title AS created_by_job_title,
# MAGIC       ecrt.company AS created_by_company,
# MAGIC
# MAGIC       -- 2. Product Info
# MAGIC       po.product_key,
# MAGIC       p.product_code,
# MAGIC       p.product_name,
# MAGIC       p.standard_cost AS product_standard_cost,
# MAGIC       p.list_price AS product_list_price,
# MAGIC       p.category AS product_category,
# MAGIC
# MAGIC       -- 3. Supplier Info
# MAGIC       po.supplier_key,
# MAGIC       s.company AS supplier_company,
# MAGIC       s.last_name AS supplier_last_name,
# MAGIC       s.first_name AS supplier_first_name,
# MAGIC       s.job_title AS supplier_job_title,
# MAGIC
# MAGIC       -- 4. Date Info (Role-Playing: Creation, Submitted, Received)
# MAGIC       po.creation_date_key,
# MAGIC       cd.day_name_of_week AS creation_day_name_of_week,
# MAGIC       cd.day_of_month AS creation_day_of_month,
# MAGIC       cd.weekday_weekend AS creation_weekday_weekend,
# MAGIC       cd.month_name AS creation_month_name,
# MAGIC       cd.calendar_quarter AS creation_quarter,
# MAGIC       cd.calendar_year AS creation_year,
# MAGIC
# MAGIC       po.submitted_date_key,
# MAGIC       sd.day_name_of_week AS submitted_day_name_of_week,
# MAGIC       sd.day_of_month AS submitted_day_of_month,
# MAGIC       sd.weekday_weekend AS submitted_weekday_weekend,
# MAGIC       sd.month_name AS submitted_month_name,
# MAGIC       sd.calendar_quarter AS submitted_quarter,
# MAGIC       sd.calendar_year AS submitted_year,
# MAGIC
# MAGIC       po.po_detail_date_received_key,
# MAGIC       rd.day_name_of_week AS received_day_name_of_week,
# MAGIC       rd.day_of_month AS received_day_of_month,
# MAGIC       rd.weekday_weekend AS received_weekday_weekend,
# MAGIC       rd.month_name AS received_month_name,
# MAGIC       rd.calendar_quarter AS received_quarter,
# MAGIC       rd.calendar_year AS received_year,
# MAGIC
# MAGIC       -- 5. Order Details
# MAGIC       po.po_detail_quantity,
# MAGIC       po.po_detail_unit_cost,
# MAGIC       po.shipping_fee,
# MAGIC       po.taxes,
# MAGIC       po.purchase_order_status,
# MAGIC       po.payment_amount,
# MAGIC       po.payment_date,
# MAGIC       po.receipt_time,
# MAGIC       po.source_file
# MAGIC
# MAGIC   FROM purchase_orders_silver_tempview AS po
# MAGIC
# MAGIC   -- Join to Product Dimension
# MAGIC   INNER JOIN northwind_dlh.dim_product AS p
# MAGIC     ON po.product_key = p.product_key
# MAGIC
# MAGIC   -- Join to Supplier Dimension
# MAGIC   INNER JOIN northwind_dlh.dim_supplier AS s
# MAGIC     ON po.supplier_key = s.supplier_key
# MAGIC
# MAGIC   -- Role-based Joins to Employee Dimension
# MAGIC   LEFT JOIN northwind_dlh.dim_employee AS esub
# MAGIC     ON po.submitted_by = esub.employee_key
# MAGIC
# MAGIC   LEFT JOIN northwind_dlh.dim_employee AS eapp
# MAGIC     ON po.approved_by = eapp.employee_key
# MAGIC
# MAGIC   LEFT JOIN northwind_dlh.dim_employee AS ecrt
# MAGIC     ON po.created_by = ecrt.employee_key
# MAGIC
# MAGIC   -- Role-Playing Date Joins
# MAGIC   LEFT JOIN northwind_dlh.dim_date AS cd
# MAGIC     ON po.creation_date_key = cd.date_key
# MAGIC
# MAGIC   LEFT JOIN northwind_dlh.dim_date AS sd
# MAGIC     ON po.submitted_date_key = sd.date_key
# MAGIC
# MAGIC   LEFT JOIN northwind_dlh.dim_date AS rd
# MAGIC     ON po.po_detail_date_received_key = rd.date_key
# MAGIC )
# MAGIC

# COMMAND ----------

(spark.table("fact_purchase_orders_silver_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{purchase_orders_output_silver}/_checkpoint")
      .outputMode("append")
      .table("fact_purchase_orders_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fact_purchase_orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED fact_purchase_orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7.3. Gold Table: Perform Aggregations
# MAGIC Create a new Gold table using the CTAS approach. The table should include the total amount (total list price) of the purchase orders placed per Supplier for each Product. Include the Suppliers' Company Name, and the Product Name.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Author a query that returns the Total List Price grouped by Supplier and Product and sorted by Total List Price descending.
# MAGIC
# MAGIC CREATE OR REPLACE TABLE northwind_dlh.fact_orders_per_supplier_gold AS
# MAGIC SELECT
# MAGIC     s.company AS supplier_company,
# MAGIC     p.product_name,
# MAGIC     SUM(po.po_detail_quantity * p.list_price) AS total_list_price
# MAGIC FROM fact_purchase_orders_silver AS po
# MAGIC INNER JOIN northwind_dlh.dim_supplier AS s
# MAGIC   ON po.supplier_key = s.supplier_key
# MAGIC INNER JOIN northwind_dlh.dim_product AS p
# MAGIC   ON po.product_key = p.product_key
# MAGIC GROUP BY
# MAGIC     s.company,
# MAGIC     p.product_name
# MAGIC ORDER BY
# MAGIC     total_list_price DESC;
# MAGIC
# MAGIC SELECT * FROM northwind_dlh.fact_orders_per_supplier_gold 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8.0. Use AutoLoader to Process Streaming (Hot Path) Inventory Transactions Fact Data 
# MAGIC ##### 8.1. Bronze Table: Process 'Raw' JSON Data

# COMMAND ----------

# Use spark.readStream and the AutoLoader to read in the JSON files in the "inventory_trans_stream_dir"
# directory and then create a TempView named "inventory_transactions_raw_tempview".
# Be sure to set the "cloudFiles.schemaLocation" Option using the "inventory_trans_output_bronze" directory
(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation", inventory_trans_output_bronze)
 .option("cloudFiles.inferColumnTypes", "true")
 .option("multiLine", "true")
 .load("file:/Workspace/Users/nqc8gh@virginia.edu/DS-2002/04-Databricks/lab_data/retail/stream/inventory_transactions")
 .createOrReplaceTempView("inventory_transactions_raw_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Add Metadata for Traceability */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW inventory_transactions_bronze_tempview AS (
# MAGIC   SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM inventory_transactions_raw_tempview
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM inventory_transactions_raw_tempview

# COMMAND ----------

(spark.table("inventory_transactions_bronze_tempview")
     .writeStream
     .format("delta")
     .option("checkpointLocation", f"{inventory_trans_output_bronze}/_checkpoint")
     .outputMode("append")
     .table("fact_inventory_transactions_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8.2. Silver Table: Include Reference Data

# COMMAND ----------

(spark.readStream
  .table("fact_inventory_transactions_bronze")
  .createOrReplaceTempView("inventory_transactions_silver_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM inventory_transactions_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED inventory_transactions_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a new Temporary View named "fact_inventory_transactions_silver_tempview" by selecting data from
# MAGIC -- "inventory_transactions_silver_tempview" and joining it to the Product and Date dimension tables.
# MAGIC -- Remember that the Date dimension can serve as a "Role Playing" dimension by being Joined upon multiple times.
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fact_inventory_transactions_silver_tempview AS (
# MAGIC   SELECT 
# MAGIC     i.fact_inventory_transaction_key,
# MAGIC
# MAGIC     -- 1. Products
# MAGIC     i.product_key,
# MAGIC     p.product_code,
# MAGIC     p.product_name,
# MAGIC     p.standard_cost AS product_standard_cost,
# MAGIC     p.list_price AS product_list_price,
# MAGIC     p.category AS product_category,
# MAGIC
# MAGIC     -- 2. Dates
# MAGIC     i.transaction_created_date_key,
# MAGIC     tcd.day_name_of_week AS transaction_created_day_name_of_week,
# MAGIC     tcd.day_of_month AS transaction_created_day_of_month,
# MAGIC     tcd.weekday_weekend AS transaction_created_weekday_weekend,
# MAGIC     tcd.month_name AS transaction_created_month_name,
# MAGIC     tcd.calendar_quarter AS transaction_created_quarter,
# MAGIC     tcd.calendar_year AS transaction_created_year,
# MAGIC
# MAGIC     i.transaction_modified_date_key,
# MAGIC     tmd.day_name_of_week AS transaction_modified_day_name_of_week,
# MAGIC     tmd.day_of_month AS transaction_modified_day_of_month,
# MAGIC     tmd.weekday_weekend AS transaction_modified_weekday_weekend,
# MAGIC     tmd.month_name AS transaction_modified_month_name,
# MAGIC     tmd.calendar_quarter AS transaction_modified_quarter,
# MAGIC     tmd.calendar_year AS transaction_modified_year,
# MAGIC
# MAGIC     -- Other inventory transaction columns
# MAGIC     i.inventory_transaction_type,
# MAGIC     i.quantity
# MAGIC
# MAGIC   FROM inventory_transactions_silver_tempview AS i
# MAGIC
# MAGIC   -- Join to Product
# MAGIC   INNER JOIN northwind_dlh.dim_product AS p
# MAGIC     ON p.product_key = i.product_key 
# MAGIC     
# MAGIC   -- Role-playing Date joins
# MAGIC   LEFT OUTER JOIN northwind_dlh.dim_date AS tcd
# MAGIC     ON tcd.date_key = i.transaction_created_date_key
# MAGIC
# MAGIC   LEFT OUTER JOIN northwind_dlh.dim_date AS tmd
# MAGIC     ON tmd.date_key = i.transaction_modified_date_key
# MAGIC )
# MAGIC

# COMMAND ----------

(spark.table("fact_inventory_transactions_silver_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{inventory_trans_output_silver}/_checkpoint")
      .outputMode("append")
      .table("fact_inventory_transactions_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fact_inventory_transactions_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED fact_inventory_transactions_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8.3. Gold Table: Perform Aggregations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Author a query that returns the Total Quantity grouped by the Quarter Created, Inventory Transaction Type, and Product
# MAGIC -- Sort by the Total Quantity Descending
# MAGIC SELECT
# MAGIC   tcd.calendar_quarter AS created_quarter,
# MAGIC   i.inventory_transaction_type,
# MAGIC   p.product_name,
# MAGIC   SUM(i.quantity) AS total_quantity
# MAGIC FROM fact_inventory_transactions_silver_tempview AS i
# MAGIC LEFT JOIN northwind_dlh.dim_date AS tcd
# MAGIC   ON i.transaction_created_date_key = tcd.date_key
# MAGIC LEFT JOIN northwind_dlh.dim_product AS p
# MAGIC   ON i.product_key = p.product_key
# MAGIC GROUP BY
# MAGIC   tcd.calendar_quarter,
# MAGIC   i.inventory_transaction_type,
# MAGIC   p.product_name
# MAGIC ORDER BY
# MAGIC   total_quantity DESC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 9.0. Clean up the File System

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/lab_data/
