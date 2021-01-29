// Databricks notebook source
// MAGIC %md
// MAGIC # CI/CD Lab
// MAGIC ## Distinct Articles

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) About this Notebook
// MAGIC 
// MAGIC In the cell provided below, we count the number of distinct articles in our data set.
// MAGIC 
// MAGIC 0. Read in Wikipedia parquet files.
// MAGIC 0. Apply the necessary transformations.
// MAGIC 0. Define a schema that matches the data we are working with.
// MAGIC 0. Assign the count to the variable `totalArticles`

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Retrieve Wikipedia Articles

// COMMAND ----------

// MAGIC %python
// MAGIC (source, sasEntity, sasToken) = getAzureDataSource()
// MAGIC spark.conf.set(sasEntity, sasToken)
// MAGIC 
// MAGIC path = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"

// COMMAND ----------

// MAGIC %python
// MAGIC # Define a schema and load the Parquet files
// MAGIC 
// MAGIC from pyspark.sql.types import *
// MAGIC 
// MAGIC parquetDir = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"
// MAGIC 
// MAGIC schema = StructType([
// MAGIC   StructField("project", StringType(), False),
// MAGIC   StructField("article", StringType(), False),
// MAGIC   StructField("requests", IntegerType(), False),
// MAGIC   StructField("bytes_served", LongType(), False)
// MAGIC ])
// MAGIC 
// MAGIC df = (spark.read
// MAGIC   .schema(schema)
// MAGIC   .parquet(parquetDir)
// MAGIC   .select("*")
// MAGIC   .distinct()
// MAGIC )
// MAGIC 
// MAGIC totalArticles = df.count()
// MAGIC 
// MAGIC print("Distinct Articles: {0:,}".format( totalArticles ))

// COMMAND ----------

// MAGIC %python
// MAGIC display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC # display(df.select("article"))