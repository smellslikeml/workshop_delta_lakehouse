# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2020/04/og-databricks.png" alt="Databricks Workshop" style="width: 300px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Workshop Overview
# MAGIC Facilitators: `{afsana.afzal,corbin.albert,salma.mayorquin}@databricks.com`
# MAGIC 
# MAGIC In this workshop we're going to walk through the power of Delta and Lakehouse paradigm by creating, managing, and querying a Delta Lake. 
# MAGIC 
# MAGIC | Section | Title                                        | Description                                                                                         |
# MAGIC |---------|----------------------------------------------|-----------------------------------------------------------------------------------------------------|
# MAGIC | 1       | From Raw JSON to Bronze Delta: Auto Loader   | Dump JSON files into COS Bucket and Auto-Ingest with Auto Loader                                    |
# MAGIC | 2       | Append Feb Data: Unified Batch and Streaming | Let Auto Loader auto-ingest new data and see how Delta unified Batch and Streaming                  |
# MAGIC | 3       | Time Travel                                  | See how you can go back in time to previous table-states. Commit-level granularity                  |
# MAGIC | 4       | Handling Schemas: Enforcement and Evolution  | Delta enforces schemas by default but allows for easily evolving schemas as well                    |
# MAGIC | 5       | Clean Data for Silver Layer: MERGE UPSERT    | Create a silver table by creating a cleaning pipeline from bronze. See `MERGE UPSERT` functionality |
# MAGIC | 6       | Aggregate to Gold: Delta Constraints         | Finish the medallion multi-hop ETL pipeline. Enforce data constraints to ensure data quality        | 
# MAGIC | 7       | Huge Performance Improvements                | Explore Data Skipping, ZORDER, OPTIMIZE and Delta Cache. Huge performance gains                     |
# MAGIC | 8       | VACUUM                                       | Delete table history older than retention period                                                    |

# COMMAND ----------

# MAGIC %md
# MAGIC # 0. Environment Setup

# COMMAND ----------

# MAGIC %md 
# MAGIC Fill in Username. 

# COMMAND ----------

# Creates Username Widget 
dbutils.widgets.text("Username","username_here")

# COMMAND ----------

# Define Data Paths
username = dbutils.widgets.get("Username")

BASE_FP = f"dbfs:/delta_workshop/{username}/health_tracker/"
RAW_FP = BASE_FP + "raw/"
BRONZE_FP = BASE_FP + "bronze/"
SILVER_FP = BASE_FP + "silver/"
GOLD_FP = BASE_FP + "gold/"

# Clean up from potential earlier runs
dbutils.fs.rm(BASE_FP, recurse=True)
spark.sql(f"DROP DATABASE IF EXISTS delta_workshop_{username} CASCADE;")

# Create and set default database to use
spark.sql(f"CREATE DATABASE IF NOT EXISTS delta_workshop_{username}")
spark.sql(f"USE delta_workshop_{username}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. From Raw JSON to Bronze Delta: Auto Loader
# MAGIC The first step of our pipeline is to ingest raw JSON files and bring them into Delta as our Bronze layer. There should be **little to no processing on the raw data before making its way to bronze**. There are a number of reasons for this:
# MAGIC 0. Ability to "go back to raw" without having to go all the way back to source.
# MAGIC   + Often "raw" history gets cleaned up (ex. Kafka retention period)
# MAGIC   + Can be expensive to go back to source
# MAGIC   + Saves developer time (which is very expensive!)
# MAGIC 0. Gives a source of truth where no ETL errors could have arisen for easier debugging and iterative development
# MAGIC 0. Ability to easily time travel to previous data state
# MAGIC 0. Downstream read and manipulation performance improvements

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve First Month of Data
# MAGIC One common use case for working with Delta Lake is to collect and process Internet of Things (IoT) Data. Here, we provide a mock IoT sensor dataset for demonstration purposes. The data simulates heart rate data measured by a health tracker device.
# MAGIC 
# MAGIC First, we use the utility function called `retrieve_data` in `/includes/utility.py` to
# MAGIC retrieve the files we will ingest and save them to a raw directory. The function takes
# MAGIC three arguments:
# MAGIC 
# MAGIC - `year: int`
# MAGIC - `month: int`
# MAGIC - `rawPath: str`

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# We'll save the raw data in the "raw" subfolder
retrieve_data(2020, 1, RAW_FP)

# Now lets look at the files we retrieved
display(dbutils.fs.ls(RAW_FP))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a peak at the JSON file

# COMMAND ----------

# MAGIC %fs head /delta_workshop/strawberry/health_tracker/raw/health_tracker_data_2020_1.json

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enter Auto Loader
# MAGIC Okay, we've saved our first month of raw JSON data to cloud storage 
# MAGIC (**IMPORTANT CONTEXT**: dbfs, or the databricks filesystem, is a FUSE-mounted cloud object bucket like S3, ADLS Gen2, Azure Blob, etc. in your cloud account). 
# MAGIC 
# MAGIC So in my case, the data is sitting in S3. This is a very common pattern--JSON being dumped by an application or upstream team into a COS bucket. How should we ingest this into a Delta table?
# MAGIC 
# MAGIC [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html) is the answer!!
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/autoloader.png">
# MAGIC 
# MAGIC Auto Loader monitors a COS URI (ex: `s3://<bucket-name>/subdir/.../`) and will incrementally ingest new files as they appear in that location. Let's try it out!

# COMMAND ----------

AUTO_LOADER_CHKPNT_FP = BASE_FP + "checkpoints/auto_loader/"

(
  # CONTINUOUSLY READ FROM COS LOCATION
  spark.readStream.format("cloudFiles") # cloud
  .option("cloudFiles.format", "json")
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaEvolutionMode", "rescue") # other options include `addNewColumns` and `failOnNewColumns`
  .option("cloudFiles.schemaLocation", AUTO_LOADER_CHKPNT_FP)
  .load(RAW_FP)
  
  # WRITE STREAM TO DELTA BRONZE
  .writeStream
  .option("mergeSchema", "true")
  .option("checkpointLocation", AUTO_LOADER_CHKPNT_FP)
  .start(BRONZE_FP)
)

# COMMAND ----------

# Give stream time to initialize
import time; time.sleep(5) # needed for demo purposes

# Register Table with the hive metastore so we can access it via SQL in later cells
spark.sql(f"CREATE TABLE health_tracker_bronze USING DELTA LOCATION '{BRONZE_FP}'")

# Read in Table
health_tracker_bronze = spark.read.table("health_tracker_bronze")

# Print statistics
print(f"num rows: {health_tracker_bronze.count()}\n")
health_tracker_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC You should see the following output:
# MAGIC 
# MAGIC ```
# MAGIC num rows: 3720
# MAGIC 
# MAGIC root
# MAGIC  |-- device_id: long (nullable = true)
# MAGIC  |-- heartrate: double (nullable = true)
# MAGIC  |-- name: string (nullable = true)
# MAGIC  |-- time: double (nullable = true)
# MAGIC  |-- _rescued_data: string (nullable = true)
# MAGIC ```
# MAGIC **PAUSE** here if your schema looks vastly different. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Display the Data
# MAGIC Displaying the data will give us an idea of the **type and quality** of the data we are working with.
# MAGIC 
# MAGIC We note a few phenomena in the data:
# MAGIC - Sensor anomalies- Sensors cannot record negative heart rates, so any negative values in the data are anomalies.
# MAGIC - Wake/Sleep cycle - We notice that users have a consistent wake/sleep cycle alternating between steady high and low heart rates.
# MAGIC - Elevated activity - Some users have irregular periods of high activity.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from health_tracker_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Visualize the data
# MAGIC Create a Databricks visualization to graph the sensor data over time. We have used the following options to configure the visualization:
# MAGIC ```
# MAGIC Keys: time
# MAGIC Series groupings: device_id
# MAGIC Values: heartrate
# MAGIC Aggregation: SUM
# MAGIC Display Type: Bar Chart
# MAGIC ```

# COMMAND ----------

# MAGIC %md 
# MAGIC To create a visualization: 
# MAGIC * Select "Display as Bar Chart" next to the table icon to graph the data
# MAGIC * Select "Plot Options..." to configure the visualization

# COMMAND ----------

# Let's visualize the data 
display(health_tracker_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Note in the above visualization that some heart rates are reported as negative integers, which doesn't make sense. Later, we will explore how we can extract these "bad" data points and replace them with the correct measures. 

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2. Append Feb Data: Unified Batch and Streaming

# COMMAND ----------

# MAGIC %md 
# MAGIC The Delta table we created only contains the data for January. 
# MAGIC 
# MAGIC Let's simulate a Feburary data dump by downloading the second month of data into the raw cloud object storage location and let Auto Loader detect it and append it to our Bronze table!

# COMMAND ----------

# First let's take a count of our table to see where we're starting.
# This table was read in with spark.read.table("health_tracker_bronze")
health_tracker_bronze.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Okay, so our batch table has 3720 records currently. Let's create a streaming version of the same table by using `spark.readStream.table("health_tracker_bronze")`. This is a very powerful feature of Delta--it can be used as either a batch or streaming source.

# COMMAND ----------

health_tracker_bronze_stream = spark.readStream.table("health_tracker_bronze").createOrReplaceTempView("health_tracker_bronze_stream")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- count from stream
# MAGIC select count(*) from health_tracker_bronze_stream

# COMMAND ----------

# MAGIC %md
# MAGIC So our stream is showing 3720 now as well as expected. Watch as this number updates in real time when we run the cells below.

# COMMAND ----------

# Dump Feb json into raw filepath
retrieve_data(2020, 2, RAW_FP)
display(dbutils.fs.ls(RAW_FP))

# COMMAND ----------

# Dump March json into raw filepath
time.sleep(5) # needed for demo purposes
retrieve_data(2020, 3, RAW_FP)
display(dbutils.fs.ls(RAW_FP))

# COMMAND ----------

# MAGIC %md
# MAGIC Wow! We just saw our streaming query update in real time from a count of 3720 to 7128 then to 10848 as Auto Loader discovered a new json files in the raw directory and propogated that file into our bronze table!
# MAGIC 
# MAGIC Did our batch table update its count too? We'd prefer it if we always had the most up-to-date table here. Do I need to reload it? No! The batch table will realize a change has been made and pull in the new data.

# COMMAND ----------

health_tracker_bronze.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY health_tracker_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Time Travel
# MAGIC 
# MAGIC Since we're keeping a history of changes over time, we can also travel back in time to a previous verion of our table! There's lots of different ways to explore your history as well. 
# MAGIC 
# MAGIC For example, by timestamp:
# MAGIC 
# MAGIC In Python:
# MAGIC ```python
# MAGIC df = spark.read \
# MAGIC   .format("delta") \
# MAGIC   .option("timestampAsOf", "2019-01-01") \
# MAGIC   .load("/path/to/my/table")
# MAGIC   ```
# MAGIC SQL syntax:
# MAGIC ```sql
# MAGIC SELECT count(*) FROM my_table TIMESTAMP AS OF "2019-01-01"
# MAGIC SELECT count(*) FROM my_table TIMESTAMP AS OF date_sub(current_date(), 1)
# MAGIC SELECT count(*) FROM my_table TIMESTAMP AS OF "2019-01-01 01:30:00.000"
# MAGIC ```
# MAGIC 
# MAGIC or by version number:
# MAGIC Python syntax:
# MAGIC ```python
# MAGIC df = spark.read \
# MAGIC   .format("delta") \
# MAGIC   .option("versionAsOf", "5238") \
# MAGIC   .load("/path/to/my/table")
# MAGIC 
# MAGIC df = spark.read \
# MAGIC   .format("delta") \
# MAGIC   .load("/path/to/my/table@v5238")
# MAGIC ```
# MAGIC SQL syntax:
# MAGIC ```sql
# MAGIC SELECT count(*) FROM my_table VERSION AS OF 5238
# MAGIC SELECT count(*) FROM my_table@v5238
# MAGIC SELECT count(*) FROM delta.`/path/to/my/table@v5238`
# MAGIC ```
# MAGIC 
# MAGIC Let's try!

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM health_tracker_bronze VERSION AS OF 1

# COMMAND ----------

for version_num in range(3):
  count = spark.read.table(f"health_tracker_bronze@v{version_num}").count()
  print(f"Num Rows at version {version_num}: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Handling Schemas: Enforcement and Evolution
# MAGIC Most of the time, we want strict schemas to be adhered to so that downstream teams and processes that rely on that schema don't break. But sometimes the data coming in is changing rapidly and we need to evolve our schemas instead of strictly enforcing them. Delta supports both [shema enforcement and evolution](https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html). Let's dive into each
# MAGIC 
# MAGIC ### Schema Enforcement
# MAGIC Schema enforcement, also known as schema validation, is a safeguard in Delta Lake that ensures data quality by **rejecting writes to a table that do not match the table’s schema**. 

# COMMAND ----------

# Let's synthesis some bad data to show how delta handles schema enforcement
bad_schema_df = (
  spark.read.table("health_tracker_bronze")
  .sample(fraction=0.1)
  .withColumnRenamed("time", "timestamp") # purposely breaking schema by changing col name to timestamp
)
display(bad_schema_df)

# COMMAND ----------

try:
  (
    bad_schema_df
    .write
    .mode("append")
    .saveAsTable("health_tracker_bronze")
  )
except Exception as e:
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution
# MAGIC Say you actually want the flexibility of ever-changing schemas. Add an extra line of code to allow for schema evolution!

# COMMAND ----------

(
  bad_schema_df
  .write
  .mode("append")
  .option("mergeSchema", "true") # <- 1 LOC to allow for schema evolution
  .saveAsTable("health_tracker_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from health_tracker_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY health_tracker_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restore Delta Table to previous version
# MAGIC Oops! Shouldn't have merged that schema! Good thing it's super simple to roll back to a previous version of a Delta table!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We're at version 2 after our most recent append, so let's roll back to version 1
# MAGIC RESTORE TABLE health_tracker_bronze TO VERSION AS OF 2;
# MAGIC 
# MAGIC DESCRIBE HISTORY health_tracker_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Clean Data for Silver Layer: MERGE UPSERT
# MAGIC 
# MAGIC Right now our data is a direct dump from the raw JSON. Let's clean up the data a bit:
# MAGIC 0. split the time column into date and time
# MAGIC 0. cast device_id to an integer from a long
# MAGIC 0. fix negative values

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_unixtime

def health_tracker_bronze2silver_pipeline(health_tracker_bronze: DataFrame) -> DataFrame:
  health_tracker_silver = (
    health_tracker_bronze
    .withColumn("time", from_unixtime("time"))
    .withColumn("time", col("time").cast("timestamp"))
    .withColumn("date", col("time").cast("date"))
    .withColumn("device_id", col("device_id").cast("integer"))
    .select("date", "time", "heartrate", "name", "device_id")
    .dropna(how="any")
  )
  return health_tracker_silver

health_tracker_silver = health_tracker_bronze2silver_pipeline(health_tracker_bronze)

# Save out the silver table
health_tracker_silver.write.mode("overwrite").format("delta").save(SILVER_FP)
spark.sql(f"CREATE TABLE IF NOT EXISTS health_tracker_silver USING DELTA LOCATION '{SILVER_FP}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix Negative Values
# MAGIC We know there are some negative values we need to clean up. Let's do it here.

# COMMAND ----------

# let's see how many we need to fix...
from pyspark.sql.functions import col

health_tracker_silver.filter(col("heartrate") < 0).count()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We want to fix these 60 negative heartrates.  Here's the idea...
# MAGIC -- - Use a SQL window function to order the heartrates by time within each device
# MAGIC -- - Whenever there is a negative reading, replace it with the AVERAGE of the PREVIOUS and FOLLOWING readings.
# MAGIC 
# MAGIC -- We'll create a table of these interpolated heartrates, then later we'll merge it.
# MAGIC 
# MAGIC DROP TABLE IF EXISTS heartrate_interpolations;
# MAGIC 
# MAGIC CREATE TABLE heartrate_interpolations AS (
# MAGIC   WITH lags_and_leads AS (
# MAGIC     SELECT
# MAGIC       *,
# MAGIC       LAG(heartrate, 1, 0)  OVER (PARTITION BY device_id ORDER BY time ASC) AS heartrate_lag,
# MAGIC       LEAD(heartrate, 1, 0) OVER (PARTITION BY device_id ORDER BY time ASC) AS heartrate_lead
# MAGIC     FROM health_tracker_silver
# MAGIC   )
# MAGIC   SELECT 
# MAGIC       date,
# MAGIC       time,
# MAGIC       ((heartrate_lag + heartrate_lead) / 2) AS heartrate,
# MAGIC       name,
# MAGIC       device_id
# MAGIC     FROM lags_and_leads
# MAGIC   WHERE heartrate < 0
# MAGIC   ORDER BY device_id ASC
# MAGIC );
# MAGIC 
# MAGIC SELECT * FROM heartrate_interpolations

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE UPSERT
# MAGIC Okay, so now we have an update table! But how should we update values in our delta table...? Why not with [MERGE UPSERT](https://docs.databricks.com/delta/delta-update.html#upsert-into-a-table-using-merge) syntax?!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now use MERGE INTO to update the historical table
# MAGIC MERGE INTO 
# MAGIC health_tracker_silver AS orig 
# MAGIC USING heartrate_interpolations AS updte 
# MAGIC 
# MAGIC ON orig.device_id = updte.device_id
# MAGIC AND orig.time = updte.time
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *;
# MAGIC 
# MAGIC -- if this was an update table that also had new rows as well, then we could insert on NOT MATCHED like so:
# MAGIC -- WHEN NOT MATCHED THEN
# MAGIC -- INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC Well that was kinda cool! I guess? Seems obvious. Well, you would think so, but it's actually non-trivial to implement.
# MAGIC 
# MAGIC In this graphic, each box represents a parquet file
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2019/03/UpsertsBlog.jpg" alt="MERGE INTO" style="width: 1000px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Aggregate to Gold: Delta Constraints

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS health_tracker_gold AS (
# MAGIC 
# MAGIC SELECT name, date, MIN(heartrate) as heartrate_min, AVG(heartrate) as heartrate_mean, MAX(heartrate) as heartrate_max
# MAGIC FROM health_tracker_silver
# MAGIC GROUP BY 1, 2
# MAGIC 
# MAGIC );
# MAGIC 
# MAGIC select * from health_tracker_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC That's all well and good, but I know these need to be GOLD level tables, and I'd like some more assurances around that. I can `ADD CONTRAINT`s to tables to ensure data integrity for downstream teams. I know that the all-time lowest BPM heartrate is 27 and the highest ever recoreded was 600 (wait... [seriously](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3273956/)?!).
# MAGIC 
# MAGIC So it's time to set some [Delta Constraints](https://docs.databricks.com/delta/delta-constraints.html).

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE health_tracker_gold ADD CONSTRAINT validHeartrates CHECK (heartrate_min > 25 and heartrate_mean > 25 and heartrate_max > 25 and heartrate_min < 600 and heartrate_mean < 600 and heartrate_max < 600);
# MAGIC ALTER TABLE health_tracker_gold CHANGE COLUMN name SET NOT NULL;
# MAGIC ALTER TABLE health_tracker_gold CHANGE COLUMN date SET NOT NULL;
# MAGIC DESCRIBE DETAIL health_tracker_gold;

# COMMAND ----------

spark.read.table("health_tracker_gold").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Huge Performance Improvements: Data Skipping, ZORDER, OPTIMIZE, Caching

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Data Skipping
# MAGIC Delta collects column statistics (min and max) to skip reading as many files as possible when querying with a `WHERE` clause.
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2018/07/image7.gif">
# MAGIC 
# MAGIC By default, Delta Lake on Databricks collects statistics on the first 32 columns defined in your table schema. (You can change this value using the table property `dataSkippingNumIndexedCols`.)
# MAGIC 
# MAGIC The above information is used for Data skipping information and is collected automatically when you write data into a Delta table. Delta Lake on Databricks takes advantage of this information (minimum and maximum values) at query time to provide faster queries. 
# MAGIC 
# MAGIC **NOTE:** This is another feature that you don't need to configure manually; the feature is **automatically** activated whenever possible. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZORDER
# MAGIC To optimize performance **even more**, you can use **Z-Ordering** which takes advantage of the information gathered for Data Skipping. 
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2018/07/Screen-Shot-2018-07-30-at-2.03.55-PM.png">
# MAGIC 
# MAGIC ### What is Z-Ordering? 
# MAGIC 
# MAGIC Z-Ordering is a technique to **colocate related information** in the same set of files (dimensionality reduction). 
# MAGIC 
# MAGIC As mentioned above, This co-locality is automatically used by Delta Lake on Databricks data-skipping algorithms to dramatically reduce the amount of data that needs to be read. 
# MAGIC 
# MAGIC To Z-Order data, you specify the columns to order on in the `ZORDER BY` clause:

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE health_tracker_gold ZORDER BY date, name

# COMMAND ----------

# MAGIC %md 
# MAGIC ## OPTIMIZE 
# MAGIC Delta Lake on Databricks can improve the speed of read queries from a table by **coalescing small files into larger ones**. 
# MAGIC 
# MAGIC <img src="https://github.com/Corbin-A/images/blob/main/databricks/1manydelta/optimize.png?raw=true">
# MAGIC 
# MAGIC You trigger compaction by running the `OPTIMIZE` command:

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- you can optimize without ZORDERING. This will only do bin-packing and will not rearrange the data
# MAGIC -- for most efficient data skipping
# MAGIC OPTIMIZE health_tracker_bronze

# COMMAND ----------

# MAGIC %md 
# MAGIC For more on all of the above topics check out the resources below after the workshop:
# MAGIC - [Optimize Delta performance with file management](https://docs.databricks.com/delta/optimizations/file-mgmt.html)
# MAGIC - [Delta Optimization Examples](https://docs.databricks.com/delta/optimizations/optimization-examples.html)
# MAGIC 
# MAGIC Additionally, check out the below resources on: 
# MAGIC - [How to query semi-structured data using SQL](https://docs.databricks.com/spark/latest/spark-sql/semi-structured.html)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Caching
# MAGIC When you use Databricks, all the tables are saved to a S3/ADLS/GCS bucket in YOUR account. When you spin up a cluster, you are launching VM's in YOUR account. So when a cluster needs to read data, it needs to get it from COS. The Delta cache accelerates subsequent data reads by **caching copies of the remote files in the nodes’ local storage** using a fast intermediate data format. Successive reads of the same data are then performed locally, which results in significantly improved reading speed.
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_images/databricks-architecture.png">
# MAGIC 
# MAGIC **NOTE:** When the Delta cache is enabled, data that has to be fetched from a remote source is **automatically** added to the cache after it is accessed the first time. This means, you don't have to do anything to cache the data you're currently using. 
# MAGIC 
# MAGIC However, if you want to preload data into the Delta cache beforehand, you can use the `CACHE` command:

# COMMAND ----------

# MAGIC %sql 
# MAGIC CACHE SELECT * FROM health_tracker_gold

# COMMAND ----------

# MAGIC %md 
# MAGIC There are ways to monitor your Delta cache and also configure the cache to dictate how much local storage you want your cached data to use. We won't go into those topics in this workshop but feel free to check out the links below: 
# MAGIC 
# MAGIC - [Monitor the Delta cache](https://docs.databricks.com/delta/optimizations/delta-cache.html#monitor-the-delta-cache)
# MAGIC - [Configure the Delta cache](https://docs.databricks.com/delta/optimizations/delta-cache.html#configure-the-delta-cache)

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. VACUUM
# MAGIC As you can imagine, keeping large table histories takes up more and more space in S3/ADLSg2/GCS. It is very important to VACUUM your Delta tables to delete history that is older than you desire to keep (and pay cloud storage fees for). This is done through the VACUUM command.
# MAGIC 
# MAGIC **NOTE**: If you run VACUUM on a Delta table, you lose the ability time travel back to a version older than the specified data retention period.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY health_tracker_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM health_tracker_bronze RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY health_tracker_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC UNCACHE table health_tracker_bronze;
# MAGIC select * from health_tracker_bronze@v1

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ## As a last step, let's clean up after ourselves
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/stop.png?raw=true" width=100/>
# MAGIC 
# MAGIC NOTE: Before you continue, be sure to run the cell below to cancel the four stream readers
# MAGIC 
# MAGIC If you do not cancel these cells, your cluster will run indefinitely!

# COMMAND ----------

# stop all streams
for s in spark.streams.active:
  s.stop()
  
# Delete files and database now that demo is over :)
dbutils.fs.rm(BASE_FP, recurse=True)
spark.sql(f"DROP DATABASE IF EXISTS delta_workshop_{username} CASCADE;")

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary & Questions
# MAGIC Alright, we've explored a ton of features above that make Delta both **reliable** and **performant**. 
# MAGIC 
# MAGIC Let's break down what we covered today: 
# MAGIC 
# MAGIC **Ingestion**
# MAGIC - We dumped JSON files in an S3 bucket and used Auto Loader to ingest into Delta Bronze
# MAGIC 
# MAGIC **Unifed Batch and Streaming**
# MAGIC - Saw how Delta tables are both streaming and batch sources / sinks
# MAGIC - Allowed Auto Loader streaming to auto-ingest 2 more months of data
# MAGIC 
# MAGIC **Schema Enforcement & Evolution**
# MAGIC - We tried adding in new data but discovered a Schema Mismatch thanks to Delta's Schema Enforcement feature
# MAGIC - We added the data to our table anyway to explore Delta's Schema Evolution feature
# MAGIC 
# MAGIC **Time Travel**
# MAGIC - We then TIME TRAVELED and restored a previous version of our Delta table
# MAGIC 
# MAGIC **Schema Evoltuion and Enforcement**
# MAGIC - We saw what happens when you try to merge in an incorrect schema
# MAGIC - We then allowed the schema to evolve with 1 extra line of code
# MAGIC - We RESTOREd our table to a previous, correct version
# MAGIC 
# MAGIC **MERGE UPSERT**
# MAGIC - Created an interpolation table for the faulty heart rate readings
# MAGIC - Used MERGE UPSERT syntax to merge the corrections into our table
# MAGIC 
# MAGIC **Delta Contraints**
# MAGIC - Ensured data quality by implementing contraints on our data
# MAGIC 
# MAGIC **Advanced Features for Delta on Databricks**
# MAGIC - We explored some advanced features of Delta on Databricks like: 
# MAGIC     - Data Skipping (collecting statistics on table columns to only read relevant subsets of data - and skip over non-relevant subsets)
# MAGIC     - Z-Ordering (co-locating data based on how we interact with it to further reduce read and write times)
# MAGIC     - Compaction (merging small files into larger ones to reduce I/O)
# MAGIC     - Caching (preloading data into the Delta cache to reduce read time)
# MAGIC     
# MAGIC **VACUUM**
# MAGIC - Delete table history older than a certain threshold

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
