# Databricks notebook source
# MAGIC %md
# MAGIC # Extraction and Loading to Bronze Layer
# MAGIC
# MAGIC This notebook covers the initial step in our ETL pipeline on Azure Databricks, focusing on extracting Airbnb data and loading it into a Bronze layer as Parquet files. 
# MAGIC
# MAGIC ## Step 1: Mounting Azure Blob Storage
# MAGIC
# MAGIC First, we mount the Azure Blob Storage to Databricks to access the Airbnb dataset. This allows us to directly read and write data from Azure Blob Storage without needing to move data manually.

# COMMAND ----------

# 1

dbutils.fs.mount(
  source = "s3://mybucket",
  mount_point = "/mnt/airbnbdata",
  extra_configs = {"fs.azure.account.key.myaccount.blob.core.windows.net": "myaccesskey"})


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC **Note**: Replace the `source` with your Azure Blob Storage URI and `fs.azure.account.key...` with your storage account key. This step is critical for establishing a connection between Databricks and Azure Blob Storage, enabling seamless data access for subsequent ETL processes.
# MAGIC
# MAGIC In the next section, we will proceed with reading the data from the mounted storage, performing initial transformations, and loading it into the Bronze layer as Parquet files for further processing.
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("/mnt/airbnbdata"))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Reading Listings Data
# MAGIC
# MAGIC After successfully mounting the Azure Blob Storage, the next step involves reading the detailed listings data for different cities from the Airbnb dataset. We are focusing on three cities: Antwerp, Brussels, and Ghent. Each city's data is stored in a separate CSV file, which we'll read into Spark DataFrames for further processing.
# MAGIC
# MAGIC ### Antwerp, Brussels & Ghent Listings Detailed Data
# MAGIC
# MAGIC The detailed listings data for Antwerp is read from the `antwerp_listings_detailed.csv` file. We ensure that the CSV file's header is used as the DataFrame column names and handle any multiline fields correctly.
# MAGIC
# MAGIC
# MAGIC Similarly, we read the detailed listings data for Brussels from the `brussels_listings_detailed.csv` file with the same CSV reading options to maintain consistency across DataFrames.
# MAGIC
# MAGIC
# MAGIC
# MAGIC Lastly, the detailed listings data for Ghent is read from the `ghent_listings_detailed.csv` file. The same approach is taken to ensure the CSV file is parsed correctly into a Spark DataFrame.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#### Listings data ###

# Read antwerp_listings_detailed.csv
antwerp_listings_detailed = spark.read.format("csv")\
    .option("header", "true")\
    .option("escape", "\"")\
    .option("multiLine", "true")\
    .load("/mnt/airbnbdata/antwerp_listings_detailed.csv")
# Read brussels_listings_detailed.csv
brussels_listings_detailed = spark.read.format("csv")\
    .option("header", "true")\
    .option("escape", "\"")\
    .option("multiLine", "true")\
    .load("/mnt/airbnbdata/brussels_listings_detailed.csv")
# Read ghent_listings_detailed.csv

ghent_listings_detailed = spark.read.format("csv")\
    .option("header", "true")\
    .option("escape", "\"")\
    .option("multiLine", "true")\
    .load("/mnt/airbnbdata/ghent_listings_detailed.csv")



# COMMAND ----------

# MAGIC %md
# MAGIC With the listings data for all three cities loaded into Spark DataFrames, we can proceed to the cleaning and transformation steps to prepare the data for loading into the Bronze layer as Parquet files.

# COMMAND ----------

ghent_listings_detailed.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading Calendar Data
# MAGIC
# MAGIC Following the listings data, we also need to process the calendar data for Antwerp, Brussels, and Ghent. This data includes availability and pricing information for listings across different dates. Each city's calendar data is stored in a separate CSV file, which we will read into separate Spark DataFrames.
# MAGIC
# MAGIC ### Antwerp, Brussels & Ghent Calendar Data
# MAGIC
# MAGIC The calendar data for Antwerp is sourced from the `antwerp_calendar.csv` file. We read this file into a Spark DataFrame, ensuring that the CSV header is utilized for column names.
# MAGIC
# MAGIC
# MAGIC Similarly, the calendar data for Brussels is read from the `brussels_calendar.csv` file. The same reading options are applied to maintain consistency in how we handle CSV files.
# MAGIC
# MAGIC
# MAGIC Lastly, we read the calendar data for Ghent from the `ghent_calendar.csv` file, ensuring the CSV file's header is properly interpreted as DataFrame column names.
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------


#### Callendar data ###

# Read antwerp_calendar.csv
antwerp_calendar = spark.read.format("csv").option("header", "true").load("/mnt/airbnbdata/antwerp_calendar.csv")
# Read brussels_calendar.csv
brussels_calendar = spark.read.format("csv").option("header", "true").load("/mnt/airbnbdata/brussels_calendar.csv")
# Read ghent_calendar.csv
ghent_calendar = spark.read.format("csv").option("header", "true").load("/mnt/airbnbdata/ghent_calendar.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC With the calendar data for all three cities now loaded into Spark DataFrames, we have completed the extraction phase of our ETL pipeline. The next steps involve cleaning and transforming this data, along with the listings data, to ensure it is ready for analytical processing and loading into the Delta tables in ADLS Gen2 as part of our cleaning and modeling phases.

# COMMAND ----------

antwerp_calendar.display(5)

# COMMAND ----------

#### GeoJSON data ###
antwerp_geojson = spark.read.format("json").load("/mnt/airbnbdata/antwerp_neighbourhoods.geojson")
brussels_geojson = spark.read.format("json").load("/mnt/airbnbdata/brussels_neighbourhoods.geojson")
ghent_geojson = spark.read.format("json").load("/mnt/airbnbdata/ghent_neighbourhoods.geojson")



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading GeoJSON Data
# MAGIC
# MAGIC In addition to listings and calendar data, we incorporate geographical data to enrich our analysis. This data, stored in GeoJSON format, outlines the neighborhoods of Antwerp, Brussels, and Ghent. By integrating this geographical context, we can perform more nuanced analyses that consider the spatial distribution of Airbnb listings.
# MAGIC
# MAGIC ### Antwerp, Brussels & Ghent Neighbourhoods GeoJSON
# MAGIC
# MAGIC
# MAGIC Similarly, the geographical data for Brussels neighbourhoods is contained within the `brussels_neighbourhoods.geojson` file. This data is also loaded into a Spark DataFrame for further processing and analysis.
# MAGIC
# MAGIC
# MAGIC Lastly, the neighbourhoods' geographical data for Ghent is available in the `ghent_neighbourhoods.geojson` file. Like the other cities, we read this data into a Spark DataFrame.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

antwerp_geojson.display(5)

# COMMAND ----------

# MAGIC %md
# MAGIC By integrating GeoJSON data into our analysis, we enhance the dataset with spatial context, allowing for more comprehensive insights into the Airbnb market across different neighbourhoods in Antwerp, Brussels, and Ghent.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Saving DataFrames to Bronze Layer
# MAGIC
# MAGIC After extracting and loading the data into Spark DataFrames, the next critical step in our ETL pipeline is to save this data to the Bronze layer. The Bronze layer serves as our raw data repository, where data is stored in a slightly processed but mostly untransformed state. This section outlines the process of saving our DataFrames as Parquet files, a columnar storage format that offers advantages in terms of compression and speed.
# MAGIC
# MAGIC ### DataFrames and Their Variables
# MAGIC
# MAGIC We have created a Python dictionary `spark_df_list` that maps the name of each DataFrame to its variable. This includes detailed listings data, calendar data, and GeoJSON data for Antwerp, Brussels, and Ghent.
# MAGIC

# COMMAND ----------

# List of DataFrame names and their variables
spark_df_list = {
    "antwerp_listings_detailed": antwerp_listings_detailed,
    "brussels_listings_detailed": brussels_listings_detailed,
    "ghent_listings_detailed": ghent_listings_detailed,
    "antwerp_calendar": antwerp_calendar,
    "brussels_calendar": brussels_calendar,
    "ghent_calendar": ghent_calendar,
    "antwerp_geo": antwerp_geojson,
    "brussels_geo": brussels_geojson,
    "ghent_geo": ghent_geojson
}


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Saving Process
# MAGIC
# MAGIC Each DataFrame is saved to the Azure Data Lake Storage (ADLS) Gen2 within the `bronze` directory, using the Parquet format. The `base_path` variable specifies the directory path, and we iterate over the `spark_df_list` dictionary to save each DataFrame. The `.write.mode("overwrite").parquet(path)` method is used to ensure that if a file already exists at the target location, it is overwritten.
# MAGIC
# MAGIC

# COMMAND ----------


# Directory to save the DataFrames
base_path = "/mnt/airbnbdata/bronze"  

# Loop through the list and save each DataFrame
for name, df in spark_df_list.items():
    path = f"{base_path}/{name}.parquet"
    df.write.mode("overwrite").parquet(path)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Conclusion
# MAGIC
# MAGIC By storing the data in the Bronze layer as Parquet files, we lay the groundwork for the next steps in our ETL pipeline, which will involve cleaning and transforming the data, and eventually loading it into the Silver and Gold layers for further analysis and reporting. This structured approach ensures that our data is organized, accessible, and ready for the subsequent stages of processing.
# MAGIC
# MAGIC
