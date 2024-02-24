# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Cleaning and Loading into Silver Layer
# MAGIC
# MAGIC In this notebook, we transition from the Bronze layer, where our raw data resides, to the Silver layer. The Silver layer is intended for cleaned and transformed data, ready for more detailed analysis or to be used in reporting and machine learning models. Here, we'll focus on reading the data from the Bronze layer, performing necessary cleaning and transformations, and then saving the processed data into the Silver layer.
# MAGIC
# MAGIC ## Reading Data from Bronze Layer
# MAGIC
# MAGIC The first step involves reading the previously saved Parquet files from the Bronze layer into Spark DataFrames. We have separate Parquet files for calendar data, detailed listings data, and GeoJSON data for each of the three cities: Antwerp, Brussels, and Ghent.
# MAGIC

# COMMAND ----------

bronze_path = "/mnt/airbnbdata/bronze"

antwerp_calendar_df = spark.read.parquet(f"{bronze_path}/antwerp_calendar.parquet")
brussels_calendar_df = spark.read.parquet(f"{bronze_path}/brussels_calendar.parquet")
ghent_calendar_df = spark.read.parquet(f"{bronze_path}/ghent_calendar.parquet")

antwerp_listings_detailed_df = spark.read.parquet(f"{bronze_path}/antwerp_listings_detailed.parquet")
brussels_listings_detailed_df = spark.read.parquet(f"{bronze_path}/brussels_listings_detailed.parquet")
ghent_listings_detailed_df = spark.read.parquet(f"{bronze_path}/ghent_listings_detailed.parquet")

antwerp_geo = spark.read.parquet(f"{bronze_path}/antwerp_geo.parquet")
brussels_geo = spark.read.parquet(f"{bronze_path}/brussels_geo.parquet")
ghent_geo = spark.read.parquet(f"{bronze_path}/ghent_geo.parquet")



# COMMAND ----------

# antwerp_geo_df.display()
antwerp_listings_detailed_df.display(5)
# antwerp_calendar_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Identifying Common Columns Between DataFrames
# MAGIC
# MAGIC As part of the data cleaning and transformation process, it's crucial to understand the relationships and potential overlaps between different datasets. One common task is to identify common columns between two DataFrames, which can aid in joining data, performing consistency checks, or understanding the dataset's structure more deeply.
# MAGIC
# MAGIC In this section, we demonstrate how to find common columns between the `antwerp_listings_detailed_df` DataFrame and the `antwerp_calendar_df` DataFrame. This process can be easily adapted for other pairs of DataFrames.
# MAGIC
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC - We first convert the columns of each DataFrame into a set, allowing us to perform set operations.
# MAGIC - Using the `.intersection()` method, we find the set of columns that exist in both DataFrames.
# MAGIC - Printing the `common_columns` variable reveals the names of these shared columns.
# MAGIC
# MAGIC ### Application
# MAGIC
# MAGIC Identifying common columns is particularly useful when planning to merge or join datasets based on shared keys or when ensuring consistency across related tables. For example, if both DataFrames contain a column for listing IDs, this common column can serve as a key for merging details from the listings DataFrame with availability data from the calendar DataFrame.
# MAGIC
# MAGIC This approach ensures a streamlined and efficient data preparation phase, laying a solid foundation for subsequent analysis in the Silver layer.
# MAGIC

# COMMAND ----------

# List of columns in both DataFrames
columns_df1 = set(antwerp_listings_detailed_df.columns)
columns_df2 = set(antwerp_calendar_df.columns)

# Find common columns
common_columns = columns_df1.intersection(columns_df2)
print("Common columns:", common_columns)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Dropping Unnecessary Columns
# MAGIC
# MAGIC As part of the data cleaning process, it's often necessary to remove columns that are not required for our analysis or might introduce redundancy after data merging. Based on the analysis and the common columns identified between our DataFrames, we have decided to drop certain columns from both the listings detailed DataFrames and the calendar DataFrames for Antwerp, Brussels, and Ghent.
# MAGIC
# MAGIC ### Dropping Columns from Listings Detailed DataFrames
# MAGIC
# MAGIC For the listings detailed DataFrames (`antwerp_listings_detailed_df`, `brussels_listings_detailed_df`, `ghent_listings_detailed_df`), we are dropping the following columns:
# MAGIC
# MAGIC - `minimum_nights`
# MAGIC - `maximum_nights`
# MAGIC - `neighbourhood`
# MAGIC
# MAGIC These columns are removed to streamline the dataset, due to redundancy (as `minimum_nights` and `maximum_nights` are also available in the calendar data).
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

antwerp_listings_detailed_df = antwerp_listings_detailed_df.drop("minimum_nights", "maximum_nights", "neighbourhood")
brussels_listings_detailed_df = brussels_listings_detailed_df.drop("minimum_nights", "maximum_nights","neighbourhood")
ghent_listings_detailed_df = ghent_listings_detailed_df.drop("minimum_nights", "maximum_nights","neighbourhood")




# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dropping Columns from Calendar DataFrames
# MAGIC
# MAGIC For the calendar DataFrames (`antwerp_calendar_df`, `brussels_calendar_df`, `ghent_calendar_df`), we are dropping the `price` column:
# MAGIC
# MAGIC - The decision to drop the `price` column from the calendar data due to it being incorporated from another source.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

antwerp_calendar_df = antwerp_calendar_df.drop("price")
brussels_calendar_df = brussels_calendar_df.drop("price")
ghent_calendar_df = ghent_calendar_df.drop("price")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Renaming Columns for Consistency
# MAGIC
# MAGIC Ensuring consistency across datasets is crucial for facilitating accurate data analysis and integration. As part of our data cleaning and preparation process, we are standardizing the column names in our detailed listings DataFrames for Antwerp, Brussels, and Ghent. This step involves renaming certain columns to have uniform names across all datasets, which is particularly important for columns that will be used as keys in joins or for aligning data semantics across different sources.
# MAGIC
# MAGIC ### Renaming Process
# MAGIC
# MAGIC We are focusing on two specific columns for renaming:
# MAGIC
# MAGIC 1. **`id` to `listing_id`**: The `id` column, which uniquely identifies each listing, is being renamed to `listing_id` to more clearly reflect its purpose and to avoid confusion with other identifiers in related datasets.
# MAGIC
# MAGIC 2. **`neighbourhood_cleansed` to `neighbourhood`**: The `neighbourhood_cleansed` column, which provides a standardized name for the neighbourhood of each listing, is being renamed to `neighbourhood`. This change simplifies the column name while maintaining the integrity of the neighbourhood data.
# MAGIC
# MAGIC

# COMMAND ----------

antwerp_listings_detailed_df = antwerp_listings_detailed_df.withColumnRenamed("id", "listing_id") \
    .withColumnRenamed("neighbourhood_cleansed", "neighbourhood")
brussels_listings_detailed_df = brussels_listings_detailed_df.withColumnRenamed("id", "listing_id") \
    .withColumnRenamed("neighbourhood_cleansed", "neighbourhood")
ghent_listings_detailed_df = ghent_listings_detailed_df.withColumnRenamed("id", "listing_id") \
    .withColumnRenamed("neighbourhood_cleansed", "neighbourhood")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Joining Listings and Calendar Data
# MAGIC
# MAGIC After cleaning and standardizing the column names in our listings and calendar DataFrames, the next step in preparing our data for the Silver layer involves joining these datasets. This process will enrich the listings data with calendar information, such as availability and pricing over time, providing a more comprehensive view of each listing.
# MAGIC
# MAGIC ### Join Operation
# MAGIC
# MAGIC We perform an inner join operation between the listings detailed DataFrames and the calendar DataFrames for each city: Antwerp, Brussels, and Ghent. The join is based on the `listing_id` column, which we have standardized across our datasets.
# MAGIC

# COMMAND ----------

antwerp_join_df = antwerp_listings_detailed_df.join(antwerp_calendar_df, "listing_id", "inner")
brussels_join_df = brussels_listings_detailed_df.join(brussels_calendar_df, "listing_id", "inner")
ghent_join_df = ghent_listings_detailed_df.join(ghent_calendar_df, "listing_id", "inner")



# COMMAND ----------

# MAGIC %md
# MAGIC ## Transforming Categorical Columns
# MAGIC
# MAGIC In our joined datasets, certain categorical columns, such as `host_is_superhost` and `available`, contain values represented by 't' (true) and 'f' (false). For analytical purposes, it is often more convenient to work with numerical data. Therefore, we will transform these categorical values into numerical ones, where 't' is mapped to 1 (indicating true) and 'f' is mapped to 0 (indicating false).
# MAGIC
# MAGIC ### Transforming `host_is_superhost`
# MAGIC
# MAGIC The `host_is_superhost` column indicates whether the host of a listing is considered a superhost. We will convert this column to numerical format for easier analysis:
# MAGIC
# MAGIC ### Transforming `available`
# MAGIC
# MAGIC The `available` column reflects the availability of a listing on a given day. Similar to `host_is_superhost`, we will convert this column to a numerical format:
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, when
# Mapping host_is_superhost column
antwerp_join_df = antwerp_join_df.withColumn("host_is_superhost", when(col("host_is_superhost") == 't', 1).otherwise(0))
brussels_join_df = brussels_join_df.withColumn("host_is_superhost", when(col("host_is_superhost") == 't', 1).otherwise(0))
ghent_join_df = ghent_join_df.withColumn("host_is_superhost", when(col("host_is_superhost") == 't', 1).otherwise(0))

# Mapping available column
antwerp_join_df = antwerp_join_df.withColumn("available", when(col("available") == 't', 1).otherwise(0))
brussels_join_df = brussels_join_df.withColumn("available", when(col("available") == 't', 1).otherwise(0))
ghent_join_df = ghent_join_df.withColumn("available", when(col("available") == 't', 1).otherwise(0))


# COMMAND ----------

antwerp_join_df.display(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ```markdown
# MAGIC ## Expanding GeoJSON Data for Analysis
# MAGIC
# MAGIC The GeoJSON data for Antwerp, Brussels, and Ghent contains rich geographical information, particularly about neighbourhoods. This data is initially in a nested format, which can be challenging to work with directly for analysis and visualization. To make this data more accessible, we will perform operations to explode the nested structures and select relevant fields for our analysis.
# MAGIC
# MAGIC ### Exploding Nested Features
# MAGIC
# MAGIC The first step is to use the `explode` function to separate the `features` array into individual rows, making it easier to access the nested data:
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import explode, col
antwerp_geo_exp = antwerp_geo.withColumn("features", explode("features"))
brussels_geo_exp = brussels_geo.withColumn("features", explode("features"))
ghent_geo_exp = ghent_geo.withColumn("features", explode("features"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Selecting Nested Fields
# MAGIC
# MAGIC After exploding the `features`, we focus on two pieces of information: the geometry coordinates and the neighbourhood names. These fields are selected and aliased for clearer understanding and easier access:

# COMMAND ----------

# # Select the nested fields from the exploded features
antwerp_geo_data = antwerp_geo_exp.select(
    col("features.geometry.coordinates").alias("geometry_coordinates"),
    col("features.properties.neighbourhood").alias("neighbourhood"),
)
brussels_geo_data = brussels_geo_exp.select(
    col("features.geometry.coordinates").alias("geometry_coordinates"),
    col("features.properties.neighbourhood").alias("neighbourhood"),
)
ghent_geo_data = ghent_geo_exp.select(
    col("features.geometry.coordinates").alias("geometry_coordinates"),
    col("features.properties.neighbourhood").alias("neighbourhood"),
)


# COMMAND ----------

antwerp_geo_exp.display(5)

# COMMAND ----------

pip install shapely

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Converting GeoJSON Coordinates to WKT Format
# MAGIC
# MAGIC For our spatial analysis and integration with other GIS tools, it's beneficial to convert the geometry coordinates from our expanded GeoJSON data into a well-known text (WKT) format. WKT is a text markup language for representing vector geometry objects, making it widely compatible with various GIS systems. We'll focus on converting the coordinates to a WKT MultiPolygon, which is suitable for representing complex polygon geometries with multiple exterior and interior boundaries.
# MAGIC
# MAGIC ### Defining a User-Defined Function (UDF)
# MAGIC
# MAGIC To perform this conversion, we define a User-Defined Function (UDF) that takes an array of coordinates and converts them into a WKT MultiPolygon string:
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from shapely.geometry import Polygon, MultiPolygon

# UDF for converting coordinates to WKT MultiPolygon
def convert_to_multipolygon(coord_array):
    polygons = []
    for polygon_coords in coord_array:
        exterior = polygon_coords[0]  # The first array is the exterior ring
        interiors = polygon_coords[1:]  # Subsequent arrays are interior rings (holes)
        polygon = Polygon(exterior, interiors)
        polygons.append(polygon)
    return str(MultiPolygon(polygons))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Registering and Applying the UDF
# MAGIC
# MAGIC After defining the function, we register it as a UDF with a return type of `StringType`, indicating that the output will be a string representing the WKT MultiPolygon:
# MAGIC

# COMMAND ----------


# Register the UDF
convert_to_multipolygon_udf = udf(convert_to_multipolygon, StringType())


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We then apply this UDF to the `geometry_coordinates` column of each city's geo data DataFrame, creating a new column `geometry_wkt` with the WKT representation. The original `geometry_coordinates` column is dropped as it's no longer needed:
# MAGIC

# COMMAND ----------


# Apply the UDF to each city's geo data DataFrame
antwerp_geo_data = antwerp_geo_data.withColumn(
    'geometry_wkt', 
    convert_to_multipolygon_udf('geometry_coordinates')
).drop('geometry_coordinates')

brussels_geo_data = brussels_geo_data.withColumn(
    'geometry_wkt', 
    convert_to_multipolygon_udf('geometry_coordinates')
).drop('geometry_coordinates')

ghent_geo_data = ghent_geo_data.withColumn(
    'geometry_wkt', 
    convert_to_multipolygon_udf('geometry_coordinates')
).drop('geometry_coordinates')

# COMMAND ----------

antwerp_geo_data.display(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combining Listings Data with Geographical Information
# MAGIC
# MAGIC Having transformed our geographical data into a more accessible format and our listings data enriched with calendar information, the next step is to integrate these datasets. This integration will allow us to analyze the listings data within the context of geographical boundaries and neighbourhoods. We achieve this by joining the listings data with the geo data based on the `neighbourhood` column, which is common to both datasets.
# MAGIC
# MAGIC ### Joining Listings and Geo Data
# MAGIC
# MAGIC For each city (Antwerp, Brussels, and Ghent), we perform an inner join operation between the listings-calendar combined DataFrame and the geographical data DataFrame. This operation links each listing to its corresponding neighbourhood's geographical information:
# MAGIC

# COMMAND ----------

antwerp_listings_data_geo_combined = antwerp_join_df.join(antwerp_geo_data, "neighbourhood", "inner")
brussels_listings_data_geo_combined = brussels_join_df.join(brussels_geo_data, "neighbourhood", "inner")
ghent_listings_data_geo_combined = ghent_join_df.join(ghent_geo_data, "neighbourhood", "inner")




# COMMAND ----------

antwerp_listings_data_geo_combined.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Saving Processed Data to the Silver Layer as Delta Tables
# MAGIC
# MAGIC With our datasets enriched by geographical information and other transformations, the final step in this notebook is to save the processed data to the Silver layer. The Silver layer is designated for cleaned, consolidated, and structured data that is ready for in-depth analysis, reporting, and machine learning. To leverage the benefits of versioning, ACID transactions, and schema enforcement, we will save our datasets as Delta tables.
# MAGIC
# MAGIC ### Delta Tables
# MAGIC
# MAGIC Delta tables offer several advantages over traditional file formats, including:
# MAGIC
# MAGIC - **ACID Transactions**: Ensures data integrity even in the presence of concurrent read and write operations.
# MAGIC - **Scalable Metadata Handling**: Efficiently manages metadata for large datasets, improving performance.
# MAGIC - **Schema Enforcement and Evolution**: Automatically checks that data matches the schema and allows for schema evolution over time.
# MAGIC
# MAGIC ### Saving DataFrames as Delta Tables
# MAGIC
# MAGIC We have prepared a list of DataFrames that represent the combined listings and geographical data for Antwerp, Brussels, and Ghent. These will be saved as Delta tables in the Silver layer:
# MAGIC
# MAGIC

# COMMAND ----------

silver_path = "/mnt/airbnbdata/silver"

# List of DataFrame names and their variables
dataframes_to_save = {
    "antwerp_listings_data_geo_combined": antwerp_listings_data_geo_combined,
    "brussels_listings_data_geo_combined": brussels_listings_data_geo_combined,
    "ghent_listings_data_geo_combined": ghent_listings_data_geo_combined,
}

# Loop through the dictionary and save each DataFrame as a Delta table in the Silver layer
for name, df in dataframes_to_save.items():
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{silver_path}/{name}.delta")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Steps for Saving
# MAGIC
# MAGIC 1. **Specify the Silver Layer Path**: Define the path where the Delta tables will be stored.
# MAGIC 2. **Prepare DataFrames**: List the DataFrames and their corresponding names as they should appear in the Silver layer.
# MAGIC 3. **Save as Delta Tables**: For each DataFrame, write it to the specified path in the Delta format, with the mode set to overwrite and schema overwrite enabled.
# MAGIC
# MAGIC ### Conclusion
# MAGIC
# MAGIC By saving our enriched datasets as Delta tables in the Silver layer, we enhance the robustness, reliability, and accessibility of our data. This preparation sets the stage for advanced analytics, reporting, and machine learning modeling, enabling us to derive deeper insights from the Airbnb data across Antwerp, Brussels, and Ghent.
# MAGIC
