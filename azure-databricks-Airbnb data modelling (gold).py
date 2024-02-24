# Databricks notebook source
# MAGIC %md
# MAGIC # Building the Gold Layer and Establishing Schemas
# MAGIC
# MAGIC In the third notebook of our ETL pipeline, we focus on constructing the Gold layer and defining schemas for our datasets. The Gold layer is the pinnacle of our data transformation process, containing data that has been cleaned, transformed, aggregated, and enriched to provide the highest value for analysis, reporting, and machine learning. This notebook outlines the steps to create temporary views from our Silver layer Delta tables and prepare for final transformations and aggregations to build the Gold layer.
# MAGIC
# MAGIC ## Creating Temporary Views from Delta Tables
# MAGIC
# MAGIC Temporary views in Spark provide a way to interact with data through SQL-like queries. We will create temporary views for our listings data from Antwerp, Brussels, and Ghent. These views facilitate easy access to the data stored in Delta tables within the Silver layer, serving as a foundation for further transformations and analyses.
# MAGIC
# MAGIC ### Creating Views for Listings Data
# MAGIC
# MAGIC We start by creating temporary views for the listings data of each city. These views point to the respective Delta tables stored in the Silver layer:
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE antwerp_listings_data;
# MAGIC -- DROP TABLE brussels_listings_data;
# MAGIC -- DROP TABLE ghent_listings_data;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW antwerp_listings_data AS
# MAGIC SELECT * FROM delta.`/mnt/airbnbdata/silver/antwerp_listings_data_geo_combined.delta`;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW brussels_listings_data AS
# MAGIC SELECT * FROM delta.`/mnt/airbnbdata/silver/brussels_listings_data_geo_combined.delta`;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW ghent_listings_data AS
# MAGIC SELECT * FROM delta.`/mnt/airbnbdata/silver/ghent_listings_data_geo_combined.delta`;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Constructing the `Dim_Host` Dimension Table
# MAGIC
# MAGIC In this section, we focus on constructing a dimension table, `Dim_Host`, as part of our Gold layer schema. Dimension tables are essential components of a star schema, designed to provide context to our facts/data through descriptive attributes. The `Dim_Host` table aims to consolidate host-related information across Antwerp, Brussels, and Ghent, offering insights into the hosts present in our Airbnb dataset.
# MAGIC
# MAGIC ### Creating the `combined_host_data` Temporary View
# MAGIC
# MAGIC To build the `Dim_Host` dimension, we first create a temporary view named `combined_host_data`. This view combines host information from the temporary views created for each city's listings data. The goal is to create a unified view of hosts that includes:
# MAGIC
# MAGIC - **HostID**: A unique identifier for each host.
# MAGIC - **host_name**: The name of the host.
# MAGIC - **host_is_superhost**: Indicates whether the host is a superhost.
# MAGIC - **host_listings_count**: The number of listings the host has.
# MAGIC - **host_total_listings_count**: The total number of listings associated with the host across all platforms.
# MAGIC - **host_response_time**: The average response time of the host to inquiries or booking requests.
# MAGIC
# MAGIC The SQL query to create this view is as follows:
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC -- DROP TABLE Dim_Host;
# MAGIC
# MAGIC -- Dim_Host
# MAGIC CREATE OR REPLACE TEMP VIEW combined_host_data AS
# MAGIC SELECT DISTINCT
# MAGIC     host_id AS HostID,
# MAGIC     host_name,
# MAGIC     host_is_superhost,
# MAGIC     host_listings_count,
# MAGIC     host_total_listings_count,
# MAGIC     host_response_time
# MAGIC FROM antwerp_listings_data
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC     host_id AS HostID,
# MAGIC     host_name,
# MAGIC     host_is_superhost,
# MAGIC     host_listings_count,
# MAGIC     host_total_listings_count,
# MAGIC     host_response_time
# MAGIC FROM brussels_listings_data
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC     host_id AS HostID,
# MAGIC     host_name,
# MAGIC     host_is_superhost,
# MAGIC     host_listings_count,
# MAGIC     host_total_listings_count,
# MAGIC     host_response_time
# MAGIC FROM ghent_listings_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finalizing the `Dim_Host` Dimension Table in the Gold Layer
# MAGIC
# MAGIC After preparing the `combined_host_data` temporary view, which consolidates host information from Antwerp, Brussels, and Ghent, we are now ready to create the `Dim_Host` dimension table. This table is a critical component of our star schema in the Gold layer, designed to facilitate in-depth analysis and reporting by providing detailed host-related context.
# MAGIC
# MAGIC ### Creating the `Dim_Host` Table Using Delta
# MAGIC
# MAGIC To ensure our `Dim_Host` table benefits from the advanced features of Delta Lake, such as ACID transactions, scalable metadata handling, and time travel, we use Delta format for this dimension table. The following SQL command creates the `Dim_Host` table from the `combined_host_data` view and specifies its location within our data lake storage:
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Dim_Host
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/airbnbdata/gold/Dim_Host'
# MAGIC AS
# MAGIC SELECT * FROM combined_host_data;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Dim_Host

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constructing the `Dim_Property_Details` Dimension Table
# MAGIC
# MAGIC As we continue to enhance our data warehouse's Gold layer, we introduce the `Dim_Property_Details` dimension table. This table aims to aggregate detailed information about properties listed on Airbnb across Antwerp, Brussels, and Ghent. By consolidating property details into a single dimension table, we facilitate richer analyses, including property comparisons, feature correlations, and regional insights.
# MAGIC
# MAGIC ### Creating the `combined_property_data` Temporary View
# MAGIC
# MAGIC To prepare for the creation of the `Dim_Property_Details` table, we first gather property data from our city-specific listings data into a unified temporary view, `combined_property_data`. This view includes a comprehensive set of attributes for each property:
# MAGIC
# MAGIC - **PropertyID**: A unique identifier for the property (derived from `listing_id`).
# MAGIC - **name**: The name of the listing.
# MAGIC - **picture_url**: URL of the listing's featured image.
# MAGIC - **property_type**: The type of property being listed (e.g., apartment, house).
# MAGIC - **room_type**: The type of room being offered (e.g., entire home/apt, private room).
# MAGIC - **accommodates**: The number of guests the property can accommodate.
# MAGIC - **bathrooms_text**: Text description of bathroom facilities.
# MAGIC - **bedrooms**: The number of bedrooms available.
# MAGIC - **beds**: The number of beds available.
# MAGIC - **amenities**: A list of amenities provided with the property.
# MAGIC - **latitude** and **longitude**: Geographical coordinates of the property.
# MAGIC
# MAGIC The SQL command to create this view is as follows:
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE Dim_Property_Details;
# MAGIC
# MAGIC -- Dim_Property_Details
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW combined_property_data AS
# MAGIC SELECT DISTINCT
# MAGIC     listing_id as PropertyID,
# MAGIC     name,
# MAGIC     picture_url,
# MAGIC     property_type,
# MAGIC     room_type,
# MAGIC     accommodates,
# MAGIC     bathrooms_text,
# MAGIC     bedrooms,
# MAGIC     beds,
# MAGIC     amenities,
# MAGIC     latitude,
# MAGIC     longitude
# MAGIC FROM antwerp_listings_data
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC     listing_id as PropertyID,
# MAGIC     name,
# MAGIC     picture_url,
# MAGIC     property_type,
# MAGIC     room_type,
# MAGIC     accommodates,
# MAGIC     bathrooms_text,
# MAGIC     bedrooms,
# MAGIC     beds,
# MAGIC     amenities,
# MAGIC     latitude,
# MAGIC     longitude
# MAGIC FROM brussels_listings_data
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC     listing_id as PropertyID,
# MAGIC     name,
# MAGIC     picture_url,
# MAGIC     property_type,
# MAGIC     room_type,
# MAGIC     accommodates,
# MAGIC     bathrooms_text,
# MAGIC     bedrooms,
# MAGIC     beds,
# MAGIC     amenities,
# MAGIC     latitude,
# MAGIC     longitude
# MAGIC FROM ghent_listings_data;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Creating the `Dim_Property_Details` Table Using Delta
# MAGIC
# MAGIC With the `combined_property_data` view ready, we create the `Dim_Property_Details` dimension table in Delta format. This approach leverages Delta Lake's robust features, such as ACID compliance and efficient metadata handling, ensuring data integrity and performance.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE Dim_Property_Details
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/airbnbdata/gold/Dim_Property_Details'
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM combined_property_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Dim_Property_Details 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Establishing the `Dim_Neighbourhood` Dimension Table
# MAGIC
# MAGIC As part of enriching our Gold layer with comprehensive dimensions for advanced analytics, we focus on creating the `Dim_Neighbourhood` dimension table. This table is designed to encapsulate neighbourhood information, including their geographical boundaries and associated cities. By detailing neighbourhood characteristics, we can facilitate spatial analyses and provide insights into the geographical distribution and characteristics of Airbnb listings.
# MAGIC
# MAGIC ### Preparing Neighbourhood Data
# MAGIC
# MAGIC To construct the `Dim_Neighbourhood` table, we first aggregate neighbourhood information from our listings data across Antwerp, Brussels, and Ghent. A temporary view named `combined_neighbourhood_data` is created to consolidate this information, ensuring distinct entries for each neighbourhood and its corresponding city.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE Dim_Neighbourhood;
# MAGIC
# MAGIC -- Dim_Neighbourhood
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW combined_neighbourhood_data AS
# MAGIC SELECT DISTINCT
# MAGIC     neighbourhood AS Neighbourhood,
# MAGIC     'Antwerp' AS city,
# MAGIC     geometry_wkt
# MAGIC    
# MAGIC FROM antwerp_listings_data
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC     neighbourhood AS Neighbourhood,
# MAGIC     'Brussels' AS city,
# MAGIC     geometry_wkt
# MAGIC     
# MAGIC FROM brussels_listings_data
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC     neighbourhood AS Neighbourhood,
# MAGIC     'Ghent' AS city,
# MAGIC     geometry_wkt
# MAGIC     
# MAGIC FROM ghent_listings_data;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finalizing the `Dim_Neighbourhood` Dimension Table in the Gold Layer
# MAGIC
# MAGIC Following the preparation of neighbourhood data across Antwerp, Brussels, and Ghent, we are set to finalize the `Dim_Neighbourhood` dimension table. This table, part of the Gold layer, is pivotal for enriching our dataset with geographical context, enabling detailed spatial analysis and insights into neighbourhood dynamics.
# MAGIC
# MAGIC ### Creating the `Dim_Neighbourhood` Table Using Delta
# MAGIC
# MAGIC To capitalize on the benefits of Delta Lake, such as enhanced data integrity through ACID transactions and efficient metadata management, we create the `Dim_Neighbourhood` dimension table in Delta format. This choice ensures that our dimension table is optimized for performance and reliability, facilitating advanced analytical operations.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Dim_Neighbourhood
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/airbnbdata/gold/Dim_Neighbourhood'
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM combined_neighbourhood_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Dim_Neighbourhood

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the `Dim_Date` Dimension Table
# MAGIC
# MAGIC The creation of the `Dim_Date` dimension table is an essential step in our Gold layer's development, offering a structured view of dates that can be used across our dataset for temporal analysis. This table enriches our star schema by providing a consistent date dimension, facilitating time-based queries, aggregations, and insights.
# MAGIC
# MAGIC ### Aggregating Date Data
# MAGIC
# MAGIC To construct the `Dim_Date` table, we first aggregate date information from our listings data for Antwerp, Brussels, and Ghent. A temporary view named `combined_date_data` is created to unify this information, ensuring we capture all relevant date components and categorize each date into its respective season.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE Dim_Date;
# MAGIC
# MAGIC -- Dim_Date
# MAGIC CREATE OR REPLACE TEMP VIEW combined_date_data AS
# MAGIC SELECT DISTINCT
# MAGIC     TO_DATE(date, 'yyyy-MM-dd') AS Date,
# MAGIC     YEAR(date) AS Year,
# MAGIC     MONTH(date) AS Month,
# MAGIC     DAY(date) AS Day,
# MAGIC     CASE 
# MAGIC         WHEN MONTH(date) BETWEEN 3 AND 5 THEN 'Spring'
# MAGIC         WHEN MONTH(date) BETWEEN 6 AND 8 THEN 'Summer'
# MAGIC         WHEN MONTH(date) BETWEEN 9 AND 11 THEN 'Fall'
# MAGIC         ELSE 'Winter'
# MAGIC     END AS Season
# MAGIC    
# MAGIC FROM antwerp_listings_data
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC     TO_DATE(date, 'yyyy-MM-dd') AS Date,
# MAGIC     YEAR(date) AS Year,
# MAGIC     MONTH(date) AS Month,
# MAGIC     DAY(date) AS Day,
# MAGIC     CASE 
# MAGIC         WHEN MONTH(date) BETWEEN 3 AND 5 THEN 'Spring'
# MAGIC         WHEN MONTH(date) BETWEEN 6 AND 8 THEN 'Summer'
# MAGIC         WHEN MONTH(date) BETWEEN 9 AND 11 THEN 'Fall'
# MAGIC         ELSE 'Winter'
# MAGIC     END AS Season
# MAGIC     
# MAGIC FROM brussels_listings_data
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC     TO_DATE(date, 'yyyy-MM-dd') AS Date,
# MAGIC     YEAR(date) AS Year,
# MAGIC     MONTH(date) AS Month,
# MAGIC     DAY(date) AS Day,
# MAGIC     CASE 
# MAGIC         WHEN MONTH(date) BETWEEN 3 AND 5 THEN 'Spring'
# MAGIC         WHEN MONTH(date) BETWEEN 6 AND 8 THEN 'Summer'
# MAGIC         WHEN MONTH(date) BETWEEN 9 AND 11 THEN 'Fall'
# MAGIC         ELSE 'Winter'
# MAGIC     END AS Season
# MAGIC     
# MAGIC FROM ghent_listings_data;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Dim_Date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finalizing the `Dim_Date` Dimension Table in the Gold Layer
# MAGIC
# MAGIC Following the aggregation of date information into the `combined_date_data` temporary view, we proceed to finalize the `Dim_Date` dimension table. This dimension table is a cornerstone of our Gold layer, enabling robust temporal analysis across the dataset. By utilizing the Delta format, we ensure that our `Dim_Date` table benefits from enhanced performance, reliability, and scalability features offered by Delta Lake.
# MAGIC
# MAGIC ### Creating the `Dim_Date` Table Using Delta
# MAGIC
# MAGIC The SQL command below creates the `Dim_Date` dimension table from the `combined_date_data` view, specifying its storage in Delta format. This approach not only capitalizes on Delta Lake's advanced capabilities but also aligns the table with our data lake's organizational structure.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Dim_Date
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/airbnbdata/gold/Dim_Date'
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM combined_date_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the Unified Listings View
# MAGIC
# MAGIC To further enhance our analytical capabilities in the Gold layer, we are creating a comprehensive view named `combined_listing_data`. This view unifies listing details across Antwerp, Brussels, and Ghent with enriched dimensions from our previously established dimension tables: `Dim_Host`, `Dim_Neighbourhood`, and `Dim_Date`. This unified view aims to provide a holistic perspective on listings, incorporating host information, neighbourhood context, and temporal dimensions.
# MAGIC
# MAGIC ### Constructing the `combined_listing_data` View
# MAGIC
# MAGIC The SQL statement below consolidates listings data from the three cities and joins this data with the relevant dimension tables. This approach ensures that each listing is associated with its host details, neighbourhood characteristics, and date information, thus providing a multidimensional view of the dataset.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- $$DROP TABLE Fact_Listing;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW combined_listing_data AS
# MAGIC SELECT DISTINCT
# MAGIC     l.listing_id AS ListingID,
# MAGIC     l.listing_id AS PropertyID, 
# MAGIC     h.HostID,
# MAGIC     n.Neighbourhood,
# MAGIC     d.Date,
# MAGIC     l.price,
# MAGIC     l.adjusted_price,
# MAGIC     l.available,
# MAGIC     l.number_of_reviews,
# MAGIC     l.review_scores_rating,
# MAGIC     l.review_scores_accuracy,
# MAGIC     l.review_scores_cleanliness,
# MAGIC     l.review_scores_checkin,
# MAGIC     l.review_scores_communication,
# MAGIC     l.review_scores_location,
# MAGIC     l.review_scores_value,
# MAGIC     l.minimum_nights,
# MAGIC     l.maximum_nights
# MAGIC FROM antwerp_listings_data l
# MAGIC JOIN Dim_Host h ON l.host_id = h.HostID
# MAGIC JOIN Dim_Neighbourhood n ON l.neighbourhood= n.Neighbourhood
# MAGIC JOIN Dim_Date d ON l.date = d.Date
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC     x.listing_id AS ListingID,
# MAGIC     x.listing_id AS PropertyID, 
# MAGIC     h.HostID,
# MAGIC     n.Neighbourhood,
# MAGIC     d.Date,
# MAGIC     x.price,
# MAGIC     x.adjusted_price,
# MAGIC     x.available,
# MAGIC     x.number_of_reviews,
# MAGIC     x.review_scores_rating,
# MAGIC     x.review_scores_accuracy,
# MAGIC     x.review_scores_cleanliness,
# MAGIC     x.review_scores_checkin,
# MAGIC     x.review_scores_communication,
# MAGIC     x.review_scores_location,
# MAGIC     x.review_scores_value,
# MAGIC     x.minimum_nights,
# MAGIC     x.maximum_nights
# MAGIC FROM brussels_listings_data x
# MAGIC JOIN Dim_Host h ON x.host_id = h.HostID
# MAGIC JOIN Dim_Neighbourhood n ON x.neighbourhood= n.Neighbourhood
# MAGIC JOIN Dim_Date d ON x.date = d.Date
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC     y.listing_id AS ListingID,
# MAGIC     y.listing_id AS PropertyID, 
# MAGIC     h.HostID,
# MAGIC     n.Neighbourhood,
# MAGIC     d.Date,
# MAGIC     y.price,
# MAGIC     y.adjusted_price,
# MAGIC     y.available,
# MAGIC     y.number_of_reviews,
# MAGIC     y.review_scores_rating,
# MAGIC     y.review_scores_accuracy,
# MAGIC     y.review_scores_cleanliness,
# MAGIC     y.review_scores_checkin,
# MAGIC     y.review_scores_communication,
# MAGIC     y.review_scores_location,
# MAGIC     y.review_scores_value,
# MAGIC     y.minimum_nights,
# MAGIC     y.maximum_nights
# MAGIC FROM ghent_listings_data y
# MAGIC JOIN Dim_Host h ON y.host_id = h.HostID
# MAGIC JOIN Dim_Neighbourhood n ON y.neighbourhood= n.Neighbourhood
# MAGIC JOIN Dim_Date d ON y.date = d.Date;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Finalizing the `Fact_Listing` Fact Table in the Gold Layer
# MAGIC
# MAGIC To enrich our Gold layer with actionable insights and support complex analytical queries, we proceed to create the `Fact_Listing` fact table. This table consolidates and structures listing details across Antwerp, Brussels, and Ghent, integrated with dimensional data to enable comprehensive analyses.
# MAGIC
# MAGIC ### Creating the `Fact_Listing` Table Using Delta
# MAGIC
# MAGIC Leveraging Delta format for the `Fact_Listing` table ensures enhanced data management capabilities, including ACID transactions and scalable metadata handling. The SQL command below creates the `Fact_Listing` table from the previously established `combined_listing_data` view, specifying its storage in Delta format for optimal performance and reliability.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Fact_Listing
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/airbnbdata/gold/Fact_Listing'
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM combined_listing_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Attributes and Insights of the `Fact_Listing` Table
# MAGIC
# MAGIC The `Fact_Listing` table encompasses a wide range of attributes, including:
# MAGIC
# MAGIC - **Listing and Property IDs**: Unique identifiers for each listing.
# MAGIC - **Host ID**: Link to host-related details.
# MAGIC - **Neighbourhood**: Geographical context of the listing.
# MAGIC - **Date**: Temporal dimension of the listing data.
# MAGIC - **Pricing Information**: Insights into listing prices and adjustments.
# MAGIC - **Availability Status**: Current availability of the listing.
# MAGIC - **Review Scores**: Metrics reflecting guest satisfaction and experiences.
# MAGIC - **Booking Constraints**: Minimum and maximum nights for booking.
# MAGIC
# MAGIC ### Utilization and Benefits
# MAGIC
# MAGIC With the `Fact_Listing` table established, we can:
# MAGIC
# MAGIC - **Drive In-depth Analysis**: Facilitate multifaceted analyses of the Airbnb market, including pricing strategies, guest satisfaction, and temporal trends.
# MAGIC - **Support Reporting and Dashboards**: Enable the creation of detailed reports and interactive dashboards, providing stakeholders with actionable insights.
# MAGIC - **Enhance Data Enrichment**: Offer a foundation for further enriching the dataset with external data sources, deepening the analytical context.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Fact_Listing

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   f.ListingID,
# MAGIC   f.PropertyID,
# MAGIC   d.HostID, d.host_name, 
# MAGIC   n.Neighbourhood, 
# MAGIC   dt.Date
# MAGIC FROM 
# MAGIC   Fact_Listing AS f
# MAGIC JOIN 
# MAGIC   Dim_Host AS d ON f.HostID = d.HostID
# MAGIC JOIN 
# MAGIC   Dim_Neighbourhood AS n ON f.Neighbourhood = n.Neighbourhood
# MAGIC JOIN 
# MAGIC   Dim_Date AS dt ON f.Date = dt.Date
# MAGIC LIMIT 20000;
# MAGIC

# COMMAND ----------


import matplotlib.pyplot as plt

# Python cell using Spark SQL to query and Matplotlib for visualization
query_result = spark.sql("""
SELECT 
  n.neighbourhood, COUNT(f.ListingID) AS listings_count
FROM 
  Fact_Listing AS f
JOIN 
  Dim_Neighbourhood AS n ON f.Neighbourhood = n.Neighbourhood
GROUP BY n.neighbourhood
ORDER BY listings_count DESC
LIMIT 10
""")

# Convert to Pandas DataFrame for plotting
pdf = query_result.toPandas()

# Plotting
pdf.plot(kind='bar', x='neighbourhood', y='listings_count', figsize=(10, 6), legend=True)
plt.title('Listings Count by Neighbourhood')
plt.ylabel('Listings Count')
plt.xlabel('Neighbourhood')
plt.xticks(rotation=45)
plt.show()

