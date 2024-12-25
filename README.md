# Spark Data Engineering Project: Halo Data Optimization

This repository demonstrates the use of Spark for optimizing data joins and aggregating large datasets. The project focuses on the Halo dataset and aims to optimize performance by applying various Spark techniques, including broadcasting, bucketing, and sorting.

## Problem / Opportunity
The task involves optimizing the performance of Spark jobs on a large dataset consisting of several tables related to Halo match data. Key objectives include:

1. Efficiently joining large datasets (match_details, matches, medals_matches_players) and small datasets (medals, maps).
2. Aggregating the data to answer analytical questions such as:
   - Which player averages the most kills per game?
   - Which playlist is played the most?
   - Which map is played the most?
   - Which map has the most Killing Spree medals?
3. Reducing data size by sorting and optimizing joins, ensuring high-performance queries.

## How I Identified the Problem
The problem was identified by analyzing the dataset's schema and understanding the challenges posed by large and small data joins. Specifically, the need for:
- Efficient handling of large datasets using Spark's bucketing and broadcasting features.
- Optimizing join performance by controlling the broadcast join threshold.
- Aggregating and sorting the data to answer complex analytical queries.

## The Solution
The solution consists of several key components:

1. **Setting up Spark Session and Configuration**:
   - Disabled automatic broadcast joins to prevent inefficient joins with large datasets.
   - Configured Spark for optimized performance.

2. **Bucketing Large Tables**:
   - Loaded the `matches`, `match_details`, and `medals_matches_players` data into Spark DataFrames and bucketed them on the `match_id` column to optimize joins.

3. **Explicit Broadcast Joins for Small Tables**:
   - Broadcasted the `maps` and `medals` tables for efficient joining with the larger dataset.

4. **Data Aggregation**:
   - Aggregated the data to answer key analytical questions, including identifying the player with the most kills per game, the most played playlist, and the most played map.

5. **Data Sorting for Optimization**:
   - Used `.sortWithinPartitions` to reduce the data size, particularly for low cardinality columns like `playlist_id` and `mapid`.

6. **Data Comparison**:
   - Compared the file sizes between the sorted and unsorted data to understand the impact of sorting on storage efficiency.

## How I Came Up with the Solution
The solution was developed by applying a combination of Spark best practices, including:
- Disabling automatic broadcast joins to avoid memory inefficiency when working with large datasets.
- Using bucketing to optimize join performance when combining large datasets on the `match_id` column.
- Broadcasting small datasets such as `maps` and `medals` to minimize shuffle operations.
- Aggregating data using SQL queries to answer the specific analytical questions posed in the problem.
- Sorting the data within partitions based on low cardinality columns to optimize storage and reduce file sizes.

## The Process for Implementing It

1. **Set Up Spark Session & Configuration**:
   - Initialized Spark session and set configuration to disable automatic broadcast joins.
   
2. **Load and Bucket Data**:
   - Loaded the CSV files and bucketed them on the `match_id` column, saving them as Iceberg tables for optimized storage and querying.

3. **Join the Data**:
   - Performed bucketed joins on the `match_details`, `matches`, and `medals_matches_players` tables.
   - Created temporary views to make the joined data available for SQL queries.

4. **Broadcast Smaller Tables**:
   - Broadcasted the `maps` and `medals` tables for efficient joining with the large dataset.

5. **Run Analytical Queries**:
   - Performed aggregation queries to identify key metrics like the most kills per game, the most played playlist, the most played map, and the map with the most Killing Spree medals.

6. **Optimize Data Storage**:
   - Sorted the data within partitions to optimize file sizes for columns with low cardinality.
   - Compared file sizes between the sorted and unsorted versions of the dataset.

7. **Store and Compare Results**:
   - Saved the sorted and unsorted data to separate tables and compared the impact of sorting on data storage efficiency.
   - 
---

The implementation files are saved as `.py` files for submission. Below are the key files for this project:

- **spark_halo_data_join_optimization.ipynb**: This is the main solution notebook, containing the Spark code for data loading, processing, optimization and queries.
