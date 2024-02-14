-- SETUP

--External table
CREATE OR REPLACE EXTERNAL TABLE ny-rides-tyler-411718.ny_taxi.green_taxi_2022_external
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://hw3-tyler-zoomcamp/2022_green_taxi.parquet']
    );

--Materiaized table
CREATE OR REPLACE TABLE ny_taxi.green_taxi_2022_materialized AS (
  SELECT
    *
  FROM
    ny-rides-tyler-411718.ny_taxi.green_taxi_2022_external
); 

--Question 1
SELECT * FROM ny-rides-tyler-411718.ny_taxi.green_taxi_2022_external

--Question 2
SELECT COUNT(DISTINCT PULocationID) FROM ny-rides-tyler-411718.ny_taxi.green_taxi_2022_external
SELECT COUNT(DISTINCT PULocationID) FROM ny-rides-tyler-411718.ny_taxi.green_taxi_2022_materialized

--Question 3
SELECT count(*) FROM ny-rides-tyler-411718.ny_taxi.green_taxi_2022_external
where fare_amount = 0

--Question 4
CREATE OR REPLACE TABLE `ny-rides-tyler-411718.ny_taxi.green_taxi_2022_partitioned_clustered`
PARTITION BY DATE(lpep_dropoff_datetime)
CLUSTER BY PULocationID AS (
  SELECT * FROM `ny-rides-tyler-411718.ny_taxi.green_taxi_2022_external`
);

--Question 5. What's the size of the tables?
SELECT * FROM `ny-rides-tyler-411718.ny_taxi.green_taxi_2022_partitioned_clustered`
SELECT * FROM `ny-rides-tyler-411718.ny_taxi.green_taxi_2022_external`

--Question 6. Where is the data stored in the External Table you created?


--Question 7. It is best practice in Big Query to always cluster your data