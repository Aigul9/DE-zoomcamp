-- Creating external table
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-376218.trips_data_all.external_trip_data`
OPTIONS  (
  format = 'CSV',
  uris = [
    'https://drive.google.com/open?id=17WaUJd9KxiT0fh2dCmpVrRoCOZDbGJBs',
    'https://drive.google.com/open?id=1UTIna54BkuntX1VkRB6h71hIiXN_v15k',
    'https://drive.google.com/open?id=1QfZpgp4WiukCY9Qx--tAZdf9af9BkjPU',
    'https://drive.google.com/open?id=1JmxAMG4Jd3gKiDWUHYSTjxSuS-7kcKub',
    'https://drive.google.com/open?id=1ohXndnqD18hh8W8vwx4MtTsVrckTznvf',
    'https://drive.google.com/open?id=1k1wTr0NMpJ7c72iKL-wAkj7-23nrimw2',
    'https://drive.google.com/open?id=1voJcCbDw8fr20ejGy-gZaBAwCewAjPLt',
    'https://drive.google.com/open?id=162uYXxd_QpCf9Op0jC8CwyMZE_VwpOFF',
    'https://drive.google.com/open?id=1Lw9vADJ1vKVih9KsXX8_rUErsU8DCnE8',
    'https://drive.google.com/open?id=13MtCNLalT3WNIaQNB31P2vVs9WDrfN8H',
    'https://drive.google.com/open?id=16F8wGhWCeArtmqb8WSFE1OAw1KyLb2X0',
    'https://drive.google.com/open?id=1evPFJ4CSpnrKsJ6xu5oqsW4ISJPGs77-'
  ]
);

-- What is the count for fhv vehicle records for year 2019?
SELECT count(1)
FROM `de-zoomcamp-376218.trips_data_all.external_trip_data`;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `de-zoomcamp-376218.trips_data_all.trips_2019_non_partitioned` AS
SELECT * FROM `de-zoomcamp-376218.trips_data_all.external_trip_data`;

-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT DISTINCT Affiliated_base_number
FROM `de-zoomcamp-376218.trips_data_all.external_trip_data`;

SELECT DISTINCT Affiliated_base_number
FROM `de-zoomcamp-376218.trips_data_all.trips_2019_non_partitioned`;

-- How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
SELECT count(*)
FROM `de-zoomcamp-376218.trips_data_all.trips_2019_non_partitioned`
WHERE PUlocationID IS NULL
  AND DOlocationID IS NULL;

-- What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
SELECT count(DISTINCT Affiliated_base_number)
FROM `de-zoomcamp-376218.trips_data_all.trips_2019_non_partitioned`;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `de-zoomcamp-376218.trips_data_all.trips_2019_partitioned_clustered`
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY
  Affiliated_base_number AS
SELECT * FROM `de-zoomcamp-376218.trips_data_all.external_trip_data`;

-- Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive). Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?
SELECT DISTINCT Affiliated_base_number
FROM `de-zoomcamp-376218.trips_data_all.trips_2019_non_partitioned`
WHERE pickup_datetime BETWEEN '2019-03-01' and '2019-03-31';

SELECT DISTINCT Affiliated_base_number
FROM `de-zoomcamp-376218.trips_data_all.trips_2019_partitioned_clustered`
WHERE pickup_datetime BETWEEN '2019-03-01' and '2019-03-31';
