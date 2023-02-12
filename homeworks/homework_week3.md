## Homework

Tasks: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_3_data_warehouse/homework.md

### Question 1. What is the count for fhv vehicle records for year 2019?

```sql
SELECT count(1)
FROM `de-zoomcamp-376218.trips_data_all.external_trip_data`;
--43244696
```

### Question 2. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

```sql
SELECT DISTINCT Affiliated_base_number
FROM `de-zoomcamp-376218.trips_data_all.external_trip_data`;
--This query will process 0 B when run.
```

```sql
SELECT DISTINCT Affiliated_base_number
FROM `de-zoomcamp-376218.trips_data_all.trips_2019_non_partitioned`;
--This query will process 317.94 MB when run.
```

### Question 3. How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

```sql
SELECT count(*)
FROM `de-zoomcamp-376218.trips_data_all.trips_2019_non_partitioned`
WHERE PUlocationID IS NULL
  AND DOlocationID IS NULL;
--717748
```

### Question 4. What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

```sql
SELECT count(DISTINCT Affiliated_base_number)
FROM `de-zoomcamp-376218.trips_data_all.trips_2019_non_partitioned`;
--3165
```

There are too many distinct values for the field, and the limit is 4000 for a partition,
so we won't use Affiliated_base_number as a partition key => options 3 and 4 are wrong.
Also BigQuery supports partitioning only on one column => option 4 is definitely wrong.
If we use raw pickup_datetime without extracting a year or month, then it would be impossible
to create a partition based on this column due to the large number of unique values.
But if we take a day, then the best approach is 2: Partition by pickup_datetime Cluster on affiliated_base_number.
Clustering by date would add extra complexity to sort the values, which is not needed.

### Question 5. 

```sql
SELECT DISTINCT Affiliated_base_number
FROM `de-zoomcamp-376218.trips_data_all.trips_2019_non_partitioned`
WHERE pickup_datetime BETWEEN '2019-03-01' and '2019-03-31';
--This query will process 647.87 MB when run.
```

```sql
SELECT DISTINCT Affiliated_base_number
FROM `de-zoomcamp-376218.trips_data_all.trips_2019_partitioned_clustered`
WHERE pickup_datetime BETWEEN '2019-03-01' and '2019-03-31';
--This query will process 23.06 MB when run.
```

### Question 6. Where is the data stored in the External Table you created?

Answer: GCP Bucket, that's why the estimated amount of processing was equal to 0 B in question 3.

### Question 7. It is best practice in Big Query to always cluster your data.

Answer: False, because there are cases when partitioning is more performant.
