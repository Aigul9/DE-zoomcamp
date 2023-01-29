## Homework

Tasks: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_1_docker_sql/homework.md

### Question 1. Knowing docker tags

```docker build --help```

### Question 2. Understanding docker first run

```docker run -it python:3.9 bash```

```pip list```

### Question 3. Count records

```sql
SELECT count(\*)
FROM green_taxi_data_jan_2019
WHERE cast(lpep_pickup_datetime AS date) = '2019-01-15';
```

### Question 4. Largest trip for each day

```sql
SELECT cast(lpep_pickup_datetime AS date),
       max(trip_distance)
FROM green_taxi_data_jan_2019
GROUP BY 1
ORDER BY 2 DESC;
```

### Question 5. The number of passengers

```sql
SELECT passenger_count,
       count(\*)
FROM green_taxi_data_jan_2019
WHERE passenger_count in (2,
                          3)
  AND cast(lpep_pickup_datetime AS date) = '2019-01-01'
GROUP BY passenger_count;
```

### Question 6. Largest tip

```sql
SELECT z2."Zone",
       tip_amount
FROM green_taxi_data_jan_2019 d
JOIN zones z1 ON d."PULocationID" = z1."LocationID"
JOIN zones z2 ON d."DOLocationID" = z2."LocationID"
WHERE z1."Zone" = 'Astoria'
ORDER BY 2 DESC;
```
