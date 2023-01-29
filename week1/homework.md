## Homework

Tasks: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_1_docker_sql/homework.md

### Question 1. Knowing docker tags

docker build --help

Answer:
--iidfile string Write the image ID to the file

### Question 2. Understanding docker first run

docker run -it python:3.9 bash
pip list

Answer:
pip 22.0.4
setuptools 58.1.0
wheel 0.38.4

### Question 3. Count records

select count(\*) from green_taxi_data_jan_2019
where cast(lpep_pickup_datetime as date) = '2019-01-15';

### Question 4. Largest trip for each day

select cast(lpep_pickup_datetime as date),
max(trip_distance)
from green_taxi_data_jan_2019
group by 1
order by 2 desc

### Question 5. The number of passengers

select passenger_count, count(\*)
from green_taxi_data_jan_2019
where passenger_count in (2, 3)
and cast(lpep_pickup_datetime as date) = '2019-01-01'
group by passenger_count

### Question 6. Largest tip

select z2."Zone", tip_amount from green_taxi_data_jan_2019 d
join zones z1 on d."PULocationID" = z1."LocationID"
join zones z2 on d."DOLocationID" = z2."LocationID"
where z1."Zone" = 'Astoria'
order by 2 desc
