## Homework

Tasks: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_5_batch_processing/homework.md

### Question 1. Spark version.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Homework5').getOrCreate()
spark.version
```

Answer: 3.3.2

### Question 2. What is the average size of the parquet files in HVFHW June 2021 dataset?

```python
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

schema = StructType([
    StructField('dispatching_base_num', StringType(), True),
    StructField('pickup_datetime', TimestampType(), True),
    StructField('dropoff_datetime', TimestampType(), True),
    StructField('PULocationID', IntegerType(), True),
    StructField('DOLocationID', IntegerType(), True),
    StructField('SR_Flag', StringType(), True),
    StructField('Affiliated_base_number', StringType(), True)
])

df = spark.read.option('header', 'true').schema(schema).csv('fhvhv_tripdata_2021-06.csv.gz')
df = df.repartition(12)
df.write.parquet('fhvhv_tripdata_2021-06')
```

Answer: 23.7 MB

### Question 3. How many taxi trips were there on June 15?

```python
from pyspark.sql import functions as F

df = (df
         .withColumn('pickup_date', F.to_date('pickup_datetime'))
         .withColumn('dropoff_date', F.to_date('dropoff_datetime'))
)

df.filter(df.pickup_date == '2021-06-15').count()
```

Answer: 452470

### Question 4. How long was the longest trip in hours?

```python
df = (df
        .withColumn(
            'diffInHours',
            (F.col('dropoff_datetime').cast('long') - F.col('pickup_datetime').cast('long')) / 3600)
)
df.select(F.max('diffInHours')).show()
```

Answer: 66.8788888888889

### Question 5. Which local port does Spark User Interface with an applications' dashboard run on?

Answer: http://localhost:4040/jobs/

### Question 6. Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone?

```python
df.registerTempTable('df')

!wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv

zones = spark.read.option('header', 'true').csv('taxi+_zone_lookup.csv')
zones.registerTempTable('zones')

spark.sql("""
SELECT zones.Zone, count(*)
FROM df
    JOIN zones
        ON df.PULocationID = zones.LocationID
GROUP BY zones.Zone
ORDER BY count(*) desc
LIMIT 1
""").show()
```

Answer: Crown Heights North