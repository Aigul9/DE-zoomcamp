import pyspark.sql.types as T

INPUT_DATA_PATHS = ['../data/week6/green_tripdata_2019-01_top100.csv', '../data/week6/fhv_tripdata_2019-02_top100.csv']
BOOTSTRAP_SERVERS = ['pkc-75m1o.europe-west3.gcp.confluent.cloud:9092']
KAFKA_TOPICS = ['rides_green', 'rides_fhv']
OUTPUT_TOPIC = 'rides_all'

RIDE_SCHEMA_GREEN = T.StructType([
    T.StructField('lpep_pickup_datetime', T.TimestampType()),
    T.StructField('lpep_dropoff_datetime', T.TimestampType()),
    T.StructField('PUlocationID', T.IntegerType()),
    T.StructField('DOlocationID', T.IntegerType())
])

RIDE_SCHEMA_FHV = T.StructType([
    T.StructField('pickup_datetime', T.TimestampType()),
    T.StructField('dropOff_datetime', T.TimestampType()),
    T.StructField('PUlocationID', T.IntegerType()),
    T.StructField('DOlocationID', T.IntegerType())
])
