from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from settings import BOOTSTRAP_SERVERS, KAFKA_TOPICS, OUTPUT_TOPIC, RIDE_SCHEMA_GREEN, RIDE_SCHEMA_FHV


def read_from_kafka(consume_topic: str):
    df_stream = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS) \
        .option('subscribe', consume_topic) \
        .option('startingOffsets', 'earliest') \
        .option('checkpointLocation', 'checkpoint') \
        .load()
    return df_stream


def parse_ride_from_kafka_message(df, schema):
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df.selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)')
    col = F.split(df['value'], ', ')

    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format('console') \
        .option('truncate', False) \
        .start()
    return write_query


def sink_memory(df, query_name, query_template):
    query_df = df \
        .writeStream \
        .queryName(query_name) \
        .format('memory') \
        .start()
    query_str = query_template.format(table_name=query_name)
    query_results = spark.sql(query_str)
    return query_results, query_df


def sink_kafka(df, topic):
    write_query = df.writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS) \
        .outputMode('complete') \
        .option('topic', topic) \
        .option('checkpointLocation', 'checkpoint') \
        .start()
    return write_query


def prepare_df_to_kafka_sink(df, value_columns, key_column=None):
    df = df.withColumn('value', F.concat_ws(', ', *value_columns))
    if key_column:
        df = df.withColumnRenamed(key_column, 'key')
        df = df.withColumn('key', df.key.cast('string'))
    return df.select(['key', 'value'])


def op_groupby(df, column_names):
    df_aggregation = df.groupBy(column_names).count()
    return df_aggregation


if __name__ == '__main__':
    spark = SparkSession.builder.appName('streaming').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    green, fhv = KAFKA_TOPICS
    df_consume_stream_green = read_from_kafka(consume_topic=green)
    print(df_consume_stream_green.printSchema())
    df_consume_stream_fhv = read_from_kafka(consume_topic=fhv)
    print(df_consume_stream_fhv.printSchema())

    df_rides_green = parse_ride_from_kafka_message(df_consume_stream_green, RIDE_SCHEMA_GREEN)
    print(df_rides_green.printSchema())
    df_rides_fhv = parse_ride_from_kafka_message(df_consume_stream_fhv, RIDE_SCHEMA_FHV)
    print(df_rides_fhv.printSchema())

    df_rides = df_rides_green.union(df_rides_fhv)

    sink_console(df_rides, output_mode='append')

    df_trip_count_by_pu_location_id = op_groupby(df_rides, ['PUlocationID'])

    sink_console(df_trip_count_by_pu_location_id)

    df_rides = prepare_df_to_kafka_sink(df=df_rides, value_columns=['values'], key_column='key')
    kafka_sink_query = sink_kafka(df=df_rides, topic=OUTPUT_TOPIC)

    spark.streams.awaitAnyTermination()
