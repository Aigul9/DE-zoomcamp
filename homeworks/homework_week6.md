## Homework

Tasks: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_6_stream_processing/homework.md

### Question 1. Select the correct statements.

- Kafka Node is responsible to store topics
- Zookeeper is removed form Kafka cluster starting from version 4.0
- ~~Retention configuration ensures the messages not get lost over specific period of time.~~
- Group-Id ensures the messages are distributed to associated consumers

### Question 2. Select the Kafka concepts that support reliability and availability.

- Topic Replication
- ~~Topic Paritioning~~
- Consumer Group Id
- Ack All

### Question 3. Select the Kafka concepts that support scaling.

- ~~Topic Replication~~
- Topic Paritioning
- Consumer Group Id
- ~~Ack All~~

### Question 4. Select the attributes that are good candidates for partitioning key.

- ~~payment_type~~
- vendor_id
- ~~passenger_count~~
- ~~total_amount~~
- tpep_pickup_datetime
- tpep_dropoff_datetime

### Question 5. Which configurations below should be provided for Kafka Consumer but not needed for Kafka Producer?

- Deserializer Configuration
- Topics Subscription
- ~~Bootstrap Server~~
- Group-Id
- Offset
- ~~Cluster Key and Cluster Secret~~
