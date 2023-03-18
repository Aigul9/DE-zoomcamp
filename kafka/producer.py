import csv
import json
import logging
from decouple import config
from typing import Dict
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from ride_fhv import RideFHV
from ride_green import RideGreen
from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATHS, KAFKA_TOPICS

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class JsonProducer(KafkaProducer):
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        # print(self.producer.config)

    @staticmethod
    def read_records(resource_path: str, cls):
        records = []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                records.append(cls(arr=row))
        return records

    def publish_rides(self, topic: str, messages):
        for ride in messages:
            try:
                record = self.producer.send(topic=topic, key=ride.pu_location_id, value=ride)
                print(f'Topic: {topic}. Location: {ride.pu_location_id}. Offset: {record.get().offset}')
            except KafkaTimeoutError as e:
                print(e.__str__())


if __name__ == '__main__':
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'api_version': (2, 0, 2),
        'security_protocol': 'SASL_SSL',
        'sasl_plain_username': config('USER'),
        'sasl_plain_password': config('PASSWORD'),
        'sasl_mechanism': 'PLAIN',
        'key_serializer': lambda key: str(key).encode(),
        'value_serializer': lambda x: json.dumps(x.__dict__, default=str).encode('utf-8')
    }

    producer = JsonProducer(props=config)

    green, fhv = zip(INPUT_DATA_PATHS, KAFKA_TOPICS)
    path, topic = green
    rides = producer.read_records(resource_path=path, cls=RideGreen)
    producer.publish_rides(topic=topic, messages=rides)
    path, topic = fhv
    rides = producer.read_records(resource_path=path, cls=RideFHV)
    producer.publish_rides(topic=topic, messages=rides)
