from typing import Dict, List
from json import loads
from kafka import KafkaConsumer

from ride_fhv import RideFHV
from ride_green import RideGreen
from settings import BOOTSTRAP_SERVERS, KAFKA_TOPICS


class JsonConsumer:
    def __init__(self, props: Dict):
        self.consumer = KafkaConsumer(**props)

    def consume_from_kafka(self, topics: List[str]):
        self.consumer.subscribe(topics)
        print('Consuming from Kafka started')
        print('Available topics to consume: ', self.consumer.subscription())
        while True:
            try:
                message = self.consumer.poll(1.0)
                if message is None or message == {}:
                    continue
                for message_key, message_value in message.items():
                    for msg_val in message_value:
                        print(msg_val.key, msg_val.value)
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == '__main__':
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'key_deserializer': lambda key: int(key.decode('utf-8')),
        'value_deserializer': lambda x: loads(x.decode('utf-8'), object_hook=lambda d: Ride.from_dict(d)),
        'group_id': 'consumer.group.id.json-example.1',
    }

    json_consumer = JsonConsumer(props=config)
    json_consumer.consume_from_kafka(topics=[KAFKA_TOPIC])