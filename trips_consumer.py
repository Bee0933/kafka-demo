# pylint: disable=unnecessary-lambda
# pylint: disable=super-init-not-called

import os
import json
from typing import Dict, List
from kafka import KafkaConsumer
from schema import Rides


class json_consumer(KafkaConsumer):
    def __init__(self, props: Dict):
        if not isinstance(props, Dict):
            raise TypeError("use dict type only")
        self.consumer = KafkaConsumer(**props)

    def consume_from_kafka(self, topics: List[str]) -> None:
        self.consumer.subscribe(topics=topics)
        print("Consuming from Kafka started")
        print("Available topics to consume: ", self.consumer.subscription())

        while True:
            try:
                message = self.consumer.poll(1.0)
                if message is None or message == {}:
                    continue
                for _, message_value in message.items():
                    for msg_val in message_value:
                        print(msg_val.key, msg_val.value)
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == "__main__":
    config = {
        "bootstrap_servers": os.environ.get("BOOTSTRAP_SERVERS"),
        "auto_offset_reset": "earliest",
        "enable_auto_commit": True,
        "key_deserializer": lambda key: int(key.decode("utf-8")),
        "value_deserializer": lambda x: json.loads(
            x.decode("utf-8"), object_hook=lambda d: Rides.from_dict(d)
        ),
        "group_id": "consumer.group.id.json-example.1",
    }

    consumer = json_consumer(props=config)
    consumer.consume_from_kafka(topics=[os.environ.get("KAFKA_TOPIC")])
