# pylint: disable=super-init-not-called

import json
import os
import time
import random
from pathlib import Path
import pandas as pd
from typing import List, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from schema import Rides

time_delays = [10,2,5,18,9,1,0]

class json_producer(KafkaProducer):
    def __init__(self, props: Dict):
        if not isinstance(props, Dict):
            raise TypeError("use dict object only")
        self.producer = KafkaProducer(**props)

    @staticmethod
    def read_data(path: Path) -> List:
        if not isinstance(path, Path):
            raise TypeError("use path only")
        data = pd.read_parquet(path).head(10) # for test data
        return [Rides(arr=list(row)) for idx, row in data.iterrows()]

    def publish_data(self, topic: str, message: List[Rides]) -> None:
        try:
            for ride in message:
                record = self.producer.send(
                    topic=topic, key=ride.PULocationID, value=ride
                )
                print(
                    f"ride {ride.PULocationID} published at offset {record.get().offset}"
                )
                time.sleep(random.choice(time_delays))
        except KafkaTimeoutError as e:
            print(f"error {e.__str__}")


if __name__ == "__main__":
    config = {
        "bootstrap_servers": os.environ.get("BOOTSTRAP_SERVERS"),
        "key_serializer": lambda key: str(key).encode("utf-8"),
        "value_serializer": lambda x: json.dumps(x.__dict__, default=str).encode(
            "utf-8"
        ),
    }
    producer = json_producer(props=config)
    rides = producer.read_data(path=Path(os.environ.get("INPUT_DATA_PATH")))
    producer.publish_data(topic=os.environ.get("KAFKA_TOPIC"), message=rides)
