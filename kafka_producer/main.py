import logging

from uuid import uuid1

from kafka.errors import KafkaTimeoutError
from pendulum import now
from random import randint
from json import dumps
from time import sleep

from kafka import KafkaProducer

TIMEOUT = .1

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if __name__ == '__main__':

    try:
        producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: dumps(v).encode('utf-8'))
        while True:
            message = {
                "event_id": str(uuid1()),
                "event_time": now(tz="UTC").to_iso8601_string(),
                "user_id": randint(10, 100),
                "action": ["click", "view", "purchase"][randint(0, 2)],
                "value": randint(1, 1000)
            }

            producer.send("stream-input", message)
            logger.info("Message sent!")

            sleep(TIMEOUT)

    except (KeyboardInterrupt, KafkaTimeoutError):
        print("Shutting down producer...")
