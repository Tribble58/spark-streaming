import logging

from uuid import uuid1

from kafka.errors import KafkaTimeoutError
from pendulum import now
from random import randint
from json import dumps
from time import sleep

from kafka import KafkaProducer

TIMEOUT = 2.5
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC_NAME = "stream-purchases"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if __name__ == '__main__':

    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: dumps(v).encode('utf-8'))
        while True:
            message = {
                "event_id": str(uuid1()),
                "event_time": now(tz="UTC").to_iso8601_string(),
                "user_id": randint(10, 100),
                "action": "purchase",
                "value": randint(1, 1000)
            }

            # Send message with timeout once in 10 times with 10 seconds delay
            if randint(35, 45) == 42:
                sleep(10)

            producer.send(KAFKA_TOPIC_NAME, message)
            logger.info("Purchase message sent!")

            sleep(TIMEOUT)

    except (KeyboardInterrupt, KafkaTimeoutError):
        print("Shutting down producer...")
