import json
import logging

from kafka import KafkaConsumer
from kafka.errors import KafkaTimeoutError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if __name__ == '__main__':

    try:
        consumer = KafkaConsumer("stream-output", bootstrap_servers="localhost:29092")
        for msg in consumer:
            logger.info(f"Offset: {msg.offset}, incoming message: {json.loads(msg.value)}")

    except (KeyboardInterrupt, KafkaTimeoutError):
        print("Shutting down consumer...")
