import time
import random
from datetime import datetime
from lib.utils import get_producer

if __name__ == "__main__":
    """
    Main program that generates random health data and sends it to a Kafka topic using a KafkaProducer.

    The program runs indefinitely, generating new data every 15 seconds.

    Dependencies:
        - lib.utils.get_producer: Function to retrieve a KafkaProducer instance.

    Usage:
        Run this script as the main entry point to start generating and sending health data to the 'health_data' topic.
    """
    producer = get_producer()

    while True:
        # Generate random health data
        data = {
            'timestamp': [datetime.now().strftime("%d/%m/%Y %H:%M:%S")],
            'heart_rate': [random.randint(60, 120)],
            'steps': [random.randint(0, 10)],
            'sleep': [random.randint(0, 4)],
            'calories': [random.uniform(100, 1000)]
        }

        # Send data to Kafka topic
        producer.send('health_data', value=data)
        producer.flush()

        # Sleep for 5 seconds before generating the next set of data
        time.sleep(5)
