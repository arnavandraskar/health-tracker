from configparser import ConfigParser
from kafka import KafkaConsumer, KafkaProducer
import json
from sqlalchemy import create_engine

def config(filename, section='postgresql'):
    """
    Reads and parses the configuration file specified by 'filename' and retrieves the parameters for the given section.

    Args:
        filename (str): The name of the configuration file.
        section (str): The section name in the configuration file (default is 'postgresql').

    Returns:
        dict: A dictionary containing the parameters for the specified section.

    Raises:
        Exception: If the specified section is not found in the configuration file.
    """
    # Create a parser
    parser = ConfigParser()

    # Read config file
    parser.read(filename)

    # Get section, default to 'postgresql'
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def get_consumer(topic):
    """
    Creates and returns a KafkaConsumer instance for consuming messages from the specified topic.

    Args:
        topic (str): The name of the topic to consume messages from.

    Returns:
        KafkaConsumer: A KafkaConsumer instance configured to consume messages from the specified topic.
    """
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=['13.235.245.66:9092'],
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer


def get_producer():
    """
    Creates and returns a KafkaProducer instance for producing messages.

    Returns:
        KafkaProducer: A KafkaProducer instance configured to produce messages.
    """
    producer = KafkaProducer(bootstrap_servers=['13.235.245.66:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    return producer
