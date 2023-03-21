from confluent_kafka import admin, Producer

from confluent_kafka.admin import AdminClient, NewTopic

from avro import schema, io

import producer_utils as utils

import json
import argparse
import json
import uuid
import time
from datetime import datetime,timezone
import random

# Define the command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--topic', help='The topic to which you want to publish the messages', default='gdp-1168-test')
parser.add_argument('--payload', help='the relative directory to the payload of your message as a Python dictionary', default='resources/payload.json')
parser.add_argument('--schema', help='the relative directory to the schema of your message as a Python dictionary', default='resources/schema.json')
parser.add_argument('--credentials', help='the relative directory to the location of your credentials object', default='/Users/daniel.ford/datadev_credentials.json')
args = parser.parse_args()


if __name__ == "__main__":

    conf = utils.parse_json_to_dict(args.credentials)

    print(type(conf))
    
    # Create the Producer & AdminClient instance
    producer = utils.get_avro_producer()
    admin = AdminClient(conf)

    topic = args.topic

    #create topic if not exist 
    if admin.list_topics().topics.get(topic) is None:
        admin.create_topics([NewTopic(topic, num_partitions=1)])

    # Define the payload of your message as a Python dictionary
    json_payload = json.dumps(utils.parse_json_to_dict(args.payload))

    # Publish the message to Kafka
    producer.produce(topic, key=str(uuid.uuid4()), value=json_payload.encode('utf-8'))

    # Wait for any outstanding messages to be delivered and delivery reports to be received
    producer.flush()