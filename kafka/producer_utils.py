from confluent_kafka import SerializingProducer

import avro
import json

def parse_json_to_dict(filepath):
    with open(filepath, 'r') as f:
        object = json.load(f)  
    f.close()
    return object


def get_avro_producer():

    # Set up the Avro serializer

    avro_schema = avro.schema.Parse(json.dumps(parse_json_to_dict('resources/schema.json')))

    #avro_serializer = serializer.Serializer(avro_schema)

    # Set up the Kafka producer
    producer_conf = {
        "bootstrap.servers": "*",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "*",
        "sasl.password": "*",
    }
    producer = SerializingProducer(producer_conf)
    return producer


# Define the callback function to handle delivery reports
def delivery_callback(err, msg):
    if err:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))