# A simple example demonstrating use of AvroSerializer.
# Heavily inspired by the confluent-kafka-python examples.

import argparse
import os
from uuid import uuid4
import json

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.admin import AdminClient, NewTopic


class Pageviews(object):
    def __init__(self, viewtime, userid, pageid):
        self.viewtime = viewtime
        self.userid = userid
        self.pageid = pageid


def to_dict(pageviews, ctx):
    return dict(viewtime=pageviews.viewtime,
                userid=pageviews.userid,
                pageid=pageviews.pageid)


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def get_schema(classification):
    if classification == "json":
        return """
            {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "pageviews",
                "description": "test",
                "type": "object",
                "properties": {
                    "viewtime": {
                    "description": "viewtime",
                    "type": "number"
                    },
                    "userid": {
                    "description": "userid",
                    "type": "string",
                    "exclusiveMinimum": 0
                    },
                    "pageid": {
                    "description": "pageid",
                    "type": "string"
                    }
                },
                "required": [ "viewtime", "userid", "pageid" ]
            }
        """
    else:
        path = os.path.realpath(os.path.dirname(__file__))
        avro_schema_file = "pageviews.avsc"
        with open(f"{path}/schemas/{avro_schema_file}") as f:
            return f.read()


def create_value_serializer(schema_registry, classfication):

    schema_registry_conf = {'url': schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    schema_str = get_schema(classification=classfication)

    if classfication == "avro" or classfication == "transactional":
        return AvroSerializer(
            schema_registry_client,
            schema_str,
            to_dict
        )
    elif classfication == "json":
        return JSONSerializer(
            schema_str,
            schema_registry_client,
            to_dict
        )


def produce_pageview_data(producer, msgs, topic, key_serializer, value_serializer):
    print(f"Producing records to topic {topic}. ^C to exit.")
    for i in range(int(msgs)):
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            viewtime = int(f"10{i}")
            userid = f"User_{i}"
            pageid = f"Page_{i}"
            pageviews = Pageviews(
                viewtime=viewtime,
                userid=userid,
                pageid=pageid
            )
            producer.produce(topic=topic,
                             key=key_serializer(str(uuid4())),
                             value=value_serializer(pageviews, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue


def produce_non_transactional_topic_data(producer, msgs, topic, key_serializer, value_serializer):
    produce_pageview_data(
        producer=producer, msgs=msgs, topic=topic,
        key_serializer=key_serializer, value_serializer=value_serializer
    )
    print("\nFlushing records...")
    producer.flush()


def produce_transactional_topic_data(producer, msgs, topic, key_serializer, value_serializer):
    producer.init_transactions()
    producer.begin_transaction()

    produce_pageview_data(
        producer=producer, msgs=msgs, topic=topic,
        key_serializer=key_serializer, value_serializer=value_serializer
    )

    producer.commit_transaction()

    print("\nFlushing records...")
    producer.flush()


def main(args):
    classification = args.classification
    topic = args.topic
    schema_registry = args.schema_registry

    default_producer_conf = {
        'avro': {
            "bootstrap.servers": args.bootstrap_servers,
        },
        'json': {
            "bootstrap.servers": args.bootstrap_servers,
        },
        'transactional': {
            "bootstrap.servers": args.bootstrap_servers,
            'transactional.id': "trans-id",
            'acks': 'all',
            'enable.idempotence': 'true',
            'max.in.flight.requests.per.connection': '1',
        }
    }

    producer_conf = default_producer_conf.get(classification, {})
    producer = Producer(producer_conf)

    admin = AdminClient(producer_conf)

    # create topic if not exist
    if admin.list_topics().topics.get(topic) is None:
        admin.create_topics([NewTopic(topic, num_partitions=3)])

    key_serializer = StringSerializer('utf_8')
    value_serializer = create_value_serializer(schema_registry=schema_registry, classfication=classification)

    print(f"Producing records to {classification} topic...")
    if classification == "avro" or classification == "json":
        produce_non_transactional_topic_data(
            producer=producer, msgs=args.msgs, topic=args.topic,
            key_serializer=key_serializer, value_serializer=value_serializer
        )
    elif classification == "transactional":
        produce_transactional_topic_data(
            producer=producer, msgs=args.msgs, topic=args.topic,
            key_serializer=key_serializer, value_serializer=value_serializer
        )
    else:
        raise Exception(f"Unsupported classification {classification}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroSerializer example")
    parser.add_argument('-c', dest="classification", default="avro",
                        help="avro, json or transactional")
    parser.add_argument('-b', dest="bootstrap_servers", default="localhost:9092",
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", default="http://localhost:8081",
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="pageviews",
                        help="Topic name")
    parser.add_argument('-p', dest="specific", default="true",
                        help="Avro specific record"),
    parser.add_argument('-n', dest="msgs", default=10),
    parser.add_argument('-su', dest="sasl_username",
                        help="sasl_username")
    parser.add_argument('-sp', dest="sasl_password",
                        help="sasl_password")

    main(parser.parse_args())
