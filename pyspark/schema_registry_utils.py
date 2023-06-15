from confluent_kafka.avro import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError

from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient

import fastavro
import json
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def fetch_schema(topic):

    sr = CachedSchemaRegistryClient({
        'url': 'http://localhost:8081'
    })

    schema_string = sr.get_latest_schema(f"{topic}-value")[1]
    #schema = json.loads(schema_string)
    return schema_string



def avro_to_json(binary_data, topic):
    try:
        schema = fetch_schema(topic)
        message = fastavro.schemaless_reader(binary_data, schema)
        return json.dumps(message)
    except SerializerError as e:
        print(e)


def manipulate_udf (spark, topic, kafka_df):
    # Register the UDF
    spark.udf.register("avro_to_json", avro_to_json, StringType())

    # Use the UDF to deserialize the value field
    expr = "avro_to_json(value, '{}')".format(topic)
    kafka_df = kafka_df.withColumn("value", F.expr(expr))

    return kafka_df
