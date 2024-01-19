from pyspark.sql import SparkSession
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
import json

from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col

from pyspark.sql.functions import col, expr



def main():
        spark = SparkSession.builder \
                .appName("kafka-example") \
                .getOrCreate()

        output = "hudi"
        base_dir = f"file:///tmp/warehouse/spark"
        topic = "pageviews-avro"

        kafka_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", topic) \
                .option("mergeSchema", "true") \
                .option("includeHeaders", "true") \
                .option("startingOffsets", "earliest") \
                .load()

        table_name = topic.replace('-', '_')

        # fetch schema from schema registry
        sr = CachedSchemaRegistryClient({
                'url': 'http://localhost:8081'
                })

        #RecordSchema obj
        schema_string = json.dumps(sr.get_latest_schema(f"{topic}-value")[1].to_json())

        # for each binary value, seek past the first 5 bytes to get the avro encoded data
        kafka_df = kafka_df.withColumn("value", expr("substring(value, 6)"))

        personDF = kafka_df.select('timestamp','offset','partition','value', 
                                from_avro(col("value"), schema_string).alias("person"))

        personDF = personDF.select('timestamp','offset','partition','person.*')              
        
        query = personDF \
                .writeStream \
                .format("hudi") \
                .option("checkpointLocation", f"{base_dir}/{output}/{table_name}") \
                .option("hoodie.table.name", table_name) \
                .option("hoodie.datasource.write.precombine.field", "timestamp") \
                .option("hoodie.datasource.write.recordkey.field", "timestamp") \
                .option("hoodie.merge.allow.duplicate.on.inserts", True) \
                .option("hoodie.datasource.write.operation", "insert") \
                .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
                .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator") \
                .outputMode("append") \
                .option("path", f"{base_dir}/{output}/{table_name}") \
                .start()

        query.awaitTermination()


if __name__ == "__main__":
    main()