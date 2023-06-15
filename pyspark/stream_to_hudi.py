from pyspark.sql import SparkSession
# from pyspark.sql.avro.functions import from_avro
# from pyspark.sql.functions import from_json, current_timestamp
# from pyspark.sql.types import TimestampType, StructType, StringType, IntegerType
import sys
from abris import from_avro_abris_config, from_avro


def main(base_dir, topic, output):

    # build spark schema

    # schema = StructType().add("name", StringType()).add("favorite_number", IntegerType()).add("favorite_color", StringType())

    spark = SparkSession.builder \
        .appName("kafka-example") \
        .getOrCreate()

    print(f"base_dir: {base_dir}")
    print(f"topic: {topic}")
    print(f"output: {output}")

    # get info on spark session
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("includeHeaders", "true") \
        .option("startingOffsets", "earliest") \
        .load()

    from_avro_abris_settings = from_avro_abris_config({'schema.registry.url': 'http://localhost:8081'}, f'{topic}', False)
    print(from_avro_abris_settings)
    df = kafka_df.withColumn("parsed", from_avro("value", from_avro_abris_settings))
    df.show()


    # table_name = topic.replace('-', '_')

    # # deserialize value field that is JSONSerializer encoded
    # kafka_df = kafka_df.selectExpr("offset", "partition", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

    # # parsed_df = kafka_df.withColumn("value", from_json("value", schema)) \
    # # .withColumn("ts", current_timestamp().cast(TimestampType()))
    # # deserialize the kafka message using the avro schema

    # query = kafka_df \
    #     .writeStream \
    #     .format("hudi") \
    #     .option("checkpointLocation", f"{base_dir}/{output}/{table_name}") \
    #     .option("hoodie.table.name", table_name) \
    #     .option("hoodie.datasource.write.precombine.field", "timestamp") \
    #     .option("hoodie.datasource.write.recordkey.field", "timestamp") \
    #     .option("hoodie.merge.allow.duplicate.on.inserts", True) \
    #     .option("hoodie.datasource.write.operation", "insert") \
    #     .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    #     .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator") \
    #     .outputMode("append") \
    #     .option("path", f"{base_dir}/{output}/{table_name}") \
    #     .start() 

    # query.awaitTermination()


if __name__ == "__main__":

    # handle positional args from command line from the form    stream_to_hudi.py $base_path $topic $output
    base_dir = sys.argv[1]
    topic = sys.argv[2]
    output = sys.argv[3]

    main(base_dir, topic, output)
