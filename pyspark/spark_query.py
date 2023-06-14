from pyspark.sql import SparkSession
# from pyspark.sql.avro.functions import from_avro
# from pyspark.sql.functions import from_json, current_timestamp
# from pyspark.sql.types import TimestampType, StructType, StringType, IntegerType
import sys


def main(base_dir, topic, output):

    # build spark schema

    # schema = StructType().add("name", StringType()).add("favorite_number", IntegerType()).add("favorite_color", StringType())

    spark = SparkSession.builder \
        .appName("query-hudi") \
        .getOrCreate()

    print(f"base_dir: {base_dir}")
    print(f"topic: {topic}")
    print(f"output: {output}")

    table_name = topic.replace('-', '_')
    base_path = f"file:///tmp/warehouse/spark/{output}/{table_name}"

    # parsed_df = kafka_df.withColumn("value", from_json("value", schema)) \
    # .withColumn("ts", current_timestamp().cast(TimestampType()))
    # deserialize the kafka message using the avro schema

    raw_df = spark.read.format("hudi").load(base_path)
    raw_df.createOrReplaceTempView(table_name)
    spark.sql(f"select * from {table_name}").show(truncate=False)


if __name__ == "__main__":

    # handle positional args from command line from the form    stream_to_hudi.py $base_path $topic $output
    base_dir = sys.argv[1]
    topic = sys.argv[2]
    output = sys.argv[3]

    main(base_dir, topic, output)
