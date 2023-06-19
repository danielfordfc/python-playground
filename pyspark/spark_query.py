from pyspark.sql import SparkSession
import sys


def main(base_dir, topic, output):
    spark = SparkSession.builder \
        .appName("query-hudi") \
        .getOrCreate()

    print(f"base_dir: {base_dir}")
    print(f"topic: {topic}")
    print(f"output: {output}")

    table_name = topic.replace('-', '_')
    base_path = f"file:///tmp/warehouse/spark/{output}/{table_name}"

    raw_df = spark.read.format("hudi").load(base_path)
    raw_df.createOrReplaceTempView(table_name)
    spark.sql(f"select * from {table_name} where userid = 'User_8'").show(truncate=False)


if __name__ == "__main__":

    # handle positional args from command line from the form    stream_to_hudi.py $base_path $topic $output
    base_dir = sys.argv[1]
    topic = sys.argv[2]
    output = sys.argv[3]

    main(base_dir, topic, output)
