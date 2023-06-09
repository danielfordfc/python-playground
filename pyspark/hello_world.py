from pyspark.sql import SparkSession

def main():
    # set up consumer
    # scala_version = '2.12'
    # spark_version = '3.3.2'
    # TODO: Ensure match above values match the correct versions
    # packages = [
    #     f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    #     'org.apache.kafka:kafka-clients:3.2.1'
    # ]
    spark = SparkSession.builder \
        .appName("kafka-example") \
        .getOrCreate()

    #get info on spark session
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test1-pageviews") \
        .option("includeHeaders", "true") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # write the stream out to the console
    query = kafka_df \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "output") \
    .option("checkpointLocation", "checkpoint") \
    .start()

    query.awaitTermination()

        

if __name__ == "__main__":
    main()