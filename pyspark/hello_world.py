from pyspark.sql import SparkSession

def main():
    # set up consumer
    spark = SparkSession \
        .builder \
        .appName("test") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("kafka.security.protocol", "SSL") \
        .option("failOnDataLoss", "false") \
        .option("subscribe", "GDP-1168-confluent-example") \
        .option("includeHeaders", "true") \
        .option("startingOffsets", "earliest") \
        .option("spark.streaming.kafka.maxRatePerPartition", "50") \
        .load()
    
    kafka_df.show() # this is just to test if it works


if __name__ == "__main__":
    main()