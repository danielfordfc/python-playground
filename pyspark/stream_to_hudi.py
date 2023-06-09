from pyspark.sql import SparkSession
import sys

def main(base_dir, topic, output):

    spark = SparkSession.builder \
        .appName("kafka-example") \
        .getOrCreate()
    
    print(f"base_dir: {base_dir}")
    print(f"topic: {topic}")
    print(f"output: {output}")

    try:
        #get info on spark session
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .option("includeHeaders", "true") \
            .option("startingOffsets", "earliest") \
            .load()

        table_name = topic.replace('-', '_')

        query = kafka_df \
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
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        sys.exit(1)

        

if __name__ == "__main__":

    #handle positional args from command line from the form    stream_to_hudi.py $base_path $topic $output
    base_dir = sys.argv[1]
    topic = sys.argv[2]
    output = sys.argv[3]
    

    main(base_dir, topic, output)