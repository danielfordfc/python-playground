from pyspark.sql import SparkSession

def main():

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

    table_name = "test1-pageviews"

    query = kafka_df \
    .writeStream \
    .format("hudi") \
    .option("checkpointLocation", f"hudi_checkpoint/{table_name}") \
    .option("hoodie.table.name", table_name) \
    .option("hoodie.datasource.write.precombine.field", "timestamp") \
    .option("hoodie.datasource.write.recordkey.field", "timestamp") \
    .option("hoodie.merge.allow.duplicate.on.inserts", True) \
    .option("hoodie.datasource.write.operation", "insert") \
    .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator") \
    .outputMode("append") \
    .option("path", f"file:///Users/daniel.ford/Documents/GitHub/danielfordfc/python-playground/pyspark/hudi_output/{table_name}") \
    .start() 

    query.awaitTermination()

        

if __name__ == "__main__":
    main()