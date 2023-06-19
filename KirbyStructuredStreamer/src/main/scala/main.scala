import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema

import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.common.model.HoodieRecord


import org.apache.spark.sql.streaming.Trigger


object MySparkJob {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .appName("MySparkJob")
            .getOrCreate()

        val schemaRegistryURL = "http://localhost:8081"

        val topicName = "pageviews-avro"
        val subjectValueName = topicName + "-value"

        //create RestService object
        val restService = new RestService(schemaRegistryURL)

        val valueRestResponseSchema = restService.getLatestVersion(subjectValueName)

        //Use Avro parsing classes to get Avro Schema
        val parser = new Schema.Parser
        val topicValueAvroSchema: Schema = parser.parse(valueRestResponseSchema.getSchema)

        //key schema is typically just string but you can do the same process for the key as the value
        val keySchemaString = "\"string\""
        val keySchema = parser.parse(keySchemaString)

        //Create a map with the Schema registry url.
        //This is the only Required configuration for Confluent's KafkaAvroDeserializer.
        val props = Map("schema.registry.url" -> schemaRegistryURL)

        //Declare SerDe vars before using Spark structured streaming map. Avoids non serializable class exception.
        var keyDeserializer: KafkaAvroDeserializer = null
        var valueDeserializer: KafkaAvroDeserializer = null

        //Create structured streaming DF to read from the topic.
        val rawTopicMessageDF = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", topicName)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 20)
            .load()

        rawTopicMessageDF.writeStream
            .format("hudi")
            .options(getQuickstartWriteConfigs)
            .option(PRECOMBINE_FIELD_OPT_KEY, "timestamp")
            .option(RECORDKEY_FIELD_OPT_KEY, "offset")
            .option(TABLE_NAME, topicName)
            .outputMode("append")
            .option("path", "file:///tmp/" + topicName)
            .option("checkpointLocation", "file:///tmp/ck_" + topicName)
            .trigger(Trigger.Once())
            .start()

        // Wait for the stream to finish
        spark.streams.awaitAnyTermination()

        // spark.read.format("hudi").load("file:///tmp/" + topicName).count()


    


    }
}
