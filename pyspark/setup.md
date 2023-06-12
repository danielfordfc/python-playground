# Setup

This project uses the confluent AIO docker-compose.yml to create a local kafka cluster.

This can then be accessed at the control center @ localhost:9021 to provision kafka topics, DataGenSourceConnectors etc.. for testing. 

Alternatively, you could use the confluent CLI or another 3rd part application to create this infra.

## Pre-requisites

You have followed https://docs.confluent.io/platform/current/platform-quickstart.html for the pre-requisites.

Ignore the section in point 1 asking you to wget the docker-compose.yml, as this is already in this repo and has been modified to work properly with kafka-connect.

You should be able to now follow steps 2 and 3 of the guide to get the infra up and running, create a topic and a connector.

pyspark on spark 3.3.2 and scala 2.12.x was used for this project and is set in the --packages argument in the spark-submit command.

### Stream to Hudi from kafka

1. Create a topic in the control center called `hudi_topic` by following step 2 of the linked guide
2. Create a DataGenSourceConnector in the control center by following step 3 of the linked guide.
3. Get some data in it!

4. Run the following command to stream the data from kafka to hudi from this dir:

```bash
bash spark_submit.sh -o hudi -t {topic_name}
```

5. If required, to clear your hudi table, run the following command:

```bash
rm -rf /tmp/warehouse/spark/hudi/{topic_name}
```

### Query Hudi

```bash
 spark-shell \
     --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.2 \
     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
     --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
```

```scala
 val topic = "topic-name"
 val tableName = () => topic.replaceAll("-", "_")
 val basePath = s"file:///tmp/warehouse/spark/hudi/${tableName()}"
 val df = spark.read.format("hudi").load(basePath)
 df.createOrReplaceTempView(s"hudi_${tableName()}")

 // spark.sql("select viewtime, userid, pageid from hudi_transactional").show()

 spark.sql(s"select _hoodie_commit_time, _hoodie_commit_seqno, _hoodie_record_key, _hoodie_partition_path from hudi_${tableName}").show()

 spark.sql(s"select * from hudi_${tableName}").show()