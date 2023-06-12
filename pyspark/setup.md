# Setup

This project uses the confluent AIO docker-compose.yml to create a local kafka cluster.

This can then be accessed at the control center @ localhost:9021 to provision kafka topics, DataGenSourceConnectors etc.. for testing. 

Alternatively, you could use the confluent CLI or another 3rd part application to create this infra.

## Pre-requisites

You have followed https://docs.confluent.io/platform/current/platform-quickstart.html for the pre-requisites.

Ignore the section in point 1 asking you to wget the docker-compose.yml, as this is already in this repo and has been modified to work properly with kafka-connect.

You should be able to now follow steps 2 and 3 of the guide to get the infra up and running, create a topic and a connector.

pyspark on spark 3.3.2 and scala 2.12.x was used for this project and is set in the --packages argument in the spark-submit command.
- Download and install spark 3.3.2 with scala 2.12: https://spark.apache.org/downloads
- Install pyspark using `pip install pyspark`

python-playground/kafka/python-avro-producer
### Utility 1 - Producing to an Avro topic

```bash
    cd python-playground/kafka/python-avro-producer 
```

```bash
    python3 confluent_avro_producer.py -b "localhost:9092" -s "http://localhost:8081" -t {topic name}  -n 4
```

-b = bootstrap server
-s = schema registry
-t = topic name
-n = number of messages to produce

### Utility 2 - Producing to a transactional topic

```bash
    python3 transactional_json_producer.py -b "localhost:9092" -s "http://localhost:8081" -t {topic name}  -n 4
```

-b = bootstrap server
-s = schema registry
-t = topic name
-n = number of messages to produce

Spam this a few of times and you'll see the transactional messages arrive in your topic in the control center.

### Utility 3 - Stream to Hudi from kafka

1. This part assumes you've got a topic, and you're ready to stream it to a hudi table!
2. Run the following command to stream the data from kafka to hudi from this dir:

Assuming you're cd into the pyspark/ dir:

```bash
bash spark_submit.sh -o hudi -t {topic_name}
```

-o = output dir (required)
-t = topic name (required)
-b = base path (optional) - defaults to /tmp/warehouse/spark/{output dir}
-l = log4j path (optional) - defaults to local log4j2.properties file
-d = debug bool (optional) - defaults to false - set to true to enable debug logging

3. If required, to clear all your hudi tables, run the following command:

```bash
rm -rf /tmp/warehouse/spark/hudi

OR for individual tables...

rm -rf /tmp/warehouse/spark/hudi/{table_name}
```

### Querying the Hudi table

Run the following to enter the spark-shell with the hudi package and extensions:

```bash
 spark-shell --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.2 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.hadoop.spark.sql.legacy.parquet.nanosAsLong=false' \
--conf 'spark.hadoop.spark.sql.parquet.binaryAsString=false' \
--conf 'spark.hadoop.spark.sql.parquet.int96AsTimestamp=true' \
--conf 'spark.hadoop.spark.sql.caseSensitive=false'
```


Then run the following to query the table:

```scala
    val topic = "your-topic-name" // change this to the topic you want to query, rest of this block should be copy paste...
    val tableName = topic.replaceAll("-", "_")
    val basePath = s"file:///tmp/warehouse/spark/hudi/${tableName}"
    val df = spark.read.format("hudi").load(basePath)
    df.createOrReplaceTempView(tableName)
    spark.sql(s"select _hoodie_commit_time, value from ${tableName}").show(false)
```
