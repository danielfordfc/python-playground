#!/usr/bin/bash

LOG4J_SETTING="-Dlog4j2.configurationFile=file:log4j2.properties"
DEBUG="-Dlog4j2.debug=true"



set -eux

spark-submit \
   --deploy-mode client \
   --driver-memory 1g \
   --executor-memory 1g \
   --executor-cores 1 \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.kafka:kafka-clients:3.2.1,org.apache.spark:spark-avro_2.12:3.3.2,org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.2 \
   --conf "spark.driver.extraJavaOptions=${LOG4J_SETTING}" \
   --properties-file spark.properties \
   --class org.apache.spark.examples.SparkPi \
   stream_to_hudi.py


# "{\"fields\": [{\"name\": \"name\",\"type\": \"string\", \"logicalType\": \"uuid\"},{\"name\": \"favorite_number\",\"type\": \"long\"},{\"name\":\"date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\",\"arg.properties\":{\"range\":{\"min\":1674909551,\"max\":1676909551}}}},{\"name\": \"favorite_color\", \"type\": \"string\"}],\"name\": \"User\",\"namespace\": \"confluent.io.examples.serialization.avro\",\"type\": \"record\"}"



# spark-shell \
#     --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.2 \
#     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
#     --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
#     --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \