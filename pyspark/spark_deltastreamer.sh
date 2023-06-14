#!/bin/bash

base_path_default="file:///tmp/warehouse/spark"
LOG4J_PATH_default="log4j2.properties"
DEBUG_BOOL_default="false"

while getopts "o:t:b:l:d:" arg; do
    case $arg in
        o) output=$OPTARG;;
        t) topic=$OPTARG;;
        b) base_path=$OPTARG;;
        l) LOG4J_PATH=$OPTARG;;
        d) DEBUG_BOOL=$OPTARG;;
    esac
done

export output=${output}
export topic=${topic}
export base_path=${base_path:-$base_path_default}/${output}
export LOG4J_PATH=${LOG4J_PATH:-$LOG4J_PATH_default}
export DEBUG_BOOL=${DEBUG_BOOL:-$DEBUG_BOOL_default}
export table=$(echo "$topic" | tr '-' '_')

#validate that each of the required parameters have been set
if [ -z "$output" ]; then
    echo "output is not set"
    exit 1
fi
if [ -z "$topic" ]; then
    echo "topic is not set"
    exit 1
fi

echo "Topic: ${topic}"
echo "Output: ${output}"
echo "Base Path: ${base_path}"

echo "Deltastreamer from Kafka topic $topic to $output"
echo "Writing to $base_path"

LOG4J_SETTING="-Dlog4j2.configurationFile=${LOG4J_PATH}"
DEBUG="-Dlog4j2.debug=${DEBUG_BOOL}"

set -eux

# sub output and base_path into spark properties file prior to moving to tmp dir
envsubst '${topic}' < hoodie-conf.properties > "/tmp/hoodie-conf-${topic}.properties"

spark-submit \
  --master local[1] \
  --num-executors=1 \
  --executor-cores=1 \
  --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
  --conf "spark.driver.extraJavaOptions=${LOG4J_SETTING}" \
  --conf "spark.executor.extraJavaOptions=${LOG4J_SETTING}" \
  --properties-file spark.properties \
    hudi-utilities-bundle_2.12-0.12.1.jar \
  --op INSERT \
  --props /tmp/hoodie-conf-${topic}.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
  --source-class org.apache.hudi.utilities.sources.AvroKafkaSource \
  --source-ordering-field viewtime  \
  --table-type COPY_ON_WRITE \
  --target-base-path $base_path/$table \
  --target-table $table
