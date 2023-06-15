#!/bin/bash

base_path_default="file:///tmp/warehouse/spark/$output"
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
export base_path=${base_path:-$base_path_default}
export LOG4J_PATH=${LOG4J_PATH:-$LOG4J_PATH_default}
export DEBUG_BOOL=${DEBUG_BOOL:-$DEBUG_BOOL_default}

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

echo "Spark Structured Streaming from Kafka topic $topic to $output"
echo "Writing to $base_path"

LOG4J_SETTING="-Dlog4j2.configurationFile=${LOG4J_SETTING}"
DEBUG="-Dlog4j2.debug=${DEBUG_BOOL}"

set -eux

# sub output and base_path into spark properties file prior to moving to tmp dir
#envsubst '${output},${base_path},${topic}' < spark.properties > "/tmp/spark.properties"

PACKAGES=(
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2"
    "org.apache.kafka:kafka-clients:3.2.1"
    "org.apache.spark:spark-avro_2.12:3.3.2"
    "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.2"
    "za.co.absa:abris_2.12:6.1.1"
    ""
)
PACKAGES=$(IFS=,; echo "${PACKAGES[*]}")

spark-submit \
   --deploy-mode client \
   --driver-memory 1g \
   --executor-memory 1g \
   --executor-cores 1 \
   --packages ${PACKAGES} \
   --repositories https://packages.confluent.io/maven/ \
   --conf "spark.driver.extraJavaOptions=${LOG4J_SETTING}" \
   --conf "spark.driver.extraJavaOptions=${DEBUG}" \
   --properties-file spark.properties \
   --class org.apache.spark.examples.SparkPi \
   stream_to_hudi.py $base_path $topic $output

