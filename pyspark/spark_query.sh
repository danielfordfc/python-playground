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

echo "Read from $base_path"

LOG4J_SETTING="-Dlog4j2.configurationFile=${LOG4J_PATH}"
DEBUG="-Dlog4j2.debug=${DEBUG_BOOL}"

set -eux

# sub output and base_path into spark properties file prior to moving to tmp dir
# envsubst '${topic}' < hoodie-conf.properties > "/tmp/hoodie-conf-${topic}.properties"

PACKAGES=(
    "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.2"
)
PACKAGES=$(IFS=,; echo "${PACKAGES[*]}")

spark-submit \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --packages ${PACKAGES} \
    --conf "spark.driver.extraJavaOptions=${LOG4J_SETTING}" \
    --conf "spark.driver.extraJavaOptions=${DEBUG}" \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
    --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
    --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
    --conf 'spark.hadoop.spark.sql.legacy.parquet.nanosAsLong=false' \
    --conf 'spark.hadoop.spark.sql.parquet.binaryAsString=false' \
    --conf 'spark.hadoop.spark.sql.parquet.int96AsTimestamp=true' \
    --conf 'spark.hadoop.spark.sql.caseSensitive=false' \
    --properties-file spark.properties \
    --class org.apache.spark.examples.SparkPi \
    spark_query.py $base_path $topic $output
