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
3. 