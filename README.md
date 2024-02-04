# python-playground

Where I go to play with Python (and scala) and experiment with new technologies, pythonically.

This is in heavy need of a cleanup.

- flink: initial attempts at using Apache Flink with Python to stream data from Kafka to some kind of file sink. Table API and datastream API currently not working.

- kafka: data producers for kafka in python

- KirbyStructuredStreamer: initial mess around with a scala based Spark Structured Streaming application to read from Kafka and write to Hudi on local disk.

- Pandera: the only polished part of this repo. A series of examples of using Pandera to validate Pandas and Spark Dataframes, and how it can bring value to an organisation.

- pyspark:
    - docker-compose for running a local kafka cluster
    - working examples of using pyspark to read from kafka and write to hudi (Structured Streaming and DeltaStreamer)



