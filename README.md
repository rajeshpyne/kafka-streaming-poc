# kafka-streaming-poc
A streaming tutorial for Kafka


Kafka Processing

1. Start Zookeeper (zookeeper-server-start.sh config/zookeeper.properties)
2. Start Kafka Server (kafka-server-start.sh config/server.properties)
3. Create a Kafka Topic (kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic <topic name>) #One Time

Optional (to check whether we are getting the messages from producer to consumer)

4. Start the Producer (kafka_2.11-0.11.0.2$ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic <given topic name>)
5. Start the consumer (kafka_2.11-0.11.0.2$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <given topic name> --from-beginning)


For Getting RealTime Twitter Data:-

1. Run the first 3 steps from Kakfa Processing mentioned above and take a note of the kafka topic you have created, because that is going to be the topic for running the python code.
2. Run KafkaPython_TwitterStreaming.py
3. Run Kafka_SparkStreaming.py to load twitter data into HDFS

