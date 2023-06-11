# Kafka-Tutorial-and-Examples

### Star kafka in kraft mode
> ~/kafka_2.13-3.4.0/bin/kafka-storage.sh random-uuid

> ~/kafka_2.13-3.4.0/bin/kafka-storage.sh format -t <uuid> -c ~/kafka_2.13-3.4.0/config/kraft/server.properties

> ~/kafka_2.13-3.4.0/bin/kafka-server-start.sh ~/kafka_2.13-3.4.0/config/kraft/server.properties

### Topics
> kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --create --partitions 3 --replication-factor 1

> kafka-topics.sh --bootstrap-server localhost:9092 --list

> kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic first_topic

> kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic first_topic

### Producer
> kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first_topic

> kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first_topic < topic-input.txt

> kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first_topic --property parse.key=true --property key.separator=:
 
### Consumer
> kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic

> kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --from-beginning

> kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --formatter kafka.tools.DefaultMessageFormatter --property print.timestamp=true --property print.key=true --property print.value=true --from-beginning

### Consumer in Group
> kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group my-first-application 

> kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-first-application

> kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-first-application --reset-offsets --to-earliest --execute --topic first_topic

> kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-first-application --reset-offsets --shift-by -2 --execute --topic first_topic

> kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-first-application

> kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list --state

> kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups  --state

> kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group my-first-application

> kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete-offsets --group my-first-application --topic first_topic
