# DDOS Detector
This project helps in detecting ips  which are periodically attacking websites by a botnet to cause [Distributed Denial of Service (DDOS)](https://en.wikipedia.org/wiki/Denial-of-service_attack#Distributed_attack) 

It basically analyse a log file in [Apache Log File Format](https://httpd.apache.org/docs/2.2/logs.html)

please find the sample log file for analysis [here](https://drive.google.com/file/d/0B8FpJBr7jL6MLS11aUstdTF3MkE/view)

## prerequisite ##
* Setup [Java](https://docs.oracle.com/cd/E19182-01/820-7851/inst_cli_jdk_javahome_t/) 
* Setup [Maven](https://maven.apache.org/install.html)
* Setup [Kafka](https://kafka.apache.org/quickstart)
* Setup [Zookeeper](https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html)
* Create [kafka topic](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.5/kafka-working-with-topics/content/creating_a_kafka_topic.html) 

## configuration ##
* ***log_file_path***       : this attribute takes log file absoulte path 
* ***kafka_brokers***       : this attribute takes kafka broker url example: localhost:9092
* ***stream_batch_size***   : this attribute helps to set no of lines we want to publish in single kafka message
* ***process_secs***        : this attribute heps tp set time in seconds for which DDOS hits will be calculated
* ***ddos_detection_hits*** : this attribute helps to set no of hits to be considered for possible DDOS
* ***output_file_path***    : this attribute helps to set output file path in which DDOS detected ips will be written
```json
{
  "log_file_path": "./apache-access-log.txt",
  "kafka_brokers": "localhost:9092",
  "topic_name": "website_logs",
  "stream_batch_size": "10000",
  "process_secs": "1",
  "ddos_detection_hits": "5",
  "output_file_path": "./results/ddos_detected_ips.txt"
}
```
## build and start app ##
* clone this project to your machine
* run below command in project root directory
  * mvn build
* ***Reading and Producing Log Messages*** use below command
  * java -jar target/DDOSDetector-v-0.1.jar stream
* ***Consuming and Detecting Ips from Messages***
  * java -jar target/DDOSDetector-v-0.1.jar process use below command



