package com.github.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.*;

import static com.github.common.Constants.*;

/**
 * This class is responsible for
 * consuming log messages sent by LogStreamProducer
 * processing and detecting DDOS IPs
 * wirting Ips to configured file
 */
public class LogStreamConsumer {
    private static final String IP = "ip";
    private static final String TIME = "time";
    private String kafkaBrokers;
    private String kafkaTopic;
    private Map<String, Integer> ddosDetectionCache = new HashMap<>();
    private Set<String> ddosIpSet = new HashSet<>();
    private int process_secs;
    private int ddos_detection_hits;
    private Long initial_time = Long.MIN_VALUE;
    private String output_file_path;


    /**
     * This method will read configuration from json file 'ddos_configuration.json'
     * @return HashMap which consists of configuration in key , value pairs
     * @throws IOException when it unable to read configuration file
     */
    private Map<String, String> readConfig() throws IOException {
        String configuration = FileUtils.readFileToString(new File(DDOS_CONFIGURATION_JSON), Charset.defaultCharset());
        return new ObjectMapper().readValue(configuration, HashMap.class);
    }


    /**
     * This method is entry point for processing log data
     * It invokes kafka consumer
     * @throws Exception
     */
    public void start() throws Exception {
        Map<String, String> config = readConfig();
        output_file_path=config.get(OUTPUT_FILE_PATH);
        FileUtils.writeStringToFile(new File(output_file_path),"",Charset.defaultCharset(),false);
        kafkaBrokers = config.get(KAFKA_BROKERS);
        kafkaTopic = config.get(TOPIC_NAME);
        process_secs = Integer.parseInt(config.get(PROCESS_SECS)) * 1000;
        ddos_detection_hits = Integer.parseInt(config.get(DDOS_DETECTION_HITS));
        System.out.println("configured process_secs : " + process_secs + " ddos_detection_hits : " + ddos_detection_hits);
        if (ddos_detection_hits <= 1) {
            throw new Exception("Inavlid ddos_detection_hits count greater than 1 ");
        }
        consume();
    }

    /**
     * This method will listen to configured kafka topic and receives message upon polling
     * @throws JsonProcessingException
     */
    void consume() throws JsonProcessingException {
        Properties props = new Properties();
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("bootstrap.servers", kafkaBrokers);
        props.put("group.id", CLIENT_ID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(kafkaTopic));
        System.out.println("Subscribed to topic " + kafkaTopic);
        int i = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                process_batch(record.value());
                System.out.println("consumed and processed batch_id : "+record.key());
            }
        }
    }

    /**
     * This method will process the batch of logs which is consumed from configured kafka topic
     * It converts string json message to java List of Maps and sends ip and time for further processing
     * @param message String object of in json format which consists of batch of logs
     * @throws JsonProcessingException when there is invalid json format is send in message
     */
    private void process_batch(String message) throws JsonProcessingException {
        List<Map<String, String>> batchMessages = new ObjectMapper().readValue(message, new TypeReference<List<Map<String, String>>>(){});
        batchMessages.forEach(batchMessage-> {
            try {
                process(batchMessage.get(IP), Long.valueOf(batchMessage.get(TIME)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * This method will process the ip hit for a given time and detects whether the ip is falls under DDOS
     * @param ip ip of String object
     * @param time Time in millinseconds
     * @throws IOException
     */
    void process(String ip, Long time) throws IOException {
        if (initial_time == Long.MIN_VALUE) initial_time = time;
        if (time - initial_time >= process_secs) {
            ddosDetectionCache.clear();
            ddosDetectionCache.put(ip, 1);
            initial_time = time;
            System.out.println("Detected ips in timeframe : "+ddosIpSet.size());
            writeIpsToFile(ddosIpSet);
            ddosIpSet.clear();
        } else if (ddosDetectionCache.containsKey(ip)) {
            int current_count = ddosDetectionCache.get(ip);
            if (current_count + 1 == ddos_detection_hits) {
                ddosIpSet.add(ip);
            } else {
                ddosDetectionCache.put(ip, current_count + 1);
            }
        } else {
            ddosDetectionCache.put(ip, 1);
        }
    }


    /**
     * This method writes set of ips which are identified as DDOS to file
     * @param ddosIpSet detected ddos ips set
     * @throws IOException when there is error occurs while writing to file
     */
    void writeIpsToFile(Set<String> ddosIpSet) throws IOException {
        if( ddosIpSet.size() > 0){
            StringBuilder ipStrings=new StringBuilder("");
            ddosIpSet.forEach(ip->ipStrings.append(ip).append("\n"));
            FileUtils.writeStringToFile(new File(output_file_path),ipStrings.toString(),Charset.defaultCharset(),true);
            System.out.println("ips written to path : ["+output_file_path+"]");
        }
    }

}
