package com.github.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.common.LogMessage;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import static com.github.common.Constants.*;

/**
 * This class is responsible for
 * reading log file data
 * streaming log data to configured kafka topic
 */
public class LogStreamProducer {

    private String kafkaBrokers;
    private String kafkaTopic;
    private int batch_size;

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
     * This method is entry point for streaming the messages
     * It reads the logs
     * converts the logs to batch messages
     * sends batch messages on kafka topic
     * @throws IOException
     */
    public void start() throws IOException {
        Map<String, String> config = readConfig();
        batch_size = Integer.parseInt(config.get(STREAM_BATCH_SIZE));
        List<String> logMessages = convertToBatches(readLogs(config.get(LOG_FILE_PATH)));
        kafkaBrokers = config.get(KAFKA_BROKERS);
        kafkaTopic = config.get(TOPIC_NAME);
        logMessages.forEach(logMessage -> streamKafkaMessage(logMessage));
    }

    /**
     * This method is will read the logfile and converts into List of LogMessage Objects
     * @param file_path
     * @return List of LogMessage Objects
     * @throws IOException
     */
    private List<LogMessage> readLogs(String file_path) throws IOException {
        List<String> file_data = Arrays.asList(FileUtils.readFileToString(new File(file_path), Charset.defaultCharset()).split("\n"));
        List<LogMessage> messageList = file_data.stream()
                .map(this::parseLog)
                .collect(Collectors.toList());
        return messageList;
    }

    /**
     * This method is responsible for parsing the log line into LogMessage Object
     * @param log line in String Object
     * @return LogMessage Object
     */
    private LogMessage parseLog(String log) {
        try {
            return new LogMessage(log);

        } catch (ParseException pe) {
            return null;
        }
    }

    /**
     * This method will convert the List of LogMessages to String Json Batches for configured batch_size
     * @param logMessages expects List of LogMessages
     * @return List of String Json Batches
     */
    private List<String> convertToBatches(List<LogMessage> logMessages) {
        List<List<LogMessage>> batches = Lists.partition(logMessages, batch_size);
        List<String> batch_messages = new ArrayList<>();
        for (List<LogMessage> batch : batches) {
            batch_messages.add(convertLogMessageToStringArray(batch));
        }
        return batch_messages;
    }

    /**
     * This method will convert a List of LogMessages object to String Json Object
     * @param logMessages expects List of Logmessages Object
     * @return String Json Object
     */
    private String convertLogMessageToStringArray(List<LogMessage> logMessages) {
        StringBuilder stringBuilder=new StringBuilder("[");
        for(int index=0;index<logMessages.size();index++){
            if(index==logMessages.size()-1) stringBuilder.append(logMessages.get(index).toString());
            else  stringBuilder.append(logMessages.get(index).toString()).append(",");
        }
        stringBuilder.append(']');
        return stringBuilder.toString();
    }



    public void streamKafkaMessage(String logMessage) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        UUID uuid=UUID.randomUUID();
        producer.send(new ProducerRecord<String, String>(kafkaTopic,
                uuid.toString(), logMessage));
        System.out.println("Message sent successfully with batch_id : "+uuid);
        producer.close();
    }

}
