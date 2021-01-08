package com.github;

import com.github.common.Constants;
import com.github.stream.LogStreamConsumer;
import com.github.stream.LogStreamProducer;

/**
 * This class is main entry point of the application
 * for producing log messages : java -jar DDOSDetector-v-0.1.jar stream
 * for consuming log messages : java -jar DDOSDetector-v-0.1.jar process
 */
public class DDOSDetectorApp {

    public static void main(String[] args) throws Exception {
        try {
            if (args[0].equals(Constants.STREAM)) {
                LogStreamProducer logStreamProducer = new LogStreamProducer();
                logStreamProducer.start();
            } else if (args[0].equals(Constants.PROCESS)) {
                LogStreamConsumer logStreamConsumer = new LogStreamConsumer();
                logStreamConsumer.start();
            } else {
                System.out.println("Invalid argument you can either 'stream' or process");
            }
        } catch (ArrayIndexOutOfBoundsException ae) {
            System.out.println("You must provide argument to (process/stream)");
        }
    }
}
