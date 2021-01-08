package com.github.common;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import static com.github.common.Constants.*;

/**
 * This class helps in extracting time and ip from raw logs
 */
public class LogMessage {

    private SimpleDateFormat dateFormat = new SimpleDateFormat(LOG_TIME_FORMAT);

    private String ip;
    private Long time;

    public String getIp() {
        return ip;
    }

    public Long getTime() {
        return time;
    }


    public LogMessage(String log) throws ParseException {
        this.extract_attributes(log);
    }

    private void extract_attributes(String log) throws ParseException {
        String[] log_attributes = log.split(LOG_IP_SPLITTER);
        this.ip = log_attributes[0];
        String[] log_attributes_excluding_ip = log_attributes[1].split(LOG_TIME_SPLITTER);
        Timestamp timestamp = new java.sql.Timestamp(dateFormat.parse(log_attributes_excluding_ip[0]).getTime());
        this.time = timestamp.getTime();
    }

    @Override
    public String toString(){
        return "{ \"ip\" : \""+this.ip+"\", \"time\": \""+this.time+"\"}";
    }
}
