package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by shash on 4/21/2018.
 */

public class Message implements Serializable {
    private String key;
    private String value;
    private MessageStatus messageStatus;
    private String version;



    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public MessageStatus getMessageStatus() {
        return messageStatus;
    }

    public void setMessageStatus(MessageStatus messageStatus) {
        this.messageStatus = messageStatus;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "Message{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", messageStatus=" + messageStatus +
                ", version=" + version +
                '}';
    }
}
