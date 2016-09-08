package com.heroku.kafka.demo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DemoMessage {
    private final String message;
    private final String topic;
    private final int partition;
    private final long offset;

    @JsonCreator
    public DemoMessage(@JsonProperty("message") String message,
                       @JsonProperty("topic") String topic,
                       @JsonProperty("partition") int partition,
                       @JsonProperty("offset") long offset) {

        this.message = message;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    @JsonProperty("message")
    public String getMessage() {
        return message;
    }

    @JsonProperty("topic")
    public String getTopic() {
        return topic;
    }

    @JsonProperty("partition")
    public int getPartition() {
        return partition;
    }

    @JsonProperty("offset")
    public long getOffset() {
        return offset;
    }
}
