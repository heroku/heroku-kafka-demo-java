package com.heroku.kafka.demo;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.joda.time.DateTime;

import java.io.IOException;

@JsonSerialize(using = DemoMessage.Serializer.class)
public class DemoMessage {
  private final int partition;

  private final long offset;

  private final String value;

  private final DateTime receivedAt;

  public DemoMessage(int partition, long offset, String value, DateTime receivedAt) {
    this.partition = partition;
    this.offset = offset;
    this.value = value;
    this.receivedAt = receivedAt;
  }

  public static class Serializer extends JsonSerializer<DemoMessage> {
    @Override
    public void serialize(DemoMessage message, JsonGenerator gen, SerializerProvider provider) throws IOException {
      gen.writeStartObject();
      gen.writeNumberField("partition", message.partition);
      gen.writeNumberField("offset", message.offset);
      gen.writeStringField("value", message.value);

      gen.writeObjectFieldStart("metadata");
      gen.writeStringField("received_at", message.receivedAt.toString());
      gen.writeEndObject();

      gen.writeEndObject();
    }
  }
}
