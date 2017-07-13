package com.heroku.kafka.demo;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

@Path("/")
public class DemoResource {
  private static final Logger LOG = LoggerFactory.getLogger(DemoResource.class);

  private final DemoProducer producer;

  private final DemoConsumer consumer;

  public DemoResource(DemoProducer producer, DemoConsumer consumer) {
    this.producer = producer;
    this.consumer = consumer;
  }

  @GET
  @Path("messages")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed
  public List<DemoMessage> getMessages() {
    return Lists.reverse(consumer.getMessages());
  }

  @POST
  @Path("messages")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Timed
  public String addMessage(DemoMessage message) throws TimeoutException, ExecutionException {
    Uninterruptibles.getUninterruptibly(producer.send(message.getMessage()), 20, TimeUnit.SECONDS);
    return format("received message: %s", message);
  }

  @GET
  @Path("cert-check")
  @Produces(MediaType.APPLICATION_JSON)
  public CertCheck certCheck() throws InterruptedException, ExecutionException, TimeoutException {
    Map<Integer, Integer> partitions = Maps.newHashMap();

    String topicName = "messages";

    consumer.listTopics().get().forEach((t, partitionInfos) -> {
      if (topicName.equals(t)) {
        for (PartitionInfo partitionInfo : partitionInfos) {
          partitions.put(partitionInfo.leader().id(), partitionInfo.partition());
        }
      }
    });

    Map<Integer, Long> expectedOffsets = Maps.newHashMap();
    List<Integer> errors = Lists.newArrayList();

    for (Integer partition : partitions.values()) {
      try {
        RecordMetadata metadata = producer.send(format("hello %d", partition), partition).get(1, TimeUnit.SECONDS);
        expectedOffsets.put(partition, metadata.offset());
      }
      catch (TimeoutException e) {
        errors.add(partition);
      }

    }

    LOG.debug("expected offsets; {}", expectedOffsets);

    Map<Integer, Long> actualOffsets = Maps.newHashMap();

    while (!actualOffsets.keySet().containsAll(expectedOffsets.keySet())) {
      for (DemoMessage message : consumer.getMessages()) {
        LOG.debug("received message; {}", message);

        if (expectedOffsets.containsKey(message.getPartition())) {
          actualOffsets.put(message.getPartition(), message.getOffset());
        }
      }

      LOG.debug("actual offsets; {}", actualOffsets);
    }

    return new CertCheck(partitions, errors, expectedOffsets, actualOffsets);
  }

  @JsonSerialize
  public static class CertCheck {
    @JsonProperty
    private final Map<Integer, Integer> partitions;

    @JsonProperty
    private final List<Integer> errors;

    @JsonProperty
    private final Map<Integer, Long> expectedOffsets;

    @JsonProperty
    private final Map<Integer, Long> actualOffsets;

    public CertCheck(Map<Integer, Integer> partitions, List<Integer> errors, Map<Integer, Long> expectedOffsets, Map<Integer, Long> actualOffsets) {
      this.partitions = partitions;
      this.errors = errors;
      this.expectedOffsets = expectedOffsets;
      this.actualOffsets = actualOffsets;
    }
  }
}