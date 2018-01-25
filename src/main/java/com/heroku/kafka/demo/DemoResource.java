package com.heroku.kafka.demo;

import static java.lang.String.format;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;

@Path("/")
public class DemoResource {
  private final DemoProducer producer;

  private final DemoConsumer consumer;

  private final EventBusManager eventBusManager;

  public DemoResource(DemoProducer producer, DemoConsumer consumer, EventBusManager eventBusManager) {
    this.producer = producer;
    this.consumer = consumer;
    this.eventBusManager = eventBusManager;
  }

  @GET
  @Path("messages")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed
  public List<DemoMessage> getMessages() {
    return Lists.reverse(consumer.getMessages());
  }

  @POST
  @Path("receive")
  @Consumes(MediaType.TEXT_PLAIN)
  @Timed
  public void receiveEvent(String message) {
    DemoMessage demoMessage = new DemoMessage(message, "messages", 0, 0);
    eventBusManager.getEventBus().post(demoMessage);
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
}