package com.heroku.kafka.demo;

import com.codahale.metrics.annotation.Timed;
import com.google.common.util.concurrent.Uninterruptibles;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

@Path("/")
public class DemoResource {
  private final DemoProducer producer;

  private final DemoConsumer consumer;

  public DemoResource(DemoProducer producer, DemoConsumer consumer) {
    this.producer = producer;
    this.consumer = consumer;
  }

  @GET
  @Path("/")
  @Produces(MediaType.TEXT_HTML)
  @Timed
  public DemoView showMessages(@Context UriInfo uriInfo) {
    String addMessageUrl = uriInfo.getBaseUriBuilder().path(DemoResource.class, "addMessage").build().toString();
    return new DemoView(addMessageUrl);
  }

  @GET
  @Path("/messages")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed
  public List<DemoMessage> getMessages() {
    return consumer.getMessages();
  }

  @POST
  @Path("/messages")
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.TEXT_PLAIN)
  @Timed
  public String addMessage(String message) throws TimeoutException, ExecutionException {
    Uninterruptibles.getUninterruptibly(producer.send(message), 20, TimeUnit.SECONDS);
    return format("received message: %s", message);
  }
}