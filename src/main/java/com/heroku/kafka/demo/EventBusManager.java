package com.heroku.kafka.demo;

import com.google.common.eventbus.EventBus;
import io.dropwizard.lifecycle.Managed;


public class EventBusManager implements Managed {

  private final EventBus eventBus;

  public EventBusManager(EventBus eventBus) {
    this.eventBus = eventBus;
  }

  @Override
  public void start() throws Exception {

  }

  @Override
  public void stop() throws Exception {

  }

  public EventBus getEventBus() {
    return eventBus;
  }
}
