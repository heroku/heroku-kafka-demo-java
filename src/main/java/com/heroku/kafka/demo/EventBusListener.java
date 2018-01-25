package com.heroku.kafka.demo;

import javax.websocket.Session;

import com.google.common.eventbus.Subscribe;


public class EventBusListener {

  private final Session session;

  public EventBusListener(Session session) {
    this.session = session;
  }

  @Subscribe
  public void onEvent(DemoMessage demoMessage) {
    session.getAsyncRemote().sendObject(demoMessage);
  }
}
