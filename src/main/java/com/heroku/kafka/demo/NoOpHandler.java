package com.heroku.kafka.demo;

import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;


@ServerEndpoint("/noop-ws")
public class NoOpHandler {
  @OnOpen
  public void onOpen(final Session session) {
  }
}
