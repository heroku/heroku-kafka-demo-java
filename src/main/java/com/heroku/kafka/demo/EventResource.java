package com.heroku.kafka.demo;

import javax.websocket.CloseReason;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.OnClose;
import javax.websocket.Session;


public class EventResource extends Endpoint implements MessageHandler.Whole<String> {
  private Session session;
  private EventBusManager eventBusManager;

  @Override
  public void onOpen(Session session, EndpointConfig endpointConfig) {
    session.addMessageHandler(this);
    this.session = session;
    eventBusManager = (EventBusManager)endpointConfig.getUserProperties().get("eventBusManager");
    eventBusManager.getEventBus().register(new EventBusListener(this.session));
  }

  @OnClose
  public void onClose(final Session session, CloseReason closeReason) {
  }

  @Override
  public void onMessage(String demoMessage) {
  }
}
