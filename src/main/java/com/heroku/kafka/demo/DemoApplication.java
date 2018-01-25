package com.heroku.kafka.demo;

import javax.websocket.server.ServerEndpointConfig;

import com.google.common.eventbus.EventBus;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.websockets.WebsocketBundle;
import org.glassfish.jersey.linking.DeclarativeLinkingFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoApplication extends Application<DemoConfiguration>  {
  private static final Logger LOG = LoggerFactory.getLogger(DemoApplication.class);

  public static void main(String[] args) throws Exception {
    new DemoApplication().run(args);
  }

  private WebsocketBundle websocketBundle;

  @Override
  public String getName() {
    return "heroku-kafka-demo";
  }

  @Override
  public void initialize(Bootstrap<DemoConfiguration> bootstrap) {
    websocketBundle = new WebsocketBundle(NoOpHandler.class);
    bootstrap.addBundle(websocketBundle);
    bootstrap.addBundle(new AssetsBundle("/assets/index.html", "/"));
    bootstrap.setConfigurationSourceProvider(
            new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
                    new EnvironmentVariableSubstitutor()
            )
    );
  }

  @Override
  public void run(DemoConfiguration config, Environment env) throws Exception {
    DemoProducer producer = new DemoProducer(config.getKafkaConfig());
    DemoConsumer consumer = new DemoConsumer(config.getKafkaConfig());
    EventBus eventBus = new EventBus();
    EventBusManager eventBusManager = new EventBusManager(eventBus);

    env.lifecycle().manage(producer);
    env.lifecycle().manage(consumer);
    env.lifecycle().manage(eventBusManager);

    final ServerEndpointConfig serverEndpointConfig =
        ServerEndpointConfig.Builder.create(EventResource.class, "/events").build();
    serverEndpointConfig.getUserProperties().put("eventBusManager", eventBusManager);

    env.jersey().register(DeclarativeLinkingFeature.class);
    env.jersey().register(new DemoResource(producer, consumer, eventBusManager));
    websocketBundle.addEndpoint(serverEndpointConfig);
  }
}