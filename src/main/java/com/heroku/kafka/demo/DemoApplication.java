package com.heroku.kafka.demo;

import com.codahale.metrics.Slf4jReporter;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;
import org.glassfish.jersey.linking.DeclarativeLinkingFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class DemoApplication extends Application<DemoConfiguration>  {
  private static final Logger LOG = LoggerFactory.getLogger(DemoApplication.class);

  public static void main(String[] args) throws Exception {
    new DemoApplication().run(args);
  }

  @Override
  public String getName() {
    return "heroku-kafka-demo";
  }

  @Override
  public void initialize(Bootstrap<DemoConfiguration> bootstrap) {
    bootstrap.addBundle(new ViewBundle<>());
    bootstrap.addBundle(new AssetsBundle());
  }

  @Override
  public void run(DemoConfiguration config, Environment env) throws Exception {
    DemoProducer producer = new DemoProducer(config.getKafkaConfig(), env.metrics());
    DemoConsumer consumer = new DemoConsumer(config.getKafkaConfig(), env.metrics());

    env.lifecycle().manage(producer);
    env.lifecycle().manage(consumer);

    env.jersey().register(DeclarativeLinkingFeature.class);
    env.jersey().register(new DemoResource(producer, consumer));

    final Slf4jReporter reporter = Slf4jReporter.forRegistry(env.metrics())
            .outputTo(LOG)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();

    reporter.start(1, TimeUnit.MINUTES);
  }
}