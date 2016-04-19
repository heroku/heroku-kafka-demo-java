package com.heroku.kafka.demo;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.lang.System.getenv;

public class KafkaConfig {
  public Properties getProperties() {
    return buildDefaults();
  }

  private Properties buildDefaults() {
    Properties properties = new Properties();
    List<String> hostPorts = Lists.newArrayList();

    for (String url : Splitter.on(",").split(checkNotNull(getenv("KAFKA_URL")))) {
      try {
        URI uri = new URI(url);
        hostPorts.add(format("%s:%d", uri.getHost(), uri.getPort()));

        switch (uri.getScheme()) {
          case "kafka":
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
            break;
          case "kafka+ssl":
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ".truststore.jks");
            properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, getTrustStorePassword());
            properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ".keystore.jks");
            properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, getKeyStorePassword());
            break;
          default:
            throw new IllegalArgumentException(format("unknown scheme; %s", uri.getScheme()));
        }
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Joiner.on(",").join(hostPorts));
    return properties;
  }

  private static String getTrustStorePassword() {
    return checkNotNull(getenv("TRUSTSTORE_PASSWORD"));
  }

  private static String getKeyStorePassword() {
    return checkNotNull(getenv("KEYSTORE_PASSWORD"));
  }

  public String getTopic() {
    return checkNotNull(getenv("KAFKA_TOPIC"));
  }
}