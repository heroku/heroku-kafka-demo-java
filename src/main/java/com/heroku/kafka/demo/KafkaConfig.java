package com.heroku.kafka.demo;

import com.github.jkutner.EnvKeyStore;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.lang.System.getenv;

public class KafkaConfig {

  @NotEmpty
  private String topic;

  @NotEmpty
  private String consumerGroup;

  @NotEmpty
  private String urlPrefix;

  @NotEmpty
  private String clientCertPrefix;

  @NotEmpty
  private String trustedCertPrefix;

  public Properties getProperties() {
    return buildDefaults();
  }

  public Properties buildDefaults() {
    Properties properties = new Properties();
    List<String> hostPorts = Lists.newArrayList();
    String scheme = "";

    for (String url : Splitter.on(",").split(checkNotNull(getenv(configVar(urlPrefix, "URL"))))) {
      try {
        URI uri = new URI(url);
        hostPorts.add(format("%s:%d", uri.getHost(), uri.getPort()));
        scheme = uri.getScheme();

      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    switch (scheme) {
      case "kafka":
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        break;
      case "kafka+ssl":
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");

        try {
          EnvKeyStore envTrustStore = EnvKeyStore.createWithRandomPassword(configVar(trustedCertPrefix, "TRUSTED_CERT"));
          EnvKeyStore envKeyStore = EnvKeyStore.createWithRandomPassword(configVar(clientCertPrefix, "CLIENT_CERT_KEY"), configVar(clientCertPrefix,"CLIENT_CERT"));

          File trustStore = envTrustStore.storeTemp();
          File keyStore = envKeyStore.storeTemp();

          properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, envTrustStore.type());
          properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStore.getAbsolutePath());
          properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, envTrustStore.password());
          properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, envKeyStore.type());
          properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStore.getAbsolutePath());
          properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, envKeyStore.password());
        } catch (Exception e) {
          throw new RuntimeException("There was a problem creating the Kafka key stores", e);
        }
        break;
      default:
        throw new IllegalArgumentException(format("unknown scheme; %s", scheme));
    }


    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Joiner.on(",").join(hostPorts));
    return properties;
  }

  private static String configVar(String prefix, String suffix) {
    return format("%s_%s", prefix, suffix);
  }

  public String getTopic() {
    return topic;
  }

  public String getConsumerGroup() {
    return consumerGroup;
  }

  public String getUrlPrefix() {
    return urlPrefix;
  }

  public String getClientCertPrefix() {
    return clientCertPrefix;
  }

  public String getTrustedCertPrefix() {
    return trustedCertPrefix;
  }
}
