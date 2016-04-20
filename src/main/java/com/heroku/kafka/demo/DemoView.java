package com.heroku.kafka.demo;

import io.dropwizard.views.View;
import org.glassfish.jersey.linking.InjectLink;

import java.net.URI;

import static org.glassfish.jersey.linking.InjectLink.Style.ABSOLUTE;

public class DemoView extends View {
  @InjectLink(resource = DemoResource.class, method = "addMessage", style = ABSOLUTE)
  private URI addMessageUrl;

  protected DemoView() {
    super("demo.mustache");
  }

  public String getAddMessageUrl() {
    return addMessageUrl.toString();
  }
}
