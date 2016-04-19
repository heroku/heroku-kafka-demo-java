package com.heroku.kafka.demo;

import io.dropwizard.views.View;

public class DemoView extends View {
  private final String addMessageUrl;

  protected DemoView(String addMessageUrl) {
    super("demo.mustache");
    this.addMessageUrl = addMessageUrl;
  }

  public String getAddMessageUrl() {
    return addMessageUrl;
  }
}
