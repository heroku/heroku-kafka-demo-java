package com.heroku.kafka.demo;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class DemoApplicationTest {

    @Test
    public void testOK() throws Exception {
        assertThat(true, equalTo(true));
    }
}