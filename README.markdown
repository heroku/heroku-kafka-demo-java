# heroku-kafka-demo-java

A simple heroku app that demonstrates using Kafka in java.
This demo app accepts HTTP POST requests and writes them to a topic, and has a simple page that shows the last 10 messages produced to that topic.

You'll need to [provision](#provisioning) the app.

## Building

```
mvn clean compile
```

## Provisioning

Install the kafka cli plugin:

```
$ heroku plugins:install heroku-kafka
```

Create a heroku app with Kafka attached:

```
$ heroku apps:create your-cool-app-name
$ heroku addons:create heroku-kafka:basic-0
```

Create the sample topic and consumer group. By default, the topic will have 8 partitions:

```
$ heroku kafka:topics:create messages
$ heroku kafka:consumer-groups:create demo-group
```

Deploy to Heroku and open the app:

```
$ git push heroku master
$ heroku open
```
