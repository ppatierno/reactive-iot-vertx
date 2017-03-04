package io.vertx.iot;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Vertx;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ppatiern on 04/03/17.
 */
public class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {

    Vertx vertx = Vertx.vertx();

    MqttServer server = MqttServer.create(vertx);
    server
      .endpointHandler(Main::endpointHandler)
      .listen(done -> {

        if (done.succeeded()) {
          LOG.info("MQTT server started on port {}", done.result().actualPort());
        } else {
          LOG.error("MQTT server not started", done.cause());
        }

      });
  }

  private static void endpointHandler(MqttEndpoint endpoint) {

    LOG.info("MQTT client [{}] connected", endpoint.clientIdentifier());

    endpoint.publishHandler(message -> {

      LOG.info("Message [{}] received with topic={}, qos={}, payload={}",
        message.messageId(), message.topicName(), message.qosLevel(), message.payload());

      if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) {
        endpoint.publishAcknowledge(message.messageId());
      }

    });

    endpoint.accept(false);
  }

}
