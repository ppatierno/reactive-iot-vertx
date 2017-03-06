package io.vertx.iot;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Created by ppatiern on 04/03/17.
 */
public class Server {

  private static final Logger LOG = LoggerFactory.getLogger(Server.class);

  private static Vertx vertx;

  public static void main(String[] args) {

    vertx = Vertx.vertx();

    Router router = Router.router(vertx);

    BridgeOptions options = new BridgeOptions();
    options.setOutboundPermitted(Collections.singletonList(new PermittedOptions().setAddress("dashboard")));
    router.route("/eventbus/*").handler(SockJSHandler.create(vertx).bridge(options));
    router.route().handler(StaticHandler.create().setCachingEnabled(false));

    HttpServer httpServer = vertx.createHttpServer();
    httpServer
      .requestHandler(router::accept)
      .listen(8080, done -> {

        if (done.succeeded()) {
          LOG.info("HTTP server started on port {}", done.result().actualPort());
        } else {
          LOG.error("HTTP server not started", done.cause());
        }
    });

    MqttServer mqttServer = MqttServer.create(vertx);
    mqttServer
      .endpointHandler(Server::endpointHandler)
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

      vertx.eventBus().publish("dashboard", String.valueOf(message.payload()));

      if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) {
        endpoint.publishAcknowledge(message.messageId());
      }

    });

    endpoint.accept(false);
  }

}
