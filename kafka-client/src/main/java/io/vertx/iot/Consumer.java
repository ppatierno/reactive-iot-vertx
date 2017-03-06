package io.vertx.iot;

import io.debezium.kafka.KafkaCluster;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by ppatiern on 04/03/17.
 */
public class Consumer {

  private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

  private static final int ZOOKEEPER_PORT = 2181;
  private static final int KAFKA_PORT = 9092;

  public static void main(String[] args) {

    Vertx vertx = Vertx.vertx();

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

    try {

      KafkaCluster kafkaCluster = new KafkaCluster().withPorts(ZOOKEEPER_PORT, KAFKA_PORT).addBrokers(1).startup();
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          kafkaCluster.shutdown();
        }
      });

    } catch (IOException e) {
      e.printStackTrace();
    }

    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config, String.class, String.class);
    consumer.handler(record -> {
      vertx.eventBus().publish("dashboard", record.value());
    });
    consumer.subscribe("mytopic");
  }
}
