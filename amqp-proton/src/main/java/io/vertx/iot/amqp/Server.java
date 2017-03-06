/*
* Copyright 2017 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package io.vertx.iot.amqp;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonServer;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Created by ppatiern on 04/03/17.
 */
public class Server {

  private static final Logger LOG = LoggerFactory.getLogger(Server.class);

  public static final String AMQP_SERVER_HOST = "localhost";
  public static final int AMQP_SERVER_PORT = 5672;

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

    ProtonServer amqpServer = ProtonServer.create(vertx);
    amqpServer
      .connectHandler(connection -> {

        connection
          .sessionOpenHandler(session -> session.open())
          .receiverOpenHandler(Server::receiverOpenHandler)
          .open();
      })
      .listen(AMQP_SERVER_PORT, AMQP_SERVER_HOST, done -> {

        if (done.succeeded()) {
          LOG.info("AMQP server started on port {}", done.result().actualPort());
        } else {
          LOG.error("AMQP server not started", done.cause());
        }
      });
  }

  private static void receiverOpenHandler(ProtonReceiver receiver) {

    receiver
      .handler((delivery, message) -> {

        Section section = message.getBody();
        if (section instanceof AmqpValue) {

          String body = ((AmqpValue) message.getBody()).getValue().toString();
          LOG.info("Message [{}] received with payload={}", message.getMessageId(), body);

          delivery.disposition(Accepted.getInstance(), true);

          vertx.eventBus().publish("dashboard", body);
        }

      })
      .open();
  }
}
