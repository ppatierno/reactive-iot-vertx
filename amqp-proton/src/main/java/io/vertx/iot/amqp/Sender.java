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
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonSession;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ppatiern on 06/03/17.
 */
public class Sender {

  private static final Logger LOG = LoggerFactory.getLogger(Sender.class);

  private static final String AMQP_ADDRESS = "myaddress";
  private static final String AMQP_MESSAGE = "Message from sender ";

  private static int counter = 0;

  public static void main(String[] args) {

    Vertx vertx = Vertx.vertx();

    ProtonClient client = ProtonClient.create(vertx);

    client.connect(Server.AMQP_SERVER_HOST, Server.AMQP_SERVER_PORT, done -> {

      ProtonConnection connection = done.result();
      connection.open();

      ProtonSession session = connection.createSession();
      session.open();

      ProtonSender sender = connection.createSender(AMQP_ADDRESS);
      sender.open();

      vertx.setPeriodic(1000, t -> {

        Message message = Message.Factory.create();
        message.setMessageId(counter);
        message.setBody(new AmqpValue(AMQP_MESSAGE + counter++));

        sender.send(message, delivery -> {
            LOG.info("Message delivered with state={}", delivery.getRemoteState());
        });

      });

    });

    try {
      System.in.read();
      vertx.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
