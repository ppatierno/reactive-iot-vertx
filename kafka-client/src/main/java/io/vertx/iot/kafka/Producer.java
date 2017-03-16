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

package io.vertx.iot.kafka;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by ppatiern on 06/03/17.
 */
public class Producer {

  private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

  private static final String KAFKA_TOPIC = "mytopic";
  private static final String KAFKA_MESSAGE = "Message from producer ";

  private static int counter = 0;

  public static void main(String[] args) {

    Vertx vertx = Vertx.vertx();

    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("%s:%d", Broker.KAFKA_HOST, Broker.KAFKA_PORT));
    config.put(ProducerConfig.ACKS_CONFIG, "1");

    KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config, String.class, String.class);

    vertx.setPeriodic(1000, t -> {
      producer.write(KafkaProducerRecord.create(KAFKA_TOPIC, (KAFKA_MESSAGE + counter++)), done -> {

        if (done.succeeded()) {

          RecordMetadata recordMetadata = done.result();
          LOG.info("Message written on topic={}, partition={}, offset={}",
            recordMetadata.getTopic(), recordMetadata.getPartition(), recordMetadata.getOffset());

        } else {
          done.cause().printStackTrace();
        }

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
