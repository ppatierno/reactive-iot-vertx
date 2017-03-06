package io.vertx.iot;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
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
  }
}
