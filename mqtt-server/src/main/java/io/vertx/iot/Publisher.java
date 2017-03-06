package io.vertx.iot;

import io.vertx.core.Vertx;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.IOException;

/**
 * Created by ppatiern on 06/03/17.
 */
public class Publisher {

  private static final String MQTT_SERVER_HOST = "localhost";
  private static final int MQTT_SERVER_PORT = 1883;
  private static final String MQTT_TOPIC = "/mytopic";
  private static final String MQTT_MESSAGE = "Message from Paho ";

  private static int counter = 0;

  public static void main(String[] args) {

    Vertx vertx = Vertx.vertx();

    try {

      MemoryPersistence persistence = new MemoryPersistence();
      MqttClient client = new MqttClient(String.format("tcp://%s:%d", MQTT_SERVER_HOST, MQTT_SERVER_PORT), "12345", persistence);

      client.connect();

      vertx.setPeriodic(1000, t -> {

        try {
          client.publish(MQTT_TOPIC, (MQTT_MESSAGE + counter++).getBytes(), 1, false);
        } catch (MqttException e) {
          e.printStackTrace();
        }

      });

      System.in.read();
      client.disconnect();

      vertx.close();

    } catch (IOException e) {
      e.printStackTrace();
    } catch (MqttException e) {
      e.printStackTrace();
    }
  }
}
