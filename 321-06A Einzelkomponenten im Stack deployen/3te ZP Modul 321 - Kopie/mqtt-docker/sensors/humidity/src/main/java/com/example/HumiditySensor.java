package com.example;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.Random;
import java.util.UUID;

public class HumiditySensor {
    private static final String BROKER_HOST = System.getenv().getOrDefault("MQTT_BROKER_HOST", "broker");
    private static final int BROKER_PORT = Integer.parseInt(System.getenv().getOrDefault("MQTT_BROKER_PORT", "1883"));
    private static final String TOPIC = System.getenv().getOrDefault("MQTT_TOPIC", "sensors/humidity");
    private static final String SENSOR_ID = System.getenv().getOrDefault("SENSOR_ID", "hum001");
    private static final String CLIENT_ID = "JavaHumSensor-" + UUID.randomUUID().toString();
    
    private static final Random random = new Random();

    public static void main(String[] args) {
        String broker = "tcp://" + BROKER_HOST + ":" + BROKER_PORT;
        MemoryPersistence persistence = new MemoryPersistence();
        Gson gson = new Gson();

        try {
            MqttClient client = new MqttClient(broker, CLIENT_ID, persistence);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setAutomaticReconnect(true);
            options.setConnectionTimeout(10);

            System.out.println("Connecting to MQTT broker: " + broker);
            
            // Retry connection until successful
            boolean connected = false;
            while (!connected) {
                try {
                    client.connect(options);
                    connected = true;
                    System.out.println("Connected to MQTT broker");
                } catch (MqttException me) {
                    System.out.println("Failed to connect, retrying in 5 seconds: " + me.getMessage());
                    Thread.sleep(5000);
                }
            }

            while (true) {
                // Generate random humidity data between 30 and 80 percent
                double humidity = 30.0 + (random.nextDouble() * 50.0);
                humidity = Math.round(humidity * 10.0) / 10.0;  // Round to 1 decimal place

                JsonObject message = new JsonObject();
                message.addProperty("sensor", "humidity");
                message.addProperty("sensor_id", SENSOR_ID);
                message.addProperty("value", humidity);
                message.addProperty("unit", "%");
                message.addProperty("timestamp", System.currentTimeMillis() / 1000.0);

                String content = gson.toJson(message);
                MqttMessage mqttMessage = new MqttMessage(content.getBytes());
                mqttMessage.setQos(1);

                System.out.println("Publishing humidity: " + humidity + "%");
                client.publish(TOPIC, mqttMessage);
                
                Thread.sleep(10000);  // Publish a reading every 10 seconds
            }
        } catch (MqttException | InterruptedException e) {
            System.out.println("Error in humidity sensor: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
