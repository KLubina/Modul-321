package com.example;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.UUID;

public class Publisher {
    private static final String BROKER_HOST = System.getenv().getOrDefault("MQTT_BROKER_HOST", "broker");
    private static final int BROKER_PORT = Integer.parseInt(System.getenv().getOrDefault("MQTT_BROKER_PORT", "1883"));
    private static final String TOPIC = System.getenv().getOrDefault("MQTT_TOPIC", "test/message");
    private static final String CLIENT_ID = "JavaPublisher-" + UUID.randomUUID().toString();

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

            int messageCount = 0;
            while (true) {
                JsonObject message = new JsonObject();
                message.addProperty("message", "Test message " + messageCount);
                message.addProperty("timestamp", System.currentTimeMillis() / 1000.0);

                String content = gson.toJson(message);
                MqttMessage mqttMessage = new MqttMessage(content.getBytes());
                mqttMessage.setQos(1);

                System.out.println("Publishing message: " + content);
                client.publish(TOPIC, mqttMessage);
                
                messageCount++;
                Thread.sleep(5000);  // Publish a message every 5 seconds
            }
        } catch (MqttException | InterruptedException e) {
            System.out.println("Error in publisher: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
