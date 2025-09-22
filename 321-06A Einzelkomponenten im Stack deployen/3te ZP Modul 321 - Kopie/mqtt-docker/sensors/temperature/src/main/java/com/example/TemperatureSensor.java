package com.example;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.util.Random;
import java.util.UUID;

public class TemperatureSensor {
    private static final String BROKER_HOST = System.getenv().getOrDefault("MQTT_BROKER_HOST", "broker");
    private static final int BROKER_PORT = Integer.parseInt(System.getenv().getOrDefault("MQTT_BROKER_PORT", "1883"));
    private static final String TOPIC = System.getenv().getOrDefault("MQTT_TOPIC", "sensors/temperature");
    private static final String SENSOR_ID = System.getenv().getOrDefault("SENSOR_ID", "temp001");
    private static final String CLIENT_ID = "JavaTempSensor-" + UUID.randomUUID().toString();
    
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
                // Generate random temperature data between 15 and 30 degrees
                double temperature = 15.0 + (random.nextDouble() * 15.0);
                temperature = Math.round(temperature * 10.0) / 10.0;  // Round to 1 decimal place

                JsonObject message = new JsonObject();
                message.addProperty("sensor", "temperature");
                message.addProperty("sensor_id", SENSOR_ID);
                message.addProperty("value", temperature);
                message.addProperty("unit", "°C");
                message.addProperty("timestamp", System.currentTimeMillis() / 1000.0);

                String content = gson.toJson(message);
                MqttMessage mqttMessage = new MqttMessage(content.getBytes());
                mqttMessage.setQos(1);

                System.out.println("Publishing temperature: " + temperature + "°C");
                client.publish(TOPIC, mqttMessage);
                
                Thread.sleep(10000);  // Publish a reading every 10 seconds
            }
        } catch (MqttException | InterruptedException e) {
            System.out.println("Error in temperature sensor: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
