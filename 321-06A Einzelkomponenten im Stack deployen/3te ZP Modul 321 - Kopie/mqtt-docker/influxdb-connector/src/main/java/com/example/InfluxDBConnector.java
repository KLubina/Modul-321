package com.example;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class InfluxDBConnector {
    // MQTT settings
    private static final String MQTT_BROKER_HOST = System.getenv().getOrDefault("MQTT_BROKER_HOST", "broker");
    private static final int MQTT_BROKER_PORT = Integer.parseInt(System.getenv().getOrDefault("MQTT_BROKER_PORT", "1883"));
    private static final String CLIENT_ID = "JavaInfluxConnector-" + UUID.randomUUID().toString();
    private static final String[] MQTT_TOPICS = {"sensors/#"};  // Subscribe to all sensor topics

    // InfluxDB settings
    private static final String INFLUXDB_URL = "http://" + System.getenv().getOrDefault("INFLUXDB_HOST", "influxdb") 
            + ":" + System.getenv().getOrDefault("INFLUXDB_PORT", "8086");
    private static final String INFLUXDB_TOKEN = System.getenv().getOrDefault("INFLUXDB_TOKEN", "");
    private static final String INFLUXDB_ORG = System.getenv().getOrDefault("INFLUXDB_ORG", "");
    private static final String INFLUXDB_BUCKET = System.getenv().getOrDefault("INFLUXDB_BUCKET", "mqtt");
    private static final String INFLUXDB_USER = System.getenv().getOrDefault("INFLUXDB_USER", "admin");
    private static final String INFLUXDB_PASSWORD = System.getenv().getOrDefault("INFLUXDB_PASSWORD", "adminpassword");
    private static final String INFLUXDB_DATABASE = System.getenv().getOrDefault("INFLUXDB_DATABASE", "mqtt");

    private static InfluxDBClient influxDBClient;
    private static Gson gson = new Gson();

    public static void main(String[] args) {
        try {
            // Initialize InfluxDB connection
            // For InfluxDB 1.8 compatibility
            String url = INFLUXDB_URL;
            influxDBClient = InfluxDBClientFactory.createV1(
                url, 
                INFLUXDB_USER, 
                INFLUXDB_PASSWORD.toCharArray(), 
                INFLUXDB_DATABASE, 
                null
            );

            System.out.println("Connected to InfluxDB at " + url);

            // Connect to MQTT broker
            String broker = "tcp://" + MQTT_BROKER_HOST + ":" + MQTT_BROKER_PORT;
            MemoryPersistence persistence = new MemoryPersistence();
            MqttClient mqttClient = new MqttClient(broker, CLIENT_ID, persistence);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setAutomaticReconnect(true);
            options.setConnectionTimeout(10);

            System.out.println("Connecting to MQTT broker: " + broker);
            
            // Set up callbacks
            mqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Connection to MQTT broker lost: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    String payload = new String(message.getPayload());
                    System.out.println("Received message on topic " + topic + ": " + payload);
                    
                    try {
                        // Parse the MQTT message
                        JsonObject jsonMessage = gson.fromJson(payload, JsonObject.class);
                        
                        // Extract the sensor type from the topic
                        String[] topicParts = topic.split("/");
                        String measurement = topicParts.length > 1 ? topicParts[1] : "unknown";
                        
                        // Extract data from JSON
                        String sensor = jsonMessage.has("sensor") ? jsonMessage.get("sensor").getAsString() : "unknown";
                        String sensorId = jsonMessage.has("sensor_id") ? jsonMessage.get("sensor_id").getAsString() : "unknown";
                        double value = jsonMessage.has("value") ? jsonMessage.get("value").getAsDouble() : 0.0;
                        String unit = jsonMessage.has("unit") ? jsonMessage.get("unit").getAsString() : "";
                        long timestamp = jsonMessage.has("timestamp") 
                                ? (long)(jsonMessage.get("timestamp").getAsDouble() * 1000) 
                                : System.currentTimeMillis();
                        
                        // Create a point for InfluxDB
                        Point point = Point.measurement(measurement)
                                .addTag("sensor", sensor)
                                .addTag("sensor_id", sensorId)
                                .addField("value", value)
                                .addField("unit", unit)
                                .time(timestamp, WritePrecision.MS);
                        
                        // Write to InfluxDB
                        try (WriteApi writeApi = influxDBClient.getWriteApi()) {
                            writeApi.writePoint(point);
                            System.out.println("Wrote point to InfluxDB: " + point.toLineProtocol());
                        }
                    } catch (JsonSyntaxException e) {
                        System.out.println("Error parsing JSON: " + e.getMessage());
                    } catch (Exception e) {
                        System.out.println("Error processing message: " + e.getMessage());
                        e.printStackTrace();
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // Not used for subscriber
                }
            });
            
            // Retry connection until successful
            boolean connected = false;
            while (!connected) {
                try {
                    mqttClient.connect(options);
                    connected = true;
                    System.out.println("Connected to MQTT broker");
                } catch (MqttException me) {
                    System.out.println("Failed to connect, retrying in 5 seconds: " + me.getMessage());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            
            // Subscribe to topics
            for (String topic : MQTT_TOPICS) {
                mqttClient.subscribe(topic, 1);
                System.out.println("Subscribed to topic: " + topic);
            }
            
            // Keep the application running
            while (true) {
                Thread.sleep(1000);
            }
            
        } catch (MqttException | InterruptedException e) {
            System.out.println("Error in connector: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (influxDBClient != null) {
                influxDBClient.close();
            }
        }
    }
}
