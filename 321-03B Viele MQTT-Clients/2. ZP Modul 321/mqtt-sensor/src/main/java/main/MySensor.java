package main;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MySensor {
    // MQTT Client-Einstellungen
    private String broker;
    private String clientId;
    private MqttClient mqttClient;
    
    // Topics
    private String pubTopic;
    private String subTopic;
    
    // QoS-Level (0, 1 oder 2)
    private final int qos = 0;
    
    // Random für Sinuswellen mit Phasenverschiebung
    private final Random random = new Random();
    private final double phaseShift = random.nextDouble() * 2 * Math.PI;
    private int sensorNumber = 1; // Standard-Sensornummer
    
    // Scheduler für regelmäßige Sendungen
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    /**
     * Konstruktor
     * @param pubTopic Topic zum Publizieren
     * @param subTopic Topic zum Abonnieren
     * @param sensorNumber Sensornummer für Identifikation und Farbunterschiede
     */
    public MySensor(String pubTopic, String subTopic, int sensorNumber) {
        // Hole MQTT_BROKER aus Umgebungsvariable oder verwende Standard
        this.broker = System.getenv("MQTT_BROKER");
        if (this.broker == null || this.broker.isEmpty()) {
            this.broker = "tcp://localhost:1883";
        }
        
        this.pubTopic = pubTopic;
        this.subTopic = subTopic;
        this.sensorNumber = sensorNumber;
        
        // Zufällige Client-ID generieren
        this.clientId = "JavaSensor-" + UUID.randomUUID();
        
        System.out.println("Starte Sensor " + sensorNumber);
        System.out.println("Broker: " + broker);
        System.out.println("Publish Topic: " + pubTopic);
        System.out.println("Subscribe Topic: " + subTopic);
    }

    /**
     * Verbindet den MQTT-Client und startet Listener
     */
    public void connect() {
        try {
            // Erstelle MQTT-Client
            mqttClient = new MqttClient(broker, clientId, new MemoryPersistence());
            
            // Verbindungsoptionen setzen
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            
            // Callback für Nachrichten-Empfang
            mqttClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Verbindung verloren! Versuche Wiederverbindung...");
                    try {
                        Thread.sleep(5000);
                        connect(); // Wiederverbindung versuchen
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    System.out.println("Nachricht empfangen: " + new String(message.getPayload()) + " von Topic: " + topic);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // Optional: Wird aufgerufen, wenn eine Nachricht erfolgreich gesendet wurde
                }
            });
            
            // Verbinden
            System.out.println("Verbinde mit Broker: " + broker);
            mqttClient.connect(connOpts);
            
            // Topic abonnieren, falls angegeben
            if (subTopic != null && !subTopic.isEmpty()) {
                mqttClient.subscribe(subTopic, qos);
                System.out.println("Abonniert: " + subTopic);
            }
            
            // Starte den Sende-Scheduler
            startPublishing();
            
        } catch (MqttException me) {
            System.out.println("MQTT-Fehler:");
            me.printStackTrace();
        }
    }

    /**
     * Startet regelmäßiges Senden von Sensorwerten
     */
    private void startPublishing() {
        // Sende alle 2 Sekunden eine Nachricht
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (pubTopic != null && !pubTopic.isEmpty() && mqttClient.isConnected()) {
                    // Generiere Sinuswelle mit Phasenverschiebung
                    double time = System.currentTimeMillis() / 1000.0;
                    double value = 10 + 10 * Math.sin(time + phaseShift);
                    
                    // JSON-Format für Grafana
                    String message = String.format(
                        "{\"value\":%.2f,\"sensor\":%d,\"timestamp\":%d}",
                        value, sensorNumber, System.currentTimeMillis()
                    );
                    
                    // Nachricht veröffentlichen
                    mqttClient.publish(pubTopic, message.getBytes(), qos, false);
                    System.out.println("Veröffentlicht: " + message + " zu Topic: " + pubTopic);
                }
            } catch (MqttException e) {
                System.out.println("Fehler beim Senden: " + e.getMessage());
            }
        }, 0, 2, TimeUnit.SECONDS);
    }

    /**
     * Ressourcen freigeben
     */
    public void disconnect() {
        try {
            scheduler.shutdown();
            if (mqttClient.isConnected()) {
                mqttClient.disconnect();
            }
            mqttClient.close();
        } catch (MqttException e) {
            System.out.println("Fehler beim Trennen: " + e.getMessage());
        }
    }

    /**
     * Hauptmethode
     */
    public static void main(String[] args) {
        String pubTopic = "sensor/data";  // Standard-Topic
        String subTopic = "sensor/control";  // Standard-Topic
        int sensorNumber = 1;  // Standard-Sensornummer
        
        // Kommandozeilenparameter verarbeiten
        for (String arg : args) {
            if (arg.startsWith("--pub=")) {
                pubTopic = "sensor/" + arg.substring(6);
                // Versuche auch die Sensornummer daraus zu extrahieren
                try {
                    sensorNumber = Integer.parseInt(arg.substring(6));
                } catch (NumberFormatException e) {
                    // Ignorieren, wenn keine Zahl
                }
            } else if (arg.startsWith("--sub=")) {
                subTopic = "sensor/" + arg.substring(6);
            }
        }
        
        // Erstelle und starte Sensor
        MySensor sensor = new MySensor(pubTopic, subTopic, sensorNumber);
        sensor.connect();
        
        // Shutdown-Hook zum sauberen Beenden
        Runtime.getRuntime().addShutdownHook(new Thread(sensor::disconnect));
    }
}
