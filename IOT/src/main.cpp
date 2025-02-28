#include "ECE140_WIFI.h"
#include "ECE140_MQTT.h"
#include <Wire.h>
#include <Adafruit_BMP085.h>  // Include the Adafruit BMP085 library
#include <Adafruit_Sensor.h>

// MQTT client - using descriptive client ID and topic
ECE140_MQTT mqtt(CLIENT_ID, TOPIC_PREFIX);
Adafruit_BMP085 bmp;  // Create an instance of the BMP sensor

// WiFi credentials
const char* ucsdUsername = UCSD_USERNAME;
const char* ucsdPassword = UCSD_PASSWORD;
const char* wifiSsid = WIFI_SSID;
const char* nonEnterpriseWifiPassword = NON_ENTERPRISE_WIFI_PASSWORD;

ECE140_WIFI wifi;

void setup() {
    Serial.begin(115200);

    // Connect to Wi-Fi
    wifi.connectToWPAEnterprise(wifiSsid, ucsdUsername, ucsdPassword);
    // If not using UCSD Wi-Fi, use:
    // wifi.connectToWiFi(wifiSsid, nonEnterpriseWifiPassword);

    // Initialize the BMP sensor
    if (!bmp.begin()) {
        Serial.println("[Main] BMP sensor initialization failed!");
        // while (1);
    }
    Serial.println("[Main] BMP sensor initialized.");

    // Connect to MQTT
    Serial.println("[Main] Connecting to MQTT...");
    while (!mqtt.connectToBroker(1883)) {
        Serial.println("[Main] Failed to connect to MQTT broker. Retrying...");
        delay(1000);
    }       
    Serial.println("[Main] Connected to MQTT broker.");
}

void loop() {
    mqtt.loop();  // Maintain MQTT connection

    // Read sensor data
    float temperature = bmp.readTemperature();
    int pressure = bmp.readPressure();

    // Create JSON payload
    String payload = "{\"temperature\":" + String(temperature) + ", \"pressure\":" + String(pressure) + "}";
    Serial.println("[Main] Publishing sensor data:");
    Serial.println(payload);

    // Publish sensor data to MQTT
    if (!mqtt.publishMessage("readings", payload)) {
        Serial.println("[Main] Failed to publish sensor data.");
    }

    delay(5000);  // Publish every 5 seconds
}
