import paho.mqtt.client as mqtt
import json
from datetime import datetime
import requests
import os

# MQTT Broker settings
BROKER = "broker.hivemq.com"
PORT = 1883
BASE_TOPIC = os.getenv("BASE_TOPIC", "YOUR_UNIQUE_TOPIC/ece140/sensors")  # Use environment variable
TOPIC = BASE_TOPIC + "/#"
WEB_SERVER_URL = "http://localhost:6543/temperature"  # Update with your web server URL

if BASE_TOPIC == "YOUR_UNIQUE_TOPIC/ece140/sensors":
    print("Please enter a unique topic for your server")
    exit()

def on_connect(client, userdata, flags, rc):
    """Callback for when the client connects to the broker."""
    if rc == 0:
        print("Successfully connected to MQTT broker")
        client.subscribe(TOPIC)
        print(f"Subscribed to {TOPIC}")
    else:
        print(f"Failed to connect with result code {rc}")

def on_message(client, userdata, msg):
    """Callback for when a message is received."""
    try:
        # Parse JSON message
        payload = json.loads(msg.payload.decode())
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Check if the topic is the base topic + /readings
        if msg.topic == BASE_TOPIC + "/readings":
            print(f"\n[{current_time}] Received sensor data:")
            print(f"Temperature: {payload['temperature']}°C")
            print(f"Pressure: {payload['pressure']} Pa")
            
            # Send data to web server
            data = {
                "temperature": payload['temperature'],
                "unit": "C",
                "timestamp": current_time
            }
            response = requests.post(WEB_SERVER_URL, json=data)
            if response.status_code == 200:
                print("Data successfully sent to web server")
            else:
                print(f"Failed to send data: {response.status_code}")
    
    except json.JSONDecodeError:
        print(f"\nReceived non-JSON message on {msg.topic}:")
        print(f"Payload: {msg.payload.decode()}")
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    # Create MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        # Connect to broker
        client.connect(BROKER, PORT, 60)
        print("Connected to broker.")

        # Start the MQTT loop
        client.loop_forever()
        
    except KeyboardInterrupt:
        print("\nDisconnecting from broker...")
        client.loop_stop()
        client.disconnect()
        print("Exited successfully.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
