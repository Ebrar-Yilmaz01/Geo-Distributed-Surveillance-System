import paho.mqtt.client as mqtt
import json

BROKER = "broker.hivemq.com"
PORT = 1883
TOPIC = "farm/data"

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[INFO] Connected to MQTT Broker: {BROKER}")
        client.subscribe(TOPIC)
        print(f"[INFO] Subscribed to topic: {TOPIC}")
    else:
        print(f"[ERROR] Failed to connect. Code: {reason_code}")

def on_message(client, userdata, message):
    try:
        data = json.loads(message.payload.decode())
        print("\n==============================")
        print("[RECEIVED MQTT MESSAGE]")
        for k, v in data.items():
            print(f"{k}: {v}")
        print("==============================")
    except Exception as e:
        print("[ERROR] Could not decode message:", e)

def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"[INFO] Connecting to {BROKER}:{PORT}...")
    client.connect(BROKER, PORT, keepalive=60)

    client.loop_forever()

if __name__ == "__main__":
    main()
