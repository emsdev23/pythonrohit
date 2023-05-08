import paho.mqtt.client as mqtt
import time 

def time_publish():
    broker_address = "10.9.39.25"
    broker_port = 1883

    topic = "swadha/50KUPS001/Time/log"

    client = mqtt.Client()

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT broker.")
        else:
            print("Failed to connect to MQTT broker. Error code: " + str(rc))

    def on_publish(client, userdata, mid):
        print("Published message with MID: " + str(mid))

    def on_disconnect(client, userdata, rc):
        print("Disconnected from MQTT broker. Error code: " + str(rc))

    # Set MQTT event callbacks
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    client.username_pw_set(username="swadha", password="dhawas@123")

    client.connect(broker_address, broker_port)

    client.loop_start()

    while not client.is_connected():
        time.sleep(1)

    while True:
        current_time = int(time.time()) # Get current time in epoch format
        result, mid = client.publish(topic, str(current_time))
        if result == mqtt.MQTT_ERR_SUCCESS:
            print("Published current time (epoch format):", current_time)
        else:
            print("Failed to publish message. Error code: " + str(result))
        time.sleep(1800)

    # Disconnect MQTT client
    client.loop_stop()
    client.disconnect()
time_publish()