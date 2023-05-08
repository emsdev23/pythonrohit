import json
import mysql.connector
import paho.mqtt.client as mqtt


def on_message(client, userdata, message):
    
    message_payload = json.loads(message.payload.decode("utf-8"))

    ack_value = message_payload.get("FN", None)
    print(ack_value)



client = mqtt.Client()

client.username_pw_set("swadha", "dhawas@123")

client.on_message = on_message

client.connect("10.9.39.25", 1883)

client.subscribe("swadha/50KUPS001/sai/log")

client.loop_forever()

