import json
import mysql.connector
import paho.mqtt.client as mqtt


def on_message(client, userdata, message):
    
    message_payload = json.loads(message.payload.decode("utf-8"))

    ack_value = message_payload.get("ACK", None)

    if ack_value is not None:
        
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="22@teneT",
            database="EMS"
        )

        cursor = conn.cursor()

        insert_query = "INSERT INTO acknowlegment (ack) VALUES (%s)"
        insert_values = (ack_value,)
        cursor.execute(insert_query, insert_values)

        conn.commit()

        cursor.close()
        conn.close()

    print("Received message:", str(message.payload.decode("utf-8")))


client = mqtt.Client()

client.username_pw_set("swadha", "dhawas@123")

client.on_message = on_message

client.connect("10.9.39.25", 1883)

client.subscribe("swadha/50KUPS001/ack/log")

client.loop_forever()
