import json
import mysql.connector
import paho.mqtt.client as mqtt
import time
import datetime
from mysql.connector import pooling

# MQTT broker information
broker_address = "10.9.39.25"
broker_port = 1883
mqtt_username = "swadha"
mqtt_password = "dhawas@123"

# Define MQTT topics to publish to
mqtt_topic_publish = "swadha/50KUPS001/shedulded/log"

# Create a connection pool for the database
pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="source_pool",
    pool_size=5,
    host="localhost",
    user="root",
    password="22@teneT",
    database="EMS"
)

# Initialize MQTT client
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(username=mqtt_username, password=mqtt_password)
mqtt_client.connect(broker_address, broker_port)

# Start the MQTT client background thread
mqtt_client.loop_start()

# Loop to periodically fetch and publish new data
while True:
    # Get a connection from the connection pool
    conn = pool.get_connection()

    # Create a cursor from the connection
    cursor = conn.cursor()

    # Fetch data from the database
    cursor.execute("SELECT functioncode, starttime, endtime, capacity, UNIX_TIMESTAMP(polledtimestamp) FROM EMSUPSbatterycontrol ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()

    if row:
        data_dict = {
            "FN": row[0],
            "ST": int(row[1].timestamp()),
            "ET" : int(row[2].timestamp()),
            "CY" : row[3]
        }

        current_time = int(datetime.datetime.now().timestamp())
        polled_time = int(row[4])
        timedifference = current_time - polled_time
        print(timedifference)
        if timedifference < 30 :
            json_data = json.dumps(data_dict)
            print(json_data)

            try:
                # Publish data to MQTT topic
                mqtt_client.publish(mqtt_topic_publish, json_data)
                print("Data published to MQTT broker")

            except Exception as e:
                print(f"An error occurred while publishing to MQTT: {e}")
                print("Attempting to reconnect to MQTT broker...")

                # Reconnect to MQTT broker
                mqtt_client.reconnect()
                mqtt_client.publish(mqtt_topic_publish, json_data)

        # Commit changes to database
        conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    time.sleep(3)



