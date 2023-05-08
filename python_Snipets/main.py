import paho.mqtt.client as mqtt
import time

# Define callback function for when a message is received
def on_message(client, userdata, message):
    print(f"Received message: {message.payload.decode()}")

# Define callback function for when the client disconnects
def on_disconnect(client, userdata, rc):
    print("Disconnected from MQTT broker. Reason: " + str(rc))
    time.sleep(5)
    client.reconnect()

# Create MQTT client instance and set credentials
client = mqtt.Client()
client.username_pw_set(username="swadha", password="dhawas@123")

# Set callback functions
client.on_message = on_message
client.on_disconnect = on_disconnect

# Loop until interrupted
while True:
    try:
        # Connect to MQTT broker and subscribe to a topic
        client.connect("10.9.39.25", port=1883)
        client.subscribe("swadha/50KUPS001/sai/log")
       
        # Print message if connected successfully
        print("Connected to MQTT broker")

        # Start MQTT client loop to handle incoming messages
        client.loop_forever()
    except KeyboardInterrupt:
        # Stop the loop and disconnect from the broker
        client.disconnect()
        client.reconnect()
    except:
        # Print message if connection failed
        print("Connection to MQTT broker failed")

        # Wait for 1 second and then attempt to reconnect
        time.sleep(1)
        client.reconnect()

    # Wait for 2 seconds before attempting to connect again
    time.sleep(2)