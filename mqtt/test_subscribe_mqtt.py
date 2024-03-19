import paho.mqtt.client as mqtt
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {str(rc)}")
# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(f"{msg.topic} {str(msg.payload)}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("localhost", 1883, 60)
client.username_pw_set(username="hadoop",password="hadoop")
topic = 'home/lights/kitchen'
client.subscribe(topic)
client.loop_forever() 
