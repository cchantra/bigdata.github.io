import paho.mqtt.client as mqtt
client = mqtt.Client("mqtt demo")
client.connect("localhost", 1883, 60)


client.username_pw_set("hadoop", "hadoop")
topic = 'home/lights/kitchen'
client.publish(topic, payload='Hn')
