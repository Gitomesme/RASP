import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import json
import time

INFLUXDB_IP = "localhost"
INFLUXDB_DATABASE = "data"
TOPIC = "Capteur/temperature_plat"

def on_connect(client,userdata, flags, rc):
    print("Connected("+str(rc)+"). Publishing Message ...")
    

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client.connect("localhost",1883)
client.on_connect=on_connect
client.loop_start()

count=0
while count<10:
    count=count+1
    client.publish(TOPIC,"test no :"+str(count))
    time.sleep(1)


print("Message Published")
client.disconnect()