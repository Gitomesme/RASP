import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import json

INFLUXDB_IP = "localhost"
INFLUXDB_DATABASE = "data"

TOPIC_TEMP_INFRAROUGE = "Capteur/temperature_infrarouge"
TOPIC_TEMP_PLAT = "Capteur/temperature_plat"
TOPIC_VIBRATION_PLAT = "Capteur/vibration_plat"
TOPIC_VIBRATION_ROND = "Capteur/vibration_rond"

TOPICS = [[TOPIC_TEMP_INFRAROUGE,0],[TOPIC_TEMP_PLAT,0],[TOPIC_VIBRATION_PLAT,0],[TOPIC_VIBRATION_ROND,0]]

client = InfluxDBClient(host='localhost',port=1883)

def database_exist(database_name):
    
    influx_client = InfluxDBClient(INFLUXDB_IP,1883,"root","root",INFLUXDB_DATABASE)
    database = [i for i in influx_client.get_list_database()]
    
    if ({'name':database_name} in database):
        influx_client.switch_database(database_name)
        
    else:
        influx_client.create_database(database_name)
        influx_client.switch_database(database_name)
        


def on_connect(client,userdata,flags,rc):
    print("connected with result code"+str(rc))
    
    client.subscribe(TOPICS)
    

def on_message(client, userdata,msg):
    print(msg.topic + " " +str(msg.payload))
    
    try:
        
        influx_client = InfluxDBClient(INFLUXDB_IP,1883,"root","root",INFLUXDB_DATABASE)
        database_exist(INFLUXDB_DATABASE)
        
        if msg.topic == TOPIC_TEMP_INFRAROUGE:
            msgrec = str(msg.payload.decode("utf-8"))
            data = json.loads(msgrec)
            values = float(data["valeur"])
            id_capteur = float(data["id_capteur"])
            id_machine = float(data["id_machine"])
            
            json_body= [
                {
                    'measurement': 'Test',
                        'fields':{
                            'message_value':values,
                            'id_capteur':id_capteur,
                            'id_machine':id_machine,
                            }
                }
            ]
            
            influx_client.write_points(json_body)
            
        elif msg.topic == TOPIC_TEMP_PLAT:
            msgrec = str(msg.payload.decode("utf-8"))
            data = json.loads(msgrec)
            values = float(data["valeur"])
            id_capteur = float(data["id_capteur"])
            id_machine = float(data["id_machine"])
            
            json_body= [
                {
                    'measurement': 'Test',
                        'fields':{
                            'message_value':values,
                            'id_capteur':id_capteur,
                            'id_machine':id_machine,
                            }
                }
            ]
            
            influx_client.write_points(json_body)
            
        elif msg.topic == TOPIC_VIBRATION_PLAT:
            msgrec = str(msg.payload.decode("utf-8"))
            data = json.loads(msgrec)
            values = float(data["valeur"])
            id_capteur = float(data["id_capteur"])
            id_machine = float(data["id_machine"])
            
            json_body= [
                {
                    'measurement': 'Test',
                        'fields':{
                            'message_value':values,
                            'id_capteur':id_capteur,
                            'id_machine':id_machine,
                            }
                }
            ]
            
            influx_client.write_points(json_body)
            
        elif msg.topic == TOPIC_VIBRATION_ROND:
            msgrec = str(msg.payload.decode("utf-8"))
            data = json.loads(msgrec)
            values = float(data["valeur"])
            id_capteur = float(data["id_capteur"])
            id_machine = float(data["id_machine"])
            
            json_body= [
                {
                    'measurement': 'Test',
                        'fields':{
                            'message_value':values,
                            'id_capteur':id_capteur,
                            'id_machine':id_machine,
                            }
                }
            ]
            
            influx_client.write_points(json_body)
    except Exception as e:
        print(f"Error : {e}")
        
if __name__ == "__main__":
    client=mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("localhost",1883)
    client.loop_forever()
        