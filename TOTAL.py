import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import json

INFLUXDB_IP = "localhost"
INFLUXDB_DATABASE = "data"

TOPIC_TEMP_INFRAROUGE_AMB = "Capteur/temperature_infrarouge_amb"
TOPIC_TEMP_INFRAROUGE_PREC = "Capteur/temperature_infrarouge_prec"
TOPIC_TEMP_PLAT = "Capteur/temperature_plat"
TOPIC_VIBRATION_PLAT = "Capteur/vibration_plat"
TOPIC_VIBRATION_ROND = "Capteur/vibration_rond"
TOPIC_CLOUD = "oui"

TOPICS = [[TOPIC_TEMP_INFRAROUGE_AMB,0],[TOPIC_TEMP_INFRAROUGE_PREC,0],[TOPIC_TEMP_PLAT,0],[TOPIC_VIBRATION_PLAT,0],[TOPIC_VIBRATION_ROND,0]]



def database_exist(database_name):
    
    influx_client = InfluxDBClient(INFLUXDB_IP,8086,"root","root",INFLUXDB_DATABASE)
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
    print(msg.topic + " " +str(msg.payload.decode("utf-8")))
    
    try:
        
        influx_client = InfluxDBClient(INFLUXDB_IP,8086,"root","root",INFLUXDB_DATABASE)
        database_exist(INFLUXDB_DATABASE)
        
        if msg.topic == TOPIC_TEMP_INFRAROUGE_AMB:
            msgrec = str(msg.payload.decode("utf-8"))
            data = json.loads(msgrec)
            id_capteur = float(data["temperature_infrarouge_ambient"])
            client_pub.publish(TOPIC_CLOUD,50)
            
            json_body= [
                {
                    'measurement': 'Test',
                        'fields':{
                            'temperature_infrarouge_ambient':id_capteur,
                            
                            }
                }
            ]
            influx_client.write_points(json_body)
            
        if msg.topic == TOPIC_TEMP_INFRAROUGE_PREC:
            msgrec = str(msg.payload.decode("utf-8"))
            data = json.loads(msgrec)
            id_capteur = float(data["temperature_infrarouge_prec"])
            
            
            json_body= [
                {
                    'measurement': 'Test',
                        'fields':{
                            'temperature_infrarouge_prec':id_capteur,
                            
                            }
                }
            ]
            influx_client.write_points(json_body)
        
        if msg.topic == TOPIC_TEMP_PLAT:
            msgrec = str(msg.payload.decode("utf-8"))
            data = json.loads(msgrec)
            id_capteur = float(data["temperature_plat"])          
            
            json_body= [
                {
                    'measurement': 'Test',
                        'fields':{
                            'temperature_plat':id_capteur,
                            
                            }
                }
            ]
            influx_client.write_points(json_body)
        
        if msg.topic == TOPIC_VIBRATION_PLAT:
            msgrec = str(msg.payload.decode("utf-8"))
            data = json.loads(msgrec)
            id_capteur = float(data["vibration_plat"])          
            
            json_body= [
                {
                    'measurement': 'Test',
                        'fields':{
                            'vibration_plat':id_capteur,
                            
                            }
                }
            ]
            influx_client.write_points(json_body)
        
        
        if msg.topic == TOPIC_VIBRATION_ROND:
            msgrec = str(msg.payload.decode("utf-8"))
            data = json.loads(msgrec)
            id_capteur = float(data["vibration_rond"])          
            
            json_body= [
                {
                    'measurement': 'Test',
                        'fields':{
                            'vibration_rond':id_capteur,
                            
                            }
                }
            ]
            
            influx_client.write_points(json_body)
            
    except Exception as e:
        print(f"Error : {e}")
        
if __name__ == "__main__":
    client= mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    client_pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("localhost",1883)
    client_pub.connect("broker.emqx.io",1883)
    client.loop_forever()
    
