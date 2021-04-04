from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import loads
from json import dumps
import json

def get(appid,sensorid,inputid):
    consumer = KafkaConsumer('Response',bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
    req = {}
    req['appid'] = appid
    req['sensorid'] = sensorid
    req['inputid'] = inputid

    producer.send('Request',req)
    for message in consumer:
        response = message.value
        if(response['appid'] == appid):
            return response['val']
            
def set(appid,sensorid,control):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
    req = {}
    req['appid'] = appid
    req['sensorid'] = sensorid
    req['controlid'] = control['name']
     
    producer.send("Control",req)

print(get(123,'abc',0))
set(123,'abc',0)