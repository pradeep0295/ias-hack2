from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import loads
from json import dumps
import json

## SENSOR API
def get(appid,sensorid,inputid):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
    req = {}
    req['appid'] = appid
    req['sensorid'] = sensorid
    req['inputid'] = inputid
    consumer = KafkaConsumer('response',bootstrap_servers=['localhost:9092'],auto_offset_reset='latest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    producer.send('request',req)
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
     
    producer.send("control",req)
     
print(get(123,'abc',0))
# set(123,'abc',0)