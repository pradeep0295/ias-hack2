from kafka import KafkaConsumer
from kafka import KafkaProducer
from flask import Flask,request
from threading import Thread
from ConnectCouch import connectCouch
import os
import json
import time
from json import loads
from json import dumps
### every sensor instance has a single intermediate server -> localhost:5000
## Sensorid=<string>, SensorType=<string>
app = Flask(__name__)
Tid = 0

# def consume(topic): In thread, sleep,  consumer('latest')
SensorRegistry = {} # dict(dict())  access using [sensorid][inputid]  update recent values of every input in a sensor instance

def listen(sensorId,topic):
    global SensorRegistry
    print("entered listen")
    os.system("~/kafka_2.13-2.7.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic "+topic)
    consumer = KafkaConsumer(topic,bootstrap_servers=['localhost:9092'],auto_offset_reset='latest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for message in consumer:
        d = message.value
        if str(sensorId) not in SensorRegistry:
            SensorRegistry[str(sensorId)]={}
        else:
            for i in d:
                SensorRegistry[str(sensorId)][i] = d[i]

@app.route('/registerInstance/', methods = ['POST'])
def registerInstance():
    Sinst  = json.loads(request.get_json())
    print(Sinst)
    connectCouch.SensorInstanceRegistration(Sinst)
    topic = "topic"+str(Sinst['sensorid'])
    x= Thread(target=listen,args=(str(Sinst['sensorid']),topic,))
    x.start()    
    return "OK"
    
@app.route('/registerType/', methods = ['POST'])
def registerType():
    Stype  = json.loads(request.get_json())
    connectCouch.SensorTypeRegistration(Stype)
    return "OK"
    
@app.route('/showTypes/',methods = ['POST'])
def showTypes():
    return connectCouch.GetAllSensorTypes()

@app.route('/showInstances/',methods = ['POST'])    
def showInstances():
    return connectCouch.GetAllSensorIDs()

def control(consumer):
    for message in consumer:
        d = message.value
        print("control requested by "+str(d['appid'])+"on sensor"+str(d['sensorid'])+"on control pin"+str(d['controlid'])+str(d['param']))

## Data Request handling from different Appids     
def serve(consumer,producer):
    global SensorRegistry
    print(message,"........")
    for message in consumer:
        reqd = message.value
        print(reqd['sensorid'],reqd['inputid'])
        print(SensorRegistry)
        # print(SensorRegistry[reqd['sensorid']])
        value = SensorRegistry[reqd['sensorid']][int(reqd['inputid'])]
        res = {'appid':reqd['appid'],'val':value}
        producer.send('Response',res)

## initializer Function of the sensormanager -> can be called in init.py and init.py can be used by serverLifeCycle to start sensorManager in a Node.    
def initializeSensorManager():
    os.system("~/kafka_2.13-2.7.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Request")
    os.system("~/kafka_2.13-2.7.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Control")
    os.system("~/kafka_2.13-2.7.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Response")
    consumer_control = KafkaConsumer('Control',bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    consumer = KafkaConsumer('Request',bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
    th = Thread(target=serve,args=(consumer, producer,))
    thc = Thread(target=control,args=(consumer_control,))
    th.start()
    thc.start()
    app.run(debug=True)
initializeSensorManager()

