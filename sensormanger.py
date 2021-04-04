from kafka import KafkaConsumer
from kafka import KafkaProducer
from flask import Flask,request
from threading import Thread
from ConnectCouch import connectCouch
import os
import json
from json import loads
from json import dumps
### every sensor instance has a single intermediate server -> localhost:5000
## Sensorid=<string>, SensorType=<string>
app = Flask(__name__)
Tid = 0

# def consume(topic): In thread, sleep,  consumer('latest')
SensorRegistry = {} # dict(dict())  access using [sensorid][inputid]  update recent values of every input in a sensor instance

def listen(sensorId,topic):
    print("entered listen")
    os.system("~/kafka_2.13-2.7.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic "+topic)
    consumer = KafkaConsumer(topic,bootstrap_servers=['localhost:9092'],auto_offset_reset='latest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for message in consumer:
        d = message.value
        if sensorId not in SensorRegistry:
            SensorRegistry[sensorId]={}
        for i in d:
            SensorRegistry[sensorId][i] = d[i]
        
@app.route('/registerInstance/', methods = ['POST'])
def registerInstance():
    Sinst  = json.loads(request.get_json())
    connectCouch.SensorInstanceRegistration(Sinst)
    topic = "topic"+str(Sinst['sensorId'])
    x= Thread(target=listen(),args=(sensorId,topic,))
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
    
## fetching data from sensorRegistry
def get_sensordata(sensorid,inputid):
    return SensorRegistry[sensorid][inputid]

## Data Request handling from different Appids     
def serve(consumer,producer):
    for message in consumer:
        reqd = message.value
        value = get_sensordata(reqd['sensorid'],reqd['inputid'])
        res = {'appid':reqd['appid'],'val':value}
        producer.send('Response',res)

## initializer Function of the sensormanager -> can be called in init.py and init.py can be used by serverLifeCycle to start sensorManager in a Node.    
def initializeSensorManager():
    os.system("~/kafka_2.13-2.7.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Request")
    os.system("~/kafka_2.13-2.7.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Control")
    os.system("~/kafka_2.13-2.7.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Response")
    consumer_control = KafkaConsumer('Control',bootstrap_servers=['localhost:9092'],auto_offset_reset='latest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    consumer = KafkaConsumer('Request',bootstrap_servers=['localhost:9092'],auto_offset_reset='latest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
    th = Thread(target=serve,args=(consumer, producer,))
    thc = Thread(target=control,args=(consumer_control,))
    th.start()
    thc.start()
    app.run(debug=True)
initializeSensorManager()

