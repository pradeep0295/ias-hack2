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
app = Flask(__name__)

SensorRegistry = {}

def print_SensorRegistry():
    while True:
        print(SensorRegistry)
        time.sleep(0.5)
        
def listen(sensorId,topic):
    global SensorRegistry
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
    topic = "Topic"+str(Sinst['sensorid'])
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

def control(topic):
    consumer = KafkaConsumer(topic,bootstrap_servers=['localhost:9092'],auto_offset_reset='latest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for message in consumer:
        d = message.value
        print("control requested by "+str(d['appid'])+"on sensor"+str(d['sensorid'])+"on control pin"+str(d['controlid'])+str(d['param']))

## Data Request handling from different Appids     
def app_serve(topic):
    consumer = KafkaConsumer(topic,bootstrap_servers=['localhost:9092'],auto_offset_reset='latest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    global SensorRegistry
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
    th = Thread(target=app_serve,args=('request',))
    th.start()
    th = Thread(target=control,args=('control',))
    th.start()
    th = Thread(target=print_SensorRegistry)
    th.start()
    app.run(debug=True)
initializeSensorManager()

