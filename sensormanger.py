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
sensorDataPath = os.getcwd()+"/db/"

def listen(sensorId,topic):
    consumer = KafkaConsumer(topic,bootstrap_servers=['localhost:9092'],auto_offset_reset='latest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for message in consumer:
        d = message.value
        f = open(sensorDataPath+str(sensorId),'w')
        json.dump(d,f)
        f.close()
        time.sleep(1.2)
        
@app.route('/registerInstance/', methods = ['POST'])
def registerInstance():
    Sinst  = json.loads(request.get_json())
    connectCouch.SensorInstanceRegistration(Sinst)
    topic = "Topic"+str(Sinst['sensorid'])
    x= Thread(target=listen,args=(str(Sinst['sensorid']),topic,))
    x.start()    
    return json.dumps({"OK":"registered"})
    
@app.route('/registerType/', methods = ['POST'])
def registerType():
    Stype  = json.loads(request.get_json())
    connectCouch.SensorTypeRegistration(Stype)
    return json.dumps({"OK":"registered"})
    
@app.route('/showTypes/',methods = ['POST'])
def showTypes():
    return connectCouch.GetAllSensorTypes()

@app.route('/showInstances/',methods = ['POST'])    
def showInstances():
    return connectCouch.GetAllSensorIDs()

def control(topic):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
    consumer = KafkaConsumer(topic,bootstrap_servers=['localhost:9092'],auto_offset_reset='latest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for message in consumer:
        reqd = message.value
        topic = "Topic"+str(reqd['sensorid'])+'-'
        producer.send(topic,reqd)

## Data Request handling from different Appids     
def app_serve():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
    consumer = KafkaConsumer('request',bootstrap_servers=['localhost:9092'],auto_offset_reset='latest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for message in consumer:
        reqd = message.value
        f = open(sensorDataPath+str(reqd['sensorid']),'r') 
        data = json.load(f)
        f.close()
        res = {'appid':reqd['appid'],'val':data[str(reqd['inputType'])]}
        time.sleep(0.2)
        producer.send('response',res)
        
## initializer Function of the sensormanager -> can be called in init.py and init.py can be used by serverLifeCycle to start sensorManager in a Node.    
def initializeSensorManager():
    th = Thread(target=app_serve)
    th.start()
    th = Thread(target=control,args=('control',))
    th.start()
    app.run(debug=False)
initializeSensorManager()

