from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
from json import dumps
from json import loads
from threading import Thread
import random
import time

print("input sensor id")
sensorid = input()
print('no. of inputs in this sensor')
numSensors = int(input())
print('Enter each input type... Example: Playlist,Title for a mediaplayer')
D = []
for i in range(numSensors):
    D.append(input())
topic = "Topic"+sensorid

# print('no. of controls in this sensor')
# numControls = int(input())
# print('Enter each control type... Example: Play,VolumeStep for a speaker')
# C = []
# for i in range(numControls):
    # C.append(input())

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
def controls(topic):
    consumer = KafkaConsumer(topic,bootstrap_servers=['localhost:9092'],auto_offset_reset='latest',value_deserializer=lambda x: loads(x.decode('utf-8')))
    for message in consumer:
        d = message.value
        print("Control request received for "+d['controlname'])
        print(d['param'])
x = Thread(target=controls,args=(topic+'-',))
x.start()

while True:
    d = {}
    for i in range(numSensors):
        d[D[i]] = random.randint(0,100)
    producer.send(topic,d)
    time.sleep(1)
