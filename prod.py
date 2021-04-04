from kafka import KafkaProducer
import json
from json import dumps
from json import loads
import random
import time

sensorid = input()
numSensors = int(input())
topic = "topic"+sensorid

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
while True:
    d = {}
    for i in range(numSensors):
        d[i] = random.randint(0,100)
    producer.send(topic,d)
    time.sleep(1)
