from sensor import Sensor

sensor = Sensor()
while True:
    print(sensor.get(123,'abc','seek'))