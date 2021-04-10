
from pymongo import MongoClient

# sensor_id -> sensor url

class ConnectMongo(object):
    def __init__(self):
        # Connecting with mongo Server 
        # mongodb+srv://anuragsahu:<password>@sensors.r1efb.mongodb.net/myFirstDatabase?retryWrites=true&w=majority 
        self.client = MongoClient("mongodb+srv://anuragsahu:321@sensors.r1efb.mongodb.net/sensorRegistry?retryWrites=true&w=majority")
        self.database = self.client.get_database("sensorRegistry") # fill the collection name here ask Pradeep
        self.SensorType = self.database.sensor_types
        self.SensorInstance = self.database.sensor_instances
        self.sensor_id = 0
    
    def GetAllSensorTypes(self):
        AllSensorTypes = []
        sensorTypes = list(self.SensorType.find({}))
        for i in sensorTypes:
            keys = list(i.keys())
            AllSensorTypes.append(keys[1])
        return AllSensorTypes
    
    def checkSensorID(self, SensorInstance):
        AllSensorInstances = []

    def GetSensorID(self):
        sensorIds = []
        sensorInstances = list(self.SensorInstance.find({}))
        for i in sensorInstances:
            sensorId = i["sensorid"]
            sensorIds.append(sensorId)
        self.sensor_id = max(sensorIds) + 1
        return self.sensor_id
    
    def SensorTypeRegistration(self, sensorType):
        # Check if the sensor type name exists
        sensorTypesPresent = self.GetAllSensorTypes()
        sensor_type = list(sensorType.keys())[0]
        if(sensor_type in sensorTypesPresent):
            return [False, "Sensor type with this name is already Registered"]
        # Register This sensor Type
        self.SensorType.insert_one(sensorType)
        return [True, "Successfully Added Sensor Type"]

    def SensorInstanceRegistration(self, SensorInstance):
        sensorTypesPresent = self.GetAllSensorTypes()
        SensorType = SensorInstance["sensorType"]
        SensorInstance["sensorid"] = self.GetSensorID()
        if(SensorType not in sensorTypesPresent):
            return [False, "Sensor type with this name is Not Avilable, Please Register this sensor type first"]
        self.SensorInstance.insert_one(SensorInstance)
        return [True, "Successfully Added Sensor", SensorInstance["sensorid"]]

    def getSensorURL(self, sensorID):
        sensorInstance = list(self.SensorInstance.find({"sensorid" : sensorID}))
        print(sensorInstance)
        return sensorInstance[0]["sensorurl"]
        



connectMongo = ConnectMongo()
