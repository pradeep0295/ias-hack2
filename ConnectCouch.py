import couchdb  # importing couchdb 

class ConnectCouch(object):
    def __init__(self):
        # Connecting with couchdb Server  
        self.database = couchdb.Server("http://admin:1234@127.0.0.1:5984/")
        self.SensorTypesNames = self.database["sensor_type_names"]
        self.SensorInstance = self.database["sensor_instances"]

    def RefreshValues(self):
        self.SensorTypesNames = self.database["sensor_type_names"]
        self.SensorInstance = self.database["sensor_instances"]

    def GetAllSensorIDs(self):
        keys = []
        for docid in self.SensorInstance.view('_all_docs'):
            doc = self.SensorInstance[docid['id']]
            keys.append(list(doc.keys())[2])
        return keys

    def GetAllSensorTypes(self):
        keys = []
        for docid in self.SensorTypesNames.view('_all_docs'):
            doc = self.SensorTypesNames[docid['id']]
            keys.append(list(doc.keys())[2])
        return keys

    def sensorTypeRegistration(self, sensorTypeName):
        """
        Assuming that the SensorInstance and SensorTypeName are json Files
        """

        # Refesh values
        self.RefreshValues()

        # Check if the sensor type name exists
        sensorTypesPresent = self.GetAllSensorTypes()
        sensorType = list(sensorTypeName.keys())[0]
        
        if(sensorType in sensorTypesPresent):
            return [False, "Sensor type with this name is already Registered"]

        # Register This sensor Type
        self.database["sensor_type_names"].save(sensorTypeName)
        return [True, "Successfully Added Sensor Type"]


    def SensorInstanceRegistration(self, SensorInstance):
        
        # Refesh values
        self.RefreshValues()

        # Check if the sensor type name exists
        sensorTypesPresent = self.GetAllSensorTypes()
        sensorType = SensorInstance["sensorType"]
        
        if(sensorType in sensorTypesPresent):
            return [False, "Sensor type with this name is Not Avilabel, Please Register this sensor type first"]

        # Register This sensor
        self.database["sensor_instances"].save(SensorInstance)
        return [True, "Successfully Added Sensor"]

connectCouch = ConnectCouch()
