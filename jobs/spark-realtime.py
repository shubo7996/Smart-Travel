import os
from confluent_kafka import SerializingProducer
import simplejson as json

class City:

    def __init__(self,name=None,latitude=None,longitude=None):
        self.name=name
        self.latitude=latitude
        self.longitude=longitude
        

    def get_name(self):
        return self.name
    
    def get_longitude(self):
        return self.longitude
    
    def get_latitude(self):
        return self.latitude
    
    def get_coordinates(self):
        return {"latitude":self.latitude,"longitude":self.longitude}
    
BENGALURU=City(name='Bengaluru',latitude=12.9716,longitude=77.5946)
MUMBAI=City(name='Mumbai',latitude=19.0760,longitude=72.8777)

