import os
import random
import uuid
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta

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
        return {
            'latitude':self.latitude,
            'longitude':self.longitude
        }
    
SOURCE=City(name='Bengaluru',latitude=12.9716,longitude=77.5946)
DESTINATION=City(name='Mumbai',latitude=19.0760,longitude=72.8777)

LATITUDE_INCREMENTS=(DESTINATION.get_latitude()-SOURCE.get_latitude())/100
LONGITUDE_INCREMENTS=(DESTINATION.get_longitude()-SOURCE.get_longitude())/100

# Environment Variables
KAFKA_BOOTSTRAP_SERVERS= os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
VEHICLE_TOPIC=os.getenv('VEHICLE_TOPIC','vehicle_data')
GPS_TOPIC=os.getenv('GPS_TOPIC','gps_data')
TRAFFIC_TOPIC=os.getenv('TRAFFIC_TOPIC','traffic_data')
WEATHER_TOPIC=os.getenv('WEATHER_TOPIC','weather_data')
EMERGENCY_TOPIC=os.getenv('EMERGENCY_TOPIC','emergency_data')

start_time=datetime.now()
start_location=SOURCE.get_coordinates() 

def get_next_time():
    global start_time
    start_time-=timedelta(seconds=random.randint(30,60))
    return start_time

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0,40),
        'direction': 'North-East',
        'vehicle_type': vehicle_type
    }

def generate_traffic_camera_data(device_id,timestamp, location, camera_id):
    return{
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'

    }

def generate_weather_data(device_id, timestamp, location ):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5,26),
        'weatherCondition': random.choice(['Sunny','CLoudy','Rain','Snow']),
        'precipitation': random.uniform(0,25),
        'windspeed': random.uniform(0,100),
        'humidity': random.uniform(0,100),
        'airQualityIndex': random.uniform(0,500)

    }

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId':uuid.uuid4(),
        'type': random.choice(['Accident','Fire','Medical','Police','None']), 
        'location': location,
        'timestamp': timestamp,
        'status': random.choice(['Active','Resolved']),
        'description': 'Description of the Incident'

    }

def simulate_vehicle_movement():
    global start_location
    
    #Move Towards Destination
    start_location['latitude']+= LATITUDE_INCREMENTS
    start_location['longitude']+=LONGITUDE_INCREMENTS

    #Add Randomness to data
    start_location['latitude']+=random.uniform(-0.0005,0.0005)
    start_location['longitude']+=random.uniform(-0.0005,0.0005)

    return start_location

def generate_vehicle_data(device_id):
    location= simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceID': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'],location['longitude']),
        'speed': random.uniform(10,40),
        'direction': 'North-West',
        'make': 'KIA',
        'model': 'EV6',
        'year': '2023',
        'fuelType': 'EV'
    }

def simulate_journey(producer,device_id):
    while True:
        vehicle_data=generate_vehicle_data(device_id)
        gps_data=generate_gps_data(device_id,vehicle_data['timestamp'])
        traffic_data=generate_traffic_camera_data(device_id,vehicle_data['timestamp'], vehicle_data['location'],camera_id='Security-Cam')
        weather_data=generate_weather_data(device_id,vehicle_data['timestamp'],vehicle_data['location'])
        emergency_incident_data=generate_emergency_incident_data(device_id,vehicle_data['timestamp'], vehicle_data['location'])
        
        break

if __name__ == "__main__":
    producer_config={
        'bootstrap.servers':KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka_ERROR: {err}')
    }

    producer= SerializingProducer(producer_config)

    try: 
        simulate_journey(producer, 'Vehicle-CodewithPaul001')

    except KeyboardInterrupt:
        print('Simulation Ended by the User')
    except Exception as e:
        print(f'Unexpected Error occured: {e}')
