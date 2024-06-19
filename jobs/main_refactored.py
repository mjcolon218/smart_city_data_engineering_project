import os
import time
from confluent_kafka import SerializingProducer
import simplejson as json
import random
from datetime import datetime, timedelta
import uuid

# Coordinates for London and Birmingham
LONDON_COORDINATES = {'latitude': 51.5074, 'longitude': -0.1278}
BIRMINGHAM_COORDINATES = {'latitude': 52.4862, 'longitude': -1.8094}

# Calculate movement increments for simulation
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

# Environment Variables for Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_VEHICLE_TOPIC = os.environ.get('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.environ.get('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.environ.get('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.environ.get('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.environ.get('EMERGENCY_TOPIC', 'emergency_data')

random.seed(42)  # Seed for reproducibility
start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

def get_next_time():
    """Generate the next timestamp for the simulation."""
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def generate_weather_data(device_id, timestamp, location):
    """Generate simulated weather data."""
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloud', 'Rain', 'Snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),
        'airQualityIndex': random.uniform(0, 500)
    }

def generate_incident_data(device_id, timestamp, location):
    """Generate simulated incident data."""
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'location': location,
        'timestamp': timestamp,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of Incident'
    }

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    """Generate simulated GPS data."""
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North-East',
        'vehicle_type': vehicle_type
    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    """Generate simulated traffic camera data."""
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }

def simulate_vehicle_movement():
    """Simulate vehicle movement towards Birmingham."""
    global start_location
    # Move towards Birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
    # Add some randomness to simulate road travel
    start_location['latitude'] += random.uniform(-0.0001, 0.0001)
    start_location['longitude'] += random.uniform(-0.0001, 0.0001)
    return start_location

def generate_vehicle_data(device_id):
    """Generate simulated vehicle data."""
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'X5',
        'year': 2024,
        'fuelType': 'Hybrid' 
    }

def json_serializer(obj):
    """Custom JSON serializer for UUID objects."""
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not serializable.')

def delivery_report(err, msg):
    """Callback for Kafka message delivery reports."""
    if err is not None:
        print(f'Message Delivery Failed: {err}')
    else:
        print(f'Message Delivered to {msg.topic()} [{msg.partition()}]')

def produce_data_to_kafka(producer, topic, data):
    """Produce data to a Kafka topic."""
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()

def simulate_journey(producer, device_id):
    """Simulate a vehicle journey and produce data to Kafka."""
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], camera_id='MadisoNAveMoe')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_data = generate_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])

        # Check if vehicle has reached Birmingham
        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude'] and
            vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
            print("Vehicle has reached Birmingham. Simulation Ending....")
            break

        # Produce data to Kafka topics
        produce_data_to_kafka(producer, KAFKA_VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_data)

        # Print the generated data
        print(vehicle_data)
        print('----------------------')
        print(gps_data)
        print('----------------------')
        print(traffic_camera_data)
        print('----------------------')
        print(weather_data)
        print('----------------------')
        print(emergency_data)

        time.sleep(3)  # Pause between iterations

if __name__ == "__main__":
    # Kafka producer configuration
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)
    
    try:
        # Start the simulation
        simulate_journey(producer, 'Vehicle-Moe-1986')
    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'An error occurred: {e}')
