# Streaming realtime Pipeline with Kafka, Zookeeper, Spark, S3, AWS Glue, Athena, and Redshift.

This project provides a comprehensive data pipeline solution to extract, transform, and load (ETL) simulated iot data into a Kafka consumer.From there the upload is ran from a master spark node and distributed by workernodes to my s3 bucket. The pipeline leverages a combination of tools and services including Amazon S3, AWS Glue, Amazon Athena, and Amazon Redshift.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [System Setup](#system-setup)

## Overview

- **Docker for Environment Consistency**: Containerized Zookeeper,Kafka for consistent and reproducible environments.
- **Data Extraction with Python**: Simulated iot streaming data with python.
- **Data Upload to S3**: (Pyspark) Uploaded extracted data to an S3 bucket.
- **Data Cataloging with AWS Glue**: Automatically detected and cataloged data.
- **Querying with Amazon Athena**: Performed SQL queries on data stored in S3.
- **Data Warehousing with Amazon Redshift**: Loaded data into a Redshift data warehouse for analysis
## Architecture
![smart-city-streaming.jpg](/images/smart-city-streaming.jpg)
1. **Python simulated data**: Source of the data.
2. **Zookeeper & Kafka**: Apache Kafka is an open-source distributed event streaming platform used for building real-time data pipelines and streaming applications.
3. **Amazon S3**: Raw data storage.
4. **AWS Glue**: Data cataloging and ETL jobs.
5. **Amazon Athena**: SQL-based data transformation.
6. **Amazon Redshift**: Data warehousing and analytics.

## Prerequisites
- AWS Account with appropriate permissions for S3, Glue, Athena, and Redshift.
- Docker Installation
- Python 3.9 or higher

## System Setup
1. Clone the repository.
   ```bash
    git clone https://github.com/mjcolon218/smart_city_data_engineering_project.git
   ```
2. Create a virtual environment.
   ```bash
    python3 -m venv venv
   ```
3. Activate the virtual environment.
   ```bash
    source venv/bin/activate
   ```
4. Install the dependencies.
   ```bash
    pip install -r requirements.txt
   ```
5. Rename the configuration file and the credentials to the file.
   ```bash
    mv config/config.conf.example config/config.conf
   ```
6. Starting the containers
   ```bash
    docker-compose up -d
   ```
## Documentation
1. 
2. **Check out the** : [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html)
2. **Check out the** : [Access Keys Documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html)
3. **Check out the** : [Docker Documentation](https://docs.docker.com/reference/)
4. **Check out the** : [Policy Documentation](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-create-and-attach-iam-policy.html)
5. **Check out the** : [Requests Documentation](https://requests.readthedocs.io/en/latest/)
6. **Check out the** : [S3fs Documentation](https://requests.readthedocs.io/en/latest/)

## Code Snippets. ##
```python
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

```
```python
from pyspark.sql import SparkSession
from config import configuration
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType


def main():
    spark = SparkSession.builder.appName("smart_city_data_engineering_project")\
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.1,""com.amazonaws:aws-java-sdk:1.11.469")\
    .config("Spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key",configuration.get('AWS_ACCESS_KEY'))\
    .config("spark.hadoop.fs.s3a.secret.key",configuration.get('AWS_SECRET_KEY'))\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
    .getOrCreate()
    
    #adjust the log level to minimize the console output on executors.
    spark.sparkContext.setLogLevel('WARN')
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True),
    ])
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp",TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True),
    ])
    
    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp",TimestampType(), True),
        StructField("snapshot", StringType(), True),
    ])
    
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp",TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", DoubleType(), True),
        StructField("precipitation", StringType(), True),
        StructField("windSpeed", StringType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", StringType(), True),
    ])
    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("incidentId",StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
        StructField("airQualityIndex", StringType(), True),
    ])
    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers','broker:29092')
                .option('subscribe',topic)
                .option('startingOffsets','earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'),schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp','2 minutes')   
        )
    def StreamWriter(DataFrame, checkpointFolder,output):
        return (DataFrame.writeStream
                .format('parquet')
                .option('checkpointLocation',checkpointFolder)
                .option('path',output)
                .outputMode('append')
                .start())
        
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')
    # join all the dfs wth id and timestamp
    query1 = StreamWriter(vehicleDF,'s3a://spark-streaming-bucket/checkpoints/vehicle_data','s3a://spark-streaming-bucket/data/vehicle_data')
    query2 = StreamWriter(gpsDF,'s3a://spark-streaming-bucket/checkpoints/gps_data','s3a://spark-streaming-bucket/data/vehicle_data')
    query3 = StreamWriter(trafficDF,'s3a://spark-streaming-bucket/checkpoints/traffic_data','s3a://spark-streaming-bucket/data/vehicle_data')
    query4 = StreamWriter(weatherDF,'s3a://spark-streaming-bucket/checkpoints/weather_data','s3a://spark-streaming-bucket/data/vehicle_data')
    query5 = StreamWriter(emergencyDF,'s3a://spark-streaming-bucket/checkpoints/emergency_data','s3a://spark-streaming-bucket/data/vehicle_data')
    
    query5.awaitTermination()
if __name__ == "__main__":
    main()

```
This script reads from five different Kafka topics, applies a schema to each topic, and writes the data.


![crawler.png](/images/crawler.PNG)

![awsprojectpics.png](/images/awsprojectpics.PNG)
# Key Takeaways #
* Apache Kafka is an open-source distributed event streaming platform used for building real-time data pipelines and streaming applications. 
* The project highlights the importance of real-time data processing for smart city applications. By using Apache Kafka and Spark Streaming, the system can handle continuous data streams, providing up-to-the-minute insights and allowing for immediate action based on incoming data.
* Apache Kafka's partitioning and replication mechanisms ensure that the system can scale horizontally and provide fault tolerance. This setup can handle a large volume of data with high availability, essential for a smart city's dynamic and often unpredictable environment.
* The project employs a distributed architecture with Docker containers for Kafka, Zookeeper, and Spark, ensuring that the system is modular, scalable, and easy to deploy. This architecture allows for the seamless addition of new components or services without disrupting the existing setup.
* Using AWS S3 for data storage demonstrates the advantage of integrating with cloud services. AWS S3 provides durable, scalable, and cost-effective storage for processed data, making it accessible for further analysis and long-term storage.
* The project includes a comprehensive simulation of various smart city data points (vehicles, GPS, traffic cameras, weather, and emergency incidents). This simulation is crucial for testing the system's capabilities and preparing for real-world deployment. 
## Common Troubleshooting Roadblocks

When working on a smart city streaming project involving Kafka, Spark, and Docker, you may encounter several common issues. Here are some troubleshooting tips and solutions to help resolve these problems.

### 1. **Kafka Broker Not Starting**

**Symptoms:**
- Kafka broker container fails to start.
- Logs indicate issues with connecting to Zookeeper.

**Possible Causes:**
- Zookeeper is not running or not accessible.
- Incorrect Zookeeper connection string in Kafka configuration.

**Troubleshooting Steps:**
- Ensure the Zookeeper container is up and running.
- Verify the `KAFKA_ZOOKEEPER_CONNECT` environment variable is correctly set in the Kafka service configuration.
- Check network settings and ensure that Zookeeper's port (2181) is accessible.

### 2. **Zookeeper Connection Issues**

**Symptoms:**
- Kafka brokers fail to connect to Zookeeper.
- Error messages related to connection timeouts or unreachable Zookeeper.

**Possible Causes:**
- Network configuration issues.
- Zookeeper not properly exposed on port 2181.

**Troubleshooting Steps:**
- Confirm that the Zookeeper container is healthy and running.
- Check Docker network settings to ensure Zookeeper is accessible from Kafka containers.
- Use `docker exec` to run a command within the Kafka container to test connectivity to Zookeeper (e.g., `nc -z zookeeper 2181`).

### 3. **Data Not Being Produced to Kafka**

**Symptoms:**
- Kafka producer script runs without errors, but no data appears in Kafka topics.

**Possible Causes:**
- Incorrect Kafka bootstrap server configuration.
- Issues with data serialization or Kafka topic configuration.

**Troubleshooting Steps:**
- Verify the `KAFKA_BOOTSTRAP_SERVERS` environment variable is set correctly.
- Ensure the Kafka broker is running and reachable.
- Check the Kafka topic names and ensure they match between the producer script and Kafka configuration.
- Enable logging in the producer script to check for any serialization errors or connection issues.

### 4. **Spark Streaming Not Consuming Data**

**Symptoms:**
- Spark Streaming job starts but does not process any data from Kafka topics.
- No errors, but no output data is generated.

**Possible Causes:**
- Incorrect Kafka topic or broker configuration in Spark.
- Schema mismatches or issues with data parsing.

**Troubleshooting Steps:**
- Verify the Kafka topic names and broker addresses in the Spark Streaming script.
- Check if the Spark application is able to connect to Kafka and list the available topics.
- Validate the schema definitions in the Spark script to ensure they match the incoming data structure.
- Enable detailed logging in Spark to capture any data parsing errors.

### 5. **Data Not Written to S3**

**Symptoms:**
- Spark Streaming processes data but does not write output to the S3 bucket.
- Errors related to S3 access or file write permissions.

**Possible Causes:**
- Incorrect S3 bucket configuration or permissions.
- Issues with AWS credentials or Spark-S3 integration.

**Troubleshooting Steps:**
- Verify the S3 bucket name and path in the Spark script.
- Ensure that the AWS credentials provided have the necessary permissions to write to the S3 bucket.
- Check the Spark configuration for correct settings related to S3 access, such as `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key`.
- Use AWS CLI or S3 browser tools to verify that the S3 bucket is accessible and writable.

### 6. **Docker Container Network Issues**

**Symptoms:**
- Containers cannot communicate with each other.
- Errors related to network timeouts or unreachable services.

**Possible Causes:**
- Misconfigured Docker network.
- Containers not connected to the same network.

**Troubleshooting Steps:**
- Ensure that all relevant services (Kafka, Zookeeper, Spark) are connected to the same Docker network.
- Check the Docker Compose file for consistent network configurations.
- Use `docker network inspect <network_name>` to verify that containers are correctly attached to the network.

### Conclusion

By understanding these common issues and their troubleshooting steps, you can effectively diagnose and resolve problems in your smart city streaming project. Ensure that all configurations are double-checked, services are running as expected, and network connectivity is properly established. Regular monitoring and logging will also help in quickly identifying and addressing any issues that arise.