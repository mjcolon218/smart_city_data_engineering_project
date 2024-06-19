# Import necessary modules and libraries
from pyspark.sql import SparkSession
from config import configuration
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

def main():
    # Create and configure a Spark session
    spark = SparkSession.builder.appName("smart_city_data_engineering_project")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469")\
        .config("Spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()
    
    # Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel('WARN')
    
    # Define schema for vehicle data
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
    
    # Define schema for GPS data
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True),
    ])
    
    # Define schema for traffic data
    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True),
    ])
    
    # Define schema for weather data
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", DoubleType(), True),
        StructField("precipitation", StringType(), True),
        StructField("windSpeed", StringType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", StringType(), True),
    ])
    
    # Define schema for emergency data
    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
        StructField("airQualityIndex", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        """
        Read data from a Kafka topic and parse it according to the given schema.
        Args:
            topic (str): Kafka topic name.
            schema (StructType): Schema to parse the data.
        Returns:
            DataFrame: Parsed data as a DataFrame.
        """
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')   
        )

    def stream_writer(dataframe, checkpoint_folder, output_path):
        """
        Write the streaming data to a specified output path in Parquet format.
        Args:
            dataframe (DataFrame): DataFrame to write.
            checkpoint_folder (str): Checkpoint folder path.
            output_path (str): Output path for the Parquet files.
        Returns:
            StreamingQuery: Streaming query object.
        """
        return (dataframe.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpoint_folder)
                .option('path', output_path)
                .outputMode('append')
                .start())
        
    # Read data from Kafka topics using the schemas defined
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    # Write the dataframes to the specified S3 paths with checkpointing to ensure fault tolerance
    query1 = stream_writer(vehicleDF, 's3a://spark-streaming-bucket/checkpoints/vehicle_data', 's3a://spark-streaming-bucket/data/vehicle_data')
    query2 = stream_writer(gpsDF, 's3a://spark-streaming-bucket/checkpoints/gps_data', 's3a://spark-streaming-bucket/data/gps_data')
    query3 = stream_writer(trafficDF, 's3a://spark-streaming-bucket/checkpoints/traffic_data', 's3a://spark-streaming-bucket/data/traffic_data')
    query4 = stream_writer(weatherDF, 's3a://spark-streaming-bucket/checkpoints/weather_data', 's3a://spark-streaming-bucket/data/weather_data')
    query5 = stream_writer(emergencyDF, 's3a://spark-streaming-bucket/checkpoints/emergency_data', 's3a://spark-streaming-bucket/data/emergency_data')
    
    # Await termination of the last query to keep the application running
    query5.awaitTermination()

# Entry point for the application
if __name__ == "__main__":
    main()
