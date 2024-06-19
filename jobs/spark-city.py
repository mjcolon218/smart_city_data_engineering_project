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