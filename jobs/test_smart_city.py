import unittest
from config import configuration
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from main_script import create_spark_session, define_schemas, read_kafka_topic, stream_writer

class SmartCityTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder.master("local[*]").appName("smart_city_test").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        os.environ['AWS_ACCESS_KEY'] = configuration.get('AWS_ACCESS_KEY')
        os.environ['AWS_SECRET_KEY'] = configuration.get('AWS_SECRET_KEY')

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()

    def test_create_spark_session(self):
        spark = create_spark_session()
        self.assertIsInstance(spark, SparkSession)
        self.assertEqual(spark.conf.get("spark.app.name"), "smart_city_data_engineering_project")

    def test_define_schemas(self):
        schemas = define_schemas()
        self.assertIn("vehicle", schemas)
        self.assertIn("gps", schemas)
        self.assertIn("traffic", schemas)
        self.assertIn("weather", schemas)
        self.assertIn("emergency", schemas)
        self.assertIsInstance(schemas["vehicle"], StructType)

    def test_read_kafka_topic(self):
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("speed", DoubleType(), True),
            StructField("direction", StringType(), True),
            StructField("vehicleType", StringType(), True),
        ])
        df = read_kafka_topic(self.spark, "gps_data", schema)
        self.assertIsNotNone(df)
        self.assertEqual(df.columns, ["id", "deviceId", "timestamp", "speed", "direction", "vehicleType"])

    def test_stream_writer(self):
        schema = StructType([
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
        data = [("1", "device_1", "2023-01-01 12:00:00", "New York", 60.0, "N", "Toyota", "Camry", 2020, "Gasoline")]
        df = self.spark.createDataFrame(data, schema)
        query = stream_writer(df, "/tmp/checkpoint_vehicle", "/tmp/output_vehicle")
        query.awaitTermination(5)  # Let it run for a short period
        self.assertTrue(os.path.exists("/tmp/checkpoint_vehicle"))
        self.assertTrue(os.path.exists("/tmp/output_vehicle"))

if __name__ == "__main__":
    unittest.main()
