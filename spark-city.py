from pyspark.sql import SparkSession
from config import configuration
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    #to access spark within the jars
    #to connect hadoop and spark with the created s3 bucket to access it
    #to access the Access Key and the Secret Key that are created
    #to access the AWS Credentials Provider
    spark = SparkSession.builder.appName("SmartCityStreaming")\
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()

    #Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel('WARN')


    #vehicle Schema
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

    #gps Schema
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True),
    ])
    
    #trafficCamera Schema
    trafficCameraSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("snapshot", StringType(), True),
    ])

    #weatherSchema
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    #emergencySchema
    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
 #       try:
 #           logger.info(f"Reading Kafka topic: {topic}")
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .option('failOnDataLoss', 'false')
                .load()
                .selectExpr('CAST(value as STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')
                )
#       except Exception as e:
#          logger.error(f"Error reading Kafka topic {topic}: {e}")
#            raise
    
    def streamWriter(input: DataFrame, checkpointFolder, output):
#        try:
#            logger.info(f"Writing stream to {output}")
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start()
                )
#        except Exception as e:
#            logger.error(f"Error writing stream to {output}: {e}")
#            raise
    
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficCameraDF = read_kafka_topic('traffic_camera_data', trafficCameraSchema).alias('trafficCamera')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_incidents_data', emergencySchema).alias('emergencyIncidents')
    

    queries = []
    queries.append(streamWriter(vehicleDF, 's3a://smartcity-kafka-streaming/checkpoints/vehicle_data', 's3a://smartcity-kafka-streaming/data/vehicle_data'))
    queries.append(streamWriter(gpsDF, 's3a://smartcity-kafka-streaming/checkpoints/gps_data', 's3a://smartcity-kafka-streaming/data/gps_data'))
    queries.append(streamWriter(trafficCameraDF, 's3a://smartcity-kafka-streaming/checkpoints/traffic_camera_data', 's3a://smartcity-kafka-streaming/data/traffic_camera_data'))
    queries.append(streamWriter(weatherDF, 's3a://smartcity-kafka-streaming/checkpoints/weather_data', 's3a://smartcity-kafka-streaming/data/weather_data'))
    queries.append(streamWriter(emergencyDF, 's3a://smartcity-kafka-streaming/checkpoints/emergency_incidents_data', 's3a://smartcity-kafka-streaming/data/emergency_incidents_data'))

    for query in queries:
        query.awaitTermination()

if __name__ == "__main__":
    main()
