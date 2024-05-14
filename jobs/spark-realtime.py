from pyspark.sql import SparkSession
from config import configuration
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,IntegerType,TimestampType

class DatabaseSchema:
    
    def vehicleSchema(self):
        return StructType(
            [StructField("id", StringType(), True)],
            [StructField("deviceId", StringType(), True)],
            [StructField("timestamp", TimestampType(), True)],
            [StructField("location", StringType(), True)],
            [StructField("speed", DoubleType(), True)],
            [StructField("direction", StringType(), True)],
            [StructField("make", StringType(), True)],
            [StructField("model", StringType(), True)],
            [StructField("year", IntegerType(), True)],
            [StructField("fuelType", StringType(), True)]                              
            )
    
    def gpsSchema(self):
        return StructType(
            [StructField("id", StringType(), True)],
            [StructField("deviceId", StringType(), True)],
            [StructField("timestamp", TimestampType(), True)],
            [StructField("speed", DoubleType(), True)],
            [StructField("direction",StringType(), True)],
            [StructField("vehicle_type",StringType(), True)]
            )
    
    def trafficSchema(self):
        return StructType(
            [StructField("id", StringType(), True)],
            [StructField("deviceId", StringType(), True)],
            [StructField("cameraId", StringType(), True)],
            [StructField("location",StringType(), True)],
            [StructField("timestamp", TimestampType(), True)],
            [StructField("snapshot",StringType(), True)]
            )
    
    def weatherSchema(self):
        return StructType(
            [StructField("id", StringType(), True)],
            [StructField("deviceId", StringType(), True)],
            [StructField("location", StringType(), True)],
            [StructField("timestamp", TimestampType(), True)],
            [StructField("weatherCondition", StringType(), True)],
            [StructField("precipitation", DoubleType(), True)],
            [StructField("windspeed", DoubleType(), True)],
            [StructField("humidity",IntegerType(), True)],
            [StructField("airQualityIndex", DoubleType(), True)]
            )
    
    def emergencySchema(self):
        return StructType(
            [StructField("id", StringType(), True)],
            [StructField("deviceId", StringType(), True)],
            [StructField("incidentId", StringType(), True)],
            [StructField("type", StringType(), True)],
            [StructField("location",StringType(), True)],
            [StructField("timestamp", TimestampType(), True)],
            [StructField("status", StringType(), True)],
            [StructField("description", StringType(), True)]
            )
    
def main():
    spark=SparkSession.builder.appname("RealtimeStreaming")\
    .cofig("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0",
                "org.apache.hadoop:hadoop-aws:3.3.1",
                "com.amazonaws:aws-java-sdk:1.11.469" )\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))\
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.impl.SimpleAWSCredentialsProvider')\
    .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    def read_kafka_topic(topic, schema):
        return(spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', broker:29092)
            .option('subscribe', topic)
            .option('startingOffsets','earliest')
            .load()
            .selectExpr('CAST(value AS STRING)')
            .selct(from_json(col('value').schema).alias('data'))
            .select('data.*')
            .withWatermark('timestamp', '2 minutes')
            )

    def streamWriter(DataFrame, checkpointFolder, output):
        return(input.writeStream
               .format('parquet')
               .option('checkpointlocation', checkpointFolder)
               .option('path', output)
               .outputMode('append')
               .start()
               )

    vehicleDF=read_kafka_topic('vehicle_data', DatabaseSchema.vehicleSchema()).alias('vehicle')
    gpsDF=read_kafka_topic('gps_data', DatabaseSchema.gpsSchema()).alias('gps')
    trafficDF=read_kafka_topic('traffic_data', DatabaseSchema.trafficSchema()).alias('traffic')
    weatherDF=read_kafka_topic('weather_data', DatabaseSchema.weatherSchema()).alias('weather')
    emergencyDF=read_kafka_topic('emergency_data', DatabaseSchema.emergencySchema()).alias('emergency')

    query1=streamWriter(vehicleDF, checkpointFolder='s3a://realtime-datastream-bucket/checkpoints/vehicle_data',
                 output='s3a://realtime-datastream-bucket/data/vehicle_data')
    query2=streamWriter(gpsDF, checkpointFolder='s3a://realtime-datastream-bucket/checkpoints/gps_data',
                 output='s3a://realtime-datastream-bucket/data/gps_data')
    query3=streamWriter(trafficDF, checkpointFolder='s3a://realtime-datastream-bucket/checkpoints/traffic_data',
                 output='s3a://realtime-datastream-bucket/data/traffic_data')
    query4=streamWriter(weatherDF, checkpointFolder='s3a://realtime-datastream-bucket/checkpoints/weather_data',
                 output='s3a://realtime-datastream-bucket/data/weather_data')
    query5=streamWriter(emergencyDF, checkpointFolder='s3a://realtime-datastream-bucket/checkpoints/emergency_data',
                 output='s3a://realtime-datastream-bucket/data/emergency_data')
    
    query5.awaitTermination()



if __name__=="__main__":
    main()
