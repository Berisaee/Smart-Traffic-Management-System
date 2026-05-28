from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, window, avg, count, max as spark_max
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, TimestampType
import pymongo
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SmartTrafficProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://localhost:27018/")
db = mongo_client["traffic_db"]
traffic_collection = db["traffic_data"]
analytics_collection = db["traffic_analytics"]

# Define schema for traffic events
schema = StructType() \
    .add("action", StringType()) \
    .add("data", StructType()
         .add("intersection_id", StringType())
         .add("location", StringType())
         .add("vehicle_count", IntegerType())
         .add("avg_speed", FloatType())
         .add("traffic_density", StringType())
         .add("signal_status", StringType())
         .add("timestamp", StringType())
         .add("weather_condition", StringType())
         .add("emergency_vehicle", StringType()))

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_events") \
    .option("startingOffsets", "latest") \
    .load()

# Process the stream
def process_batch(batch_df, batch_id):
    if batch_df.count() > 0:
        # Parse JSON data
        events = batch_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("parsed")) \
            .select("parsed.*")
        
        # Collect events and process them
        for row in events.collect():
            action = row.action
            data = row.data
            
            if action == "TRAFFIC_UPDATE":
                # Insert traffic data into MongoDB
                traffic_doc = {
                    "intersection_id": data.intersection_id,
                    "location": data.location,
                    "vehicle_count": data.vehicle_count,
                    "avg_speed": data.avg_speed,
                    "traffic_density": data.traffic_density,
                    "signal_status": data.signal_status,
                    "timestamp": datetime.now(),
                    "weather_condition": data.weather_condition,
                    "emergency_vehicle": data.emergency_vehicle
                }
                traffic_collection.insert_one(traffic_doc)
                
                # Analyze and update signal timing if needed
                analyze_traffic_and_update_signal(data)
                
                print(f"Traffic data updated for intersection {data.intersection_id}")
                
            elif action == "SIGNAL_CONTROL":
                # Update signal status
                traffic_collection.update_many(
                    {"intersection_id": data.intersection_id},
                    {"$set": {"signal_status": data.signal_status}}
                )
                print(f"Signal updated for intersection {data.intersection_id}")
                
            elif action == "EMERGENCY_ALERT":
                # Handle emergency vehicle priority
                handle_emergency_priority(data)
                print(f"Emergency vehicle detected at {data.location}")

def analyze_traffic_and_update_signal(data):
    """Analyze traffic conditions and suggest signal changes"""
    try:
        # Simple traffic management logic
        vehicle_count = data.vehicle_count
        density = data.traffic_density
        
        # Determine optimal signal timing
        if density == "HIGH" and vehicle_count > 50:
            signal_recommendation = "EXTEND_GREEN"
            timing = 90  # seconds
        elif density == "LOW" and vehicle_count < 10:
            signal_recommendation = "REDUCE_GREEN"
            timing = 30  # seconds
        else:
            signal_recommendation = "NORMAL"
            timing = 60  # seconds
        
        # Store analytics
        analytics_doc = {
            "intersection_id": data.intersection_id,
            "timestamp": datetime.now(),
            "vehicle_count": vehicle_count,
            "traffic_density": density,
            "signal_recommendation": signal_recommendation,
            "recommended_timing": timing,
            "current_signal": data.signal_status
        }
        analytics_collection.insert_one(analytics_doc)
        
    except Exception as e:
        print(f"Error in traffic analysis: {str(e)}")

def handle_emergency_priority(data):
    """Handle emergency vehicle priority signaling"""
    try:
        # Update intersection to emergency mode
        traffic_collection.update_one(
            {"intersection_id": data.intersection_id},
            {"$set": {
                "signal_status": "EMERGENCY_GREEN",
                "emergency_vehicle": "PRESENT",
                "emergency_timestamp": datetime.now()
            }}
        )
        
        # Log emergency event
        emergency_doc = {
            "intersection_id": data.intersection_id,
            "location": data.location,
            "timestamp": datetime.now(),
            "event_type": "EMERGENCY_PRIORITY",
            "status": "ACTIVATED"
        }
        db["emergency_events"].insert_one(emergency_doc)
        
    except Exception as e:
        print(f"Error handling emergency: {str(e)}")

# Start streaming query
query = df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .start()

print("Smart Traffic Management System started...")
print("Processing real-time traffic events...")
print("Emergency vehicle detection active...")

query.awaitTermination()