import pymongo
from datetime import datetime, timedelta
import json

class TrafficCRUD:
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://localhost:27018/")
        self.db = self.client["traffic_db"]
        self.traffic_collection = self.db["traffic_data"]
        self.analytics_collection = self.db["traffic_analytics"]
        self.intersection_collection = self.db["intersections"]
    
    def create_intersection(self, intersection_data):
        """Create a new intersection record"""
        intersection_data["created_at"] = datetime.now()
        intersection_data["status"] = "ACTIVE"
        result = self.intersection_collection.insert_one(intersection_data)
        print(f"Created intersection with ID: {result.inserted_id}")
        return str(result.inserted_id)
    
    def read_traffic_data(self, intersection_id=None, hours=24):
        """Read traffic data"""
        query = {}
        if intersection_id:
            query["intersection_id"] = intersection_id
        
        # Filter by time range
        start_time = datetime.now() - timedelta(hours=hours)
        query["timestamp"] = {"$gte": start_time}
        
        traffic_data = list(self.traffic_collection.find(query).sort("timestamp", -1))
        for data in traffic_data:
            data["_id"] = str(data["_id"])
        return traffic_data
    
    def read_intersection(self, intersection_id=None):
        """Read intersection information"""
        if intersection_id:
            intersection = self.intersection_collection.find_one({"intersection_id": intersection_id})
            if intersection:
                intersection["_id"] = str(intersection["_id"])
                return intersection
            return None
        else:
            intersections = list(self.intersection_collection.find())
            for intersection in intersections:
                intersection["_id"] = str(intersection["_id"])
            return intersections
    
    def update_intersection(self, intersection_id, update_data):
        """Update intersection configuration"""
        update_data["updated_at"] = datetime.now()
        result = self.intersection_collection.update_one(
            {"intersection_id": intersection_id},
            {"$set": update_data}
        )
        if result.modified_count > 0:
            print(f" Updated intersection {intersection_id}")
            return True
        return False
    
    def delete_intersection(self, intersection_id):
        """Delete intersection (mark as inactive)"""
        result = self.intersection_collection.update_one(
            {"intersection_id": intersection_id},
            {"$set": {"status": "INACTIVE", "deleted_at": datetime.now()}}
        )
        if result.modified_count > 0:
            print(f"Deactivated intersection {intersection_id}")
            return True
        return False
    
    def get_traffic_analytics(self, intersection_id=None, hours=24):
        """Get traffic analytics and patterns"""
        query = {}
        if intersection_id:
            query["intersection_id"] = intersection_id
        
        start_time = datetime.now() - timedelta(hours=hours)
        query["timestamp"] = {"$gte": start_time}
        
        analytics = list(self.analytics_collection.find(query).sort("timestamp", -1))
        for data in analytics:
            data["_id"] = str(data["_id"])
        return analytics
    
    def get_high_congestion_areas(self, threshold=50):
        """Get areas with high traffic congestion"""
        pipeline = [
            {"$match": {
                "vehicle_count": {"$gte": threshold},
                "timestamp": {"$gte": datetime.now() - timedelta(hours=2)}
            }},
            {"$group": {
                "_id": "$intersection_id",
                "location": {"$first": "$location"},
                "avg_vehicle_count": {"$avg": "$vehicle_count"},
                "max_vehicle_count": {"$max": "$vehicle_count"},
                "avg_speed": {"$avg": "$avg_speed"}
            }},
            {"$sort": {"avg_vehicle_count": -1}}
        ]
        
        congestion_areas = list(self.traffic_collection.aggregate(pipeline))
        return congestion_areas
    
    def get_emergency_events(self, hours=24):
        """Get recent emergency vehicle events"""
        start_time = datetime.now() - timedelta(hours=hours)
        emergency_events = list(self.db["emergency_events"].find({
            "timestamp": {"$gte": start_time}
        }).sort("timestamp", -1))
        
        for event in emergency_events:
            event["_id"] = str(event["_id"])
        return emergency_events
    
    def get_signal_performance(self, intersection_id):
        """Analyze signal performance for optimization"""
        pipeline = [
            {"$match": {
                "intersection_id": intersection_id,
                "timestamp": {"$gte": datetime.now() - timedelta(hours=24)}
            }},
            {"$group": {
                "_id": "$signal_status",
                "count": {"$sum": 1},
                "avg_vehicle_count": {"$avg": "$vehicle_count"},
                "avg_speed": {"$avg": "$avg_speed"}
            }}
        ]
        
        performance_data = list(self.traffic_collection.aggregate(pipeline))
        return performance_data
    
    def search_traffic_data(self, location_keyword):
        """Search traffic data by location"""
        query = {
            "location": {"$regex": location_keyword, "$options": "i"},
            "timestamp": {"$gte": datetime.now() - timedelta(hours=24)}
        }
        traffic_data = list(self.traffic_collection.find(query))
        for data in traffic_data:
            data["_id"] = str(data["_id"])
        return traffic_data

# Example usage and testing
if __name__ == "__main__":
    crud = TrafficCRUD()
    
    # Test intersection creation
    sample_intersection = {
        "intersection_id": "INT001",
        "location": "MG Road & FC Road Junction",
        "coordinates": {"lat": 18.5204, "lng": 73.8567},
        "signal_type": "AUTOMATIC",
        "lanes": 8,
        "traffic_light_count": 4
    }
    
    crud.create_intersection(sample_intersection)
    
    # Test data retrieval
    intersections = crud.read_intersection()
    print(f"Total intersections: {len(intersections)}")
    
    # Test analytics
    congestion = crud.get_high_congestion_areas()
    print(f"High congestion areas: {len(congestion)}")