from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime
import threading

class TrafficProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9093'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.topic = 'traffic_events'
        
        # Predefined intersections
        self.intersections = [
            {"id": "INT001", "location": "MG Road & FC Road Junction"},
            {"id": "INT002", "location": "Pune Station Signal"},
            {"id": "INT003", "location": "Shivaji Nagar Circle"},
            {"id": "INT004", "location": "Kothrud Depot Signal"},
            {"id": "INT005", "location": "Hinjewadi IT Park Gate"}
        ]
        
        self.weather_conditions = ["CLEAR", "RAIN", "FOG", "CLOUDY"]
        self.traffic_densities = ["LOW", "MEDIUM", "HIGH"]
        self.signal_statuses = ["RED", "GREEN", "YELLOW"]
    
    def send_traffic_event(self, action, traffic_data):
        """Send traffic event to Kafka"""
        event = {
            "action": action,
            "data": traffic_data,
            "timestamp": datetime.now().isoformat()
        }
        
        self.producer.send(self.topic, value=event)
        self.producer.flush()
        print(f" Sent {action} event for intersection: {traffic_data.get('location', 'Unknown')}")
    
    def generate_traffic_data(self, intersection):
        """Generate realistic traffic data"""
        current_hour = datetime.now().hour
        
        # Rush hour logic (7-9 AM, 6-8 PM)
        if current_hour in [7, 8, 18, 19]:
            base_count = random.randint(40, 80)
            density = random.choice(["MEDIUM", "HIGH"])
            avg_speed = random.uniform(15, 35)
        elif current_hour in [22, 23, 0, 1, 2, 3, 4, 5]:
            # Night time - low traffic
            base_count = random.randint(5, 20)
            density = "LOW"
            avg_speed = random.uniform(40, 60)
        else:
            # Normal hours
            base_count = random.randint(20, 45)
            density = random.choice(["LOW", "MEDIUM"])
            avg_speed = random.uniform(25, 45)
        
        return {
            "intersection_id": intersection["id"],
            "location": intersection["location"],
            "vehicle_count": base_count,
            "avg_speed": round(avg_speed, 2),
            "traffic_density": density,
            "signal_status": random.choice(self.signal_statuses),
            "timestamp": datetime.now().isoformat(),
            "weather_condition": random.choice(self.weather_conditions),
            "emergency_vehicle": "NONE"
        }
    
    def simulate_traffic_updates(self, duration_minutes=60):
        """Continuously generate traffic updates"""
        end_time = time.time() + (duration_minutes * 60)
        
        while time.time() < end_time:
            for intersection in self.intersections:
                traffic_data = self.generate_traffic_data(intersection)
                self.send_traffic_event("TRAFFIC_UPDATE", traffic_data)
                
                # Randomly generate emergency vehicles (5% chance)
                if random.random() < 0.05:
                    emergency_data = traffic_data.copy()
                    emergency_data["emergency_vehicle"] = "AMBULANCE"
                    self.send_traffic_event("EMERGENCY_ALERT", emergency_data)
            
            # Wait before next update cycle
            time.sleep(random.randint(15, 30))
    
    def control_signal(self, intersection_id, signal_status):
        """Manually control traffic signal"""
        signal_data = {
            "intersection_id": intersection_id,
            "signal_status": signal_status,
            "timestamp": datetime.now().isoformat()
        }
        self.send_traffic_event("SIGNAL_CONTROL", signal_data)
    
    def emergency_override(self, intersection_id, location):
        """Emergency vehicle override"""
        emergency_data = {
            "intersection_id": intersection_id,
            "location": location,
            "emergency_vehicle": "FIRE_TRUCK",
            "signal_status": "EMERGENCY_GREEN",
            "timestamp": datetime.now().isoformat()
        }
        self.send_traffic_event("EMERGENCY_ALERT", emergency_data)

def run_simulation():
    """Run traffic simulation"""
    producer = TrafficProducer()
    print("Starting Smart Traffic Simulation...")
    print(" Generating realistic traffic patterns...")
    
    # Start continuous simulation
    producer.simulate_traffic_updates(duration_minutes=60)

def manual_control():
    """Manual traffic control interface"""
    producer = TrafficProducer()
    
    while True:
        print("\n Smart Traffic Control Panel")
        print("1. Update Signal Status")
        print("2. Emergency Override")
        print("3. View Intersections")
        print("4. Start Auto Simulation")
        print("5. Exit")
        
        choice = input("Select option: ")
        
        if choice == "1":
            print("\nAvailable Intersections:")
            for i, intersection in enumerate(producer.intersections):
                print(f"{i+1}. {intersection['id']} - {intersection['location']}")
            
            idx = int(input("Select intersection: ")) - 1
            signal = input("Enter signal status (RED/GREEN/YELLOW): ").upper()
            
            if 0 <= idx < len(producer.intersections) and signal in ["RED", "GREEN", "YELLOW"]:
                producer.control_signal(producer.intersections[idx]["id"], signal)
        
        elif choice == "2":
            print("\nAvailable Intersections:")
            for i, intersection in enumerate(producer.intersections):
                print(f"{i+1}. {intersection['id']} - {intersection['location']}")
            
            idx = int(input("Select intersection: ")) - 1
            if 0 <= idx < len(producer.intersections):
                intersection = producer.intersections[idx]
                producer.emergency_override(intersection["id"], intersection["location"])
        
        elif choice == "3":
            print("\n Traffic Intersections:")
            for intersection in producer.intersections:
                print(f"• {intersection['id']}: {intersection['location']}")
        
        elif choice == "4":
            simulation_thread = threading.Thread(target=run_simulation)
            simulation_thread.daemon = True
            simulation_thread.start()
            print("Auto simulation started in background...")
        
        elif choice == "5":
            break

if __name__ == "__main__":
    manual_control()