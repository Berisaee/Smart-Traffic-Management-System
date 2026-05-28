from flask import Flask, request, jsonify
from crud_operations import TrafficCRUD
from kafka_producer import TrafficProducer
from datetime import datetime, timedelta
import uuid

app = Flask(__name__)
crud = TrafficCRUD()
producer = TrafficProducer()

@app.route('/api/traffic', methods=['GET'])
def get_traffic_data():
    """Get traffic data with optional filters"""
    intersection_id = request.args.get('intersection_id')
    hours = request.args.get('hours', 24, type=int)
    
    traffic_data = crud.read_traffic_data(intersection_id, hours)
    return jsonify(traffic_data)

@app.route('/api/intersections', methods=['GET'])
def get_intersections():
    """Get all intersections"""
    intersections = crud.read_intersection()
    return jsonify(intersections)

@app.route('/api/intersections/<intersection_id>', methods=['GET'])
def get_intersection(intersection_id):
    """Get specific intersection"""
    intersection = crud.read_intersection(intersection_id)
    if intersection:
        return jsonify(intersection)
    return jsonify({"error": "Intersection not found"}), 404

@app.route('/api/intersections', methods=['POST'])
def create_intersection():
    """Create new intersection"""
    data = request.json
    
    # Generate unique ID if not provided
    if 'intersection_id' not in data:
        data['intersection_id'] = f"INT{str(uuid.uuid4())[:6].upper()}"
    
    intersection_id = crud.create_intersection(data)
    return jsonify({"message": "Intersection created", "id": data['intersection_id']}), 201

@app.route('/api/intersections/<intersection_id>', methods=['PUT'])
def update_intersection(intersection_id):
    """Update intersection configuration"""
    data = request.json
    success = crud.update_intersection(intersection_id, data)
    
    if success:
        return jsonify({"message": "Intersection updated"})
    return jsonify({"error": "Intersection not found"}), 404

@app.route('/api/intersections/<intersection_id>', methods=['DELETE'])
def delete_intersection(intersection_id):
    """Delete/deactivate intersection"""
    success = crud.delete_intersection(intersection_id)
    
    if success:
        return jsonify({"message": "Intersection deactivated"})
    return jsonify({"error": "Intersection not found"}), 404

@app.route('/api/signal/control', methods=['POST'])
def control_signal():
    """Manually control traffic signal"""
    data = request.json
    intersection_id = data.get('intersection_id')
    signal_status = data.get('signal_status')
    
    if not intersection_id or not signal_status:
        return jsonify({"error": "Missing intersection_id or signal_status"}), 400
    
    if signal_status not in ['RED', 'GREEN', 'YELLOW']:
        return jsonify({"error": "Invalid signal status"}), 400
    
    # Send signal control event to Kafka
    producer.control_signal(intersection_id, signal_status)
    
    return jsonify({"message": f"Signal control event sent for {intersection_id}"})

@app.route('/api/emergency/override', methods=['POST'])
def emergency_override():
    """Activate emergency vehicle override"""
    data = request.json
    intersection_id = data.get('intersection_id')
    location = data.get('location', 'Unknown Location')
    
    if not intersection_id:
        return jsonify({"error": "Missing intersection_id"}), 400
    
    producer.emergency_override(intersection_id, location)
    
    return jsonify({"message": f"Emergency override activated for {intersection_id}"})

@app.route('/api/analytics/congestion', methods=['GET'])
def get_congestion_data():
    """Get high congestion areas"""
    threshold = request.args.get('threshold', 50, type=int)
    congestion_data = crud.get_high_congestion_areas(threshold)
    return jsonify(congestion_data)

@app.route('/api/analytics/intersection/<intersection_id>', methods=['GET'])
def get_intersection_analytics(intersection_id):
    """Get analytics for specific intersection"""
    hours = request.args.get('hours', 24, type=int)
    analytics = crud.get_traffic_analytics(intersection_id, hours)
    return jsonify(analytics)

@app.route('/api/analytics/performance/<intersection_id>', methods=['GET'])
def get_signal_performance(intersection_id):
    """Get signal performance data"""
    performance = crud.get_signal_performance(intersection_id)
    return jsonify(performance)

@app.route('/api/emergency/events', methods=['GET'])
def get_emergency_events():
    """Get recent emergency events"""
    hours = request.args.get('hours', 24, type=int)
    events = crud.get_emergency_events(hours)
    return jsonify(events)

@app.route('/api/search/traffic', methods=['GET'])
def search_traffic():
    """Search traffic data by location"""
    location = request.args.get('location', '')
    if not location:
        return jsonify({"error": "Location parameter required"}), 400
    
    traffic_data = crud.search_traffic_data(location)
    return jsonify(traffic_data)

@app.route('/api/dashboard/summary', methods=['GET'])
def get_dashboard_summary():
    """Get dashboard summary data"""
    # Get current traffic summary
    summary = {
        "total_intersections": len(crud.read_intersection()),
        "high_congestion_count": len(crud.get_high_congestion_areas()),
        "emergency_events_today": len(crud.get_emergency_events(24)),
        "system_status": "OPERATIONAL",
        "last_updated": datetime.now().isoformat()
    }
    
    return jsonify(summary)

@app.route('/api/health', methods=['GET'])
def health_check():
    """System health check"""
    return jsonify({
        "status": "healthy",
        "service": "Smart Traffic Management API",
        "timestamp": datetime.now().isoformat()
    })

if __name__ == '__main__':
    print(" Starting Smart Traffic Management API...")
    print("Traffic monitoring and control system ready...")
    app.run(debug=True, host='0.0.0.0', port=5000)